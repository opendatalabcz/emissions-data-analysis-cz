from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import gzip
import shutil
from functools import partial

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree # type: ignore <- pylance milně hlásí chybu
import requests

from utils import *
from schemas import *
import config


# Nalezení datumů, které už byly zpracovány
def downloaded_dates(directories):
    result = set()
    for directory in directories:
        # Složka ještě nebyla vytvořena
        if not directory.is_dir():
            continue
        # Přidání datumů ze souborů do složky
        for file in directory.iterdir():
            try:
                date = date_from_file_path(file)
            except Exception:
                continue
            result.add(date)
    return result


# Výběr adres v intervalu, které ješte nebyly staženy
def select_addresses(all_titles, all_download_urls, already_processed, start_date, end_date):
    titles = []
    download_urls = []
    for title, download_url in zip(all_titles, all_download_urls):
        date = date_from_file_name(title)
        # Filtrace na základě intervalu
        if start_date is not None:
            if date < str_to_date(start_date):
                continue
        if end_date is not None:
            if date > str_to_date(end_date):
                continue
        # Filtrace na základě předchozího zpracování
        if date in already_processed:
            continue

        titles.append(title)
        download_urls.append(download_url)
    return titles, download_urls

#--------------------------------------------------------------------------------------------------------------

def get_url_addresses(sparql_endpoint, parent_dataset_iri, start_date, end_date, already_processed, verbosity):
    # Definice dotazu
    get_download_url_query = f'''
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dcterms: <http://purl.org/dc/terms/>

    SELECT ?title ?downloadURL 
    WHERE {{
        <{parent_dataset_iri}> dcat:seriesMember ?dataset.
        ?dataset dcat:distribution ?distribution.
        ?dataset dcterms:title ?title.
        ?distribution dcat:downloadURL ?downloadURL.

        FILTER(LANG(?title) = "cs")
    }}
    '''
    headers = {'Accept': 'application/sparql-results+json'}
    params = {'query': get_download_url_query}

    # Získání odpovědi ze serveru
    if verbosity > Verbosity.QUIET:
        print('Získávání seznamu URL adres pro stažení přes SPARQL API...')
    response = requests.get(sparql_endpoint, params=params, headers=headers, timeout=60)
    response.raise_for_status()  # Kontrola chyb HTTP
    
    # Ziskani jmen z JSON formatu
    all_titles = [binding['title']['value'] for binding in response.json()['results']['bindings']]
    # Ziskani seznamu URL adres z JSON formatu
    all_download_urls = [binding['downloadURL']['value'] for binding in response.json()['results']['bindings']]

    # Vyfiltrování pouze požadovaných adres
    titles, download_urls = select_addresses(all_titles, all_download_urls, already_processed, start_date, end_date)

    if verbosity > Verbosity.NORMAL:
        print(f'Získáno {len(download_urls)} URL adres k datovým sadám.')

    return titles, download_urls


def download_file(title, download_url, target_dir, max_attempts, verbosity):
    # Definice cesty k uložení souboru
    path = target_dir / (title + '.xml.gz')

    # Kontrola, zda už soubor není uložen
    if skip_file(path, verbosity):
        return

    # Samotné stažení souboru
    for attempt in range(max_attempts):
        try:
            with requests.get(download_url, stream=True, timeout=60) as response:
                response.raise_for_status() # Kontrola statusu odpovědi
                with open(path, 'wb') as f: # Zápis do souboru po částech
                    for chunk in response.iter_content(chunk_size=8192): 
                        f.write(chunk)
                        
            if verbosity > Verbosity.NORMAL:
                print(f'Stahuji: "{title}".')

            return
        # Vyřešení chyby ve stahování
        except requests.exceptions.RequestException as e:
            if verbosity > Verbosity.NORMAL:
                print(f'Chyba při dotazu na stahovaní souboru "{title}": {e}.')
            if path.exists(): # Smazání neúplně zapsaného souboru
                path.unlink()
                if verbosity > Verbosity.NORMAL:
                    print(f'Smazán nekompletní soubor "{title}".')
            if attempt < max_attempts - 1:
                if verbosity > Verbosity.NORMAL:
                    print(f'Zahajuji pokus číslo {attempt + 1}.')
                elif verbosity > Verbosity.QUIET:
                    print(attempt + 1, end='', flush=True)
            else:
                raise requests.exceptions.RequestException(f'Stahování souboru "{title}" selhalo po {max_attempts} pokusech.') from e


# Stažení seznamu stanic z data.gov.cz
def download_stations(sparql_endpoint, download_dir, dataset_iri, verbosity):
    # Vytvoření adresáře pro uložení souborů
    create_directory(download_dir, verbosity)

    # Definice dotazu
    get_download_url_query = f'''
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dcterms: <http://purl.org/dc/terms/>

    SELECT ?downloadURL 
    WHERE {{
        <{dataset_iri}> dcat:distribution ?distribution.
        ?distribution dcat:downloadURL ?downloadURL.
    }}
    '''
    headers = {'Accept': 'application/sparql-results+json'}
    params = {'query': get_download_url_query}

    # Získání odpovědi ze serveru
    if verbosity > Verbosity.QUIET:
        print('Získávání URL adresy pro stažení seznamu stanic přes SPARQL API...')
    response = requests.get(sparql_endpoint, params=params, headers=headers, timeout=60)
    response.raise_for_status()  # Kontrola chyb HTTP

    # Stažení souboru
    download_url = response.json()['results']['bindings'][0]['downloadURL']['value']
    download_file('Stanice STK a SME', download_url, download_dir, 1, verbosity)

    # Nová řádka pro vizuélní odlišení konce úkonu
    if verbosity > Verbosity.QUIET:
        print('\nSTAHOVÁNÍ DONONČENO.\n')


#--------------------------------------------------------------------------------------------------------------

def extract_file(file_name, source_dir, target_dir, verbosity, delete):
    # Definice cesty k souboru
    source_file_path = source_dir / file_name
    destination_file_path = (target_dir / file_name).with_suffix('')

    # Kontrola, zda vyextrahovaný soubor již existuje
    if not skip_file(destination_file_path, verbosity):
        # Samotná extrakce souboru
        with gzip.open(source_file_path, 'rb') as f_in:
            with open(destination_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out) # type: ignore <- pylance milně hlásí chybu

        if verbosity > Verbosity.NORMAL:
            print(f'Extrahuji: "{file_name}".')
    
    # Smazání původního souboru
    if delete:
        delete_path(source_file_path, verbosity)


#--------------------------------------------------------------------------------------------------------------

def safe_get(element, xpath, namespaces):
    if element is None:
        return None
    node = element.find(xpath, namespaces)
    return node.text if node is not None else None


def safe_find(element, xpath, namespaces):
    if element is None:
        return None
    return element.find(xpath, namespaces)


def safe_findall(element, xpath, namespaces):
    if element is None:
        return []
    return element.findall(xpath, namespaces)


def safe_get_attribute(element, attribute_name):
    if element is None:
        return None
    return element.get(attribute_name)


def safe_index(list, index):
    try:
        return list[index]
    except (TypeError, IndexError):
        return None

#--------------------------------------------------------------------------------------------------------------

def get_stanice(element, prefix, namespaces):
    return {
        f'{prefix}_Stanice_Cislo': safe_get(element, 'p:Cislo', namespaces),
        f'{prefix}_Stanice_Kraj': safe_get(element, 'p:Kraj', namespaces),
        f'{prefix}_Stanice_ORP': safe_get(element, 'p:ORP', namespaces),
        f'{prefix}_Stanice_Obec': safe_get(element, 'p:Obec', namespaces),
    }


def get_casove_udaje(element, prefix, namespaces):
    return {
        f'{prefix}_Zahajeni': safe_get(element, 'p:Zahajeni', namespaces),
        f'{prefix}_Ukonceni': safe_get(element, 'p:Ukonceni', namespaces),
    }


def get_administrativni_oprava(element, namespaces):
    return {
        'AdministrativniOprava_CisloProtokolu': safe_get(element, 'p:CisloProtokolu', namespaces),
        'AdministrativniOprava_DatumProhlidky': safe_get(element, 'p:DatumProhlidky', namespaces)
    }


def get_vozidlo(element, namespaces):
    return {
        'Vozidlo_Vin': safe_get(element, 'p:Vin', namespaces),
        'Vozidlo_Druh': safe_get(element, 'p:Druh', namespaces),
        'Vozidlo_Kategorie': safe_get(element, 'p:Kategorie', namespaces),
        'Vozidlo_Provedeni': safe_get(element, 'p:Provedeni', namespaces),
        'Vozidlo_Znacka': safe_get(element, 'p:Znacka', namespaces),
        'Vozidlo_ObchodniOznaceni': safe_get(element, 'p:ObchodniOznaceni', namespaces),
        'Vozidlo_TypMotoru': safe_get(element, 'p:TypMotoru', namespaces)
    }


def get_registrace(element, namespaces):
    return {
        'Registrace_DatumPrvni': safe_get(element, 'p:DatumPrvniRegistrace', namespaces),
        'Registrace_Stat': safe_get(element, 'p:Stat', namespaces),
        'Registrace_CisloDokladu': safe_get(element, 'p:CisloDokladu', namespaces)
    }


def get_emisni_cast(element, namespaces):
    emise_record = {}
    emise_record['Emise_CisloProtokolu'] = safe_get(element, 'p:CisloProtokolu', namespaces)
    emise_record['Emise_DatumProhlidky'] = safe_get(element, 'p:DatumProhlidky', namespaces)
    emise_record['Emise_StaniceCislo'] = safe_get(safe_find(element, 'p:Stanice', namespaces), 'p:Cislo', namespaces)
    emise_record.update(get_casove_udaje(safe_find(element, 'p:CasoveUdaje', namespaces), 'Emise', namespaces))
    emise_record['Emise_OdpovednaOsoba'] = safe_get(element, 'p:OdpovednaOsoba', namespaces)
    emise_record['Emise_ZakladniPalivo'] = safe_get(element, 'p:ZakladniPalivo', namespaces)
    emise_record['Emise_AlternativniPalivo'] = safe_get(element, 'p:AlternativniPalivo', namespaces)
    emise_record['Emise_EmisniSystem'] = safe_get(element, 'p:EmisniSystem', namespaces)
    emise_record['Emise_VyrobceMotoru'] = safe_get(element, 'p:VyrobceMotoru', namespaces)
    emise_record['Emise_CisloMotoru'] = safe_get(element, 'p:CisloMotoru', namespaces)
    return emise_record


def get_vysledek(element, namespaces):
    vysledek_record = {}
    vysledek_record['Vysledek_Odometr'] = safe_get(element, 'p:Odometr', namespaces)
    vysledek_record['Vysledek_Poznamka'] = safe_get(element, 'p:Poznamka', namespaces)
    vysledek_record['Vysledek_DatumPristiProhlidky'] = safe_get(element, 'p:DatumPristiProhlidky', namespaces)
    vysledek_record['Vysledek_NalepkaVylepena'] = safe_get(element, 'p:NalepkaVylepena', namespaces)
    vysledek_record['Vysledek_Celkovy'] = safe_get(element, 'p:VysledekCelkovy', namespaces)
    return vysledek_record


def get_zavady_lists(element, namespaces):
    zavady_a, zavady_b, zavady_c = [], [], []
    if element is not None:
        for zavada_element in safe_findall(element, 'p:Zavada', namespaces):
            kod = safe_get(zavada_element, 'p:Kod', namespaces)
            zavaznost = safe_get(zavada_element, 'p:Zavaznost', namespaces)
            if zavaznost == 'A' and kod:
                zavady_a.append(kod)
            elif zavaznost == 'B' and kod:
                zavady_b.append(kod)
            elif zavaznost == 'C' and kod:
                zavady_c.append(kod)
            # TODO smazat
            else:
                print('jina zavada')
    return zavady_a, zavady_b, zavady_c


def parse_prohlidka(element, namespaces):
    # Získání prvního údaje, který ověří, zda údaj existuje
    cislo_protokolu = safe_get(element, 'p:CisloProtokolu', namespaces)
    if not cislo_protokolu:
        return None
    emisni_cast_element = safe_find(element, 'p:EmisniCast', namespaces)
    if emisni_cast_element is None:
        return None

    # Vytvoření slovníku obsahujího rozparsovanou prohlídku a postupné přidání elementů podle XSD
    prohlidka_record = {}
    prohlidka_record['CisloProtokolu'] = cislo_protokolu
    prohlidka_record['DatumProhlidky'] = safe_get(element, 'p:DatumProhlidky', namespaces)
    prohlidka_record.update(get_stanice(safe_find(element, 'p:Stanice', namespaces), 'Prohlidka', namespaces))
    prohlidka_record.update(get_casove_udaje(safe_find(element, 'p:CasoveUdaje', namespaces), 'Prohlidka', namespaces))
    prohlidka_record['Prohlidka_OdpovednaOsoba'] = safe_get(element, 'p:OdpovednaOsoba', namespaces)
    prohlidka_record['DruhProhlidky'] = safe_get(element, 'p:DruhProhlidky', namespaces)
    prohlidka_record['RozsahProhlidky'] = safe_get(element, 'p:RozsahProhlidky', namespaces)
    prohlidka_record.update(get_administrativni_oprava(safe_find(element, 'p:AdministrativniOprava', namespaces), namespaces))
    prohlidka_record.update(get_vozidlo(safe_find(element, 'p:Vozidlo', namespaces), namespaces))
    prohlidka_record.update(get_registrace(safe_find(element, 'p:Registrace', namespaces), namespaces))
    prohlidka_record.update(get_emisni_cast(emisni_cast_element, namespaces))
    prohlidka_record.update(get_vysledek(safe_find(element, 'p:Vysledek', namespaces), namespaces))
    
    # Indikátory přítomnosti bloků
    prohlidka_record['TechnickaCast_Pritomno'] = str(safe_find(element, 'p:TechnickaCast', namespaces) is not None)
    prohlidka_record['AdrCast_Pritomno'] = str(safe_find(element, 'p:AdrCast', namespaces) is not None)
    prohlidka_record['TskCast_Pritomno'] = str(safe_find(element, 'p:TskCast', namespaces) is not None)

    # Závady z výsledku rozdělené do kategorií A, B, C
    vysledek_zavady_element = safe_find(safe_find(element, 'p:Vysledek', namespaces), 'p:ZavadaSeznam', namespaces)
    zavady_a, zavady_b, zavady_c = get_zavady_lists(vysledek_zavady_element, namespaces)

    prohlidka_record['Zavady_A'] = zavady_a if zavady_a else None
    prohlidka_record['Zavady_B'] = zavady_b if zavady_b else None
    prohlidka_record['Zavady_C'] = zavady_c if zavady_c else None

    return prohlidka_record

#--------------------------------------------------------------------------------------------------------------

def get_list(element, xpath, namespaces):
    element_list = safe_findall(element, f'm:{xpath}', namespaces)
    text_list = [element.text for element in element_list]
    if len(text_list) == 0:
        return None
    return text_list


def get_attribute_list(element, xpath, namespaces):
    element_list = safe_findall(element, f'm:{xpath}', namespaces)
    text_list = [safe_get_attribute(element, 'text') for element in element_list]
    if len(text_list) == 0:
        return None
    return text_list


def determine_result_attributes(element):
    return (safe_get_attribute(element, 'hodnota'), safe_get_attribute(element, 'vysledek'))


def determine_boundary_attributes(element):
    return (safe_get_attribute(element, 'hodnota'), safe_get_attribute(element, 'rucniZadani'))


def get_boundary_attributes(element, prefix):
    hodnota, rucni_zadani = determine_boundary_attributes(element)
    return {
        f'{prefix}_Hodnota': hodnota,
        f'{prefix}_RucniZadani': rucni_zadani
    }


def get_monitor_attributes(element, prefix):
    return {
        f'{prefix}_Podporovano': safe_get_attribute(element, 'podporovano'),
        f'{prefix}_Otestovano': safe_get_attribute(element, 'otestovano')
    }


# Inicializuje slovník listů, kde každá požadovaná hodnota představuje klíč
def initialize_result_list(required_list, categories, prefix):
    # Create list of names of the categories of placeholders
    category_list = []
    for category, (_, repetitions) in categories.items():
        if repetitions == 1:
            category_list.append(category)
        else:
            for i in range(repetitions):
                category_list.append(f'{category}{i}')

    result_lists = {}
    for category in category_list:
        for required in required_list:
            if required[2]:
                result_lists[f'{prefix}_{category}_{required[0]}_Min_Hodnota'] = []
                result_lists[f'{prefix}_{category}_{required[0]}_Min_RucniZadani'] = []
            if required[3]:
                result_lists[f'{prefix}_{category}_{required[0]}_Max_Hodnota'] = []
                result_lists[f'{prefix}_{category}_{required[0]}_Max_RucniZadani'] = []
            result_lists[f'{prefix}_{category}_{required[0]}_Hodnota'] = []
            result_lists[f'{prefix}_{category}_{required[0]}_Vysledek'] = []
    return result_lists


# Vyplní slovník hodnotami z jednotlivých vyústění
def fill_result_list(vyusteni_element_list, result_lists, required_list, categories, prefix, namespaces):
    # Iterace přes vyústění
    for vyusteni_element in vyusteni_element_list:
        # Iterace přes kategorie ve vyústění (jako například otáčky volnoběžné x otáčky zvýšené)
        for category, (category_xpath, repetitions) in categories.items():
            # Doplnění kategorií do definovaného počtu - relevantní pro kategorii měření, kterých je více
            category_element_list = pad_list_with_none(safe_findall(vyusteni_element, f'm:{category_xpath}', namespaces), repetitions)
            for i, category_element in enumerate(category_element_list):
                # Index kategorie pro rozlišení jednotlivých měření
                category_id = ''
                if repetitions != 1:
                    category_id = str(i)
                # Iterace přes požadované hodnoty na extrakci
                for required in required_list:
                    required_element = safe_find(category_element, f'm:{required[1]}', namespaces)
                    if required[2]:
                        hodnota, rucni_zadani = determine_boundary_attributes(safe_find(required_element, f'm:min', namespaces))
                        if hodnota is not None: result_lists[f'{prefix}_{category}{category_id}_{required[0]}_Min_Hodnota'].append(hodnota)
                        if rucni_zadani is not None: result_lists[f'{prefix}_{category}{category_id}_{required[0]}_Min_RucniZadani'].append(rucni_zadani)
                    if required[3]:
                        hodnota, rucni_zadani = determine_boundary_attributes(safe_find(required_element, f'm:max', namespaces))
                        if hodnota is not None: result_lists[f'{prefix}_{category}{category_id}_{required[0]}_Max_Hodnota'].append(hodnota)
                        if rucni_zadani is not None: result_lists[f'{prefix}_{category}{category_id}_{required[0]}_Max_RucniZadani'].append(rucni_zadani)
                    hodnota, vysledek = determine_result_attributes(required_element)
                    if hodnota is not None: result_lists[f'{prefix}_{category}{category_id}_{required[0]}_Hodnota'].append(hodnota)
                    if vysledek is not None: result_lists[f'{prefix}_{category}{category_id}_{required[0]}_Vysledek'].append(vysledek)


# Vybere z každého vyústění hodnotu, která je považována za nejhorší
def select_worst(result_lists, strategy_dict, already_parsed):
    result = {}
    for name, result_list in result_lists.items():
        try:
            if not result_list:
                result[name] = None
                continue
            if 'Min' in name or 'Max' in name or 'Vysledek' in name:
                result[name] = next((val for val in result_list if val is not None), None)
                continue
            strategy = strategy_dict[name.split('_')[-2]]
            floats = floats_sublist(result_list)
            float_result = None
            match strategy:
                case 'max':
                    float_result = max(floats, default=None)
                case 'min':
                    float_result = min(floats, default=None)
                case 'max_diff_1':
                    float_result = max(floats, default=None, key=lambda x: abs(x - 1.0))
                case 'bounds':
                    try:
                        if name.startswith('Nafta'):
                            # Pro naftu jsou limity v samostatném bloku MereniVznetLimit
                            param = name.split('_')[-2]
                            min_value = float(already_parsed[f'Nafta_MereniVznetLimit_{param}_Min_Hodnota'])
                            max_value = float(already_parsed[f'Nafta_MereniVznetLimit_{param}_Max_Hodnota'])
                        else:
                            # Pro benzín/plyn jsou limity součástí aktuálního záznamu
                            name_stem = name.partition("_Hodnota")[0]
                            min_value = float(result[f'{name_stem}_Min_Hodnota'])
                            max_value = float(result[f'{name_stem}_Max_Hodnota'])
                        
                        optimal_value = (max_value + min_value) / 2
                        float_result = max(floats, default=None, key=lambda x: abs(x - optimal_value))
                    except Exception:
                        float_result = next((f for f in floats if f is not None), None)
            # Cast na string, aby bylo zachováno načtení všech hodnot jako string
            if float_result is not None:
                result[name] = str(float_result)
            else:
                result[name] = None
        except Exception:
            result[name] = None
    return result


def get_detail_benzin(element, prefix, namespaces):
    result = {}
    result[f'{prefix}_Palivo'] = safe_get_attribute(element, 'palivo')
    benzin_vyusteni_element_list = safe_findall(element, 'm:vyusteni', namespaces)
    result[f'{prefix}_PocetVyusteni'] = str(len(benzin_vyusteni_element_list))
    # Definice seznamů pro vyplnění hodnot z jednotlivých vyústění (název, xpath, min hodnota, max hodnota)
    required_list = [('CO', 'CO', False, True), ('CO2', 'CO2', False, False), ('COCOOR', 'COCOOR', False, False), ('HC', 'HC', False, True), ('LAMBDA', 'LAMBDA', True, True), ('N', 'N', True, True), ('NOX', 'NOX', False, False), ('O2', 'O2', False, False), ('TPS', 'TPS', False, False)]
    # {název: (xpath, počet opakování}
    categories = {'OtackyVolnobezne': ('otackyVolnobezne', 1), 'OtackyZvysene': ('otackyZvysene', 1)}
    result_lists = initialize_result_list(required_list, categories, prefix)
    fill_result_list(benzin_vyusteni_element_list, result_lists, required_list, categories, prefix, namespaces)
    strategy_dict = {'CO': 'max', 'CO2': 'min', 'COCOOR': 'max', 'HC': 'max', 'LAMBDA': 'max_diff_1', 'N': 'bounds', 'NOX': 'max', 'O2': 'max', 'TPS': 'min'}
    result |= select_worst(result_lists, strategy_dict, result)
    return result


def get_detail_nafta(element, prefix, namespaces):
    result = {}
    # Limitní hodnoty
    limit_element = safe_find(element, 'm:mereniVznetLimit', namespaces)
    otacky_volnobezne = safe_find(limit_element, 'm:otackyVolnobezne', namespaces)
    result |= get_boundary_attributes(safe_find(otacky_volnobezne, 'm:min', namespaces), f'{prefix}_MereniVznetLimit_OtackyVolnobezne_Min')
    result |= get_boundary_attributes(safe_find(otacky_volnobezne, 'm:max', namespaces), f'{prefix}_MereniVznetLimit_OtackyVolnobezne_Max')
    otacky_prebehove = safe_find(limit_element, 'm:otackyPrebehove', namespaces)
    result |= get_boundary_attributes(safe_find(otacky_prebehove, 'm:min', namespaces), f'{prefix}_MereniVznetLimit_OtackyPrebehove_Min')
    result |= get_boundary_attributes(safe_find(otacky_prebehove, 'm:max', namespaces), f'{prefix}_MereniVznetLimit_OtackyPrebehove_Max')
    cas_akcelerace = safe_find(limit_element, 'm:casAkcelerace', namespaces)
    result |= get_boundary_attributes(safe_find(cas_akcelerace, 'm:max', namespaces), f'{prefix}_MereniVznetLimit_CasAkcelerace_Max')
    kourivost = safe_find(limit_element, 'm:kourivost', namespaces)
    result |= get_boundary_attributes(safe_find(kourivost, 'm:max', namespaces), f'{prefix}_MereniVznetLimit_Kourivost_Max')
    kourivost_rozpeti = safe_find(limit_element, 'm:kourivostRozpeti', namespaces)
    result |= get_boundary_attributes(safe_find(kourivost_rozpeti, 'm:max', namespaces), f'{prefix}_MereniVznetLimit_KourivostRozpeti_Max')

    # Hodnoty pro jednotlivá měření
    result[f'{prefix}_Palivo'] = safe_get_attribute(element, 'palivo')
    nafta_vyusteni_element_list = safe_findall(element, 'm:vyusteni', namespaces)
    result[f'{prefix}_PocetVyusteni'] = str(len(nafta_vyusteni_element_list))
    # Definice seznamů pro vyplnění hodnot z jednotlivých vyústění (název, xpath, min hodnota, max hodnota)
    required_list = [('TPS', 'TPS', False, False), ('CasAkcelerace', 'casAkcelerace', False, False), ('Kourivost', 'kourivost', False, False), ('OtackyPrebehove', 'otackyPrebehove', False, False), ('OtackyVolnobezne', 'otackyVolnobezne', False, False), ('Teplota', 'teplota', False, False)]
    # {název: (xpath, počet opakování}
    categories = {'MereniPrumer': ('mereniPrumer', 1), 'Mereni': ('mereni', 4)}
    result_lists = initialize_result_list(required_list, categories, prefix)
    fill_result_list(nafta_vyusteni_element_list, result_lists, required_list, categories, prefix, namespaces)
    strategy_dict = {'TPS': 'min', 'CasAkcelerace': 'max', 'Kourivost': 'max', 'OtackyPrebehove': 'bounds', 'OtackyVolnobezne': 'bounds', 'Teplota': 'min'}
    result |= select_worst(result_lists, strategy_dict, result)
    return result
    

def get_detail_plyn(element, prefix, namespaces):
    result = get_detail_benzin(element, prefix, namespaces)

    nadrz_plyn_element = safe_find(safe_find(element, 'm:kontrolaNadrzi', namespaces), 'm:nadrz', namespaces)
    result[f'{prefix}_Nadrz_Vyrobce'] = safe_get_attribute(nadrz_plyn_element, 'vyrobce')
    result[f'{prefix}_Nadrz_Homologace'] = safe_get_attribute(nadrz_plyn_element, 'homologace')
    result[f'{prefix}_Nadrz_Zivotnost'] = safe_get_attribute(nadrz_plyn_element, 'zivotnost')
    result[f'{prefix}_Nadrz_Kontrola'] = safe_get_attribute(nadrz_plyn_element, 'kontrola')

    return result


def parse_mereni(element, namespaces):
    emise_record = {}
    emise_record['CisloProtokolu'] = safe_get(element, 'm:CisloProtokolu', namespaces)
    emise_record['DatumProhlidky'] = safe_get(element, 'm:DatumProhlidky', namespaces)
    emise_record['StaniceCislo'] = safe_get(safe_find(element, 'm:Stanice', namespaces), 'm:Cislo', namespaces)
    emise_record['Zahajeni'] = safe_get(safe_find(element, 'm:CasoveUdaje', namespaces), 'm:Zahajeni', namespaces)
    emise_record['Ukonceni'] = safe_get(safe_find(element, 'm:CasoveUdaje', namespaces), 'm:Ukonceni', namespaces)
    emise_record['OdpovednaOsoba'] = safe_get(element, 'm:OdpovednaOsoba', namespaces)

    pristroj_data_element = safe_find(element, 'm:PristrojData', namespaces)
    prohlidka_element = safe_find(pristroj_data_element, 'm:prohlidka', namespaces)
    emise_record['Prohlidka_CisloProtokolu'] = safe_get_attribute(prohlidka_element, 'cisloProtokolu')
    emise_record['Prohlidka_DatumProhlidky'] = safe_get_attribute(prohlidka_element, 'datumProhlidky')

    merici_pristroj_element = safe_find(prohlidka_element, 'm:mericiPristroj', namespaces)
    emise_record['MericiPristroj_Vyrobce'] = safe_get_attribute(merici_pristroj_element, 'vyrobce')
    emise_record['MericiPristroj_Typ'] = safe_get_attribute(merici_pristroj_element, 'typ')
    emise_record['MericiPristroj_Verze'] = safe_get_attribute(merici_pristroj_element, 'verze')
    emise_record['MericiPristroj_OBD'] = safe_get_attribute(merici_pristroj_element, 'OBD')
    emise_record['MericiPristroj_VerzeSoftware'] = safe_get_attribute(merici_pristroj_element, 'verzeSoftware')

    emise_record['Poznamky'] = get_list(prohlidka_element, 'poznamka', namespaces)

    vozidlo_element = safe_find(pristroj_data_element, 'm:vozidlo', namespaces)
    emise_record['Vozidlo_Vin'] = safe_get(vozidlo_element, 'm:VIN', namespaces)
    emise_record['Vozidlo_Znacka'] = safe_get(vozidlo_element, 'm:tovazniZnacka', namespaces)
    emise_record['Vozidlo_ObchodniOznaceni'] = safe_get(vozidlo_element, 'm:typVozidla', namespaces)
    emise_record['Vozidlo_TypMotoru'] = safe_get(vozidlo_element, 'm:typMotoru', namespaces)
    emise_record['Vozidlo_CisloMotoru'] = safe_get(vozidlo_element, 'm:cisloMotoru', namespaces)
    emise_record['Vozidlo_Odometer'] = safe_get(vozidlo_element, 'm:stavTachometru', namespaces)
    emise_record['Vozidlo_RokVyroby'] = safe_get(vozidlo_element, 'm:rokVyroby', namespaces)
    emise_record['Vozidlo_DatumPrvniRegistrace'] = safe_get(vozidlo_element, 'm:datumPrvniRegistrace', namespaces)
    emise_record['Vozidlo_Palivo'] = safe_get(vozidlo_element, 'm:palivo', namespaces)

    vysledek_mereni_element = safe_find(pristroj_data_element, 'm:vysledekMereni', namespaces)
    emise_record['Vysledek_VisualniKontrola'] = safe_get_attribute(vysledek_mereni_element, 'vysledekVisualniKontroly')
    emise_record['Vysledek_Readiness'] = safe_get_attribute(vysledek_mereni_element, 'vysledekReadiness')
    emise_record['Vysledek_RidiciJednotka'] = safe_get_attribute(vysledek_mereni_element, 'vysledekRidiciJednotka')
    emise_record['Vysledek_RidiciJednotkaStav'] = safe_get_attribute(vysledek_mereni_element, 'vysledekRidiciJednotkaStav')
    emise_record['Vysledek_Mil'] = safe_get_attribute(vysledek_mereni_element, 'vysledekMIL')
    emise_record['Vysledek_TesnostPlynovehoZarizeni'] = safe_get_attribute(vysledek_mereni_element, 'vysledekTesnostPlynovehoZarizeni')

    vyhovuje_element = safe_find(vysledek_mereni_element, 'm:vyhovuje', namespaces)
    emise_record['Vysledek_Vyhovuje'] = 'true' if vyhovuje_element is not None else None
    emise_record['PristiProhlidka'] = safe_get_attribute(vyhovuje_element, 'pristiProhlidka')

    emisni_system_element = safe_find(pristroj_data_element, 'm:emisniSystem', namespaces)
    rizeny_obd_element = safe_find(emisni_system_element, 'm:rizenyOBD', namespaces)
    rizeny_element = safe_find(emisni_system_element, 'm:rizeny', namespaces)
    nerizeny_element = safe_find(emisni_system_element, 'm:nerizeny', namespaces)
    emise_record['EmisniSystem'] = 'Rizeny_Obd' if rizeny_obd_element is not None else 'Rizeny' if rizeny_element is not None else 'Nerizeny' if nerizeny_element is not None else None
    emise_record['Obd_KomunikacniProtokol'] = safe_get(rizeny_obd_element, 'm:komunikacniProtokol', namespaces)
    emise_record['Obd_Vin'] = safe_get(rizeny_obd_element, 'm:VIN', namespaces)
    emise_record['Obd_PocetDtc'] = safe_get(rizeny_obd_element, 'm:pocetDTC', namespaces) if not None else safe_get(rizeny_element, 'm:pocetDTC', namespaces)
    emise_record['Obd_VzdalenostDtc'] = safe_get(rizeny_obd_element, 'm:vzdalenostDTC', namespaces)
    emise_record['Obd_CasDtc'] = safe_get(rizeny_obd_element, 'm:casDTC', namespaces)
    emise_record['Obd_KontrolaMil'] = safe_get(rizeny_obd_element, 'm:kontrolaMIL', namespaces)
    emise_record['Obd_VypisDtc'] = get_attribute_list(safe_find(rizeny_obd_element, 'm:vypisDTC', namespaces), 'DTC', namespaces)

    emise_record['Zavady'] = get_list(safe_find(pristroj_data_element, 'm:zavady', namespaces), 'kod', namespaces)

    readiness_element = safe_find(rizeny_obd_element, 'm:readiness', namespaces)
    emise_record['Obd_Readiness_Vysledek'] = safe_get_attribute(readiness_element, 'vysledek')

    all_monitors = [
        ('Zazeh', 'OBDzazeh', ['AC', 'CAT-FUNC', 'COMP', 'EGR-VVT', 'EVAP', 'FUEL', 'HCAT', 'MISF', 'O2S-FUNC', 'O2S-HEAT', 'SAS']),
        ('Vznet', 'OBDvznet', ['AC', 'BOOST', 'COMP', 'DPF', 'EGR-VVT', 'EGS', 'FUEL', 'MISF', 'NMHC', 'NOX', 'RESERVE']),
        ('J1939', 'J1939', ['AC', 'BOOST', 'CAT-FUNC', 'COLD', 'COMP', 'DPF', 'EGR-VVT', 'EGS-FUNC', 'EGS-HEAT', 'EVAP', 'FUEL', 'HCAT', 'MISF', 'NM-HC', 'NOX', 'SAS'])
    ]
    for col_name, element_name, monitor_list in all_monitors:
        parent = safe_find(readiness_element, f'm:{element_name}', namespaces)
        prefix = f'Obd_Readiness_{col_name}'
        emise_record.update({key: value for monitor in monitor_list for key, value in get_monitor_attributes(safe_find(parent, f'm:{monitor}', namespaces), f'{prefix}_{monitor}').items()})

    detail_benzin_element = safe_find(pristroj_data_element, 'm:detailBenzin', namespaces)
    emise_record.update(get_detail_benzin(detail_benzin_element, 'Benzin', namespaces))

    detail_nafta_element = safe_find(pristroj_data_element, 'm:detailNafta', namespaces)
    emise_record.update(get_detail_nafta(detail_nafta_element, 'Nafta', namespaces))

    detail_plyn_element = safe_find(pristroj_data_element, 'm:detailPlyn', namespaces)
    emise_record.update(get_detail_plyn(detail_plyn_element, 'Plyn', namespaces))

    return emise_record

#--------------------------------------------------------------------------------------------------------------

# Získání základních adresních údajů
def get_adresa_short(element, prefix, namespaces):
    return {
        f'{prefix}_Obec': safe_get(element, 's:Obec', namespaces),
        f'{prefix}_Ulice': safe_get(element, 's:Ulice', namespaces),
        f'{prefix}_Psc': safe_get(element, 's:PSC', namespaces),
    }


# Získání adresních údajů včetně Kraj, Okres, Orp
def get_adresa_long(element, prefix, namespaces):
    result = {}
    result[f'{prefix}_Kraj'] = safe_get(element, 's:Kraj', namespaces)
    result[f'{prefix}_Okres'] = safe_get(element, 's:Okres', namespaces)
    result[f'{prefix}_Orp'] = safe_get(element, 's:ORP', namespaces)
    result.update(get_adresa_short(element, prefix, namespaces))
    return result


# Získání kontaktních údajů
def get_kontakt(element, prefix, namespaces):
    return {
        f'{prefix}_Telefon': safe_get(element, 's:Telefon', namespaces),
        f'{prefix}_Email': safe_get(element, 's:Email', namespaces),
    }


# Pomocná funkce pro extrakci seznamu hodnot z Osvědčení
def get_osvedceni_list(element, xpath, namespaces):
    return [el.text for el in safe_findall(element, xpath, namespaces) if el.text]


# Extrahuje data osvědčení kategorií pro konkrétní typ stanice
def get_osvedceni_data_short(element, prefix, namespaces):
    kategorie_set = set()

    osvedceni_seznam_element = safe_find(element, 's:OsvedceniSeznam', namespaces)
    for osvedceni_element in safe_findall(osvedceni_seznam_element, 's:Osvedceni', namespaces):
        kategorie_set.update(get_osvedceni_list(osvedceni_element, 's:Kategorie', namespaces))

    # Vrací None, pokud by byl seznam prázdný
    return {
        f'{prefix}_Osvedceni_Kategorie': sorted(list(kategorie_set)) if kategorie_set else None,
    }


# Extrahuje data osvědčení pro konkrétní typ stanice včetně EmisniSystem a Palivo (pole validní pouze pro SME)
def get_osvedceni_data_long(element, prefix, namespaces):
    result = get_osvedceni_data_short(element, prefix, namespaces)

    emisni_system_set = set()
    palivo_set = set()

    osvedceni_seznam_element = safe_find(element, 's:OsvedceniSeznam', namespaces)
    for osvedceni_element in safe_findall(osvedceni_seznam_element, 's:Osvedceni', namespaces):
        emisni_system_set.update(get_osvedceni_list(osvedceni_element, 's:EmisniSystem', namespaces))
        palivo_set.update(get_osvedceni_list(osvedceni_element, 's:Palivo', namespaces))

    # Vrací None, pokud by byl seznam prázdný
    result[f'{prefix}_Osvedceni_EmisniSystem'] = sorted(list(emisni_system_set)) if emisni_system_set else None # RIZENY / NERIZENY
    result[f'{prefix}_Osvedceni_Palivo'] = sorted(list(palivo_set)) if palivo_set else None # BA / NM / LPG / LNG / CNG

    return result


def parse_stanice(element, namespaces):
    # Parsování jednoho záznamu stanice
    stanice_record = {}
    
    # Identifikace
    stanice_record['Stanice_Cislo'] = safe_get(element, 's:Cislo', namespaces)

    # Adresa a kontakt stanice
    stanice_record.update(get_adresa_long(safe_find(element, 's:Adresa', namespaces), 'Stanice_Adresa', namespaces))
    stanice_record.update(get_kontakt(safe_find(element, 's:Kontakt', namespaces), 'Stanice_Kontakt', namespaces))

    # Provozovatel
    provozovatel_element = safe_find(element, 's:Provozovatel', namespaces)
    stanice_record['Provozovatel_Ico'] = safe_get(provozovatel_element, 's:ICO', namespaces)
    stanice_record['Provozovatel_Nazev'] = safe_get(provozovatel_element, 's:Nazev', namespaces)
    stanice_record.update(get_adresa_short(safe_find(provozovatel_element, 's:Adresa', namespaces), 'Provozovatel_Adresa', namespaces))
    stanice_record.update(get_kontakt(safe_find(provozovatel_element, 's:Kontakt', namespaces), 'Provozovatel_Kontakt', namespaces))

    # Zpracování typů stanic a jejich specifických osvědčení
    druh_element = safe_find(element, 's:Druh', namespaces)
    
    # Mapování XML elementů na prefixy sloupců
    types_map = {'s:KontrolniStanice': 'Stk', 's:ZkusebniStanice': 'SmeZkusebni', 's:EmisniStanice': 'Sme', 's:AdrStanice': 'Adr'}
    for tag, prefix in types_map.items():
        type_element = safe_find(druh_element, tag, namespaces)
        # Indikátor zda je stanice daného typu
        stanice_record[f'{prefix}_Pritomno'] = str(type_element is not None)
        # Pro SME je slodováno více polí
        if prefix == 'Sme':
            stanice_record.update(get_osvedceni_data_long(type_element, prefix, namespaces))
        else:
            stanice_record.update(get_osvedceni_data_short(type_element, prefix, namespaces))

    return stanice_record

#--------------------------------------------------------------------------------------------------------------

def write_batch(output_dir, batch_data, file_stem):
    if not batch_data: return
    
    file_name = f"{file_stem}.parquet"
    
    # Zapsání souboru na disk v požadovaném formátu
    df = pd.DataFrame(batch_data)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, output_dir / file_name)

def process_single_pipeline(title, download_url, base_dir, parser_func, max_attempts, verbosity):
    gz_dir = base_dir / 'gz'
    xml_dir = base_dir / 'xml'
    parquet_dir = base_dir / 'parquet'

    create_directory(gz_dir, Verbosity.QUIET)
    create_directory(xml_dir, Verbosity.QUIET)
    create_directory(parquet_dir, Verbosity.QUIET)

    # Stažení archivu
    download_file(title, download_url, gz_dir, max_attempts, verbosity)
    
    # Extrakce a smazání .gz
    xml_file_name = title + '.xml'
    extract_file(xml_file_name + '.gz', gz_dir, xml_dir, verbosity, delete=True)
    
    # Parsování a smazání .xml
    xml_file_path = xml_dir / xml_file_name
    parser_func(parquet_dir, xml_file_path, verbosity, delete=True)
    
    return title

def process_dataset_pipeline(sparql_endpoint, base_dir, parent_dataset_iri, start_date, end_date, parser_func, no_processes, max_attempts, verbosity):
    # Nalezení již hotových souborů podle existujících Parquet souborů
    parquet_dir = base_dir / 'parquet'
    already_processed = downloaded_dates([parquet_dir])
    
    if verbosity > Verbosity.QUIET:
        print(f'Nalezeno {len(already_processed)} již zpracovaných souborů.')

    # Získání URL adres ke stažení
    titles, download_urls = get_url_addresses(sparql_endpoint, parent_dataset_iri, start_date, end_date, already_processed, verbosity)
    total_files = len(titles)
    
    if total_files == 0:
        if verbosity > Verbosity.QUIET:
            print('Žádné nové soubory ke zpracování.')
        return

    if verbosity > Verbosity.QUIET:
        print(f'Spouštím zpracování {total_files} souborů (download -> extract -> parquet) v {no_processes} procesech.')

    # Definice hranice pro tisk (každých 10 %)
    print_threshold = max(1, total_files // 10)
    processed_count = 0

    # Příprava funkce pro multiprocessing
    worker = partial(process_single_pipeline, base_dir=base_dir, parser_func=parser_func, max_attempts=max_attempts, verbosity=verbosity)

    with ProcessPoolExecutor(max_workers=no_processes) as executor:
        # Vytvoření mapy úloh
        futures = {executor.submit(worker, t, u): t for t, u in zip(titles, download_urls)}
        
        for future in as_completed(futures):
            try:
                future.result()
                processed_count += 1
                # Tisk stavu při dosažení 10% hranice nebo na konci
                if verbosity > Verbosity.QUIET and (processed_count % print_threshold == 0 or processed_count == total_files or processed_count == 1):
                    print(f'Zpracováno {processed_count}/{total_files} souborů.')
            except Exception as e:
                print(f'Kritická chyba při zpracování souboru: {e}')

#--------------------------------------------------------------------------------------------------------------

def parse_inspections_file(target_dir, xml_file, verbosity, delete):
    namespaces = {
        'p': 'istp:opendata:schemas:ProhlidkaSeznam:v1', 
        'd': 'istp:opendata:schemas:DatovaSada:v1'      
    }

    target_path = (target_dir / xml_file.stem).with_suffix('.parquet')

    # Kontrola, zda rozparsovaný soubor již existuje
    if not skip_file(target_path, verbosity):
        # Seznam, který bude obsahovat naparsované části XML
        prohlidky_batch = []
        
        # Načtení celého stromu do paměti a identifikace elementu DatovyObsah
        tree = etree.parse(xml_file)
        datovy_obsah = tree.find(f'd:DatovyObsah', namespaces)
        if datovy_obsah is None or len(datovy_obsah) == 0:
            raise KeyError(f'V souboru "{xml_file}" chybí element DatovyObsah')
        prohlidka_element_list = datovy_obsah[0]

        # Postupné načtení a zprasování všech prohlídek
        for element in prohlidka_element_list.iterchildren(tag=f'{{{namespaces["p"]}}}Prohlidka'):
            prohlidka_record = parse_prohlidka(element, namespaces)
            if prohlidka_record:
                prohlidky_batch.append(prohlidka_record)
        
        # Zapsání souboru na disk
        write_batch(target_dir, prohlidky_batch, xml_file.stem)

        if verbosity > Verbosity.NORMAL:
            print(f'Zapisuji vyparsovaný parquet soubor ze: "{xml_file.stem}".')

    # Smazání původního souboru
    if delete:
        delete_path(xml_file, verbosity)


#--------------------------------------------------------------------------------------------------------------

def parse_measurements_file(target_dir, xml_file, verbosity, delete):
    namespaces = {
        'm': 'istp:opendata:schemas:MereniSeznam:v1', 
        'd': 'istp:opendata:schemas:DatovaSada:v1'      
    }

    target_path = (target_dir / xml_file.stem).with_suffix('.parquet')

    # Kontrola, zda rozparsovaný soubor již existuje
    if not skip_file(target_path, verbosity):
        mereni_batch = []
        
        # Načtení celého stromu do paměti a identifikace elementu DatovyObsah
        tree = etree.parse(xml_file)
        datovy_obsah = tree.find(f'd:DatovyObsah', namespaces)
        if datovy_obsah is None or len(datovy_obsah) == 0:
            raise KeyError(f'V souboru "{xml_file}" chybí element DatovyObsah')
        mereni_element_list = datovy_obsah[0]

        # Postupné načtení a zprasování všech prohlídek
        for element in mereni_element_list.iterchildren(tag=f'{{{namespaces["m"]}}}Mereni'):
            mereni_record = parse_mereni(element, namespaces)
            if mereni_record:
                mereni_batch.append(mereni_record)

        # Explicitní uvolnění celého stromu z paměti
        del tree
        
        # Zapsání souborů na disk
        write_batch(target_dir, mereni_batch, xml_file.stem)
        if verbosity > Verbosity.NORMAL:
            print(f'Zapisuji vyparsované parquet soubory ze: "{xml_file.stem}".')

    # Smazání původního souboru
    if delete:
        delete_path(xml_file, verbosity)


#--------------------------------------------------------------------------------------------------------------

def parse_stations_file(target_dir, xml_file, verbosity, delete):
    namespaces = {
        's': 'istp:opendata:schemas:StaniceSeznam:v1',
        'd': 'istp:opendata:schemas:DatovaSada:v1'
    }

    # Definice cesty k uložení souboru
    target_path = (target_dir / xml_file.stem).with_suffix('.parquet')
    
    # Kontrola, zda rozparsovaný soubor již existuje
    if skip_file(target_path, verbosity):
        return

    stanice_batch = []

    try:
        # Načtení stromu
        tree = etree.parse(xml_file)
        
        # Přímý přístup k DatovyObsah a StaniceSeznam (dle poskytnuté struktury XML)
        datovy_obsah = tree.find('d:DatovyObsah', namespaces)
        stanice_list_element = safe_find(datovy_obsah, 's:StaniceSeznam', namespaces)
        
        if stanice_list_element is None:
             raise KeyError(f'V souboru "{xml_file}" chybí element DatovyObsah nebo StaniceSeznam')

        # Iterace přes stanice
        for element in stanice_list_element.iterchildren(tag=f'{{{namespaces["s"]}}}Stanice'):
            stanice_record = parse_stanice(element, namespaces)
            if stanice_record:
                stanice_batch.append(stanice_record)
        
        # Uvolnění celého stromu
        del tree

        # Zapsání souboru na disk
        write_batch(target_dir, stanice_batch, xml_file.stem)

        if verbosity > Verbosity.NORMAL:
            print(f'Zapisuji stanice z: "{xml_file.stem}".')

    except Exception as e:
        print(f'Chyba při parsování "{xml_file}": {e}')
        return

    # Smazání původního souboru
    if delete:
        delete_path(xml_file, verbosity)


#--------------------------------------------------------------------------------------------------------------
def run_preprocessing():
    print('—————————————————————————————————Stanice STK a SME:—————————————————————————————————————————————\n')
    # Seznam stanic prochází denní aktualizací
    clear_folder(config.STATIONS_DIR, config.VERBOSITY)
    download_stations(config.SPARQL_ENDPOINT, config.STATIONS_DIR / 'gz', config.DATASET_STATIONS, config.VERBOSITY)
    extract_file('Stanice STK a SME.xml.gz', config.STATIONS_DIR / 'gz', config.STATIONS_DIR / 'xml', config.VERBOSITY, delete=True)
    parse_stations_file(config.STATIONS_DIR / 'parquet', config.STATIONS_DIR / 'xml' / 'Stanice STK a SME.xml', config.VERBOSITY, delete=True)

    print('——————————————————————————————————PROHLÍDKY VOZIDEL STK A SME:——————————————————————————————————\n')
    process_dataset_pipeline(
        sparql_endpoint=config.SPARQL_ENDPOINT, 
        base_dir=config.INSPECTIONS_DIR, 
        parent_dataset_iri=config.PARENT_DATASET_INSPECTIONS, 
        start_date=config.START_DATE, 
        end_date=config.END_DATE, 
        parser_func=parse_inspections_file, 
        no_processes=config.NO_PARSE_PROCESSES, 
        max_attempts=config.MAX_DOWNLOAD_ATTEMPTS, 
        verbosity=config.VERBOSITY
    )

    print('\n————————————————————————————————DATA Z MĚŘÍCÍCH PŘÍSTROJŮ:————————————————————————————————————\n')
    process_dataset_pipeline(
        sparql_endpoint=config.SPARQL_ENDPOINT, 
        base_dir=config.MEASUREMENTS_DIR, 
        parent_dataset_iri=config.PARENT_DATASET_MEASUREMENTS, 
        start_date=config.START_DATE, 
        end_date=config.END_DATE, 
        parser_func=parse_measurements_file, 
        no_processes=config.NO_PARSE_PROCESSES, 
        max_attempts=config.MAX_DOWNLOAD_ATTEMPTS, 
        verbosity=config.VERBOSITY
    )
    


if __name__ == '__main__':
    run_preprocessing()