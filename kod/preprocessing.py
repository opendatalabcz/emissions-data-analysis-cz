from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import gzip
import shutil
from pathlib import Path
from functools import partial
from statistics import mean

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree # type: ignore <- pylance milně hlásí chybu
import requests
import polars as pl

from utils import create_directory, Verbosity, str_to_date, date_from_file_path, date_from_file_name, delete_path, floats_sublist, pad_list_with_none
from schemas import mereni_schema, nafta_schema


def explain_verbosity(verbosity):
    if verbosity == Verbosity.NORMAL:
        print('"."\t- provedení operace se souborem\n"-"\t- přeskočení souboru\n n \t- číslo pokusu o provedení operace\n')


def skip_file(file, verbosity):
    if file.exists():
        if verbosity > Verbosity.NORMAL:
            print(f'Přeskakuji soubor "{file.stem}", již zpracován.')
        elif verbosity > Verbosity.QUIET:
            print('-', end='', flush=True)
        return True
    return False


#--------------------------------------------------------------------------------------------------------------

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
            except:
                continue
            result.add(date)
    return result

#--------------------------------------------------------------------------------------------------------------

def get_url_addresses(sparql_endpoint, parent_dateset_iri, start_date, end_date, already_downloaded, verbosity):
    # Definice dotazu
    get_download_url_query = f'''
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dcterms: <http://purl.org/dc/terms/>

    SELECT ?title ?downloadURL 
    WHERE {{
        <{parent_dateset_iri}> dcat:seriesMember ?dataset.
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
    titles = []
    download_urls = []
    for title, download_url in zip(all_titles, all_download_urls):
        date = date_from_file_name(title)
        # Filtrace na zaklade intervalu
        if date < str_to_date(start_date):
            continue
        if end_date is not None:
            if date > str_to_date(end_date):
                continue
        
        # Filtrace na zaklade predchoziho zpracovani
        if date in already_downloaded:
            continue

        titles.append(title)
        download_urls.append(download_url)
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
            elif verbosity > Verbosity.QUIET:
                print('.', end='', flush=True)

            return

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


def download_files(download_dir, parent_dataset_iri, start_date, end_date, already_downloaded, no_threads, max_attempts, verbosity):
    # Vytvoření adresáře pro uložení souborů
    create_directory(download_dir, verbosity)

    # Stažení URL adres souborů spolu s jejich názvy
    sparql_endpoint = 'https://data.gov.cz/sparql'
    titles, download_urls = get_url_addresses(sparql_endpoint, parent_dataset_iri, start_date, end_date, already_downloaded, verbosity)

    # Paralelní stažení souborů
    if verbosity > verbosity.QUIET:
        print(f'Stahuji souběžně {len(download_urls)} souborů při použití {no_threads} vláken.')
    with ThreadPoolExecutor(max_workers=no_threads) as executor:
        wrapper = lambda title, download_url: download_file(title, download_url, download_dir, max_attempts, verbosity)
        results = executor.map(wrapper, titles, download_urls)
        list(results) # Vynucení počkání na dokončení stahování
    
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
        elif verbosity > Verbosity.QUIET:
            print('.', end='', flush=True)
    
    # Smazání původního souboru
    if delete:
        delete_path(source_file_path, verbosity)


def extract_files(source_dir, target_dir, no_threads, verbosity, delete=True):
    # Vytvoření adresáře pro uložení souborů
    create_directory(target_dir, verbosity)

    # Paralelní extrahování souborů
    extractable = list(source_dir.glob('*.gz'))
    if verbosity > verbosity.QUIET:
        print(f'Extrahuji souběžně {len(extractable)} souborů při použití {no_threads} vláken.')
    with ThreadPoolExecutor(max_workers=no_threads) as executor:
        wrapper = lambda file_path: extract_file(file_path.name, source_dir, target_dir, verbosity, delete)
        results = executor.map(wrapper, extractable)
        list(results) # Vynuceni pockani na dokonceni extrahovani
    
    # Smazání zdrojového repozitáře
    if delete:
        delete_path(source_dir, verbosity)

    # Nová řádka pro vizuélní odlišení konce úkonu
    if verbosity > Verbosity.QUIET:
        print('\nEXTRAKCE DONONČENA.\n')

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
    emise_record.update(get_stanice(safe_find(element, 'p:Stanice', namespaces), 'Emise', namespaces))
    emise_record.update(get_casove_udaje(safe_find(element, 'p:CasoveUdaje', namespaces), 'Emise', namespaces))
    emise_record['Emise_OdpovednaOsoba'] = safe_get(element, 'p:OdpovednaOsoba', namespaces)
    emise_record['Emise_ZakladniPalivo'] = safe_get(element, 'p:ZakladniPalivo', namespaces)
    emise_record['Emise_AlternativniPalivo'] = safe_get(element, 'p:AlternativniPalivo', namespaces)
    emise_record['Emise_EmisniSystem'] = safe_get(element, 'p:EmisniSystem', namespaces)
    emise_record['Emise_VyrobceMotoru'] = safe_get(element, 'p:VyrobceMotoru', namespaces)
    emise_record['Emise_CisloMotoru'] = safe_get(element, 'p:CisloMotoru', namespaces)
    emise_record['Emise_RokVyroby'] = safe_get(element, 'p:RokVyroby', namespaces)
    return emise_record


def get_technicka_cast(element, namespaces):
    technicka_record = {}
    technicka_record.update(get_casove_udaje(safe_find(element, 'p:CasoveUdaje', namespaces), 'Technicka', namespaces))
    technicka_record['Technicka_OdpovednaOsoba'] = safe_get(element, 'p:OdpovednaOsoba', namespaces)
    return technicka_record


def get_platnost(element, namespaces):
    return {
        'Adr_Platnost_Periodicka': safe_get(element, 'p:Periodicka', namespaces),
        'Adr_Platnost_Meziperiodicka': safe_get(element, 'p:Meziperiodicka', namespaces)
    }


def get_adr_cast(element, namespaces):
    adr_record = {}
    adr_record.update(get_casove_udaje(safe_find(element, 'p:CasoveUdaje', namespaces), 'Adr', namespaces))
    adr_record['Adr_OdpovednaOsoba'] = safe_get(element, 'p:OdpovednaOsoba', namespaces)
    adr_record.update(get_platnost(safe_find(element, 'p:Platnost', namespaces), namespaces))
    adr_record['Adr_KodCisterny'] = safe_get(element, 'p:KodCisterny', namespaces)
    adr_record['Adr_CisloOsvedceni'] = safe_get(element, 'p:CisloOsvedceni', namespaces)
    adr_record['Adr_ZavadyText'] = safe_get(element, 'p:ZavadyText', namespaces)
    adr_record['Adr_Poznamka'] = safe_get(element, 'p:Poznamka', namespaces)
    return adr_record


def get_tsk_cast(element, namespaces):
    tsk_record = {}
    tsk_record.update(get_casove_udaje(safe_find(element, 'p:CasoveUdaje', namespaces), 'Tsk', namespaces))
    tsk_record['Tsk_OdpovednaOsoba'] = safe_get(element, 'p:OdpovednaOsoba', namespaces)
    return tsk_record


def get_vysledek(element, namespaces):
    vysledek_record = {}
    vysledek_record['Vysledek_Odometr'] = safe_get(element, 'p:Odometr', namespaces)
    vysledek_record['Vysledek_Poznamka'] = safe_get(element, 'p:Poznamka', namespaces)
    vysledek_record['Vysledek_DatumPristiProhlidky'] = safe_get(element, 'p:DatumPristiProhlidky', namespaces)
    vysledek_record['Vysledek_NalepkaVylepena'] = safe_get(element, 'p:NalepkaVylepena', namespaces)
    vysledek_record['Vysledek_Celkovy'] = safe_get(element, 'p:VysledekCelkovy', namespaces)
    return vysledek_record


def parse_zavada_seznam(element, cislo_protokolu, source, namespaces):
    if element is None:
        return []
    zavady = []
    for zavada_element in element.findall('p:Zavada', namespaces):
        zavady.append({
            'CisloProtokolu': cislo_protokolu,
            'Zdroj': source,
            'Kod': safe_get(zavada_element, 'p:Kod', namespaces),
            'Zavaznost': safe_get(zavada_element, 'p:Zavaznost', namespaces)
        })
    return zavady


def parse_kontrolni_ukon_seznam(element, cislo_protokolu, namespaces):
    if element is None:
        return []
    ukony = []
    for ukon_element in element.findall('p:KontrolniUkon', namespaces):
        ukony.append({
            'CisloProtokolu': cislo_protokolu,
            'Typ': safe_get(ukon_element, 'p:Typ', namespaces),
            'Nevyhovujici': safe_get(ukon_element, 'p:Nevyhovujici', namespaces),
        })
    return ukony


def parse_adr_typ_seznam(element, cislo_protokolu, namespaces):
    if element is None:
        return []
    adr_typy = []
    for adr_typ_element in element.findall('p:TypVozidlaADR', namespaces):
        adr_typy.append({
            'CisloProtokolu': cislo_protokolu,
            'TypVozidlaADR': adr_typ_element.text,
        })
    return adr_typy


def parse_prohlidka(element, namespaces):
    # Získání prvního údaje, který ověří, zda údaj existuje
    cislo_protokolu = safe_get(element, 'p:CisloProtokolu', namespaces)
    if not cislo_protokolu:
        return None, [], [], []

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
    prohlidka_record.update(get_emisni_cast(safe_find(element, 'p:EmisniCast', namespaces), namespaces))
    prohlidka_record.update(get_technicka_cast(safe_find(element, 'p:TechnickaCast', namespaces), namespaces))
    prohlidka_record.update(get_adr_cast(safe_find(element, 'p:AdrCast', namespaces), namespaces))
    prohlidka_record.update(get_tsk_cast(safe_find(element, 'p:TskCast', namespaces), namespaces))
    prohlidka_record.update(get_vysledek(safe_find(element, 'p:Vysledek', namespaces), namespaces))
    
    # Přidání 1:n vztahů
    zavady_records = []
    adr_typy_records = []
    ukony_records = []

    # Parsování závad z technické části, tsk části a výsledku
    zavady_records.extend(parse_zavada_seznam(safe_find(safe_find(element, 'p:TechnickaCast', namespaces), 'p:ZavadaSeznam', namespaces), cislo_protokolu, 'TechnickaCast', namespaces))
    zavady_records.extend(parse_zavada_seznam(safe_find(safe_find(element, 'p:TskCast', namespaces), 'p:ZavadaSeznam', namespaces), cislo_protokolu, 'TskCast', namespaces))
    zavady_records.extend(parse_zavada_seznam(safe_find(safe_find(element, 'p:Vysledek', namespaces), 'p:ZavadaSeznam', namespaces), cislo_protokolu, 'Vysledek', namespaces))
    # Parsování adr typů z adr části
    adr_typy_records.extend(parse_adr_typ_seznam(safe_find(safe_find(element, 'p:AdrCast', namespaces), 'p:TypVozidlaADRSeznam', namespaces), cislo_protokolu, namespaces))
    # Parsování úkonů z tsk části
    ukony_records.extend(parse_kontrolni_ukon_seznam(safe_find(safe_find(element, 'p:TskCast', namespaces), 'p:KontrolniUkonSeznam', namespaces), cislo_protokolu, namespaces))

    return prohlidka_record, zavady_records, ukony_records, adr_typy_records

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
def select_worst(result_lists, strategy_dict):
    result = {}
    for name, result_list in result_lists.items():
        try:
            if not result_list:
                result[name] = None
                continue
            if 'Min' in name or 'Max' in name or 'Vysledek' in name:
                result[name] = next((result for result in result_list if result is not None), None)
                continue
            strategy = strategy_dict[name.split('_')[-2]]
            floats = floats_sublist(result_list)
            float_result = None
            match strategy:
                case 'max':
                    float_result = max(floats, default=None)
                case 'min':
                    float_result = min(floats, default=None)
                case 'mean':
                    float_result = mean(floats) # Vyvolá výjimku v případě prázdného seznamu
                case 'max_diff_1':
                    float_result = max(floats, default=None, key=lambda x: abs(x - 1.0))
                case 'bounds':
                    name_stem = name.partition("_Hodnota")[0]
                    try:
                        min_value = float(result[f'{name_stem}_Min_Hodnota'])
                        max_value = float(result[f'{name_stem}_Max_Hodnota'])
                        optimal_value = (max_value + min_value) / 2
                        float_result = max(floats, default=None, key=lambda x: abs(x - optimal_value))
                    # Pokud by některá z krajních hodnot chyběla vezmu první záznam o otáčkách
                    except Exception:
                        float_result = next((float for float in floats if float is not None), None)
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
    strategy_dict = {'CO': 'max', 'CO2': 'min', 'COCOOR': 'max', 'HC': 'max', 'LAMBDA': 'max_diff_1', 'N': 'bounds', 'NOX': 'max', 'O2': 'max', 'TPS': 'max'}
    result |= select_worst(result_lists, strategy_dict)
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
    strategy_dict = {'TPS': 'mean', 'CasAkcelerace': 'max', 'Kourivost': 'max', 'OtackyPrebehove': 'mean', 'OtackyVolnobezne': 'mean', 'Teplota': 'min'}
    result |= select_worst(result_lists, strategy_dict)
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
    emise_record['EmisniSystem'] = "Rizeny_Obd" if rizeny_obd_element is not None else "Rizeny" if rizeny_element is not None else "Nerizeny" if nerizeny_element is not None else None
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

def parse_to_parquet(source_dir, file_parser, no_threads, verbosity, delete):
    # Vyhledání souborů pro parsování
    xml_files = sorted(list(source_dir.glob('*.xml')), key=date_from_file_path)
    # if not xml_files:
    #     raise FileNotFoundError(f'Žádně .xml soubory nebyly nalezeny v {source_dir}.')
    if verbosity > Verbosity.QUIET:
        print(f'Nalezeno {len(xml_files)} .xml souborů. Spouštím {no_threads} vláken.')

    # Samotně paralelní parsování souborů
    futures = []
    with ProcessPoolExecutor(max_workers=no_threads) as executor:
        # Poslání úkolů do poolu
        for xml_file in xml_files:
            future = executor.submit(file_parser, xml_file)
            futures.append(future)

        # Process results as they complete
        for future in as_completed(futures):
            # .result() zpropaguje výjimku, pokud nastane uvnitř parsování
            future.result() 

    # Nová řádka pro vizuélní odlišení konce úkonu
    if verbosity > Verbosity.QUIET:
        print('\nPARSOVÁNÍ DONONČENO.\n')

    # Smazání zdrojového repozitáře
    if delete:
        delete_path(source_dir, verbosity)


def write_batch(output_dir, batch_data, file_stem):
    if not batch_data: return
    
    file_name = f"{file_stem}.parquet"
    
    # Zapsání souboru na disk v požadovaném formátu
    df = pd.DataFrame(batch_data)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, output_dir / file_name)

#--------------------------------------------------------------------------------------------------------------

def parse_inspections_file(inspections_dir, defects_dir, actions_dir, adr_type_dir, xml_file, namespaces, verbosity, delete):
    target_path = (inspections_dir / xml_file.stem).with_suffix('.parquet')

    # Kontrola, zda rozparsovaný soubor již existuje
    if not skip_file(target_path, verbosity):
        # Seznamy, které budou obsahovat naparsované části XML
        prohlidky_batch = []
        zavady_batch = []
        ukony_batch = []
        adr_typy_batch = []
        
        # Načtení celého stromu do paměti a identifikace elementu DatovyObsah
        tree = etree.parse(xml_file)
        datovy_obsah = tree.find(f'd:DatovyObsah', namespaces)
        if datovy_obsah is None or len(datovy_obsah) == 0:
            raise KeyError(f'V souboru "{xml_file}" chybí element DatovyObsah')
        prohlidka_element_list = datovy_obsah[0]

        # Postupné načtení a zprasování všech prohlídek
        for element in prohlidka_element_list.iterchildren(tag=f'{{{namespaces['p']}}}Prohlidka'):
            prohlidka_record, zavady_records, ukony_records, adr_typy_records = parse_prohlidka(element, namespaces)
            
            if prohlidka_record:
                prohlidky_batch.append(prohlidka_record)
                zavady_batch.extend(zavady_records)
                ukony_batch.extend(ukony_records)
                adr_typy_batch.extend(adr_typy_records)
            
            # Smazáni elementu z paměti pro vyšší efektivitu
            element.clear()

        # Explicitní uvolnění celého stromu z paměti
        del tree
        
        # Zapsání souborů na disk
        write_batch(inspections_dir, prohlidky_batch, xml_file.stem)
        write_batch(defects_dir, zavady_batch, xml_file.stem)
        write_batch(actions_dir, ukony_batch, xml_file.stem)
        write_batch(adr_type_dir, adr_typy_batch, xml_file.stem)

        if verbosity > Verbosity.NORMAL:
            print(f'Zapisuji vyparsované parquet soubory ze: "{xml_file.stem}".')
        elif verbosity > Verbosity.QUIET:
            print('.', end='', flush=True)

    # Smazání původního souboru
    if delete:
        delete_path(xml_file, verbosity)


def parse_inspections_to_parquet(dataset_dir, inspections_subdir, defects_subdir, actions_subdir, adr_type_subdir, no_threads, verbosity, delete=True):
    # Definice jmenných prostorů
    namespaces = {
        'p': 'istp:opendata:schemas:ProhlidkaSeznam:v1', 
        'd': 'istp:opendata:schemas:DatovaSada:v1'      
    }

    # Nastavení repozitárů
    parquet_dir = dataset_dir / 'parquet'

    inspections_dir = parquet_dir / inspections_subdir
    defects_dir = parquet_dir / defects_subdir
    actions_dir = parquet_dir / actions_subdir
    adr_type_dir = parquet_dir / adr_type_subdir
    
    create_directory(inspections_dir, verbosity)
    create_directory(defects_dir, verbosity)
    create_directory(actions_dir, verbosity)
    create_directory(adr_type_dir, verbosity)

    # Vytvoření partial funkce pro podporu serializace
    inspections_parser = partial(
        parse_inspections_file,
        inspections_dir,
        defects_dir,
        actions_dir,
        adr_type_dir,
        namespaces=namespaces,
        verbosity=verbosity,
        delete=delete
    )
    # Zavolání funcke pro provedení parsování
    parse_to_parquet(dataset_dir / 'xml', inspections_parser, no_threads, verbosity, delete)

#--------------------------------------------------------------------------------------------------------------

def parse_measurements_file(target_dir, xml_file, namespaces, verbosity, delete):
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
        for element in mereni_element_list.iterchildren(tag=f'{{{namespaces['m']}}}Mereni'):
            mereni_record = parse_mereni(element, namespaces)
            
            if mereni_record:
                mereni_batch.append(mereni_record)
            
            # Smazáni elementu z paměti pro vyšší efektivitu
            element.clear()

        # Explicitní uvolnění celého stromu z paměti
        del tree
        
        # Zapsání souborů na disk
        write_batch(target_dir, mereni_batch, xml_file.stem)
        if verbosity > Verbosity.NORMAL:
            print(f'Zapisuji vyparsované parquet soubory ze: "{xml_file.stem}".')
        elif verbosity > Verbosity.QUIET:
            print('.', end='', flush=True)

    # Smazání původního souboru
    if delete:
        delete_path(xml_file, verbosity)


def parse_measurements_to_parquet(dataset_dir, measurements_subdir, no_threads, verbosity, delete=True):
    # Definice jmenných prostorů
    namespaces = {
        'm': 'istp:opendata:schemas:MereniSeznam:v1', 
        'd': 'istp:opendata:schemas:DatovaSada:v1'      
    }

    # Nastavení repozitárů
    parquet_dir = dataset_dir / 'parquet'
    measurements_dir = parquet_dir / measurements_subdir
    create_directory(measurements_dir, verbosity)

    # Vytvoření partial funkce pro podporu serializace
    measurements_parser = partial(
        parse_measurements_file,
        measurements_dir,
        namespaces=namespaces,
        verbosity=verbosity,
        delete=delete
    )
    # Zavolání funcke pro provedení parsování
    parse_to_parquet(dataset_dir / 'xml', measurements_parser, no_threads, verbosity, delete)

#--------------------------------------------------------------------------------------------------------------

def split_measurements(mereni_dir, diesel_personal_dir, verbosity):
    source_files = list(mereni_dir.iterdir())
    if verbosity > Verbosity.QUIET:
        print(f'Nalezeno {len(source_files)} souborů obsahující data o měření. Zahajuji jejich filtrování')

    create_directory(diesel_personal_dir, verbosity)

    for file in source_files:
        # Přeskočení souboru, pokud už byl zpracován
        target_file = diesel_personal_dir / file.name
        if skip_file(target_file, verbosity):
            continue

        df = pl.read_parquet(file, schema=mereni_schema)

        j1939_cols = [name for name in mereni_schema.keys() if 'J1939' in name]
        obd_zazeh_cols = [name for name in mereni_schema.keys() if 'Obd_Readiness_Zazeh' in name]
        # Podmínka pro záznam osobního dieselového vozidla
        diesel_personal_cond = [
            # Alespoň jedno výústění dieselu bylo změřeno (a žádné vyústění jiného pohonu neměřeno)
            pl.col('Benzin_PocetVyusteni').eq('0') & pl.col('Nafta_PocetVyusteni').ne('0') & pl.col('Plyn_PocetVyusteni').eq('0'),
            # U diagnostických chybových kódů pro užitková vozidla nebyl proveden pokus o testování
            pl.all_horizontal(pl.col(j1939_cols).is_null()),
            # U diagnostických chybových kódů pro zážehová vozidla nebyl proveden pokus o testování
            pl.all_horizontal(pl.col(obd_zazeh_cols).is_null())
        ]

        # Sloupce, které nesou význam pro naftová vozidla
        diesel_columns = nafta_schema.keys()

        df_diesel_personal = df.filter(diesel_personal_cond).select(diesel_columns)
        df_diesel_personal.write_parquet(target_file)

        # Oznámění úspěchu uživateli
        if verbosity > Verbosity.NORMAL:
            print(f'Zapisuji vyfiltrované parquet soubory ze: "{file.stem}".')
        elif verbosity > Verbosity.QUIET:
            print('.', end='', flush=True)

    # Nová řádka pro vizuélní odlišení konce úkonu
    if verbosity > Verbosity.QUIET:
        print('\nFILTROVÁNÍ DONONČENO.\n')


#--------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    # Definice konstant
    INSPECTIONS_DIR = Path('kod/data/prohlidky_vozidel_stk_a_sme')
    PARENT_DATASET_INSPECTIONS = 'https://data.gov.cz/zdroj/datové-sady/66003008/9c95ebdba1dc7a2fbcfc5b6c07d25705'
    MEASUREMENTS_DIR = Path('kod/data/data_z_mericich_pristroju')
    # MEASUREMENTS_DIR = Path('kod/data/data_z_mericich_pristroju/data_z_mericich_pristoroju')
    PARENT_DATASET_MEASUREMENTS = 'https://data.gov.cz/zdroj/datové-sady/66003008/e8e07fa264f3bd2179be03381ec324de'
    START_DATE = '01-01-2019'
    END_DATE = None
    NO_DOWNLOAD_THREADS = 30
    MAX_DOWNLOAD_ATTEMPTS = 10
    NO_EXTRACT_THREADS = 30
    VERBOSITY = Verbosity.NORMAL
    INSPECTIONS_SUBDIR = 'prohlidky'
    DEFECTS_SUBDIR = 'zavady'
    ACTIONS_SUBDIR = 'ukony'
    ADR_TYPE_SUBDIR = 'adr_typy'
    MEASUREMENTS_ALL_SUBDIR = 'mereni_all'
    NO_PARSE_PROCESSES = 4
    DIESEL_PERSONAL_SUBDIR = 'nafta_osobni'


    explain_verbosity(VERBOSITY)

    # print('——————————————————————————————————PROHLÍDKY VOZIDEL STK A SME:——————————————————————————————————\n')
    # downloaded_inspection_dates = downloaded_dates([INSPECTIONS_DIR / 'gz', INSPECTIONS_DIR / 'xml', INSPECTIONS_DIR / 'parquet' / INSPECTIONS_SUBDIR])
    # download_files(INSPECTIONS_DIR / 'gz', PARENT_DATASET_INSPECTIONS, START_DATE, END_DATE, downloaded_inspection_dates, NO_DOWNLOAD_THREADS, MAX_DOWNLOAD_ATTEMPTS, verbosity=VERBOSITY)
    # extract_files(INSPECTIONS_DIR / 'gz', INSPECTIONS_DIR / 'xml', NO_EXTRACT_THREADS, verbosity=VERBOSITY)
    # parse_inspections_to_parquet(INSPECTIONS_DIR, INSPECTIONS_SUBDIR, DEFECTS_SUBDIR, ACTIONS_SUBDIR, ADR_TYPE_SUBDIR, NO_PARSE_PROCESSES, VERBOSITY)

    # print('\n——————————————————————————————————DATA Z MĚŘÍCÍCH PŘÍSTROJŮ:——————————————————————————————————\n')
    # downloaded_measurement_dates = downloaded_dates([MEASUREMENTS_DIR / 'gz', MEASUREMENTS_DIR / 'xml', MEASUREMENTS_DIR / 'parquet' / MEASUREMENTS_ALL_SUBDIR])
    # download_files(MEASUREMENTS_DIR / 'gz', PARENT_DATASET_MEASUREMENTS, START_DATE, END_DATE, downloaded_measurement_dates, NO_DOWNLOAD_THREADS, MAX_DOWNLOAD_ATTEMPTS, verbosity=VERBOSITY)
    # extract_files(MEASUREMENTS_DIR / 'gz', MEASUREMENTS_DIR / 'xml', NO_EXTRACT_THREADS, verbosity=VERBOSITY)
    # parse_measurements_to_parquet(MEASUREMENTS_DIR, MEASUREMENTS_ALL_SUBDIR, NO_PARSE_PROCESSES, VERBOSITY, False)
    split_measurements(MEASUREMENTS_DIR / 'parquet' / MEASUREMENTS_ALL_SUBDIR, MEASUREMENTS_DIR / 'parquet' / DIESEL_PERSONAL_SUBDIR, VERBOSITY)
