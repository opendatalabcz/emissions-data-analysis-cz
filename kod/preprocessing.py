from concurrent.futures import ThreadPoolExecutor, as_completed
import gzip
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree # type: ignore <- pylance milně hlásí chybu
import requests

from utils import create_directory, Verbosity, str_to_date, date_from_file_path, date_from_file_name, delete_path


def explain_verbosity(verbosity):
    if verbosity == Verbosity.NORMAL:
        print('"."\t- provedení operace se souborem\n"-"\t- přeskočení souboru\n n \t- číslo pokusu o provedení operace\n')

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
    if path.exists():
        if verbosity > Verbosity.NORMAL:
            print(f'Přeskakuji soubor "{title}", již existuje.')
        elif verbosity > Verbosity.QUIET:
            print('-', end='', flush=True)
        return
    
    # Samotné stažení souboru
    if verbosity > Verbosity.NORMAL:
        print(f'Stahuji: "{title}".')
    elif verbosity > Verbosity.QUIET:
        print('.', end='', flush=True)

    for attempt in range(max_attempts):
        try:
            with requests.get(download_url, stream=True, timeout=60) as response:
                response.raise_for_status() # Kontrola statusu odpovědi
                with open(path, 'wb') as f: # Zápis do souboru po částech
                    for chunk in response.iter_content(chunk_size=8192): 
                        f.write(chunk)
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
    if destination_file_path.is_file():
        if verbosity > Verbosity.NORMAL:
            print(f'Přeskakuji soubor "{file_name}", již vyextrahován.')
        elif verbosity > Verbosity.QUIET:
            print('-', end='', flush=True)
        
        # Smazání původního souboru
        if delete:
            delete_path(source_file_path, verbosity)

        return

    # Samotná extrakce souboru
    if verbosity > Verbosity.NORMAL:
        print(f'Extrahuji: "{file_name}".')
    elif verbosity > Verbosity.QUIET:
        print('.', end='', flush=True)

    with gzip.open(source_file_path, 'rb') as f_in:
        with open(destination_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out) # type: ignore <- pylance milně hlásí chybu
    
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


def write_batch(output_dir, batch_data, file_stem):
    if not batch_data: return
    
    file_name = f"{file_stem}.parquet"
    
    # Zapsání souboru na disk v požadovaném formátů
    df = pd.DataFrame(batch_data)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, output_dir / file_name)


def parse_inspections_file(inspections_dir, defects_dir, actions_dir, adr_type_dir, xml_file, namespaces, verbosity, delete):
    file_name = xml_file.stem

    # Kontrola, zda vyextrahovaný soubor již existuje
    if (inspections_dir / file_name).with_suffix('.parquet').is_file():
        if verbosity > Verbosity.NORMAL:
            print(f'Přeskakuji soubor "{file_name}", již vyextrahován.')
        elif verbosity > Verbosity.QUIET:
            print('-', end='', flush=True)
            
        # Smazání původního souboru
        if delete:
            delete_path(xml_file, verbosity)

        return

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
    prohlidka_seznam_element = datovy_obsah[0]

    # Postupné načtení a zprasování všech prohlídek
    for element in prohlidka_seznam_element.iterchildren(tag=f'{{{namespaces['p']}}}Prohlidka'):
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
    if verbosity > Verbosity.NORMAL:
        print(f'Zapisuji vyparsované parquet soubory ze: "{file_name}".')
    elif verbosity > Verbosity.QUIET:
        print('.', end='', flush=True)

    write_batch(inspections_dir, prohlidky_batch, file_name)
    write_batch(defects_dir, zavady_batch, file_name)
    write_batch(actions_dir, ukony_batch, file_name)
    write_batch(adr_type_dir, adr_typy_batch, file_name)

    # Smazání původního souboru
    if delete:
        delete_path(xml_file, verbosity)
    

def parse_to_parquet(source_dir, file_parser, no_threads, verbosity, delete):
    # Vyhledání souborů pro parsování
    xml_files = sorted(list(source_dir.glob('*.xml')), key=date_from_file_path)
    if not xml_files:
        raise FileNotFoundError(f'Žádně .xml souborů nebyly nalezeny v {source_dir}.')
    if verbosity > Verbosity.QUIET:
        print(f'Nalezeno {len(xml_files)} .xml souborů. Spouštím {no_threads} vláken.')

    # Samotně paralelní parsování souborů
    futures = []
    with ThreadPoolExecutor(max_workers=no_threads) as executor:
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

    # Zavolání funcke pro provedení parsování
    inspections_parser = lambda xml_file: parse_inspections_file(inspections_dir, defects_dir, actions_dir, adr_type_dir, xml_file, namespaces, verbosity, delete)
    parse_to_parquet(dataset_dir / 'xml', inspections_parser, no_threads, verbosity, delete)


# def parse_measurements_to_parquet():


if __name__ == '__main__':
    # Definice konstant
    INSPECTIONS_DIR = Path('kod/data/prohlidky_vozidel_stk_a_sme')
    PARENT_DATASET_INSPECTIONS = 'https://data.gov.cz/zdroj/datové-sady/66003008/9c95ebdba1dc7a2fbcfc5b6c07d25705'
    MEASUREMENTS_DIR = Path('kod/data/data_z_mericich_pristroju')
    PARENT_DATASET_MEASUREMENTS = 'https://data.gov.cz/zdroj/datové-sady/66003008/e8e07fa264f3bd2179be03381ec324de'
    START_DATE = '01-01-2019'
    END_DATE = None #'31-12-2024'
    NO_DOWNLOAD_THREADS = 30
    MAX_DOWNLOAD_ATTEMPTS = 10
    NO_EXTRACT_THREADS = 30
    VERBOSITY = Verbosity.NORMAL
    INSPECTIONS_SUBDIR = 'prohlidky'
    DEFECTS_SUBDIR = 'zavady'
    ACTIONS_SUBDIR = 'ukony'
    ADR_TYPE_SUBDIR = 'adr_typy'
    NO_PARSE_THREADS = 8


    explain_verbosity(VERBOSITY)

    downloaded_inspection_dates = downloaded_dates([INSPECTIONS_DIR / 'gz', INSPECTIONS_DIR / 'xml', INSPECTIONS_DIR / 'parquet' / INSPECTIONS_SUBDIR])
    download_files(INSPECTIONS_DIR / 'gz', PARENT_DATASET_INSPECTIONS, START_DATE, END_DATE, downloaded_inspection_dates, NO_DOWNLOAD_THREADS, MAX_DOWNLOAD_ATTEMPTS, verbosity=VERBOSITY)
    extract_files(INSPECTIONS_DIR / 'gz', INSPECTIONS_DIR / 'xml', NO_EXTRACT_THREADS, verbosity=VERBOSITY)
    parse_inspections_to_parquet(INSPECTIONS_DIR, INSPECTIONS_SUBDIR, DEFECTS_SUBDIR, ACTIONS_SUBDIR, ADR_TYPE_SUBDIR, NO_PARSE_THREADS, VERBOSITY)

    downloaded_measurement_dates = downloaded_dates([MEASUREMENTS_DIR / 'gz', MEASUREMENTS_DIR / 'xml'])#, INSPECTIONS_DIR / 'parquet' / INSPECTIONS_SUBDIR])
    download_files(MEASUREMENTS_DIR / 'gz', PARENT_DATASET_MEASUREMENTS, START_DATE, END_DATE, downloaded_measurement_dates, NO_DOWNLOAD_THREADS, MAX_DOWNLOAD_ATTEMPTS, verbosity=VERBOSITY)
    extract_files(MEASUREMENTS_DIR / 'gz', MEASUREMENTS_DIR / 'xml', NO_EXTRACT_THREADS, verbosity=VERBOSITY)
