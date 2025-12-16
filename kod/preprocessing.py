from concurrent.futures import ThreadPoolExecutor, as_completed
import gzip
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree # type: ignore <- pylance milně hlásí chybu
import requests

from utils import create_directory, Verbosity, to_date, date_from_file_name


def explain_verbosity(verbosity):
    """
    Vypíše do konzole popis symbolů používaných při NORMAL úrovni podrobnosti.

    Args:
        verbosity (Verbosity): Aktuální úroveň podrobnosti, která se má potenciélně vysvětlit.

    Returns:
        None: Funkce má vedlejší účinek (tisk na konzoli) a nevrací žádnou hodnotu.
    """
    if verbosity == Verbosity.NORMAL:
        print('"."\t- provedení operace se souborem\n"-"\t- přeskočení souboru\n n \t- číslo pokusu o provedení operace\n')

#--------------------------------------------------------------------------------------------------------------

def get_url_addresses(sparql_endpoint, parent_dateset_iri, start_date, end_date, verbosity):
    """
    Získá názvy a URL adresy pro stažení datových sad ze SPARQL koncového bodu a filtruje je podle data.

    Odesílá SPARQL dotaz, který vyhledá všechny členy série definované pomocí `parent_dateset_iri`. Výsledky jsou dále filtrovány na základě data extrahovaného z názvu datové sady, které musí spadat do rozmezí mezi `start_date` a `end_date`.

    Args:
        sparql_endpoint (str): URL adresa SPARQL koncového bodu pro dotazování.
        parent_dateset_iri (str): IRI nadřazené datové sady nebo série dat (dcat:seriesMember).
        start_date (str): Počáteční datum časového rozmezí ve formátu 'DD-MM-YYYY' (včetně).
        end_date (str): Koncové datum časového rozmezí ve formátu 'DD-MM-YYYY' (včetně).
        verbosity (Verbosity): Úroveň podrobnosti výstupu.

    Returns:
        tuple[list[str], list[str]]: Dvojice seznamů. První seznam obsahuje filtrované názvy datových sad (titles), druhý seznam obsahuje odpovídající filtrované URL adresy (download_urls).

    Raises:
        requests.exceptions.HTTPError: Pokud dojde k chybě HTTP během požadavku.
        requests.exceptions.RequestException: Obecná chyba požadavku, např. chyba připojení.
        ValueError: Pokud `start_date` nebo `end_date` nejsou ve správném formátu nebo datum v názvu souboru není platné (vyvoláno funkcemi `to_date` a `date_from_file_name`).
    """
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
        if to_date(start_date) <= date_from_file_name(title) <= to_date(end_date):
            titles.append(title)
            download_urls.append(download_url)
    if verbosity > Verbosity.NORMAL:
        print(f'Získáno {len(download_urls)} URL adres k datovým sadám.') 

    return titles, download_urls


def download_file(title, download_url, target_dir, max_attempts, verbosity):
    """
    Stáhne soubor z URL do cílového adresáře s možností opakování pokusu.

    Funkce nejprve zkontroluje, zda soubor s daným názvem (`title` s příponou `.xml.gz`) již v cílovém adresáři existuje. Pokud ne, pokusí se soubor stáhnout s využitím streamování a ověřování stavu HTTP. V případě selhání se pokus opakuje maximálně `max_attempts` krát, přičemž neúplně stažený soubor je smazán.

    Args:
        title (str): Název souboru, který bude použit pro uložení v lokálním systému (bez přípony .xml.gz).
        download_url (str): Přímá URL adresa ke stažení souboru.
        target_dir (str): Cesta k adresáři, kam má být soubor uložen.
        max_attempts (int): Maximální počet pokusů o stažení souboru v případě selhání.
        verbosity (Verbosity): Úroveň podrobnosti výstupu. Řídí, zda a jaké zprávy se tisknou do konzole

    Returns:
        None: Funkce má vedlejší účinek (stažení souboru na disk) a nevrací žádnou hodnotu.

    Raises:
        requests.exceptions.RequestException: Vyvolá se v případě, že se stažení nepodaří ani po vyčerpání maximálního počtu pokusů (`max_attempts`). Zahrnuje chyby HTTP a chyby připojení/timeout.
    """
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


def download_files(download_dir, parent_dataset_iri, start_date, end_date, no_threads, max_attempts, verbosity):
    """
    Řídí celý proces získání URL adres a paralelního stažení souborů datových sad.

    Vytvoří cílový adresář, získá filtrované seznamy názvů a URL adres datových sad pomocí SPARQL dotazu a následně spustí paralelní stahování těchto souborů pomocí vláken (ThreadPoolExecutor).

    Args:
        download_dir (str): Cesta k adresáři, kam budou stažené soubory uloženy.
        start_date (str): Počáteční datum časového rozmezí pro filtrování URL adres ve formátu 'DD-MM-YYYY' (včetně).
        end_date (str): Koncové datum časového rozmezí ve formátu 'DD-MM-YYYY' (včetně).
        no_threads (int): Maximální počet vláken pro souběžné stahování souborů.
        max_attempts (int): Maximální počet pokusů o stažení každého souboru v případě selhání.
        verbosity (Verbosity): Úroveň podrobnosti výstupu.

    Returns:
        None: Funkce řídí celý proces s vedlejšími účinky (vytváření adresářů, tisk na konzoli, stahování souborů) a nevrací žádnou hodnotu.

    Raises:
        requests.exceptions.RequestException: Vyvolá se v případě, že selže získání URL adres nebo stahování jednotlivého souboru po vyčerpání pokusů.
        ValueError: Pokud formát data není správný.
    """
    # Vytvoření adresáře pro uložení souborů
    create_directory(download_dir, verbosity)

    # Stažení URL adres souborů spolu s jejich názvy
    sparql_endpoint = 'https://data.gov.cz/sparql'
    titles, download_urls = get_url_addresses(sparql_endpoint, parent_dataset_iri, start_date, end_date, verbosity)

    # Paralelní stažení souborů
    if verbosity > verbosity.QUIET:
        print(f'Stahuji souběžně {len(download_urls)} souborů při použití {no_threads} vláken.')
    with ThreadPoolExecutor(max_workers=no_threads) as executor:
        wrapper = lambda title, download_url: download_file(title, download_url, download_dir, max_attempts, verbosity)
        results = executor.map(wrapper, titles, download_urls)
        list(results) # Vynucení počkání na dokončení stahování
    
    # Nová řádka pro vizuélní odlišení konce úkonu
    if verbosity > Verbosity.QUIET:
        print()

#--------------------------------------------------------------------------------------------------------------

def extract_file(file_name, source_dir, target_dir, verbosity):
    """
    Dekomprimuje soubor GZIP (`.gz`) do cílového adresáře a maže původní komprimovaný soubor.

    Funkce ověří, zda cílový soubor již existuje. Pokud ano, operace je přeskočena. V opačném případě dekomprimuje soubor GZIP ze `source_dir` do `target_dir` (odstraní příponu `.gz`) a po úspěšné extrakci smaže původní komprimovaný soubor.

    Args:
        file_name (str): Název souboru k extrakci (očekává se s příponou `.gz`).
        source_dir (str): Cesta ke zdrojovému adresáři, kde se nachází komprimovaný soubor.
        target_dir (str): Cesta k cílovému adresáři, kam má být soubor dekomprimován.
        verbosity (Verbosity): Úroveň podrobnosti výstupu. Řídí, zda a jaké zprávy se tisknou do konzole.

    Returns:
        None: Funkce má vedlejší účinky (vytvoření souboru, smazání souboru) a nevrací žádnou hodnotu.

    Raises:
        FileNotFoundError: Pokud komprimovaný soubor určený k extrakci neexistuje.
        IOError: V případě problémů se čtením nebo zápisem souboru.
        OSError: Během dekomprese nebo mazání souboru, pokud dojde k chybě.
    """
    # Definice cesty k souboru
    source_file_path = source_dir / file_name
    destination_file_path = (target_dir / file_name).with_suffix('')

    # Kontrola, zda vyextrahovaný soubor již existuje
    if destination_file_path.is_file():
        if verbosity > Verbosity.NORMAL:
            print(f'Přeskakuji soubor "{file_name}", již vyextrahován.')
        elif verbosity > Verbosity.QUIET:
            print('-', end='', flush=True)
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
    if verbosity > Verbosity.NORMAL:
        print(f'Mažu: "{file_name}".')
    source_file_path.unlink()


def extract_files(source_dir, target_dir, no_threads, verbosity):
    """
    Řídí souběžnou dekompresi všech GZIP souborů v daném adresáři.

    Vytvoří cílový adresář, zjistí seznam souborů ke zpracování ve `source_dir` a spustí paralelní extrakci těchto souborů do `target_dir` pomocí vláken (ThreadPoolExecutor).

    Args:
        source_dir (str): Cesta ke zdrojovému adresáři obsahujícímu soubory GZIP k extrakci.
        target_dir (str): Cesta k cílovému adresáři, kam budou dekomprimované soubory uloženy.
        no_threads (intoptional): Maximální počet vláken pro souběžnou extrakci souborů.
        verbosity (Verbosity): Úroveň podrobnosti výstupu.

    Returns:
        None: Funkce řídí proces s vedlejšími účinky (vytváření adresářů, tisk na konzoli, dekomprese souborů) a nevrací žádnou hodnotu.

    Raises:
        FileNotFoundError: Pokud se nepodaří najít soubor určený k extrakci.
        IOError: V případě problémů se čtením/zápisem souboru během dekomprese.
    """
    # Vytvoření adresáře pro uložení souborů
    create_directory(target_dir, verbosity)

    # Paralelní extrahování souborů
    extractable = list(source_dir.glob('*.gz'))
    if verbosity > verbosity.QUIET:
        print(f'Extrahuji souběžně {len(extractable)} souborů při použití {no_threads} vláken.')
    with ThreadPoolExecutor(max_workers=no_threads) as executor:
        wrapper = lambda file_path: extract_file(file_path.name, source_dir, target_dir, verbosity)
        results = executor.map(wrapper, extractable)
        list(results) # Vynuceni pockani na dokonceni extrahovani
    
    # Nová řádka pro vizuélní odlišení konce úkonu
    if verbosity > Verbosity.QUIET:
        print()

#--------------------------------------------------------------------------------------------------------------

def safe_get(element, xpath, namespaces):
    """
    Bezpečně získá textový obsah podřízeného uzlu XML na základě zadaného XPath.

    Tato pomocná funkce nejprve ověří, zda je vstupní `element` platný (není None). Poté se pokusí najít podřízený uzel pomocí XPath a jmenných prostorů. Pokud uzel existuje, vrátí jeho textový obsah; jinak vrátí None, čímž zabraňuje chybám při pokusu o přístup k atributu `text` neexistujícího uzlu.

    Args:
        element (ElementTree.Element or None): Rodičovský uzel XML/Element, ve kterém se má hledat. Může být None.
        xpath (str): Výraz XPath pro nalezení podřízeného uzlu.
        namespaces (dict): Slovník jmenných prostorů (namespace prefix: URI) pro správnou interpretaci XPath.

    Returns:
        str or None: Textový obsah nalezeného uzlu, nebo None, pokud je `element` None nebo uzel nebyl nalezen.
    """
    if element is None:
        return None
    node = element.find(xpath, namespaces)
    return node.text if node is not None else None


def safe_find(element, xpath, namespaces):
    if element is None:
        return None
    node = element.find(xpath, namespaces)
    return node if node is not None else None


def get_stanice(element, prefix, namespaces):
    """
    Získá údaje o lokalizaci stanice ('Cislo', 'Kraj', 'ORP', 'Obec') z daného XML elementu.

    Funkce využívá pomocnou funkci `safe_get` pro bezpečné získání textových hodnot podřízených uzlů. Vrací slovník, kde klíče jsou tvořeny dynamicky ze zadaného `prefixu` a názvu údaje.

    Args:
        element (ElementTree.Element or None): Rodičovský uzel XML, ze kterého se mají extrahovat údaje o stanici.
        prefix (str): Prefix použitý pro pojmenování klíčů ve výsledném slovníku
        namespaces (dict): Slovník jmenných prostorů pro správné hledání pomocí XPath.

    Returns:
        dict: Slovník s klíči ve formátu `{prefix}_Stanice_Cislo`, `{prefix}_Stanice_Kraj`, `{prefix}_Stanice_ORP` a `{prefix}_Stanice_Obec`. Hodnoty jsou buď textový řetězec nebo None.
    """
    return {
        f'{prefix}_Stanice_Cislo': safe_get(element, 'p:Cislo', namespaces),
        f'{prefix}_Stanice_Kraj': safe_get(element, 'p:Kraj', namespaces),
        f'{prefix}_Stanice_ORP': safe_get(element, 'p:ORP', namespaces),
        f'{prefix}_Stanice_Obec': safe_get(element, 'p:Obec', namespaces),
    }


def get_casove_udaje(element, prefix, namespaces):
    """
    Získá údaje o časovém rozmezí ('Zahajeni' a 'Ukonceni') z daného XML elementu.

    Funkce využívá pomocnou funkci `safe_get` pro bezpečné získání textových hodnot 
    podřízených uzlů 'Zahajeni' a 'Ukonceni' (předpokládá prefix 'p:'). 
    Vrací slovník, kde klíče jsou tvořeny dynamicky ze zadaného `prefixu` a názvu údaje.

    Args:
        element (ElementTree.Element or None): Rodičovský uzel XML, ze kterého se 
            mají extrahovat časové údaje.
        prefix (str): Prefix použitý pro pojmenování klíčů ve výsledném slovníku (např. 'Platnost' nebo 'Projekt').
        namespaces (dict): Slovník jmenných prostorů pro správné hledání pomocí XPath.

    Returns:
        dict: Slovník s klíči ve formátu `{prefix}_Zahajeni` a `{prefix}_Ukonceni`. 
            Hodnoty jsou buď textový řetězec (např. datum) nebo None.
    """
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
    for ukon_elem in element.findall('p:KontrolniUkon', namespaces):
        ukony.append({
            'CisloProtokolu': cislo_protokolu,
            'Typ': safe_get(ukon_elem, 'p:Typ', namespaces),
            'Nevyhovujici': safe_get(ukon_elem, 'p:Nevyhovujici', namespaces),
        })
    return ukony


def parse_adr_typ_seznam(element, cislo_protokolu, namespaces):
    if element is None:
        return []
    adr_typy = []
    for adr_typ_elem in element.findall('p:TypVozidlaADR', namespaces):
        adr_typy.append({
            'CisloProtokolu': cislo_protokolu,
            'TypVozidlaADR': adr_typ_elem.text,
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
    """
    Writes a list of dictionaries to a unique Parquet file.
    Raises exceptions on failure.
    """
    if not batch_data: return
    
    file_name = f"{file_stem}.parquet"
    
    # Zapsání souboru na disk v požadovaném formátů
    df = pd.DataFrame(batch_data)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, output_dir / file_name)


def parse_inspections_file(inspections_dir, defects_dir, actions_dir, adr_type_dir, xml_file, namespaces):
    """
    Reads one XML file, extracts all data, and writes the results to Parquet.
    This function is executed concurrently by each thread.
    If an error occurs, it will be raised and caught by the main executor.
    """
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
    write_batch(inspections_dir, prohlidky_batch, xml_file.stem)
    write_batch(defects_dir, zavady_batch, xml_file.stem)
    write_batch(actions_dir, ukony_batch, xml_file.stem)
    write_batch(adr_type_dir, adr_typy_batch, xml_file.stem)
    

def parse_to_parquet(source_dir, file_parser, no_threads, verbosity):
    """
    Initializes environment and runs the thread pool with fail-fast logic.
    If any thread raises an exception, all other threads are cancelled
    and the main script exits.
    """
    # Vyhledání souborů pro parsování
    xml_files = sorted(list(source_dir.glob('*.xml')), key=date_from_file_name)
    if not xml_files:
        raise FileNotFoundError(f'Žádně .xml souborů nebyly nalezeny v {source_dir}.')
    if verbosity > Verbosity.QUIET:
        print(f'Nalezeno {len(xml_files)} .xml souborů. Spouštím {no_threads} vláken.')

    # Samotně paralelní parsování souborů
    futures = []
    with ThreadPoolExecutor(max_workers=no_threads) as executor:
        # Poslání úkolů do poolu
        for i, xml_file in enumerate(xml_files):
            future = executor.submit(file_parser, xml_file)
            futures.append(future)

        # Process results as they complete
        for i, future in enumerate(as_completed(futures)):
            # .result() zpropaguje výjimku, pokud nastane uvnitř parsování
            future.result() 

            # Výpis průběhu parsování
            processed_count = i + 1
            print_progress = False
            if verbosity > Verbosity.QUIET:
                if processed_count % 10 == 0 or processed_count == len(xml_files):
                    print_progress = True
            elif verbosity > Verbosity.NORMAL:
                if processed_count % 100 == 0 or processed_count == len(xml_files):
                    print_progress = True
            if print_progress:
                print(f'{processed_count / len(xml_files) * 100:.2f} % souborů zpracováno')


def parse_inspections_to_parquet(source_dir, target_dir, inspections_subdir, defects_subdir, actions_subdir, adr_type_subdir, no_threads, verbosity):
    # Definice jmenných prostorů
    namespaces = {
    'p': 'istp:opendata:schemas:ProhlidkaSeznam:v1', 
    'd': 'istp:opendata:schemas:DatovaSada:v1'      
    }

    # Nastavení repozitárů
    inspections_dir = target_dir / inspections_subdir
    defects_dir = target_dir / defects_subdir
    actions_dir = target_dir / actions_subdir
    adr_type_dir = target_dir / adr_type_subdir
    create_directory(inspections_dir, verbosity)
    create_directory(defects_dir, verbosity)
    create_directory(actions_dir, verbosity)
    create_directory(adr_type_dir, verbosity)

    # Zavolání funcke pro provedení parsování
    inspections_parser = lambda xml_file: parse_inspections_file(inspections_dir, defects_dir, actions_dir, adr_type_dir, xml_file, namespaces)
    parse_to_parquet(source_dir, inspections_parser, no_threads, verbosity)


# def parse_measurements_to_parquet():


if __name__ == '__main__':
    # Definice konstant
    DOWNLOAD_DIR_INSPECTIONS = Path('kod/data/datove_sady_stk')
    PARENT_DATASET_INSPECTIONS = 'https://data.gov.cz/zdroj/datové-sady/66003008/9c95ebdba1dc7a2fbcfc5b6c07d25705'
    DOWNLOAD_DIR_MEASUREMENTS = Path('kod/data/data_z_mericich_pristroju')
    PARENT_DATASET_MEASUREMENTS = 'https://data.gov.cz/zdroj/datové-sady/66003008/e8e07fa264f3bd2179be03381ec324de'
    START_DATE = '01-01-2019'
    END_DATE = '31-12-2024'
    NO_DOWNLOAD_THREADS = 30
    MAX_DOWNLOAD_ATTEMPTS = 10
    NO_EXTRACT_THREADS = 30
    VERBOSITY = Verbosity.NORMAL
    PARQUET_DIR = Path('kod/data/parquet')
    INSPECTIONS_SUBDIR = 'prohlidky'
    DEFECTS_SUBDIR = 'zavady'
    ACTIONS_SUBDIR = 'ukony'
    ADR_TYPE_SUBDIR = 'adr_typy'
    NO_PARSE_THREADS = 8


    # explain_verbosity(VERBOSITY)
    # download_files(DOWNLOAD_DIR_INSPECTIONS, PARENT_DATASET_INSPECTIONS, START_DATE, END_DATE, NO_DOWNLOAD_THREADS, MAX_DOWNLOAD_ATTEMPTS, verbosity=VERBOSITY)
    # extract_files(DOWNLOAD_DIR_INSPECTIONS, DOWNLOAD_DIR_INSPECTIONS, NO_EXTRACT_THREADS, verbosity=VERBOSITY)
    # parse_inspections_to_parquet(DOWNLOAD_DIR_INSPECTIONS, PARQUET_DIR, INSPECTIONS_SUBDIR, DEFECTS_SUBDIR, ACTIONS_SUBDIR, ADR_TYPE_SUBDIR, NO_PARSE_THREADS, VERBOSITY)

    # download_files(DOWNLOAD_DIR_MEASUREMENTS, PARENT_DATASET_MEASUREMENTS, START_DATE, END_DATE, NO_DOWNLOAD_THREADS, MAX_DOWNLOAD_ATTEMPTS, verbosity=VERBOSITY)
    # extract_files(DOWNLOAD_DIR_MEASUREMENTS, DOWNLOAD_DIR_MEASUREMENTS, NO_EXTRACT_THREADS, verbosity=VERBOSITY)
    # parse_to_parquet(DOWNLOAD_DIR_INSPECTIONS, PARQUET_DIR, INSPECTIONS_SUBDIR, DEFECTS_SUBDIR, ACTIONS_SUBDIR, ADR_TYPE_SUBDIR, NO_PARSE_THREADS, VERBOSITY)
