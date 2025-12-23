from datetime import datetime
from enum import IntEnum
from pathlib import Path


class Verbosity(IntEnum):
    """
    Definuje standardizované úrovně podrobnosti (verbosity) pro výstupy funkcí.

    Tato výčtová třída (IntEnum) používá celočíselné hodnoty pro porovnávání. Vyšší hodnota znamená podrobnější výstup.

    Atributes:
        QUIET (int): 0. Potlačuje všechny výstupy.
        NORMAL (int): 1. Zahrnuje základní informace o průběhu (např. spuštění hlavní operace).
        VERBOSE (int): 2. Zahrnuje detailní informace.
    """
    QUIET = 0
    NORMAL = 1
    VERBOSE = 2  

def str_to_date(str):
    """
    Převede textový řetězec data ve formátu 'DD-MM-YYYY' na objekt datetime.

    Args:
        date_str (str): Textový řetězec data k převodu (např. '31-12-2025').

    Returns:
        datetime: Výsledný objekt data a času (datetime).

    Raises:
        ValueError: Pokud vstupní řetězec neodpovídá formátu 'DD-MM-YYYY' nebo představuje neplatné datum (např. '30-02-2025').
    """
    return datetime.strptime(str, '%d-%m-%Y')

def date_to_str(date):
    


def create_directory(dir_name, verbosity=Verbosity.QUIET):
    """
    Vytvoří adresář, pokud neexistuje, nebo vypíše jeho stav, pokud již existuje.

    Funkce zkontroluje, zda daný adresář existuje. Pokud neexistuje, vytvoří jej včetně všech potřebných nadřazených adresářů. 
    Pokud adresář již existuje, vypíše počet souborů, které se v něm nacházejí.

    Args:
        dir_name (str | pathlib.Path): Název nebo cesta k adresáři, který má být vytvořen nebo zkontrolován.
        verbosity (Verbosity, optional): Úroveň podrobnosti výstupu. Řídí, zda a jaké zprávy se tisknou do konzole. Výchozí hodnota je Verbosity.QUIET.

    Returns:
        None: Funkce má vedlejší účinky (vytvoření adresáře nebo výpis stavu) a nevrací žádnou hodnotu.
    """
    dir_path = Path(dir_name)
    if not dir_path.exists():
        dir_path.mkdir(parents=True, exist_ok=True)
        if verbosity > Verbosity.QUIET:
            print(f'Vytvořen adresář {dir_path}.')
    else:
        no_files_in_dir = sum(1 for p in dir_path.iterdir() if p.is_file())
        if verbosity > Verbosity.QUIET:
            print(f'Adresář "{dir_path}" již existuje, obsahuje {no_files_in_dir} souborů.')


def date_from_file_name(file_name):
    """
    Extrhuje a převede část názvu souboru na objekt datetime.

    Funkce předpokládá, že datum je posledním slovem v názvu souboru (před první tečkou a příponou) a je ve formátu akceptovaném funkcí `to_date`. Výsledek převodu se vrací.
    
    Args:
        file_name (str | pathlib.Path): Název souboru nebo cesta k souboru, ze kterého se extrahuje datum.

    Returns:
        datetime: Objekt datetime získaný z názvu souboru.

    Raises:
        ValueError: Pokud extrahovaný řetězec data neodpovídá formátu očekávanému funkcí `to_date` (vyvoláno funkcí `to_date`).
    """
    return str_to_date(str(file_name).split('.')[0].split(' ')[-1])