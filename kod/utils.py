from datetime import datetime
from enum import IntEnum
from pathlib import Path
import polars as pl


class Verbosity(IntEnum):
    QUIET = 0
    NORMAL = 1
    VERBOSE = 2  

def str_to_date(str):
    return datetime.strptime(str, '%d-%m-%Y')

def date_to_str(date):
    return date.strftime('%d-%m-%Y')


def create_directory(dir_name, verbosity):
    dir_path = Path(dir_name)
    if not dir_path.exists():
        dir_path.mkdir(parents=True, exist_ok=True)
        if verbosity > Verbosity.QUIET:
            print(f'Vytvořen adresář {dir_path}.')
    else:
        no_files_in_dir = sum(1 for p in dir_path.iterdir() if p.is_file())
        if verbosity > Verbosity.QUIET:
            print(f'Adresář "{dir_path}" již existuje, obsahuje {no_files_in_dir} souborů.')


def delete_path(path, verbosity):
    if path.is_dir():
        if verbosity > Verbosity.QUIET:
            print(f'\nMažu: "{path}".')
        path.rmdir()
    else:
        if verbosity > Verbosity.NORMAL:
            print(f'Mažu: "{path}".')
        path.unlink()


def date_from_file_path(file_path):
    return str_to_date(file_path.stem.split(' ')[-1])


def date_from_file_name(file_name):
    return str_to_date(file_name.split('.')[0].split(' ')[-1])


def pad_list_with_none(lst, length):
    return lst[:length] + [None] * max(0, length - len(lst))


# Iterátor vracící hodnoty ze seznamu, které podporují konverzi na float
def floats_sublist(original_list):
    for x in original_list:
        try:
            yield float(x)
        except (ValueError, TypeError):
            continue


# Pravidla pro přetypování sloupců v měření
def cast_mereni(df):
    bool_map = {'true': True, '1': True, 'false': False, '0': False}
    return df.with_columns([
        # Jednotlivé sloupce
        pl.col('DatumProhlidky').cast(pl.Date),
        pl.col('StaniceCislo').cast(pl.Int32),
        pl.col('Zahajeni').cast(pl.Datetime),
        pl.col('Ukonceni').cast(pl.Datetime),
        pl.col('OdpovednaOsoba').cast(pl.Int32),
        pl.col('Prohlidka_DatumProhlidky').cast(pl.Datetime),
        pl.col('Vozidlo_Odometer').cast(pl.Int32),
        pl.col('Vozidlo_RokVyroby').cast(pl.Int16),
        pl.col('Vysledek_VisualniKontrola').replace_strict(bool_map, return_dtype=pl.Boolean),
        pl.col('Vysledek_Readiness').replace_strict(bool_map, return_dtype=pl.Boolean),
        pl.col('Vysledek_RidiciJednotkaStav').cast(pl.Int8),
        pl.col('Vysledek_Mil').cast(pl.Int8),
        pl.col('Vysledek_TesnostPlynovehoZarizeni').replace_strict(bool_map, return_dtype=pl.Boolean),
        pl.col('Vysledek_Vyhovuje').replace_strict(bool_map, return_dtype=pl.Boolean),
        pl.col('PristiProhlidka').str.replace(r'T.*', '').cast(pl.Date),
        pl.col('EmisniSystem').cast(pl.Enum(['Nerizeny', 'Rizeny', 'Rizeny_Obd'])),
        pl.col('Obd_PocetDtc').cast(pl.Int32),
        pl.col('Obd_VzdalenostDtc').cast(pl.Int32),
        pl.col('Obd_CasDtc').cast(pl.Int32),
        pl.col('Obd_KontrolaMil').cast(pl.Int8),
        pl.col('Obd_Readiness_Vysledek').replace_strict(bool_map, return_dtype=pl.Boolean),
        
        # Skupiny sloupců
        pl.col('^.*(RucniZadani|Podporovano|Otestovano)$').replace_strict(bool_map, return_dtype=pl.Boolean),
        pl.col('^.*Hodnota$').str.strip_chars().cast(pl.Float32),
        pl.col('^.*Vysledek$').exclude('Obd_Readiness_Vysledek').cast(pl.Int8),
        pl.col('^.*Pritomno$').replace_strict(bool_map, return_dtype=pl.Boolean),
    ])


# Konverze času v sekundách na string v přirozeném formátu
def sec_to_hms(x, _):
    result = ''
    if x <= 0:
        x = -x
        result = result + '- '
    h = int(x // 3600)
    m = int((x % 3600) // 60)
    s = int(x % 60)
    if h > 0:
        return result + f"{h}h {m}m"
    if m > 0:
        return result + f"{m}m {s}s"
    return result + f"{s}s"
