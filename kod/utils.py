import warnings
from enum import IntEnum
from datetime import datetime
from pathlib import Path

import polars as pl
import pandas as pd
from IPython.display import display


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

#--------------------------------------------------------------------------------------------------------------

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


# Nahrazení seznamů počtem prvků
def get_short(df):
    list_cols = [name for name, dtype in df.schema.items() if isinstance(dtype, pl.List)]
    return df.with_columns(pl.col(list_cols).list.len())


# Zobrazení v pandas s nahrazenými seznamy počtem jejich prvků
def short_display(df, len=10):
    df_short = get_short(df).head(len)
    print(df.shape)
    display(df_short.to_pandas())


# Zobrazí počtů jednotlivých sloupců v pandas
def display_counts(df):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)
        height = len(df)
        counts = df.null_count().to_pandas().T.astype('Int64') * (-1) + height
        display(counts.astype(str) + f" / {height}")


# Odhad velikosti polars dataframu v paměti
def size_gb(df):
    return f'{df.estimated_size() / (1024**3):.3f} GB'