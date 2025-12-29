from datetime import datetime
from enum import IntEnum
from pathlib import Path


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
        if verbosity > Verbosity.NORMAL:
            print(f'Mažu: "{path.name}".')
        path.rmdir()
    else:
        if verbosity > Verbosity.QUIET:
            print(f'Mažu: "{path.name}".')
        path.unlink()


def date_from_file_path(file_path):
    return str_to_date(file_path.stem.split(' ')[-1])


def date_from_file_name(file_name):
    return str_to_date(file_name.split('.')[0].split(' ')[-1])

