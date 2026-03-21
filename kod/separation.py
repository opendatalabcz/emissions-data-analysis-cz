import polars as pl

from utils import *
from schemas import *

# UDELAT VICEPROCESOVE, AZ BUDE FILTRACE VICE NEZ DIESELU
def split_measurements(mereni_dir, prohlidky_dir, diesel_dir, verbosity):
    # Vytvoření množin obsahujících požadované záznamy na základě datasetu prohlídek
    diesel_ids = []
    for file in prohlidky_dir.iterdir():
        df_diesel_personal_ids = (
            pl.read_parquet(file, schema=prohlidky_schema)
            .select(['Emise_CisloProtokolu', 'Vozidlo_Druh', 'Emise_ZakladniPalivo', 'Emise_AlternativniPalivo'])
            .filter(
                (pl.col('Vozidlo_Druh') == 'OSOBNÍ AUTOMOBIL') &
                (pl.col('Emise_ZakladniPalivo') == 'Nafta') &
                (pl.col('Emise_AlternativniPalivo').is_null())
            )
            .select(
                pl.col('Emise_CisloProtokolu')
                .str.replace(r'^CZ-(0+)(\d+)', r'CZ-${2}')
                .alias('CisloProtokolu') # Přejměnování, aby jméno odpovídalo datasetu měření
            )
        )
        diesel_ids.append(df_diesel_personal_ids)
    # Spojení id do jednoho dataframu
    filter_diesel_personal_df = pl.concat(diesel_ids).unique()
        
    # Filtrace v datasetu měření
    source_files = list(mereni_dir.iterdir())
    if verbosity > Verbosity.QUIET:
        print(f'Nalezeno {len(source_files)} souborů obsahující data o měření. Zahajuji jejich filtrování')

    create_directory(diesel_dir, verbosity)

    for file in source_files:
        # Přeskočení souboru, pokud už byl zpracován
        target_file = diesel_dir / file.name
        if skip_file(target_file, verbosity):
            continue

        # Sloupce obsahující testování typů OBD
        obd_zazeh_cols = [name for name in mereni_schema.keys() if 'Obd_Readiness_Zazeh' in name]
        obd_vznet_cols = [name for name in mereni_schema.keys() if 'Obd_Readiness_Vznet' in name]
        obd_j1939_cols = [name for name in mereni_schema.keys() if 'Obd_Readiness_J1939' in name]

        # Sloupce, které nesou význam pro naftová vozidla
        diesel_columns = nafta_schema.keys()
        # Vytvoření datasetu pro dieselová vozidla
        (
            pl.scan_parquet(file, schema=mereni_schema)
            .with_columns(
                pl.col('CisloProtokolu').str.replace(r'^CZ-(0+)(\d+)', r'CZ-${2}')
            )
            .join(filter_diesel_personal_df.lazy(), on='CisloProtokolu', how='semi')
            .with_columns([
                pl.any_horizontal(pl.col(obd_j1939_cols).is_not_null()).cast(pl.String).alias('Obd_Readiness_J1939_Pritomno'),
                pl.any_horizontal(pl.col(obd_zazeh_cols).is_not_null()).cast(pl.String).alias('Obd_Readiness_Zazeh_Pritomno')
            ])
            .select(list(diesel_columns))
            .collect()
            .write_parquet(target_file)
        )

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
    split_measurements(MEASUREMENTS_DIR / 'parquet' / MEASUREMENTS_ALL_SUBDIR, INSPECTIONS_DIR / 'parquet' / INSPECTIONS_SUBDIR, MEASUREMENTS_DIR / 'parquet' / DIESEL_SUBDIR, VERBOSITY)