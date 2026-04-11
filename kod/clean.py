import polars as pl

from utils import *

# Pravidla pro přetypování sloupců ve stanicích
def cast_stanice(df):
    bool_map = {'true': True, '1': True, 'false': False, '0': False}
    return df.with_columns([
        # Jednotlivé sloupce
        pl.col('Stanice_Cislo').str.replace_all(r'\s+', '').cast(pl.Int32),
        pl.col('Provozovatel_Ico').cast(pl.Int32),

        # Skupiny sloupců
        pl.col('^.*Pritomno$').str.to_lowercase().replace_strict(bool_map, return_dtype=pl.Boolean),
    ])


# Pravidla pro přetypování sloupců v prohlídkách
def cast_prohlidka(df):
    bool_map = {'True': True, 'true': True, '1': True, 'False': False, 'false': False, '0': False}
    return df.with_columns([
        # Jednotlivé sloupce
        pl.col('DatumProhlidky').cast(pl.Date),
        pl.col('Prohlidka_Stanice_Cislo').cast(pl.Int32),
        pl.col('Registrace_DatumPrvni').cast(pl.Datetime),
        pl.col('AdministrativniOprava_DatumProhlidky').cast(pl.Date),
        pl.col('Emise_DatumProhlidky').cast(pl.Datetime),
        pl.col('Emise_StaniceCislo').cast(pl.Int32),
        pl.col('Vysledek_Odometr').cast(pl.Int32),
        pl.col('Vysledek_DatumPristiProhlidky').str.replace(r'T.*', '').cast(pl.Date),
        pl.col('Vysledek_NalepkaVylepena').replace_strict(bool_map, return_dtype=pl.Boolean),
        pl.col('Vysledek_Celkovy').cast(pl.Int8),

        # Skupiny sloupců
        pl.col('^.*CisloProtokolu$').str.replace(r'^CZ-(0+)(\d+)', r'CZ-${2}'),
        pl.col('^.*OdpovednaOsoba$').cast(pl.Int32),
        pl.col('^.*(Zahajeni|Ukonceni)$').cast(pl.Datetime),
        pl.col('^.*Pritomno$').replace_strict(bool_map, return_dtype=pl.Boolean),
    ])


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
        pl.col('Vozidlo_DatumPrvniRegistrace').cast(pl.Date),
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
        pl.col('^.*CisloProtokolu$').str.replace(r'^CZ-(0+)(\d+)', r'CZ-${2}'),
        pl.col('^.*(RucniZadani|Podporovano|Otestovano)$').replace_strict(bool_map, return_dtype=pl.Boolean),
        pl.col('^.*Hodnota$').str.strip_chars().cast(pl.Float32),
        pl.col('^.*Vysledek$').exclude('Obd_Readiness_Vysledek').cast(pl.Int8),
        pl.col('^.*Pritomno$').replace_strict(bool_map, return_dtype=pl.Boolean), #TODO zkontrolovat
        pl.col('^.*PocetVyusteni$').cast(pl.Int8),
    ])



