import polars as pl

from utils import *

# Pravidla pro přetypování sloupců ve stanicích
def cast_stanice(df):
    bool_map = {'true': True, '1': True, 'false': False, '0': False}
    return df.with_columns([
        # Jednotlivé sloupce
        pl.col('Stanice_Cislo').cast(pl.String).str.replace_all(r'\s+', '').cast(pl.Int32, strict=False),
        pl.col('Provozovatel_Ico').cast(pl.Int32, strict=False),

        # Skupiny sloupců
        pl.col('^.*Pritomno$').cast(pl.String).str.to_lowercase().replace_strict(bool_map, default=None, return_dtype=pl.Boolean),
    ])


# Pravidla pro přetypování sloupců v prohlídkách
def cast_prohlidka(df):
    bool_map = {'True': True, 'true': True, '1': True, 'False': False, 'false': False, '0': False}
    return df.with_columns([
        # Jednotlivé sloupce
        pl.col('DatumProhlidky').cast(pl.Date, strict=False),
        pl.col('Prohlidka_Stanice_Cislo').cast(pl.Int32, strict=False),
        pl.col('Registrace_DatumPrvni').cast(pl.Datetime, strict=False),
        pl.col('AdministrativniOprava_DatumProhlidky').cast(pl.Date, strict=False),
        pl.col('Emise_DatumProhlidky').cast(pl.Datetime, strict=False),
        pl.col('Emise_StaniceCislo').cast(pl.Int32, strict=False),
        pl.col('Vysledek_Odometr').cast(pl.Int32, strict=False),
        pl.col('Vysledek_DatumPristiProhlidky').cast(pl.String).str.replace(r'T.*', '').cast(pl.Date, strict=False),
        pl.col('Vysledek_NalepkaVylepena').cast(pl.String).replace_strict(bool_map, default=None, return_dtype=pl.Boolean),
        pl.col('Vysledek_Celkovy').cast(pl.Int8, strict=False),

        # Skupiny sloupců
        pl.col('^.*CisloProtokolu$').cast(pl.String).str.replace(r'^CZ-(0+)(\d+)', r'CZ-${2}'),
        pl.col('^.*OdpovednaOsoba$').cast(pl.Int32, strict=False),
        pl.col('^.*(Zahajeni|Ukonceni)$').cast(pl.Datetime, strict=False),
        pl.col('^.*Pritomno$').cast(pl.String).replace_strict(bool_map, default=None, return_dtype=pl.Boolean),
    ])


# Pravidla pro přetypování sloupců v měření
def cast_mereni(df):
    bool_map = {'true': True, '1': True, 'false': False, '0': False}
    return df.with_columns([
        # Jednotlivé sloupce
        pl.col('DatumProhlidky').cast(pl.Date, strict=False),
        pl.col('StaniceCislo').cast(pl.Int32, strict=False),
        pl.col('Zahajeni').cast(pl.Datetime, strict=False),
        pl.col('Ukonceni').cast(pl.Datetime, strict=False),
        pl.col('OdpovednaOsoba').cast(pl.Int32, strict=False),
        pl.col('Prohlidka_DatumProhlidky').cast(pl.Datetime, strict=False),
        pl.col('Vozidlo_Odometer').cast(pl.Int32, strict=False),
        pl.col('Vozidlo_RokVyroby').cast(pl.Int16, strict=False),
        pl.col('Vozidlo_DatumPrvniRegistrace').cast(pl.Date, strict=False),
        pl.col('Vysledek_VisualniKontrola').cast(pl.String).replace_strict(bool_map, default=None, return_dtype=pl.Boolean),
        pl.col('Vysledek_Readiness').cast(pl.String).replace_strict(bool_map, default=None, return_dtype=pl.Boolean),
        pl.col('Vysledek_RidiciJednotkaStav').cast(pl.Int8, strict=False),
        pl.col('Vysledek_Mil').cast(pl.Int8, strict=False),
        pl.col('Vysledek_TesnostPlynovehoZarizeni').cast(pl.String).replace_strict(bool_map, default=None, return_dtype=pl.Boolean),
        pl.col('Vysledek_Vyhovuje').cast(pl.String).replace_strict(bool_map, default=None, return_dtype=pl.Boolean),
        pl.col('PristiProhlidka').cast(pl.String).str.replace(r'T.*', '').cast(pl.Date, strict=False),
        pl.col('EmisniSystem').cast(pl.Enum(['Nerizeny', 'Rizeny', 'Rizeny_Obd']), strict=False),
        pl.col('Obd_PocetDtc').cast(pl.Int32, strict=False),
        pl.col('Obd_VzdalenostDtc').cast(pl.Int32, strict=False),
        pl.col('Obd_CasDtc').cast(pl.Int32, strict=False),
        pl.col('Obd_KontrolaMil').cast(pl.Int8, strict=False),
        pl.col('Obd_Readiness_Vysledek').cast(pl.String).replace_strict(bool_map, default=None, return_dtype=pl.Boolean),
        
        # Skupiny sloupců
        pl.col('^.*CisloProtokolu$').cast(pl.String).str.replace(r'^CZ-(0+)(\d+)', r'CZ-${2}'),
        pl.col('^.*(RucniZadani|Podporovano|Otestovano)$').cast(pl.String).replace_strict(bool_map, default=None, return_dtype=pl.Boolean),
        pl.col('^.*Hodnota$').cast(pl.String).str.strip_chars().cast(pl.Float32, strict=False),
        pl.col('^.*Vysledek$').exclude('Obd_Readiness_Vysledek').cast(pl.Int8, strict=False),
        pl.col('^.*Pritomno$').cast(pl.String).replace_strict(bool_map, default=None, return_dtype=pl.Boolean), 
        pl.col('^.*PocetVyusteni$').cast(pl.Int8, strict=False),
    ])
