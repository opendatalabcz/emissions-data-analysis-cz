import polars as pl

from utils import *

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
        pl.col('^.*Pritomno$').replace_strict(bool_map, return_dtype=pl.Boolean),
        pl.col('^.*PocetVyusteni$').cast(pl.Int8),
    ])


# Pravidla pro přetypování sloupců v prohlídkách
def cast_prohlidka(df):
    bool_map = {'true': True, '1': True, 'false': False, '0': False}
    return df.with_columns([
        # Jednotlivé sloupce
        pl.col('DatumProhlidky').cast(pl.Date),
        pl.col('RozsahProhlidky').cast(pl.Enum(['Plný', 'Částečný'])),
        pl.col('Prohlidka_Stanice_Cislo').cast(pl.Int32),
        pl.col('Registrace_DatumPrvni').cast(pl.Datetime),
        pl.col('AdministrativniOprava_DatumProhlidky').cast(pl.Date),
        pl.col('Emise_DatumProhlidky').cast(pl.Datetime),
        pl.col('Emise_StaniceCislo').cast(pl.Int32),
        pl.col('Emise_EmisniSystem').cast(pl.Enum(['Řízený s OBD', 'Řízený bez OBD', 'Neřízený'])),
        pl.col('Adr_Platnost_Periodicka').cast(pl.Datetime),
        pl.col('Adr_Platnost_Meziperiodicka').cast(pl.Datetime),
        pl.col('Vysledek_Odometr').cast(pl.Int32),
        pl.col('Vysledek_DatumPristiProhlidky').str.replace(r'T.*', '').cast(pl.Date),
        pl.col('Vysledek_NalepkaVylepena').replace_strict(bool_map, return_dtype=pl.Boolean),
        pl.col('Vysledek_Celkovy').cast(pl.Int8),

        # Skupiny sloupců
        pl.col('^.*CisloProtokolu$').str.replace(r'^CZ-(0+)(\d+)', r'CZ-${2}'),
        pl.col('^.*OdpovednaOsoba$').cast(pl.Int32),
        pl.col('^.*(Zahajeni|Ukonceni)$').cast(pl.Datetime),
    ])


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


# Odstranění duplicitních ID - Čísel protokolu pro různýmy názvy
def remove_duplicate_cislo_protokolu(df, prefix=None):
    print(f'Délka datasetu před odstraněním duplicitních řádků: {df.height}')

    df = df.unique()
    print(f'Délka datasetu po odstranění duplicitních řádků: {df.height}')

    # Nalezení řádků, které nejsou duplicitní, ale mají shodná id 
    prefix_dash = f'{prefix}_' if prefix is not None else ''
    id = f'{prefix_dash}CisloProtokolu'
    df_duplicit_id = df.filter(pl.col(id).count().over(id) > 1)
    print(f'Neduplicitní řádky se shodným {id}:')
    short_display(df_duplicit_id)
    # Odstranění nalezených řádků v případě, že panuje neshoda mezi id a číslem stanice
    df = df.filter(
        (~pl.col(id).is_in(set(df_duplicit_id[id]))) |
        (pl.col(id).str.split('-').list.get(1).cast(pl.Int32) == pl.col(f'{prefix_dash}StaniceCislo'))
    )
    # Ponechání pouze prvního z id, pokud nějaká duplicitní stále existují
    df = df.unique(subset=id)
    print(f'Délka datasetu po odstranění duplicitních {id}: {df.height}')

    return df


# Převedení prohlídky do stavu relevantního pro další analýzu
def transform_prohlidka(df):
    # ID na základě bude prohlídka párována
    df = df.filter(pl.col('Emise_CisloProtokolu').is_not_null())
    # Odstranění prázdných sloupců a sloupců, které by měly konstantní hodnotu, protože na základě nich byla měření filtrována
    adr_cols = [col for col in df.columns if 'Adr' in col]
    df = df.drop(adr_cols + ['Tsk_OdpovednaOsoba', 'Vozidlo_Druh', 'Emise_ZakladniPalivo', 'Emise_AlternativniPalivo'])
    # Převedení sloupců týkajících se technické prohlídky na indikátor přítomnosti z důvodu řídkého výskytu
    technicka_cols = ['RozsahProhlidky', 'Technicka_Zahajeni', 'Technicka_Ukonceni', 'Technicka_OdpovednaOsoba', 'Vysledek_NalepkaVylepena']
    df = df.with_columns(pl.any_horizontal(pl.col(technicka_cols).is_not_null()).alias('Technicka_Pritomo')).drop(technicka_cols)
    return df


# Převedení stanice do stavu relevantního pro další analýzu
def transform_stanice(df):
    # Odstranění proměnných týkajících se osvědční pro jiné úkony než měření emisí s výjimkou indikátorů
    kategorie_cols = ['Stk_Osvedceni_Kategorie', 'SmeZkusebni_Osvedceni_Kategorie', 'Adr_Osvedceni_Kategorie']
    df = df.drop(kategorie_cols)
    # Odstranění příliš detailních hodnot o provozovateli spolu s převedením částečně vyplněných sloupců na indikátory (provozovatel, co o sobě skrývá informace může být zajímavý pro analýzu)
    provozovatel_indicator = ['Provozovatel_Kontakt_Telefon', 'Provozovatel_Kontakt_Email']
    provozovatel_detailed = ['Provozovatel_Adresa_Ulice', 'Provozovatel_Adresa_Psc', 'Provozovatel_Ico']
    df = df.drop(provozovatel_detailed).with_columns(pl.col(provozovatel_indicator).is_not_null())
    # Odstranění příliš detaliních hodnot o stanici
    stanice_detailed = ['Stanice_Adresa_Ulice', 'Stanice_Adresa_Psc', 'Stanice_Kontakt_Telefon', 'Stanice_Kontakt_Email']
    df = df.drop(stanice_detailed)
    return df


