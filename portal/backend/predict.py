import polars as pl
import polars.selectors as cs
import json
from catboost import CatBoostClassifier, Pool
import config
import os

# Globální cache pro model a metadata, aby se nenačítaly při každém dotazu
_MODEL = None
_FEATURES = None
_CAT_FEATURES = None
_THRESHOLDS = None
_VIN_INDEX = None

def build_vin_index():
    global _VIN_INDEX
    print("Vytvářím index posledních úspěšných prohlídek pro všechna VIN v paměti (může to chvíli trvat)...", flush=True)
    MERENI_PATH = str(config.MEASUREMENTS_DIR / 'parquet' / '*.parquet')
    PROHLIDKY_PATH = str(config.INSPECTIONS_DIR / 'parquet' / '*.parquet')

    # Získání posledních úspěšných měření a přidání cest k souborům
    lf_m = pl.scan_parquet(MERENI_PATH, include_file_paths="mereni_file_path")
        
    lf_m = lf_m.filter(pl.col("Vysledek_Vyhovuje") == True)
    lf_m = lf_m.select(["Vozidlo_Vin", "CisloProtokolu", "DatumProhlidky", "mereni_file_path"]).drop_nulls("Vozidlo_Vin")

    # Získání souborů prohlídek
    lf_p = pl.scan_parquet(PROHLIDKY_PATH, include_file_paths="prohlidky_file_path")
    lf_p = lf_p.select(["CisloProtokolu", "prohlidky_file_path"])

    # Spojení a nalezení poslední prohlídky pro každé VIN
    lf_joined = lf_m.join(lf_p, on="CisloProtokolu", how="inner")
    
    _VIN_INDEX = (
        lf_joined
        .group_by("Vozidlo_Vin")
        .agg(pl.all().sort_by("DatumProhlidky", descending=True).first())
        .collect()
    )
    print("Index VIN kódů úspěšně vytvořen a uložen v paměti.", flush=True)

def init_model():
    global _MODEL, _FEATURES, _CAT_FEATURES, _THRESHOLDS, _VIN_INDEX

    PRECOMPUTED_DIR = config.DATA_DIR / 'precomputed'
    MODEL_PATH = str(PRECOMPUTED_DIR / 'model.bin')
    FEATURES_PATH = str(PRECOMPUTED_DIR / 'features.json')
    CAT_FEATURES_PATH = str(PRECOMPUTED_DIR / 'cat_features.json')
    THRESHOLDS_PATH = str(PRECOMPUTED_DIR / 'optimalizovane_prahy.csv')

    if not os.path.exists(MODEL_PATH):
        print("VAROVÁNÍ: Chybí předpočítané parametry modelů v adresáři data/precomputed.", flush=True)
        return

    print("Načítám CatBoost model a metadata do paměti...", flush=True)
    with open(FEATURES_PATH, 'r') as f:
        _FEATURES = json.load(f)
    with open(CAT_FEATURES_PATH, 'r') as f:
        _CAT_FEATURES = json.load(f)
    _THRESHOLDS = pl.read_csv(THRESHOLDS_PATH)
    
    _MODEL = CatBoostClassifier()
    _MODEL.load_model(MODEL_PATH)
    print("Model úspěšně načten.", flush=True)

def infer_vin(vin: str) -> dict:
    global _MODEL, _FEATURES, _CAT_FEATURES, _THRESHOLDS, _VIN_INDEX

    if _MODEL is None:
        return {"error": "Chybí předpočítané parametry modelů v adresáři data/precomputed."}

    if _VIN_INDEX is None:
        return {"error": "Index VIN kódů se právě vytváří. Zkuste to prosím za chvíli."}

    vin_normalized = vin.strip().upper()

    # 1. Bleskové prohledání indexu v paměti
    df_vin = _VIN_INDEX.filter(pl.col("Vozidlo_Vin") == vin_normalized)
    if df_vin.height == 0:
        return {"error": "Záznam pro zadané VIN neexistuje nebo vozidlo u poslední prohlídky nevyhovělo."}

    cislo_protokolu = df_vin["CisloProtokolu"][0]
    mereni_file = df_vin["mereni_file_path"][0]
    prohlidky_file = df_vin["prohlidky_file_path"][0]

    # 2. Načtení pouze specifických souborů z disku
    lf_latest = pl.scan_parquet(prohlidky_file).filter(pl.col("CisloProtokolu") == cislo_protokolu)
    lf_mereni = pl.scan_parquet(mereni_file).filter(pl.col("CisloProtokolu") == cislo_protokolu).rename({"DatumProhlidky": "DatumProhlidky_mereni"})

    lf_joined = lf_latest.join(
        lf_mereni,
        on="CisloProtokolu",
        how="inner"
    )

    if lf_joined.collect_schema()["DatumProhlidky"] in [pl.String, pl.Utf8]:
        lf_joined = lf_joined.with_columns(pl.col("DatumProhlidky").str.to_date(strict=False))
    if "Registrace_DatumPrvni" in lf_joined.collect_schema() and lf_joined.collect_schema()["Registrace_DatumPrvni"] in [pl.String, pl.Utf8]:
        lf_joined = lf_joined.with_columns(pl.col("Registrace_DatumPrvni").str.to_datetime(strict=False))

    lf_transformed = lf_joined.with_columns([
        pl.col("DatumProhlidky").dt.year().alias("_rok_prohlidky"),
        pl.col("Registrace_DatumPrvni").dt.year().alias("_rok_registrace")
    ]).with_columns([
        pl.lit(False).alias("AdministrativneOpraveno"),
        (pl.col("_rok_prohlidky") - pl.col("_rok_registrace")).cast(pl.Float32).alias("Stari_Vozidla_Let"),
        (pl.col("Prohlidka_Ukonceni") - pl.col("Prohlidka_Zahajeni")).dt.total_seconds().cast(pl.Float32).alias("Doba_Trvani_Prohlidky"),
        (pl.col("Emise_Ukonceni") - pl.col("Emise_Zahajeni")).dt.total_seconds().cast(pl.Float32).alias("Doba_Trvani_Emisi"),
        pl.col("Prohlidka_Stanice_Cislo").cast(pl.String)
    ]).with_columns(
        cs.list().list.len().fill_null(0)
    )

    df = lf_transformed.collect()

    if df.height == 0:
        return {"error": "Záznam pro zadané VIN neexistuje nebo byl odfiltrován."}

    datum_p = df["DatumProhlidky"][0]
    datum_m = df["DatumProhlidky_mereni"][0]
    if datum_p is None or datum_m is None or str(datum_p).split(" ")[0] != str(datum_m).split(" ")[0]:
        return {"error": "Datum prohlídky a měření se neshoduje nebo chybí."}

    kategorie = df["Vozidlo_Kategorie"][0] if "Vozidlo_Kategorie" in df.columns else None
    zakladni_palivo = df["Emise_ZakladniPalivo"][0] if "Emise_ZakladniPalivo" in df.columns else None
    alt_palivo = df["Emise_AlternativniPalivo"][0] if "Emise_AlternativniPalivo" in df.columns else None
    emisni_system = df["EmisniSystem"][0] if "EmisniSystem" in df.columns else None

    if kategorie != 'M1' or zakladni_palivo not in ['Benzín', 'Nafta'] or alt_palivo is not None:
        return {"error": "Predikce je k dispozici pouze pro osobní vozidla (kategorie M1) spalující výhradně benzín nebo naftu."}
        
    if emisni_system not in ['Rizeny', 'Rizeny_Obd']:
        return {"error": "Predikce je k dispozici pouze pro vozidla s řízeným emisním systémem."}

    make = df["Vozidlo_Znacka"][0] if "Vozidlo_Znacka" in df.columns else "Neznámá"
    model_name = df["Vozidlo_ObchodniOznaceni"][0] if "Vozidlo_ObchodniOznaceni" in df.columns else "Neznámý"
    first_reg = df["Registrace_DatumPrvni"][0] if "Registrace_DatumPrvni" in df.columns else None
    if first_reg is not None:
        first_reg = str(first_reg).split(" ")[0]

    active_cat_features = [c for c in _CAT_FEATURES if c in _FEATURES]
    num_features = [c for c in _FEATURES if c not in active_cat_features]

    missing_exprs = []
    for c in _FEATURES:
        if c not in df.columns:
            if c in active_cat_features:
                missing_exprs.append(pl.lit("missing").alias(c))
            else:
                missing_exprs.append(pl.lit(0.0).alias(c).cast(pl.Float32))

    if missing_exprs:
        df = df.with_columns(missing_exprs)

    df = df.with_columns([
        pl.col(c).cast(pl.Float32) for c in num_features if c in df.columns
    ]).with_columns([
        pl.col(c).cast(pl.String).fill_null("missing") for c in active_cat_features if c in df.columns
    ])

    df_pd = df.to_pandas()

    inference_pool = Pool(
        data=df_pd[_FEATURES],
        cat_features=active_cat_features
    )

    prob_fail = _MODEL.predict_proba(inference_pool)[0, 1]
    
    match = _THRESHOLDS.filter(
        (pl.col("Dolni_Prah_Prob") <= prob_fail) & 
        (pl.col("Horni_Prah_Prob") >= prob_fail)
    )

    if match.height > 0:
        skupina = match["Skupina"][0]
        chybovost = match["Skutecna_Chybovost"][0]
    else:
        skupina = _THRESHOLDS["Skupina"][-1]
        chybovost = _THRESHOLDS["Skutecna_Chybovost"][-1]

    return {
        "vin": vin_normalized,
        "make": make,
        "model": model_name,
        "last_inspection": str(datum_p).split(" ")[0] if datum_p is not None else None,
        "first_registration": first_reg,
        "Pravdepodobnost_Selhani": prob_fail,
        "Skupina": int(skupina),
        "Prumerna_Neuspesnost_Skupiny": float(chybovost)
    }