import polars as pl
import pandas as pd
import numpy as np
import json
from catboost import CatBoostClassifier, Pool
from tqdm import tqdm

MERENI_PATH = r"E:\CVUT_BAP\kod\data\processed\mereni_model_raw.parquet"
PROHLIDKY_PATH = r"E:\CVUT_BAP\kod\data\processed\prohlidky_model.parquet"
MODEL_PATH = "model.bin"
SPLITS_PATH = "data_splits.json"
FEATURES_PATH = "features.json"
CAT_FEATURES_PATH = "cat_features.json"
TARGET_COL = 'Vysledek_Pristi'

# Načtení metadat
with open(SPLITS_PATH, 'r') as f:
    test_ids = json.load(f).get('test_indices', [])
with open(FEATURES_PATH, 'r') as f:
    features = json.load(f)
with open(CAT_FEATURES_PATH, 'r') as f:
    cat_features = json.load(f)

active_cat_features = [c for c in cat_features if c in features]
num_features = [c for c in features if c not in active_cat_features]

print("Načítání a transformace dat přes Polars...")
lf_prohlidky = pl.scan_parquet(PROHLIDKY_PATH)
lf_mereni = pl.scan_parquet(MERENI_PATH)
df = lf_prohlidky.join(lf_mereni, on='CisloProtokolu', how='inner').collect()

# Aplikace identických transformací jako při trénování
df = df.with_columns([
    pl.col(c).cast(pl.Float32) for c in num_features if c in df.columns
]).with_columns([
    pl.col(c).cast(pl.String).fill_null("missing") for c in active_cat_features if c in df.columns
]).with_columns(
    (1 - pl.col(TARGET_COL)).cast(pl.Int8).alias(TARGET_COL)
)

print("Konverze na Pandas a filtrace testovací sady...")
df_pd = df.to_pandas()

# Filtrace pomocí původních pozičních indexů z test_indices
test_indices = [int(i) for i in test_ids]

# Zajištění platnosti indexů proti aktuální velikosti DataFrame
valid_indices = [i for i in test_indices if i < len(df_pd)]
df_test = df_pd.iloc[valid_indices].copy()

if len(df_test) == 0:
    print("CHYBA: Testovací sada je prázdná. Poziční indexy neodpovídají rozměrům sloučených dat.")
    exit()

print(f"Počet záznamů k inferenci: {len(df_test)}")

# Načtení modelu
model = CatBoostClassifier()
model.load_model(MODEL_PATH)

# Inference po blocích
chunk_size = 10000 
probabilities = []

for i in tqdm(range(0, len(df_test), chunk_size), desc="Inference"):
    chunk = df_test.iloc[i:i+chunk_size]
    
    inference_pool = Pool(
        data=chunk[features], 
        cat_features=active_cat_features
    )
    
    probs = model.predict_proba(inference_pool)[:, 1]
    probabilities.extend(probs)

df_test['prob_fail'] = probabilities

print("Výpočet kvantilových prahů a fail rate...")
df_test['category'], bins = pd.qcut(df_test['prob_fail'], q=5, retbins=True, labels=False, duplicates='drop')

summary = []
for i in range(len(bins) - 1):
    group = df_test[df_test['category'] == i]
    summary.append({
        "Category": i + 1,
        "Lower_Threshold": bins[i],
        "Upper_Threshold": bins[i+1],
        "Actual_Fail_Rate": group[TARGET_COL].mean(),
        "Count": len(group)
    })

results_df = pd.DataFrame(summary)
print("\n--- Výsledné kategorie rizika ---")
print(results_df.to_string(index=False))

with open('risk_thresholds.json', 'w') as f:
    json.dump({"categories": summary, "bin_edges": bins.tolist()}, f, indent=4)
print("\nExport dokončen: risk_thresholds.json")