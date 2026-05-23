import polars as pl
import polars.selectors as cs
import matplotlib
# Nutné pro běh matplotlibu bez vizuálního grafického rozhraní (headless v Dockeru)
matplotlib.use('Agg')
from pathlib import Path
import numpy as np
import json

from visualisation_utils import time_series_monthly_expr, distribution_density_plot


def generate_all_graphs():
    print("Zahajuji generování grafů do /app/public/graphs...")
    graphs_dir = Path("/app/public/graphs")
    graphs_dir.mkdir(parents=True, exist_ok=True)
    
    # Cesty v dockeru
    prohlidky_path = "/app/data/extracted/prohlidky_vozidel_stk_a_sme/parquet/*.parquet"
    mereni_path = "/app/data/extracted/data_z_mericich_pristroju/parquet/*.parquet"
    
    lf_prohlidky = pl.scan_parquet(prohlidky_path)
    lf_mereni = pl.scan_parquet(mereni_path)
    
    # Stažení menší podmnožiny dat potřebné pro základní grafy
    df_selected = lf_mereni.select([
        'CisloProtokolu', 'DatumProhlidky', 'Vysledek_Vyhovuje', 
        'StaniceCislo', 'Zahajeni', 'Ukonceni'
    ]).collect()
    
    if df_selected.schema["DatumProhlidky"] in [pl.String, pl.Utf8]:
        df_selected = df_selected.with_columns(pl.col("DatumProhlidky").str.to_datetime(strict=False))

    max_date = df_selected.select(pl.col("DatumProhlidky").max()).item()
    global_max_year = max_date.year
    global_max_month = max_date.month

    df_selected = df_selected.filter(
        ~((pl.col("DatumProhlidky").dt.year() == global_max_year) & (pl.col("DatumProhlidky").dt.month() == global_max_month))
    )

    # --- 1. Vývoj celkové průchodnosti ---
    expr_overall = (pl.col("Vysledek_Vyhovuje").is_null().sum() / pl.len()).alias("Celkový podíl nevyhovujících vozidel")

    time_series_monthly_expr(
        df_selected, 'DatumProhlidky', [expr_overall], 
        'Vývoj podílu vozidel, která měření absolvují neúspěšně', 
        'Podíl z měsíčních prohlídek',
        save_path=str(graphs_dir / 'vyvoj_pruchodnosti.svg'), 
        decimals=2
    )
    
    # --- 2. Délka měření ---
    df_duration = df_selected.filter(
        pl.col("Zahajeni").is_not_null() & 
        pl.col("Ukonceni").is_not_null() &
        pl.col("Vysledek_Vyhovuje").is_not_null()
    ).with_columns(
        pl.col("Zahajeni").dt.truncate("1mo").alias("_ts_month"),
        ((pl.col("Ukonceni") - pl.col("Zahajeni")).dt.total_seconds() / 60.0).alias("delka_mereni_min")
    ).filter(
        (pl.col("delka_mereni_min") > 0) & 
        (pl.col("delka_mereni_min") < 180)
    )

    df_quantiles_duration = df_duration.group_by("_ts_month").agg([
        pl.col("delka_mereni_min").median().alias("50. percentil (Medián)"),
        pl.col("delka_mereni_min").quantile(0.10).alias("10. percentil (10 % nejkratších)"),
        pl.col("delka_mereni_min").quantile(0.01).alias("1. percentil (1 % nejkratších)"),
        pl.col("delka_mereni_min").quantile(0.001).alias("0,1. percentil (0,1 % nejkratších)"),
    ])

    time_series_monthly_expr(
        df=df_quantiles_duration, time_col="_ts_month", exprs=None,                
        title="Rozložení délky měření emisí v čase (kvantily)", y_title="Délka měření (minuty)", decimals=0,
        save_path=str(graphs_dir / 'delka_prohlidky.svg')
    )
    
    del df_selected

    # --- 3. Filtrování a normalizace (Anomálie, Otáčky atd.) ---
    lf_uzsi_prohlidky = lf_prohlidky.filter(
        (pl.col('Vozidlo_Kategorie') == 'M1') & 
        pl.col('Emise_ZakladniPalivo').is_in(['Benzín', 'Nafta']) & 
        pl.col('Emise_AlternativniPalivo').is_null()
    ).select('CisloProtokolu')

    required_benzin = ["Benzin_OtackyVolnobezne_CO_Hodnota", "Benzin_OtackyVolnobezne_CO_Max_Hodnota", "Benzin_OtackyVolnobezne_N_Hodnota", "Benzin_OtackyVolnobezne_N_Min_Hodnota", "Benzin_OtackyVolnobezne_N_Max_Hodnota", "Benzin_OtackyZvysene_LAMBDA_Hodnota", "Benzin_OtackyZvysene_LAMBDA_Min_Hodnota", "Benzin_OtackyZvysene_LAMBDA_Max_Hodnota", "Benzin_OtackyZvysene_CO_Hodnota", "Benzin_OtackyZvysene_CO_Max_Hodnota", "Benzin_OtackyZvysene_N_Hodnota", "Benzin_OtackyZvysene_N_Min_Hodnota", "Benzin_OtackyZvysene_N_Max_Hodnota"]
    required_nafta = ["Nafta_MereniPrumer_CasAkcelerace_Hodnota", "Nafta_MereniPrumer_Kourivost_Hodnota", "Nafta_MereniPrumer_OtackyVolnobezne_Hodnota", "Nafta_MereniPrumer_OtackyPrebehove_Hodnota", "Nafta_MereniVznetLimit_CasAkcelerace_Max_Hodnota", "Nafta_MereniVznetLimit_Kourivost_Max_Hodnota", "Nafta_MereniVznetLimit_OtackyVolnobezne_Max_Hodnota", "Nafta_MereniVznetLimit_OtackyPrebehove_Max_Hodnota", "Nafta_MereniVznetLimit_OtackyVolnobezne_Min_Hodnota", "Nafta_MereniVznetLimit_OtackyPrebehove_Min_Hodnota"]
    benzin_mask = pl.all_horizontal(pl.col(required_benzin).is_not_null())
    nafta_mask = pl.all_horizontal(pl.col(required_nafta).is_not_null())

    limits_bounds_benzin = [('Benzin_OtackyVolnobezne_CO_Max_Hodnota', 0.05, 5.0), ('Benzin_OtackyVolnobezne_N_Min_Hodnota', 300, 3000), ('Benzin_OtackyVolnobezne_N_Max_Hodnota', 300, 3000), ('Benzin_OtackyZvysene_CO_Max_Hodnota', 0.01, 1.0), ('Benzin_OtackyZvysene_LAMBDA_Min_Hodnota', 0.9, 1.00), ('Benzin_OtackyZvysene_LAMBDA_Max_Hodnota', 1.00, 1.1), ('Benzin_OtackyZvysene_N_Min_Hodnota', 1000, 10000), ('Benzin_OtackyZvysene_N_Max_Hodnota', 1000, 10000)]
    limits_bounds_nafta = [('Nafta_MereniVznetLimit_CasAkcelerace_Max_Hodnota', 0.1, 10.0), ('Nafta_MereniVznetLimit_Kourivost_Max_Hodnota', 0.01, 3.0), ('Nafta_MereniVznetLimit_OtackyVolnobezne_Min_Hodnota', 300, 3000), ('Nafta_MereniVznetLimit_OtackyVolnobezne_Max_Hodnota', 300, 3000), ('Nafta_MereniVznetLimit_OtackyPrebehove_Min_Hodnota', 1000, 10000), ('Nafta_MereniVznetLimit_OtackyPrebehove_Max_Hodnota', 1000, 10000)]
    check_limits_integrity = [("Benzin_OtackyVolnobezne_N_Min_Hodnota", "Benzin_OtackyVolnobezne_N_Max_Hodnota"), ("Benzin_OtackyZvysene_LAMBDA_Min_Hodnota", "Benzin_OtackyZvysene_LAMBDA_Max_Hodnota"), ("Benzin_OtackyZvysene_N_Min_Hodnota", "Benzin_OtackyZvysene_N_Max_Hodnota"), ("Nafta_MereniVznetLimit_OtackyVolnobezne_Min_Hodnota", "Nafta_MereniVznetLimit_OtackyVolnobezne_Max_Hodnota"), ("Nafta_MereniVznetLimit_OtackyPrebehove_Min_Hodnota", "Nafta_MereniVznetLimit_OtackyPrebehove_Max_Hodnota")]

    lf_mereni_base = lf_mereni.select(
        ['CisloProtokolu', 'EmisniSystem', 'Vysledek_Vyhovuje', 'DatumProhlidky'] + required_benzin + required_nafta
    ).filter(
        pl.col('EmisniSystem').is_in(['Rizeny', 'Rizeny_Obd']),
        pl.col('Vysledek_Vyhovuje').is_not_null()
    )

    df_valid = lf_mereni_base.join(lf_uzsi_prohlidky, on='CisloProtokolu', how='inner').filter(
        pl.any_horizontal(~(cs.numeric() < 0)),
        benzin_mask | nafta_mask,
        *[(pl.col(col).is_between(low, high) | nafta_mask) for col, low, high in limits_bounds_benzin],
        *[(pl.col(col).is_between(low, high) | benzin_mask) for col, low, high in limits_bounds_nafta],
        *[(pl.col(min_col) < pl.col(max_col)).fill_null(True) for min_col, max_col in check_limits_integrity],
    ).collect()
    
    if df_valid.schema["DatumProhlidky"] in [pl.String, pl.Utf8]:
        df_valid = df_valid.with_columns(pl.col("DatumProhlidky").str.to_datetime(strict=False))

    df_valid = df_valid.filter(
        ~((pl.col("DatumProhlidky").dt.year() == global_max_year) & (pl.col("DatumProhlidky").dt.month() == global_max_month))
    )

    all_mappings = [
        ("Benzin_OtackyVolnobezne_CO_Hodnota", None, "Benzin_OtackyVolnobezne_CO_Max_Hodnota"),
        ("Benzin_OtackyVolnobezne_N_Hodnota", "Benzin_OtackyVolnobezne_N_Min_Hodnota", "Benzin_OtackyVolnobezne_N_Max_Hodnota"),
        ("Benzin_OtackyZvysene_LAMBDA_Hodnota", "Benzin_OtackyZvysene_LAMBDA_Min_Hodnota", "Benzin_OtackyZvysene_LAMBDA_Max_Hodnota"),
        ("Benzin_OtackyZvysene_CO_Hodnota", None, "Benzin_OtackyZvysene_CO_Max_Hodnota"),
        ("Benzin_OtackyZvysene_N_Hodnota", "Benzin_OtackyZvysene_N_Min_Hodnota", "Benzin_OtackyZvysene_N_Max_Hodnota"),
        ("Nafta_MereniPrumer_CasAkcelerace_Hodnota", None, "Nafta_MereniVznetLimit_CasAkcelerace_Max_Hodnota"),
        ("Nafta_MereniPrumer_Kourivost_Hodnota", None, "Nafta_MereniVznetLimit_Kourivost_Max_Hodnota"),
        ("Nafta_MereniPrumer_OtackyVolnobezne_Hodnota", "Nafta_MereniVznetLimit_OtackyVolnobezne_Min_Hodnota", "Nafta_MereniVznetLimit_OtackyVolnobezne_Max_Hodnota"),
        ("Nafta_MereniPrumer_OtackyPrebehove_Hodnota", "Nafta_MereniVznetLimit_OtackyPrebehove_Min_Hodnota", "Nafta_MereniVznetLimit_OtackyPrebehove_Max_Hodnota")
    ]

    norm_exprs = [
        ((pl.col(val_col) - (pl.col(min_col) if min_col else pl.lit(0))) / 
         (pl.col(max_col) - (pl.col(min_col) if min_col else pl.lit(0)))
        ).alias(f"{val_col}_Norm")
        for val_col, min_col, max_col in all_mappings
    ]

    df_valid = df_valid.with_columns(norm_exprs).drop(required_benzin + required_nafta)
    df_benzin = df_valid.select(['DatumProhlidky'] + [f"{m[0]}_Norm" for m in all_mappings if m[0].startswith('Benzin')]).drop_nulls()
    df_nafta = df_valid.select(['DatumProhlidky'] + [f"{m[0]}_Norm" for m in all_mappings if m[0].startswith('Nafta')]).drop_nulls()
    df_base = pl.concat([df_benzin, df_nafta], how="diagonal")

    epsilon = 0.01
    def get_suspect_expr(col):
        clean_name = (
            col.replace('Benzin_OtackyVolnobezne_N_Hodnota_Norm', 'Benzín - otáčky volnoběžné')
               .replace('Benzin_OtackyZvysene_N_Hodnota_Norm' , 'Benzín - otáčky zvýšené')
               .replace('Nafta_MereniPrumer_OtackyVolnobezne_Hodnota_Norm', 'Nafta - otáčky volnoběžné')
               .replace('Nafta_MereniPrumer_OtackyPrebehove_Hodnota_Norm', 'Nafta - otáčky přeběhové')
        )
        is_suspect = pl.col(col).is_between(-epsilon, epsilon) | pl.col(col).is_between(1 - epsilon, 1 + epsilon)
        return (is_suspect.fill_null(False).sum() / pl.col(col).is_not_null().sum()).alias(clean_name)

    cols_otacky = ['Benzin_OtackyVolnobezne_N_Hodnota_Norm', 'Benzin_OtackyZvysene_N_Hodnota_Norm', 'Nafta_MereniPrumer_OtackyVolnobezne_Hodnota_Norm', 'Nafta_MereniPrumer_OtackyPrebehove_Hodnota_Norm']
    cols_otacky_exist = [c for c in cols_otacky if c in df_valid.columns]
    exprs_otacky = [get_suspect_expr(c) for c in cols_otacky_exist]

    time_series_monthly_expr(
        df=df_valid, time_col="DatumProhlidky", exprs=exprs_otacky,
        title="Podíl měření otáček na hranici povoleného intervalu", y_title="Podíl z úspěšných měření daného typu", decimals=2,
        save_path=str(graphs_dir / 'mereni_krajni_hodnoty_otacky.svg')
    )

    col_akcelerace = 'Nafta_MereniPrumer_CasAkcelerace_Hodnota_Norm'
    if col_akcelerace in df_base.columns:
        exprs_akcelerace = [get_suspect_expr(col_akcelerace)]
        time_series_monthly_expr(
            df=df_base, time_col="DatumProhlidky", exprs=exprs_akcelerace,
            title="Podíl času akcelerace na hranici intervalu", y_title="Podíl z úspěšných měření", decimals=1,
            save_path=str(graphs_dir / 'mereni_krajni_hodnoty_akcelerace.svg')
        )

    norm_cols = [c for c in df_base.columns if c.endswith('_Norm')]
    out_of_bounds_exprs = [(pl.col(c) < 0) | (pl.col(c) > 1) for c in norm_cols]
    expr_anomalies = (pl.any_horizontal(out_of_bounds_exprs).fill_null(False).sum() / pl.len()).alias("CELKOVÉ Anomálie (mimo rozsah 0-1)")

    time_series_monthly_expr(
        df=df_base, time_col="DatumProhlidky", exprs=[expr_anomalies],
        title="Podíl úspěšných měření s povinnými hodnotami mimo povolený rozsah", y_title="Podíl z úspěšných měření", decimals=2,
        save_path=str(graphs_dir / 'mereni_anomalie_celkove.svg')
    )

    print("Generuji grafy rozložení normovaných hodnot pro jednotlivé měsíce...", flush=True)
    df_valid = df_valid.with_columns(
        pl.col("DatumProhlidky").dt.year().alias("year"),
        pl.col("DatumProhlidky").dt.month().alias("month")
    )
    
    max_date = df_valid.select(pl.col("DatumProhlidky").max()).item()

    months = df_valid.select(["year", "month"]).unique().sort(["year", "month"]).to_dicts()
    
    months_list = [f"{m['year']}-{m['month']:02d}" for m in months]
    with open(graphs_dir / "available_months.json", "w") as f:
        json.dump(months_list, f)
        
    print("Zjišťuji maximální hodnoty osy Y pro konzistentní měřítko grafů...", flush=True)
    max_y_per_metric = {}
    for val_col, _, _ in all_mappings:
        max_y_per_metric[f"{val_col}_Norm"] = 0.0
        
    for m in months:
        y, mo = m['year'], m['month']
        df_month = df_valid.filter((pl.col("year") == y) & (pl.col("month") == mo))
        for val_col, _, _ in all_mappings:
            col_norm = f"{val_col}_Norm"
            if col_norm in df_month.columns:
                df_plot = df_month.filter(pl.col(col_norm).is_between(-0.1, 1.1))
                if df_plot.height > 0:
                    vals = df_plot[col_norm].drop_nulls().to_numpy()
                    if len(vals) > 0:
                        a_start, a_end = 0, 1.00001
                        bin_w = (a_end - a_start) / 100
                        low_bound = a_start - np.ceil((a_start - vals.min()) / bin_w) * bin_w
                        high_bound = a_start + np.ceil((vals.max() - a_start) / bin_w) * bin_w
                        bins = np.arange(low_bound, high_bound + bin_w, bin_w)
                        counts, _ = np.histogram(vals, bins=bins, density=True)
                        if len(counts) > 0 and counts.max() > max_y_per_metric[col_norm]:
                            max_y_per_metric[col_norm] = counts.max()

    for m in months:
        y, mo = m['year'], m['month']
        df_month = df_valid.filter((pl.col("year") == y) & (pl.col("month") == mo))
        for val_col, min_col, max_col in all_mappings:
            col_norm = f"{val_col}_Norm"
            if col_norm in df_month.columns:
                df_plot = df_month.filter(pl.col(col_norm).is_between(-0.1, 1.1))
                if df_plot.height > 0:
                    clean_name = (
                        col_norm.replace('Benzin_OtackyVolnobezne_N_Hodnota_Norm', 'Benzín - otáčky volnoběžné')
                               .replace('Benzin_OtackyZvysene_N_Hodnota_Norm' , 'Benzín - otáčky zvýšené')
                               .replace('Nafta_MereniPrumer_OtackyVolnobezne_Hodnota_Norm', 'Nafta - otáčky volnoběžné')
                               .replace('Nafta_MereniPrumer_OtackyPrebehove_Hodnota_Norm', 'Nafta - otáčky přeběhové')
                               .replace('_Norm', '')
                    )
                    title = f"Rozložení: {clean_name} ({mo:02d}/{y})"
                    
                    y_max = max_y_per_metric.get(col_norm, 0)
                    y_max = y_max * 1.05 if y_max > 0 else None
                    
                    distribution_density_plot(df_plot, col_norm, title, 'Normovaná hodnota', 100, (0, 1.00001), y_max=y_max, decimals=0, save_path=str(graphs_dir / f"rozdeleni_{col_norm}_{y}_{mo:02d}.svg"))

    print("Grafy byly úspěšně vygenerovány a uloženy.")