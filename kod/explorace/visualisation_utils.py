import os
import time
import random
import warnings

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree # type: ignore <- pylance milně hlásí chybu
from pathlib import Path
import time
import sys
import polars as pl
import polars.selectors as cs
import json
import pickle
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as path_effects
import matplotlib.ticker as mtick
import matplotlib.patheffects as path_effects
from ydata_profiling import ProfileReport
import geopandas as gpd


def horizontal_bar(labels, counts, title, save_path = None, decimals=0, height=6, group_indices=[], group_descriptions=None):
    """Vykreslí horizontální sloupcový graf. """
    # Kopie vstupů pro zabránění mutaci původních seznamů
    labels = list(labels)
    counts = list(counts)
    group_indices = list(group_indices)

    # Definice palety
    palette = [
        '#1e88e5', '#43a047', "#fdd835", '#e53935', 
        '#8e24aa', '#3949ab', "#706A4C", '#fb8c00',
        "#c950a4"
    ]

    # Mapování barev a příprava legendy před otočením dat
    bar_colors = []
    legend_handles = []
    
    if group_indices:
        # Průběžné barvy pro jednotlivé sloupce
        bar_colors = [palette[g % len(palette)] for g in group_indices]
        
        # Identifikace unikátních skupin v pořadí výskytu pro legendu
        unique_groups_seen = []
        for g_idx in group_indices:
            if g_idx not in unique_groups_seen:
                unique_groups_seen.append(g_idx)
        unique_groups_seen.sort()
                
        for g_idx in unique_groups_seen:
            # Získání popisu (pokud existuje)
            if group_descriptions:
                desc = group_descriptions[g_idx]
            else:
                desc = 'ERROR'
            patch = mpatches.Rectangle((0, 0), 1, 1, color=palette[g_idx % len(palette)], label=desc)
            legend_handles.append(patch)
    else:
        bar_colors = '#1e88e5'

    # Otočení dat pro vykreslení shora dolů (Matplotlib indexuje odspodu)
    labels.reverse()
    counts.reverse()
    if isinstance(bar_colors, list):
        bar_colors.reverse()

    fig, ax = plt.subplots(figsize=(12, height), facecolor='white')
    ax.set_facecolor('white')

    # Vykreslení
    bars = ax.barh(labels, counts, color=bar_colors, height=0.7)
    
    ax.set_ylim(-0.8, len(labels) - 0.2)
    
    # Textové popisky hodnot
    max_val = max(counts) if counts else 1
    for bar in bars:
        width = bar.get_width()
        ax.text(
            width + (max_val * 0.01), 
            bar.get_y() + bar.get_height()/2, 
            f'{int(width)}' if decimals == 0 else f'{width:.{decimals}f}', 
            va='center', 
            fontsize=11, 
            color='#333333'
        )

    # Legenda
    if legend_handles:
        fig.legend(
            handles=legend_handles,
            loc='lower center',
            bbox_to_anchor=(0.5, 0.02),
            ncol=len(legend_handles),
            frameon=False,
            fontsize=11,
            columnspacing=1.5,
            handletextpad=0.5
        )

    # Formátování
    fig.suptitle(
        title, 
        fontsize=16, 
        x=0.5, 
        y=0.98, 
        color='black', 
        fontweight='normal', 
        ha='center', 
        va='top'
    )
    
    ax.tick_params(axis='both', which='both', length=0, labelsize=11, labelcolor='#333333')

    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.xaxis.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    plt.tight_layout(rect=(0.0, 0.08, 1.0, 0.98)) 
    if save_path:
        plt.savefig(save_path, format="svg", bbox_inches='tight', facecolor='white')
    plt.show()

def plot_stacked_ratios(df, cols, title, save_path = None):
    """Vytvoří horizontální skládaný sloupcový graf (True/False) ve stylu předchozího grafu."""
    true_ratios = []
    false_ratios = []
    
    # Výpočet poměrů
    for col in cols:
        total = df.height
        # Ošetření různých typů (Boolean vs String 'True')
        t_count = df.select((pl.col(col).cast(pl.String) == 'True').sum()).item()
        
        t_ratio = t_count / total
        f_ratio = 1 - t_ratio
        
        true_ratios.append(t_ratio)
        false_ratios.append(f_ratio)

    # Inicializace obrázku
    fig, ax = plt.subplots(figsize=(12, len(cols) * 0.8 + 2), facecolor='white')
    ax.set_facecolor('white')

    # Barvy korespondující s paletou (modrá pro True, šedá pro False)
    color_true = '#1e88e5'
    color_false = '#e0e0e0'

    # Vykreslení skládaných sloupců
    bars_t = ax.barh(cols, true_ratios, color=color_true, height=0.6)
    bars_f = ax.barh(cols, false_ratios, left=true_ratios, color=color_false, height=0.6)

    # Přidání textových popisků (pomer 0.xx) přímo do sloupců
    for i, (t_rect, f_rect) in enumerate(zip(bars_t, bars_f)):
        t_w = t_rect.get_width()
        f_w = f_rect.get_width()
        
        # Popisek pro True část
        if t_w > 0.05:
            ax.text(
                t_w / 2, i, f'{t_w:.3f}', 
                va='center', ha='center', color='white', fontsize=11
            )
        
        # Popisek pro False část
        if f_w > 0.05:
            ax.text(
                t_w + (f_w / 2), i, f'{f_w:.3f}', 
                va='center', ha='center', color='#333333', fontsize=11
            )

    # Vycentrovaný nadpis na střed celého plátna
    fig.suptitle(title, fontsize=16, x=0.5, y=0.98, ha='center', va='top', fontweight='normal')

    # Nastavení os a mřížky
    ax.set_xlim(0, 1)
    ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_xticklabels(['0.0', '0.2', '0.4', '0.6', '0.8', '1.0'])
    
    ax.tick_params(axis='both', which='both', length=0, labelsize=11, labelcolor='#333333')
    
    # Odstranění linek (spines)
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Vertikální mřížka
    ax.xaxis.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    # Legenda pod grafem pomocí proxy objektů
    legend_handles = [
        mpatches.Rectangle((0, 0), 1, 1, color=color_true, label='True'),
        mpatches.Rectangle((0, 0), 1, 1, color=color_false, label='False')
    ]
    
    # fig.legend s těmito parametry vynutí střed celého obrázku
    fig.legend(
        handles=legend_handles,
        loc='lower center',
        bbox_to_anchor=(0.5, 0.05), # 0.5 je přesný horizontální střed plátna
        ncol=2,
        frameon=False,
        fontsize=11,
        columnspacing=2.0,
        handletextpad=0.5
    )

    # Úprava okrajů - rect=(left, bottom, right, top)
    plt.tight_layout(rect=(0.0, 0.15, 1.0, 0.97))
    if save_path:
        plt.savefig(save_path, format="svg", bbox_inches='tight', facecolor='white')
    plt.show()


def plot_czech_regional_map(counts_pl, value_column, title, legend_label, output_path):
    """Vykreslí kartogram krajů ČR s korektním pořadím vrstev (Praha navrchu) a popisky."""
    # Načtení GeoJSON
    KRAJE_URL = "https://raw.githubusercontent.com/siwekm/czech-geojson/refs/heads/master/kraje.json"
    kraje_geo = gpd.read_file(KRAJE_URL)

    # Join dat
    map_data = kraje_geo.merge(counts_pl.to_pandas(), left_on="id", right_on="kod_kraje", how="left").fillna(0)

    # Seřazení zajistí, že Praha (uprostřed Středočeského kraje) se vykreslí jako poslední/navrchu
    map_data["z_order"] = map_data["id"].apply(lambda x: 1 if x == "CZ0100000000" else 0)
    map_data = map_data.sort_values("z_order") # Přeskupení řádků v GeoDataFrame

    _, ax = plt.subplots(figsize=(15, 10)) # Inicializace figur a osy
    
    # Vykreslení ploch: zorder=1 zajistí základní vrstvu pro polygony
    map_data.plot(column=value_column, cmap="Reds", edgecolor="black", vmin=0, linewidth=0.7, ax=ax, 
                  legend=True, legend_kwds={'label': legend_label, 'orientation': "vertical", 'shrink': 0.7})

    # Iterace pro anotace: popisky se vykreslují na vypočtené středy (centroidy)
    for _, row in map_data.iterrows():
        centroid = row.geometry.centroid # Geometrický střed kraje
        xy_text_offset = (0, 0) # Defaultní pozice bez posunu
        
        # Korekce pozic popisků: Středočeský kraj (offset od Prahy) a Olomoucký kraj
        if row['id'] == "CZ0200000000": xy_text_offset = (40, -10) # Posun textu Středočeského kraje doprava
        elif row['id'] == "CZ0710000000": xy_text_offset = (0, -20) # Posun textu Olomouckého kraje dolů

        # Bílý obrys (halo efekt) pro čitelnost textu přes hranice a tmavé barvy
        text_outline = [path_effects.withStroke(linewidth=2, foreground='white')]

        # Vložení textu: int() odstraňuje desetinná místa, path_effects aplikuje obrys
        ax.annotate(text=str(int(row[value_column])), xy=(centroid.x, centroid.y), xytext=xy_text_offset,
                    textcoords="offset points", ha='center', va='center', fontsize=10, fontweight='bold',
                    color='black', path_effects=text_outline)

    ax.set_axis_off() # Odstranění souřadnicového rámce
    ax.set_title(title) # Titulek grafu
    plt.savefig(output_path, format="svg", bbox_inches='tight') # Export do SVG s oříznutím okrajů


def time_series(values, title, granularity, save_path = None):
    # Agregace dat pomocí Polars API
    # values musí být Series typu Datetime nebo Date
    df_counts = (
        values.to_frame("ts")
        .sort("ts")
        .group_by_dynamic("ts", every=granularity)
        .count()
    )

    # Objektové API Matplotlibu
    fig, ax = plt.subplots(figsize=(10, 5))
    
    ax.plot(
        df_counts["ts"], 
        df_counts["count"]
    )

    ax.set_title(title)
    ax.set_xlabel("Čas")
    ax.set_ylabel("Frekvence")
    ax.grid(True, linestyle="--", alpha=0.6)

    fig.autofmt_xdate()

    if save_path:
        fig.savefig(save_path, format="svg", bbox_inches="tight")
    
    plt.show()


def time_series_all(values, title, y_title, granularity, save_path = None):
    # Agregace dat pomocí Polars API
    # values musí být Series typu Datetime nebo Date
    df_counts = (
        values.to_frame("ts")
        .sort("ts")
        .group_by_dynamic("ts", every=granularity)
        .agg(pl.len().alias("count"))
        .head(-1)
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(
        df_counts["ts"], 
        df_counts["count"],
        marker=',' if len(df_counts) > 10 else 'o'
    )

    ax.set_title(title)
    ax.set_xlabel("Čas")
    ax.set_ylabel(y_title)
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.set_ylim(bottom=0.0, top=df_counts['count'].max()*1.1)
    ax.get_yaxis().set_major_formatter(
        mtick.FuncFormatter(lambda x, p: format(int(x), ','))
    )
    
    fig.autofmt_xdate()

    if save_path:
        fig.savefig(save_path, format="svg", bbox_inches="tight")
 
    plt.show()


def time_series_year(values, title, y_title, save_path=None):
    # Převod na DataFrame a extrakce roku a měsíce
    df = values.to_frame("ts")
    
    # Identifikace pouze úplných let (všech 12 měsíců přítomno v datech)
    complete_years = (
        df.with_columns(pl.col("ts").dt.month().alias("month"))
        .group_by(pl.col("ts").dt.year().alias("year"))
        .agg(pl.col("month").n_unique().alias("unique_months"))
        .filter(pl.col("unique_months") == 12)
        .select("year")
    )

    # Filtrace na úplné roky, agregace měsíčních počtů a následný průměr přes roky
    df_monthly_avg = (
        df.with_columns([
            pl.col("ts").dt.year().alias("year"),
            pl.col("ts").dt.month().alias("month")
        ])
        .join(complete_years, on="year", how="inner")
        .group_by(["year", "month"])
        .agg(pl.len().alias("count"))
        .group_by("month")
        .agg(pl.col("count").mean().alias("avg_count"))
        .sort("month")
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(
        df_monthly_avg["month"], 
        df_monthly_avg["avg_count"],
        marker='o'
    )

    ax.set_title(title)
    ax.set_xlabel("Měsíc")
    ax.set_ylabel(y_title)
    ax.set_xticks(range(1, 13))
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.set_ylim(bottom=0.0, top=df_monthly_avg["avg_count"].max()*1.1)
    ax.get_yaxis().set_major_formatter(
        mtick.FuncFormatter(lambda x, p: format(int(x), ','))
    )

    if save_path:
        fig.savefig(save_path, format="svg", bbox_inches="tight")
 
    plt.show()