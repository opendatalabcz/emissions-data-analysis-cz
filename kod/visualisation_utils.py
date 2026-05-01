# -*- coding: utf-8 -*-
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


def horizontal_bar(labels, counts, title, save_path = None, decimals=0, height=6, group_indices=[], group_descriptions=None, max_bars=None, x_label='Podíl z celkového počtu'):
    """Vykreslí horizontální sloupcový graf. """
    # Kopie vstupů pro zabránění mutaci původních seznamů
    labels = list(labels)
    counts = list(counts)
    group_indices = list(group_indices)

    # Aplikace limitu poctu sloupcu
    if max_bars is not None and len(labels) > max_bars:
        limit = max_bars - 1
        
        # Výpočet sumy pro "Ostatní"
        other_count = sum(counts[limit:])
        
        # Ořezání a přidání agregovaného řádku
        labels = labels[:limit] + ["Ostatní"]
        counts = counts[:limit] + [other_count]
        
        # Úprava indexů skupin, pokud se používají
        if group_indices:
            # Přiřazení nové unikátní barvy pro "Ostatní"
            other_group_idx = max(group_indices) + 1
            group_indices = group_indices[:limit] + [other_group_idx]
            
            # Přidání popisu pro legendu
            if group_descriptions is not None:
                group_descriptions[other_group_idx] = "Ostatní"

    # Definice palety
    palette = ['#1e88e5', '#43a047', "#fdd835", '#e53935', '#8e24aa', '#3949ab', "#706A4C", '#fb8c00', "#c950a4"]

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
    ax.set_xlabel(x_label)
    
    # Textové popisky hodnot
    max_val = max(counts) if counts else 1
    for bar in bars:
        width = bar.get_width()
        ax.text(width + (max_val * 0.01), bar.get_y() + bar.get_height()/2, f'{int(width)}' if decimals == 0 else f'{width:.{decimals}f}', va='center', fontsize=11, color='#333333')

    # Legenda
    if legend_handles:
        fig.legend(handles=legend_handles, loc='lower center', bbox_to_anchor=(0.5, 0.02), ncol=len(legend_handles), frameon=False, fontsize=11, columnspacing=1.5, handletextpad=0.5)

    # Formátování
    fig.suptitle(title, fontsize=16, x=0.5, y=0.98, color='black', fontweight='normal', ha='center', va='top')
    
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
    cols_copy = cols[::-1]
    for col in cols_copy:
        total = df.height
        # Ošetření různých typů (Boolean vs String 'True')
        t_count = df.select((pl.col(col).cast(pl.String).str.to_lowercase() == 'true').sum()).item()
        
        t_ratio = t_count / total
        f_ratio = 1 - t_ratio
        
        true_ratios.append(t_ratio)
        false_ratios.append(f_ratio)

    # Inicializace obrázku
    fig, ax = plt.subplots(figsize=(12, len(cols_copy) * 0.8 + 2), facecolor='white')
    ax.set_facecolor('white')
    ax.set_xlabel('Podíl z celkového počtu')

    # Barvy korespondující s paletou (modrá pro True, šedá pro False)
    color_true = '#1e88e5'
    color_false = '#e0e0e0'

    # Vykreslení skládaných sloupců
    bars_t = ax.barh(cols_copy, true_ratios, color=color_true, height=0.6)
    bars_f = ax.barh(cols_copy, false_ratios, left=true_ratios, color=color_false, height=0.6)

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

        # Formatovani textu
        formatted_text = f"{int(row[value_column]):,}".replace(",", " ")

        # Vložení textu: int() odstraňuje desetinná místa, path_effects aplikuje obrys
        ax.annotate(text=formatted_text, xy=(centroid.x, centroid.y), xytext=xy_text_offset,
                    textcoords="offset points", ha='center', va='center', fontsize=10, fontweight='bold',
                    color='black', path_effects=text_outline)
        # Vložení textu: int() odstraňuje desetinná místa, path_effects aplikuje obrys

    ax.set_axis_off() # Odstranění souřadnicového rámce
    ax.set_title(title) # Titulek grafu
    plt.savefig(output_path, format="svg", bbox_inches='tight') # Export do SVG s oříznutím okrajů


def time_series_all(values, title, y_title, granularity, save_path = None, relative=False):
    # Agregace dat pomocí Polars API
    # values musí být Series typu Datetime nebo Date
    df_counts = (
        values.to_frame("ts")
        .sort("ts")
        .group_by_dynamic("ts", every=granularity)
        .agg(pl.len().alias("count"))
        .head(-1)
    )
    if relative:
        df_counts = df_counts.with_columns((pl.col("count") / pl.col("count").sum()).alias("count"))

    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(
        df_counts["ts"], 
        df_counts["count"],
        marker=',' if len(df_counts) > 10 else 'o'
    )

    ax.set_title(title)
    ax.set_xlabel("Rok")
    ax.set_ylabel(y_title)
    ax.spines[['top', 'right']].set_visible(False)
    ax.set_ylim(bottom=0.0, top=df_counts['count'].max()*1.1)
    if relative:
        ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
    else:
        ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, p: format(int(x), ',')))
    
    fig.autofmt_xdate()

    ax.yaxis.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    if save_path:
        fig.savefig(save_path, format="svg", bbox_inches="tight")
 
    plt.show()


def time_series_year(values, title, y_title, save_path=None, relative=False):
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
        # Tato sekce zajistí relativní přepočet před finálním průměrem
        .with_columns(
            (pl.col("count") / pl.col("count").sum().over("year")) if relative else pl.col("count")
        )
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
    ax.spines[['top', 'right']].set_visible(False)
    ax.set_ylim(bottom=0.0, top=df_monthly_avg["avg_count"].max()*1.1)
    if relative:
        ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
    else:
        ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, p: format(int(x), ',')))

    ax.yaxis.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    if save_path:
        fig.savefig(save_path, format="svg", bbox_inches="tight")
 
    plt.show()

def active_prohlidky_day_plot(df, start_col, end_col, title, y_title, interval="5m", save_path=None):
    """
    Vypočítá a vykreslí průměrný počet aktivních prohlídek v průběhu dne.
    interval: Rozlišení grafu (např. '1m', '5m', '15m').
    """
    koef = 1 / (1 - df['TechnickaCast_Pritomno'].mean())
    df = df.filter(pl.col('TechnickaCast_Pritomno') == False)
    # Vytvoření událostí: začátek (+1), konec (-1)
    starts = df.select([
        pl.col(start_col).alias("ts"),
        pl.lit(1, dtype=pl.Int32).alias("change")
    ]).filter(pl.col("ts").is_not_null())
    
    ends = df.select([
        pl.col(end_col).alias("ts"),
        pl.lit(-1, dtype=pl.Int32).alias("change")
    ]).filter(pl.col("ts").is_not_null())

    # Seřazení událostí a výpočet okamžitého počtu aktivních prohlídek
    events = (
        pl.concat([starts, ends])
        .sort("ts")
        .with_columns(
            pl.col("change").cum_sum().alias("active_count")
        )
    )

    # Převod na pravidelnou časovou řadu (resampling)
    # Tím získáme stav systému v pravidelných bodech
    resampled = (
        events.group_by_dynamic("ts", every=interval)
        .agg(pl.col("active_count").last())
        .fill_null(strategy="forward")
        .fill_null(0)
    )

    # Odstranění posledního dne pro zamezení zkreslení průměru
    last_day = resampled.select(pl.col("ts").dt.date().max()).item()
    
    # Výpočet průměru pro každou část dne
    df_avg = (
        resampled
        .filter(pl.col("ts").dt.date() < last_day)
        .with_columns([
            pl.col("ts").dt.truncate(interval).dt.time().alias("time_of_day")
        ])
        .group_by("time_of_day")
        .agg(pl.col("active_count").mean().alias("avg_active"))
        .sort("time_of_day")
    )

    # Vizualizace
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Převedení Time objektů na numerickou osu pro Matplotlib
    x_values = [t.hour + t.minute/60 + t.second/3600 for t in df_avg["time_of_day"]]
    
    ax.plot(x_values, df_avg["avg_active"] * koef, linestyle='-', linewidth=2)

    ax.set_title(title)
    ax.set_xlabel("Hodina")
    ax.set_ylabel(y_title)
    ax.set_xticks(range(0, 25))
    ax.set_xlim(0, 24)
    ax.spines[['top', 'right']].set_visible(False)
    ax.set_ylim(bottom=0.0, top=df_avg["avg_active"].max() * 1.1)
    
    ax.get_yaxis().set_major_formatter(
        mtick.FuncFormatter(lambda x, p: format(int(x), ','))
    )

    ax.yaxis.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    if save_path:
        fig.savefig(save_path, format="svg", bbox_inches="tight")

    plt.tight_layout()
    plt.show()

def duration_prohlidky_plot(df, start_col, end_col, title, y_title, save_path=None):
    df_duration = (
        df.select([
            pl.col(start_col).alias("ts"),
            ((pl.col(end_col) - pl.col(start_col)).dt.total_seconds() / 60).alias("duration_min")
        ])
    )
    
    last_day = df_duration.select(pl.col("ts").dt.date().max()).item()

    df_avg_duration = (
        df_duration
        .filter(pl.col("ts").dt.date() < last_day)
        .with_columns(pl.col("ts").dt.hour().alias("hour"))
        .group_by("hour")
        .agg(pl.col("duration_min").mean().alias("avg_duration"))
        .sort("hour")
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(
        df_avg_duration["hour"], 
        df_avg_duration["avg_duration"],
        marker='o',
    )

    ax.set_title(title)
    ax.set_xlabel("Hodina zahájení prohlídky")
    ax.set_ylabel(f"{y_title} [minuty]")
    ax.set_xticks(range(0, 24))
    ax.spines[['top', 'right']].set_visible(False)
    
    if not df_avg_duration.is_empty():
        ax.set_ylim(bottom=0.0, top=df_avg_duration["avg_duration"].max() * 1.2)

    ax.yaxis.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    if save_path:
        fig.savefig(save_path, format="svg", bbox_inches="tight")

    plt.tight_layout()
    plt.show()

def duration_density_plot(df, start_col, end_col, title, x_label, unit='minutes', log_scale=False, save_path=None):
    """Podporované jednotky: 'seconds', 'minutes', 'hours', 'days', 'years'"""
    configs = {'seconds': 1, 'minutes': 60, 'hours': 3600, 'days': 86400, 'years': 86400 * 365.24}
    divisor = configs.get(unit, 60)

    # Výpočet délky
    df_duration = (
        df.select(
            ((pl.col(end_col) - pl.col(start_col)).dt.total_seconds() / divisor).alias("duration")
        )
        .filter(pl.col("duration") > 0)
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    
    durations = df_duration["duration"]

    if log_scale:
        bins = np.logspace(np.log10(durations.min()), np.log10(durations.max()), 100)
        ax.set_xscale('log')
    else:
        bins = 100

    ax.set_xlabel(x_label)

    ax.hist(durations, bins=bins, density=True, color='#1e88e5', edgecolor='white', linewidth=0.5)

    ax.set_title(title)
    ax.set_ylabel("Hustota pravděpodobnosti")
    ax.spines[['top', 'right']].set_visible(False)

    ax.yaxis.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    if save_path:
        fig.savefig(save_path, format="svg", bbox_inches="tight")

    plt.tight_layout()
    plt.show()

def distribution_density_plot(df, value_col, title, x_label, no_bins=100, anchor_range=None, log_scale=False, save_path=None):
    """Generuje histogram s hustotou pravděpodobnosti pro numerický sloupec."""
    # Filtrace neplatných hodnot (pro logaritmické měřítko musí být hodnoty kladné)
    if log_scale:
        df_filtered = df.select(pl.col(value_col)).filter(pl.col(value_col) > 0)
    else:
        df_filtered = df.select(pl.col(value_col)).filter(pl.col(value_col).is_not_null())

    values = df_filtered[value_col]

    fig, ax = plt.subplots(figsize=(12, 6))

    if anchor_range:
        # Výpočet šířky binu na základě zadaného intervalu
        a_start, a_end = anchor_range
        bin_w = (a_end - a_start) / no_bins
        
        # Dynamické rozšíření hranic tak, aby pokryly celá data, 
        # ale zůstaly zarovnané na anchor_range
        low_bound = a_start - np.ceil((a_start - values.min()) / bin_w) * bin_w
        high_bound = a_start + np.ceil((values.max() - a_start) / bin_w) * bin_w
        bins = np.arange(low_bound, high_bound + bin_w, bin_w)
    elif log_scale:
        bins = np.logspace(np.log10(values.min()), np.log10(values.max()), no_bins + 1)
        ax.set_xscale('log')
    else:
        bins = no_bins

    ax.hist(values, bins=bins, density=True, color='#1e88e5', edgecolor='white', linewidth=0.5)

    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel("Hustota pravděpodobnosti")
    ax.spines[['top', 'right']].set_visible(False)
    ax.set_xlim(-0.1, 1.1)

    ax.yaxis.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    if save_path:
        fig.savefig(save_path, format="svg", bbox_inches="tight")

    plt.tight_layout()
    plt.show()


def time_series_preaggregated(x, y, title, y_title, save_path=None):
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(x, y, marker=',' if len(x) > 10 else 'o')

    ax.set_title(title)
    ax.set_xlabel("Rok")
    ax.set_ylabel(y_title)
    ax.spines[['top', 'right']].set_visible(False)
    ax.set_ylim(bottom=0.0, top=y.max() * 1.1)
    
    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.4f'))
    
    fig.autofmt_xdate()

    ax.yaxis.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    if save_path:
        fig.savefig(save_path, format="svg", bbox_inches="tight")
 
    plt.show()