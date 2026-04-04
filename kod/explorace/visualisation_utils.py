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

def horizontal_bar(labels, counts, title, save_path: str, whole=True, height=6, groups=None, group_descriptions=None):
    """Vykreslí horizontální sloupcový graf se skupinovým barevným kódováním a vodorovnou legendou."""
    # Kontrola konzistence dat, pokud jsou zadány skupiny
    if groups is not None and sum(groups) != len(labels):
        raise ValueError(f'Součet skupin ({sum(groups)}) neodpovídá počtu popisků ({len(labels)})')
        
    # Inicializace obrázku s bílým pozadím
    fig, ax = plt.subplots(figsize=(12, height), facecolor='white')
    ax.set_facecolor('white')

    # Paleta barev pro odlišení logických skupin dat
    palette = [
        '#1e88e5', '#ffb300', '#43a047', '#e53935', 
        '#8e24aa', '#3949ab', '#00acc1', '#fb8c00'
    ]
    
    legend_handles = []
    bar_colors = '#1e88e5' # Výchozí barva
    
    if groups is not None:
        original_colors = []
        for i, group_size in enumerate(groups):
            color = palette[i % len(palette)]
            original_colors.extend([color] * group_size)
            
            # Vytvoření grafického prvku (čtverečku) a popisu pro legendu
            desc = group_descriptions[i] if group_descriptions and i < len(group_descriptions) else f'Skupina {i+1}'
            patch = mpatches.Rectangle((0, 0), 1, 1, color=color, label=desc)
            legend_handles.append(patch)
        
        # Obrácení barev, protože osa Y se vykresluje odspodu (při předání labels[::-1])
        bar_colors = original_colors[::-1]

    # Samotné vykreslení horizontálních sloupců
    bars = ax.barh(labels, counts, color=bar_colors, height=0.7)
    
    # Nastavení rozsahů osy Y pro vizuální čistotu
    ax.set_ylim(-0.8, len(labels) - 0.8)
    
    # Vykreslení číselných hodnot přímo ke koncům sloupců
    for bar in bars:
        width = bar.get_width()
        ax.text(
            width + (max(counts) * 0.01), 
            bar.get_y() + bar.get_height()/2, 
            f'{int(width)}' if whole else f'{width:.2f}', 
            va='center', 
            fontsize=11, 
            color='#333333'
        )

    # Konfigurace legendy vycentrované na střed celého bílého plátna
    if groups is not None:
        fig.legend(
            handles=legend_handles,
            loc='lower center',
            bbox_to_anchor=(0.5, 0.02), # Umístění u spodního okraje obrázku
            ncol=len(groups),            # Vodorovné uspořádání (všechny v jedné řadě)
            frameon=False,
            fontsize=11,
            columnspacing=1.5,
            handletextpad=0.5
        )

    # Formátování nadpisu a popisků osy
    fig.suptitle(
        title, 
        fontsize=16, 
        x=0.5,               # Přesný střed šířky celého obrázku
        y=0.98,              # Pozice těsně pod horním okrajem
        color='black', 
        fontweight='normal', 
        ha='center',         # Horizontální zarovnání textu na střed
        va='top'             # Vertikální zarovnání k horní hraně
    )
    ax.tick_params(axis='both', which='both', length=0, labelsize=11, labelcolor='#333333')

    # Odstranění všech ohraničujících linek grafu (spines)
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Přidání jemné vertikální mřížky pro lepší čitelnost hodnot na ose X
    ax.xaxis.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    # Úprava vnitřních okrajů s ponecháním místa pro legendu dole
    plt.tight_layout(rect=(0.0, 0.06, 1.0, 0.98)) 
    
    # Uložení do formátu SVG pro zachování vektorové kvality
    plt.savefig(save_path, format="svg", bbox_inches='tight', facecolor='white')
    plt.show()

def plot_stacked_ratios(df, cols, title, save_path):
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
        if row['id'] == "CZ0200000000": xy_text_offset = (40, 0) # Posun textu Středočeského kraje doprava
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