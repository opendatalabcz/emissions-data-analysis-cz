# Analýza dat z Informačního systému technických prohlídek (ISTP)

Tento repozitář obsahuje zdrojové kódy, analytické skripty a Jupyter notebooky vytvořené v rámci bakalářské práce. Práce se zaměřuje na analýzu otevřených dat Ministerstva dopravy ČR z měření emisí (SME) a technických kontrol (STK). Hlavním cílem bylo identifikovat anomálie, metodická selhání a trendy spojené s obcházením emisních limitů.

Repozitář pokrývá celý proces od automatizovaného stahování a parsování surových XML dat přes jejich analýzu a vizualizaci až po návrh prediktivního modelu. Součástí repozitáře jsou pouze zdrojové kódy a skripty k natrénování modelu strojového učení, výsledný natrénovaný model zde uložen není.

## Cíle projektu

* **Zpracování dat:** Automatizovaná extrakce a transformace více než 150 GB surových XML dat (přes 20 milionů záznamů) do formátu Parquet.
* **Detekce anomálií:** Identifikace nestandardních jevů v datech (např. fyzikálně nereálné hodnoty nebo koncentrace hodnot na krajích tolerančních pásem).
* **Časové řady:** Vyhodnocení vývoje sledovaných jevů a reakcí stanic na legislativní zásahy.
* **Prediktivní modelování:** Návrh a kód pro natrénování modelu (CatBoost), který odhaduje pravděpodobnost selhání vozidla při další emisní kontrole.

## Použité technologie

* **Datová pipeline:** Python, Polars (pro zpracování větších datasetů), Pandas, PyArrow, LXML.
* **Strojové učení a analytika:** CatBoost, Scikit-learn.
* **Vizualizace:** Matplotlib, Geopandas, YData Profiling.

## Struktura repozitáře

* `kod/` – Hlavní adresář se zdrojovými kódy projektu.
  * `main.py`, `clean.py`, `preprocess.py`, `separate.py` – Skripty pro zpracování XML dat a jejich konverzi.
  * `utils.py`, `visualisation_utils.py`, `schemas.py` – Pomocné funkce a datová schémata.
  * `data/` – Složka pro ukládání dat (obsahuje pouze XSD schémata, datové soubory jsou ignorovány v .gitignore).
  * `explorace/` – Jupyter notebooky s explorační analýzou dat (EDA) a vygenerované grafy.
  * `casove_rady/` – Kódy pro analýzu a vizualizaci vývoje v čase.
  * `modelovani/` – Experimenty, příprava datasetu a skripty pro trénování modelu CatBoost.
  * `reporty/` – Vygenerované HTML reporty datových profilů.
* `poznamky/` – Pracovní poznámky, osnovy a textové materiály k metodice.
* `zdroje/` – Podkladové materiály, rešeršní literatura a metodické pokyny.
* `sablona/` – Odkazy na LaTeX šablonu práce.

***
<img src="https://fit.cvut.cz/static/images/fit-cvut-logo-cs.svg" alt="logo FIT ČVUT" height="200">

Tento software vznikl za podpory **Fakulty informačních technologií ČVUT v Praze**.
Více informací naleznete na [fit.cvut.cz](https://fit.cvut.cz).
Otevřený repozitář naleznete na [https://github.com/opendatalabcz/emissions-data-analysis-cz](https://github.com/opendatalabcz/emissions-data-analysis-cz).
