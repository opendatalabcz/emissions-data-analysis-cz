# Analýza dat ISTP a SME Portál

Tento repozitář obsahuje komplexní řešení pro analýzu otevřených dat z Informačního systému technických prohlídek (ISTP) Ministerstva dopravy ČR a následnou vizualizaci výsledků prostřednictvím interaktivního webového portálu.

Projekt je rozdělen do dvou hlavních částí:
1. **Analytická část** (kořenový adresář a složka `kod/`) – Skripty pro stažení, transformaci a analýzu dat, včetně trénování ML modelu.
2. **Webový portál** (složka `portal/`) – Uživatelské rozhraní a backend pro prezentaci dat a spouštění predikcí.

---

## 1. Analýza dat z ISTP

Tato část se zaměřuje na analýzu otevřených dat z měření emisí (SME) a technických kontrol (STK). Hlavním cílem bylo identifikovat anomálie, metodická selhání a trendy spojené s obcházením emisních limitů.

Repozitář pokrývá celý proces od automatizovaného stahování a parsování surových XML dat přes jejich analýzu a vizualizaci až po návrh prediktivního modelu. Součástí jsou pouze zdrojové kódy a skripty k natrénování modelu strojového učení, výsledný natrénovaný model zde uložen není.

### Cíle projektu
* **Zpracování dat:** Automatizovaná extrakce a transformace více než 150 GB surových XML dat (přes 20 milionů záznamů) do formátu Parquet.
* **Detekce anomálií:** Identifikace nestandardních jevů v datech (např. fyzikálně nereálné hodnoty nebo koncentrace hodnot na krajích tolerančních pásem).
* **Časové řady:** Vyhodnocení vývoje sledovaných jevů a reakcí stanic na legislativní zásahy.
* **Prediktivní modelování:** Návrh a kód pro natrénování modelu (CatBoost), který odhaduje pravděpodobnost selhání vozidla při další emisní kontrole.

### Použité technologie
* **Datová pipeline:** Python, Polars, Pandas, PyArrow, LXML.
* **Strojové učení a analytika:** CatBoost, Scikit-learn.
* **Vizualizace:** Matplotlib, Geopandas, YData Profiling.

---

## 2. SME Portál (složka `portal/`)

Analytický a informační web postavený nad otevřenými daty z ISTP MDČR. Zaměřuje se na oblast Stanic měření emisí (SME). Portál poskytuje interaktivní grafy agregující průchodnosti a emisní měření v čase a obsahuje zabudovaný model strojového učení (CatBoost) pro predikci selhání vozidla.

### Prerekvizity
Pro spuštění portálu je vyžadováno nainstalované prostředí Docker:
* [Docker](https://docs.docker.com/engine/install/)
* [Docker Compose](https://docs.docker.com/compose/install/)

### Příprava před spuštěním

#### 1. Model a předpočítaná data
Pro správnou funkci predikcí vložte natrénovaný ML model a jeho metadata do složky `portal/data/precomputed/`. Tyto soubory nejsou součástí repozitáře kvůli své velikosti.

Požadovaná struktura souborů:
```text
portal/
└── data/
    └── precomputed/
        ├── cat_features.json
        ├── features.json
        ├── model.bin
        └── optimalizovane_prahy.csv
```

#### 2. Konfigurace prostředí
Přejděte do složky `portal/` a vytvořte konfigurační soubor `.env` ze vzoru:
```bash
cd portal
cp .env.example .env
```
V souboru `.env` lze upravit port, počet vláken pro stahování dat z NKOD a interval automatické aktualizace (`UPDATE_INTERVAL_DAYS`).

### Spuštění

Sestavení a spuštění kontejnerů provádějte vždy z adresáře `portal/`:
```bash
cd portal
docker-compose up --build -d
```
Webové rozhraní bude dostupné na adrese `http://localhost:3000` (nebo na portu definovaném v `.env`).

### Průběh startu aplikace
1. Nastartuje se webový kontejner a backendový (FastAPI) kontejner.
2. Backend stáhne otevřená data a seznam stanic přes SPARQL.
3. XML sady se paralelně rozparsují do formátu Parquet a vygenerují se SVG grafy.
4. Načte se ML model a sestaví se In-Memory index VIN kódů.
5. Během tohoto procesu (který může trvat desítky minut v závislosti na hardwaru) vrací backend na endpointu `/health` stav `503` a web informuje o přípravě dat. Proces se automaticky opakuje podle nastaveného intervalu.

---

## Struktura celého repozitáře

* `kod/` – Zdrojové kódy analytické části (konverze XML, EDA, trénování CatBoost).
* `portal/` – Kompletní zdrojové kódy webového portálu (FastAPI backend, frontend, Docker konfigurace).
* `zdroje/` – Podkladové materiály a metodické pokyny.

---

<img src="https://fit.cvut.cz/static/images/fit-cvut-logo-cs.svg" alt="logo FIT ČVUT" height="200">

Tento software vznikl za podpory **Fakulty informačních technologií ČVUT v Praze**.
Více informací naleznete na [fit.cvut.cz](https://fit.cvut.cz).
Otevřený repozitář naleznete na [https://github.com/opendatalabcz/emissions-data-analysis-cz](https://github.com/opendatalabcz/emissions-data-analysis-cz).