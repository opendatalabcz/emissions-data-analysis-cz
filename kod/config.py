from pathlib import Path
from utils import Verbosity

# Kořenové adresáře
DATA_DIR = Path('kod/data')
EXTRACTED_DIR = DATA_DIR / 'extracted'

# Prohlídky vozidel STK a SME
INSPECTIONS_DIR = EXTRACTED_DIR / 'prohlidky_vozidel_stk_a_sme'
PARENT_DATASET_INSPECTIONS = 'https://data.gov.cz/zdroj/datové-sady/66003008/9c95ebdba1dc7a2fbcfc5b6c07d25705'

# Data z měřících přístrojů
MEASUREMENTS_DIR = EXTRACTED_DIR / 'data_z_mericich_pristroju'
PARENT_DATASET_MEASUREMENTS = 'https://data.gov.cz/zdroj/datové-sady/66003008/e8e07fa264f3bd2179be03381ec324de'

# Stanice STK a SME
STATIONS_DIR = EXTRACTED_DIR / 'stanice_stk_a_sme'
DATASET_STATIONS = 'https://data.gov.cz/zdroj/datové-sady/66003008/05660b2a9412493bc68940a86b4821fc'

# Parametry běhu
SPARQL_ENDPOINT = 'https://data.gov.cz/sparql'
START_DATE = '01-01-2019'
END_DATE = None
NO_DOWNLOAD_THREADS = 30
MAX_DOWNLOAD_ATTEMPTS = 10
NO_EXTRACT_THREADS = 15
NO_PARSE_PROCESSES = 4
VERBOSITY = Verbosity.NORMAL