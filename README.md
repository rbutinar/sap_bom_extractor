# SAP BOM Extractor (Pandas Edition)

This project demonstrates how to extract, transform, and analyze SAP Bill of Materials (BOM) data using Python and pandas, with all data stored as CSV files for easy inspection and processing. No Spark or Delta Lake dependencies are required.

**Key Features:**
- CSV-based data for maximum transparency and portability
- Multi-plant and multi-alternative BOM support (SAP-style)
- BOM explosion with effectivity date filtering and cost rollup (from MBEW)
- Clear separation between raw/source data (`source_data/`) and processed outputs (`processed_data/`)
- **Advanced variant management:**
  - Uses STAS as the primary source for BOM components, including deletion flag (`LOEKZ`) and historical quantities (`ALT_MENGE`, `NEU_MENGE`).
  - Excludes deleted components from explosion and highlights them in the output.
  - Adds columns to the output for `deleted`, `alt_qty`, and `new_qty`.
- Support for engineering changes (STZU)

## Setup

1. **Create a virtual environment (recommended):**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Workflow

- **Generate BOM Data:**
  - Run `create_silver_bom_tables.py` to generate example BOM tables as CSV files in the `source_data/` directory.
- **Preview Data:**
  - Use `preview_tables.py` to print the contents of each generated CSV table.
- **Explode BOMs:**
  - Run `explode_bom_multilevel.py` to recursively explode all BOMs and output a flat, multi-level exploded BOM (`processed_data/exploded_bom.csv`). This file lists, for each material/plant/alternative, all leaf components, their explosion depth, and rolled-up cost.

## Project Structure

- `create_silver_bom_tables.py` — Generates sample BOM tables as CSV files in `source_data/`
- `preview_tables.py` — Prints the content of all CSV tables for review
- `explode_bom_multilevel.py` — Recursively explodes BOMs and outputs a flat exploded BOM CSV in `processed_data/`
- `source_data/` — Contains all raw/source tables (STKO, STPO, MAST, MBEW, STAS, STZU, etc.)
- `source_data/` — Contains all raw/source tables (STKO, STPO, MAST, MBEW, etc.)
- `processed_data/` — Contains all processed outputs (e.g., `exploded_bom.csv`)
- `requirements.txt` — Python dependencies (pandas)
- `README.md` — Project documentation

## Output Columns

The exploded BOM output includes:
- `material`: Root material
- `bom`: BOM number
- `component`: Leaf component
- `level`: Explosion level
- `total_quantity`: Quantity required
- `WERKS`: Plant
- `STLAL`: BOM alternative
- `total_cost`: Rolled-up cost for the path

## Streamlit App: SAP BOM Explorer

L'app Streamlit (`bom_explorer_app.py`) offre un'interfaccia interattiva per esplorare, filtrare e scaricare l'esploso multi-livello delle distinte base SAP. È pensata per analisti, ingegneri e utenti business che vogliono:

- **Navigare la struttura BOM multi-livello** per qualsiasi materiale, plant e alternativa.
- **Filtrare** per materiale, plant (WERKS), alternativa BOM (STLAL) e data di validità.
- **Visualizzare e scaricare** i risultati filtrati in formato CSV.
- **Gestire varianti e modifiche:**
  - Visualizza lo stato storico dei componenti (colonne `deleted`, `alt_qty`, `new_qty`).
  - Permette di mostrare/nascondere i componenti cancellati tramite un toggle.
  - Evidenzia visivamente i componenti cancellati nella tabella.
- **Preview delle tabelle di input:**
  - Dal menu laterale puoi selezionare e visualizzare qualsiasi tabella sorgente (`MAST`, `STKO`, `STPO`, `STAS`, `STZU`, ecc.) per trasparenza e debug.
- **Info box** che spiega la logica di esplosione e la struttura dei dati.

### Tabelle gestite e visualizzate
- **MAST**: Collegamento tra materiale padre e numero BOM.
- **STKO**: Header della BOM.
- **STPO**: Componenti/items della BOM (tutti gli storici, inclusi cancellati e modificati).
- **STAS**: Gestione avanzata delle varianti, flag di cancellazione (`LOEKZ`), quantità storiche (`ALT_MENGE`, `NEU_MENGE`).
- **STZU**: Informazioni sulle modifiche/alternative BOM (engineering change).
- **MBEW**: Costi standard dei materiali.

> L'app riflette lo stato dei dati prodotti dalla pipeline Python: tutte le logiche di esplosione, gestione varianti, modifiche e cancellazioni sono già applicate nei dati visualizzati.

---

Per avviare l'app:
```bash
streamlit run bom_explorer_app.py
```

## Field Explanations
- **WERKS**: SAP Plant — a physical or logical location for production, inventory, or services.
- **STLAL**: BOM Alternative — identifies different valid versions of a BOM for the same material and plant.

## SAP Coverage & Advanced Tables
- This tool covers multi-level, multi-plant, multi-alternative BOM explosion with cost rollup, suitable for most analytics/reporting.
- Not all advanced SAP features (engineering change management, variant BOMs, etc.) are implemented.
- **STAS** and **STZU** tables are included for completeness but are not populated unless you implement advanced SAP logic.

## Example Usage

```bash
python create_silver_bom_tables.py
python preview_tables.py
python explode_bom_multilevel.py
```

## Git Workflow

- Data files in `source_data/` and `processed_data/` are mostly gitignored except for `.gitkeep` and key outputs like `exploded_bom.csv`.
- Always check `processed_data/exploded_bom.csv` for the latest results.

## Example Usage

```bash
python create_silver_bom_tables.py
python preview_tables.py
python explode_bom_multilevel.py
```

## Notes
- All data handling is done with pandas and pyarrow. No Spark/Delta required.
- The exploded BOM CSV lists for each root material: the BOM used, each unique leaf component, and the explosion level (distance from root to leaf).

---

Feel free to reach out for help with further BOM analysis, visualization, or automation!
