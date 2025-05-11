
# 📘 Notebook: create_silver_bom_tables
# Scopo: creare tabelle Delta in Lakehouse con dati simulati simili a SAP (già tipizzati, livello silver)

## 1. Inizializzazione


from datetime import date
import pandas as pd
import numpy as np
import os

# Ensure source_data directory exists
os.makedirs("source_data", exist_ok=True)


## 2. Tabella STKO (testata distinta base)


stko_columns = ["MATNR", "WERKS", "STLNR", "STLAL", "STLAN", "DATUV", "BMENG"]
stko_data = [
    # PL01, main alternative
    ("MAT001", "PL01", "BOM001", "01", "1", pd.Timestamp(date(2020, 1, 1)), 1.0),
    ("MAT002", "PL01", "BOM002", "01", "1", pd.Timestamp(date(2020, 1, 1)), 1.0),
    # PL02, different alternative for MAT001
    ("MAT001", "PL02", "BOM101", "01", "1", pd.Timestamp(date(2020, 1, 1)), 1.0),
    # PL01, alternative BOM for MAT001
    ("MAT001", "PL01", "BOM002A", "02", "1", pd.Timestamp(date(2021, 1, 1)), 1.0),
    # PL02, alternative BOM for MAT002
    ("MAT002", "PL02", "BOM202", "01", "1", pd.Timestamp(date(2020, 1, 1)), 1.0),
    # Existing
    ("MAT003", "PL01", "BOM003", "01", "1", pd.Timestamp(date(2020, 1, 1)), 1.0),
    ("MAT004", "PL01", "BOM004", "01", "1", pd.Timestamp(date(2020, 1, 1)), 1.0),
    ("MAT005", "PL01", "BOM005", "01", "1", pd.Timestamp(date(2020, 1, 1)), 1.0),
]
stko_df = pd.DataFrame(stko_data, columns=stko_columns)
stko_df.to_csv("source_data/STKO.csv", index=False)

## 3. Tabella STPO (componenti)


stpo_columns = ["STLNR", "POSNR", "IDNRK", "MENGE", "DATUV"]
stpo_data = [
    # MAT001's BOM (BOM001): MAT002 and MAT003 as components
    ("BOM001", 10, "MAT002", 2.0, pd.Timestamp(date(2020, 1, 1))),
    ("BOM001", 20, "MAT003", 4.0, pd.Timestamp(date(2020, 1, 1))),
    # MAT002's BOM (BOM002): MAT004 and MAT005 as components
    ("BOM002", 10, "MAT004", 1.0, pd.Timestamp(date(2020, 1, 1))),
    ("BOM002", 20, "MAT005", 3.0, pd.Timestamp(date(2020, 1, 1))),
    # MAT003's BOM (BOM003): MAT004 and MAT006 as components (MAT004 is also a parent, for multi-level)
    ("BOM003", 10, "MAT004", 2.0, pd.Timestamp(date(2020, 1, 1))),
    ("BOM003", 20, "MAT006", 1.0, pd.Timestamp(date(2020, 1, 1))),
    # MAT004's BOM (BOM004): MAT007 as a component
    ("BOM004", 10, "MAT007", 5.0, pd.Timestamp(date(2020, 1, 1))),
    # MAT005's BOM (BOM005): no components (leaf)
    # MAT006 and MAT007 are leaves (no BOMs)
]
stpo_df = pd.DataFrame(stpo_data, columns=stpo_columns)
stpo_df.to_csv('source_data/STPO.csv', index=False)

## 4. Tabella MAST (link MATNR → STLNR)

mast_columns = ["MATNR", "WERKS", "STLNR", "STLAL"]
mast_data = [
    ("MAT001", "PL01", "BOM001", "01"),
    ("MAT002", "PL01", "BOM002", "01"),
    ("MAT003", "PL01", "BOM003", "01"),
    ("MAT004", "PL01", "BOM004", "01"),
    ("MAT005", "PL01", "BOM005", "01"),
]
mast_df = pd.DataFrame(mast_data, columns=mast_columns)
mast_df.to_csv("source_data/MAST.csv", index=False)


## 5. Tabelle STAS / STZU (inizialmente vuote)

# STAS: per distinte alternative (empty)
stas_columns = ["STLNR", "STLAL", "PRIOR"]
stas_df = pd.DataFrame([], columns=stas_columns)
stas_df.to_csv("source_data/STAS.csv", index=False)

# STZU: modifiche e varianti (engineering change, empty)
stzu_columns = ["STLNR", "AENNR", "DATUV"]
stzu_df = pd.DataFrame([], columns=stzu_columns)
stzu_df.to_csv("source_data/STZU.csv", index=False)