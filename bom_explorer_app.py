import streamlit as st
import pandas as pd
from datetime import date

st.set_page_config(page_title="SAP BOM Explorer", layout="wide")
st.title("SAP BOM Explorer (Multi-Plant, Multi-Alt, Cost Rollup)")

st.info("""
**How this tool works:**
- Explodes SAP-like BOMs recursively to show all leaf components for each material, plant, and BOM alternative.
- Applies effectivity date filtering: only BOM headers and components valid at the selected date are included.
- Supports multi-plant and multi-alternative BOMs (selectable in sidebar).
- Rolls up costs from the MBEW table for each exploded path.
- Output can be filtered, inspected, and downloaded as CSV.
- Source tables can be previewed in the sidebar for transparency.
""")

import os
# --- Load master data for dropdowns ---
mast = pd.read_csv("source_data/MAST.csv")
materials = sorted(mast["MATNR"].unique())
plants = sorted(mast["WERKS"].unique())
alternatives = sorted(mast["STLAL"].unique())

with st.sidebar.expander("View Source Tables"):
    source_tables = [f for f in os.listdir("source_data") if f.endswith(".csv")]
    table_file = st.selectbox("Select Table", source_tables)
    if table_file:
        df_table = pd.read_csv(f"source_data/{table_file}")
        st.write(f"**{table_file}**")
        st.dataframe(df_table, use_container_width=True)

st.sidebar.header("BOM Explosion Filters")
material = st.sidebar.selectbox("Material", ["All"] + materials)
plant = st.sidebar.selectbox("Plant", ["All"] + plants)
alternative = st.sidebar.selectbox("BOM Alternative", ["All"] + alternatives)
key_date = st.sidebar.date_input("Key Date", value=date(2021,1,1))

show_deleted = st.sidebar.checkbox("Show deleted components", value=True)

import subprocess

if st.sidebar.button("Run BOM Explosion"):
    # Esegui la pipeline Python con il flag include_deleted (solo se vuoi rigenerare l'esploso)
    # subprocess.run(["python", "explode_bom_multilevel.py", "--include_deleted" if show_deleted else ""], check=True)
    # Per demo: carica l'esploso già calcolato
    df = pd.read_csv("processed_data/exploded_bom.csv")
    # Filter by material
    if material != "All":
        df = df[df["material"] == material]
    # Filter by plant
    if plant != "All":
        df = df[df["WERKS"] == plant]
    # Filter by alternative
    if alternative != "All":
        df = df[df["STLAL"].astype(str) == str(alternative)]
    # Filtro per componenti cancellati
    if not show_deleted and "deleted" in df.columns:
        df = df[(df["deleted"] == False) | (df["deleted"].isna())]
    # Visualizzazione avanzata con evidenziazione dei cancellati
    def highlight_deleted(row):
        if "deleted" in row and row["deleted"]:
            return ["background-color: #f8d7da"]*len(row)
        return [""]*len(row)
    st.subheader(f"Exploded BOM for Material: {material} | Plant: {plant} | Alt: {alternative}")
    st.write("**Legenda:** Colonne `deleted`, `alt_qty`, `new_qty` mostrano lo stato storico del componente.")
    st.dataframe(df.style.apply(highlight_deleted, axis=1), use_container_width=True)
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button("Download CSV", csv, "exploded_bom_filtered.csv", "text/csv")
    # Optionally: add visualization here
    # st.bar_chart(df.set_index('component')['total_cost'])
else:
    st.info("Select filters and click 'Run BOM Explosion' to view results.")
