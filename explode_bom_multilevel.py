import pandas as pd
from datetime import date

def load_tables():
    stko = pd.read_parquet('data/STKO/stko.parquet')
    stpo = pd.read_parquet('data/STPO/stpo.parquet')
    mast = pd.read_parquet('data/MAST/mast.parquet')
    return stko, stpo, mast

def explode_bom_leaves(root_matnr, mast, stpo, root_bom=None, level=1, visited=None, leaves=None):
    # Recursively collect ONLY leaf components for a given root material
    if visited is None:
        visited = set()
    if leaves is None:
        leaves = []
    if root_matnr in visited:
        return leaves
    visited.add(root_matnr)
    mast_row = mast[mast['MATNR'] == root_matnr]
    if mast_row.empty:
        return leaves
    bom_id = mast_row.iloc[0]['STLNR']
    if root_bom is None:
        root_bom = bom_id
    components = stpo[stpo['STLNR'] == bom_id]['IDNRK'].tolist()
    if not components:
        leaves.append({'material': root_matnr, 'bom': root_bom, 'component': root_matnr, 'level': level-1})
        return leaves
    for comp in components:
        # For each branch, follow all paths from the root material
        _explode_bom_path(root_matnr, root_bom, comp, mast, stpo, level+1, set(visited), leaves)
    return leaves

def _explode_bom_path(root_matnr, root_bom, current_matnr, mast, stpo, level, visited, leaves):
    # Helper for path traversal, always keeps the root material and root BOM
    if current_matnr in visited:
        return
    visited.add(current_matnr)
    mast_row = mast[mast['MATNR'] == current_matnr]
    if mast_row.empty:
        # If not found as a parent, treat as a leaf
        leaves.append({'material': root_matnr, 'bom': root_bom, 'component': current_matnr, 'level': level-1})
        return
    bom_id = mast_row.iloc[0]['STLNR']
    components = stpo[stpo['STLNR'] == bom_id]['IDNRK'].tolist()
    if not components:
        leaves.append({'material': root_matnr, 'bom': root_bom, 'component': current_matnr, 'level': level-1})
        return
    for comp in components:
        _explode_bom_path(root_matnr, root_bom, comp, mast, stpo, level+1, set(visited), leaves)

def main():
    stko, stpo, mast = load_tables()
    all_leaves = []
    for matnr in mast['MATNR'].unique():
        leaves = explode_bom_leaves(matnr, mast, stpo, level=0)
        all_leaves.extend(leaves)
    df_leaves = pd.DataFrame(all_leaves)
    print(df_leaves)
    df_leaves.to_csv('exploded_bom.csv', index=False)
    print('\nExploded BOM saved to exploded_bom.csv')

if __name__ == "__main__":
    main()
