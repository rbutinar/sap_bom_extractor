import pandas as pd
from datetime import date

# Set the key date for effectivity filtering
key_date = pd.Timestamp(date(2021, 1, 1))

def load_tables():
    stko = pd.read_csv('source_data/STKO.csv', parse_dates=['DATUV'])
    stpo = pd.read_csv('source_data/STPO.csv', parse_dates=['DATUV'])
    mast = pd.read_csv('source_data/MAST.csv')
    mbew = pd.read_csv('source_data/MBEW.csv')
    stas = pd.read_csv('source_data/STAS.csv')
    stzu = pd.read_csv('source_data/STZU.csv', parse_dates=['DATUV'])
    return stko, stpo, mast, mbew, stas, stzu

def get_preferred_variant(stlnr, stas, prefer_prior=1):
    variants = stas[stas['STLNR'] == stlnr]
    if variants.empty:
        return None
    # Scegli la variante con priorità più bassa (più importante)
    variant = variants.sort_values('PRIOR').iloc[0]
    return variant['STLAL']

def get_active_change(stlnr, stzu, key_date):
    changes = stzu[(stzu['STLNR'] == stlnr) & (stzu['DATUV'] <= key_date)]
    if changes.empty:
        return None
    change = changes.sort_values('DATUV', ascending=False).iloc[0]
    return change

def get_effective_bom_row(matnr, mast, stko, key_date, werks=None, stlal=None, stas=None, stzu=None):
    # Trova la variante preferita se non specificata
    if stlal is None and stas is not None:
        mast_rows = mast[(mast['MATNR'] == matnr)]
        if not mast_rows.empty:
            stlnr = mast_rows.iloc[0]['STLNR']
            stlal = get_preferred_variant(stlnr, stas)
    mast_rows = mast[mast['MATNR'] == matnr]
    if werks is not None:
        mast_rows = mast_rows[mast_rows['WERKS'] == werks]
    if stlal is not None:
        mast_rows = mast_rows[mast_rows['STLAL'] == stlal]
    if mast_rows.empty:
        return None, None
    merged = mast_rows.merge(stko, on=["STLNR", "STLAL", "WERKS"], how="left")
    valid = merged[merged['DATUV'] <= key_date]
    if valid.empty:
        return None, None
    row = valid.sort_values('DATUV', ascending=False).iloc[0]
    # Applica eventuale change (STZU)
    if stzu is not None:
        change = get_active_change(row['STLNR'], stzu, key_date)
        if change is not None:
            row['AENNR'] = change['AENNR']
            row['DATUV'] = change['DATUV']
    return row['STLNR'], row

def get_effective_components(bom_id, stpo, key_date, stas=None, include_deleted=False):
    # Se STAS è disponibile, usala come fonte principale per i componenti (con flag cancellazione e quantità storiche)
    if stas is not None:
        comps = stas[stas['STLNR'] == bom_id]
        if not include_deleted:
            comps = comps[comps['LOEKZ'] != 'X']
        # Restituisci colonne chiave: POSNR, IDNRK, ALT_MENGE, NEU_MENGE, LOEKZ
        if comps.empty:
            return comps
        return comps[['POSNR','IDNRK','ALT_MENGE','NEU_MENGE','LOEKZ']].copy()
    # Altrimenti fallback su STPO classico
    comps = stpo[stpo['STLNR'] == bom_id]
    valid = comps[comps['DATUV'] <= key_date]
    if valid.empty:
        return valid
    idx = valid.groupby('POSNR')['DATUV'].idxmax()
    return valid.loc[idx]

def explode_bom_leaves_with_qty(root_matnr, mast, stko, stpo, werks=None, stlal=None, root_bom=None, level=1, visited=None, leaves=None, qty=1.0, stas=None, stzu=None, include_deleted=False):
    if visited is None:
        visited = set()
    if leaves is None:
        leaves = []
    if root_matnr in visited:
        return leaves
    visited.add(root_matnr)
    bom_id, bom_row = get_effective_bom_row(root_matnr, mast, stko, key_date, werks=werks, stlal=stlal, stas=stas, stzu=stzu)
    if bom_id is None:
        # No BOM, treat as leaf
        leaves.append({'material': root_matnr, 'bom': root_bom, 'component': root_matnr, 'level': level-1, 'total_quantity': qty, 'deleted': False, 'alt_qty': qty, 'new_qty': qty})
        return leaves
    comps = get_effective_components(bom_id, stpo, key_date, stas=stas, include_deleted=include_deleted)
    if comps.empty:
        # BOM exists but no components: treat as leaf
        leaves.append({'material': root_matnr, 'bom': bom_id, 'component': root_matnr, 'level': level-1, 'total_quantity': qty, 'deleted': False, 'alt_qty': qty, 'new_qty': qty})
        return leaves
    for _, comp in comps.iterrows():
        comp_matnr = comp['IDNRK']
        comp_alt_qty = comp['ALT_MENGE'] if 'ALT_MENGE' in comp else None
        comp_new_qty = comp['NEU_MENGE'] if 'NEU_MENGE' in comp else None
        loekz = comp['LOEKZ'] if 'LOEKZ' in comp else ''
        deleted = (loekz == 'X') or (comp_new_qty == 0.0) if comp_new_qty is not None else False
        qty_to_use = comp_new_qty if comp_new_qty is not None else 1.0
        leaves.append({'material': root_matnr, 'bom': bom_id, 'component': comp_matnr, 'level': level, 'total_quantity': qty*qty_to_use, 'deleted': deleted, 'alt_qty': comp_alt_qty, 'new_qty': comp_new_qty})
        # Esplodi solo se il componente non è cancellato oppure se include_deleted è True
        if not deleted:
            explode_bom_leaves_with_qty(comp_matnr, mast, stko, stpo, werks=werks, stlal=stlal, root_bom=bom_id, level=level+1, visited=visited, leaves=leaves, qty=qty*qty_to_use, stas=stas, stzu=stzu, include_deleted=include_deleted)
    return leaves

def _explode_bom_path_with_qty(root_matnr, root_bom, current_matnr, mast, stko, stpo, level, visited, leaves, qty):
    if current_matnr in visited:
        return
    visited.add(current_matnr)
    bom_id, bom_row = get_effective_bom_row(current_matnr, mast, stko, key_date)
    if bom_row is None:
        leaves.append({'material': root_matnr, 'bom': root_bom, 'component': current_matnr, 'level': level-1, 'total_quantity': qty})
        return
    components = get_effective_components(bom_id, stpo, key_date)
    if components.empty:
        leaves.append({'material': root_matnr, 'bom': root_bom, 'component': current_matnr, 'level': level-1, 'total_quantity': qty})
        return
    for _, row in components.iterrows():
        comp = row['IDNRK']
        comp_qty = row['MENGE']
        _explode_bom_path_with_qty(root_matnr, root_bom, comp, mast, stko, stpo, level+1, set(visited), leaves, qty * comp_qty)

def main(include_deleted=False):
    stko, stpo, mast, mbew, stas, stzu = load_tables()
    all_leaves = []
    # Loop over all (material, plant, alternative) combinations
    for _, mast_row in mast.iterrows():
        matnr = mast_row['MATNR']
        werks = mast_row['WERKS']
        stlal = mast_row['STLAL']
        leaves = explode_bom_leaves_with_qty(matnr, mast, stko, stpo, werks=werks, stlal=stlal, level=0, stas=stas, stzu=stzu, include_deleted=include_deleted)
        # Add plant and alternative to each result
        for leaf in leaves:
            leaf['WERKS'] = werks
            leaf['STLAL'] = stlal
        all_leaves.extend(leaves)
    df_leaves = pd.DataFrame(all_leaves)

    # Add cost info: lookup cost for each leaf component by MATNR and BWKEY (plant)
    df_leaves = df_leaves.merge(mbew[['MATNR', 'BWKEY', 'STPRS']], left_on=['component', 'WERKS'], right_on=['MATNR', 'BWKEY'], how='left')
    df_leaves['total_cost'] = df_leaves['total_quantity'] * df_leaves['STPRS']
    df_leaves = df_leaves.drop(['MATNR', 'BWKEY', 'STPRS'], axis=1)

    print(df_leaves)
    df_leaves.to_csv('processed_data/exploded_bom.csv', index=False)
    print('\nExploded BOM saved to processed_data/exploded_bom.csv')

if __name__ == "__main__":
    import sys
    include_deleted = False
    if len(sys.argv) > 1 and sys.argv[1] == "--include_deleted":
        include_deleted = True
    main(include_deleted=include_deleted)
