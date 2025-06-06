import os
import pandas as pd
import numpy as np
import warnings
from datetime import date
from generate_prod_report import generate_prod_report
from generate_boss_report import generate_boss_report

warnings.filterwarnings("ignore")

def fetch_config(filepath="config.txt"):
    config_vars = {}
    if not os.path.exists(filepath):
        print(f"Warning: Configuration file not found at '{filepath}'.")
        return config_vars
    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    name, value = line.split('=', 1)
                    config_vars[name.strip()] = value.strip()
                else:
                    print(f"Warning: Invalid line format in '{filepath}': '{line}'. Skipping.")
    except Exception as e:
        print(f"Error reading configuration file '{filepath}': {e}")
    return config_vars

def custom_agg(series):
    if pd.api.types.is_numeric_dtype(series):
        return series.sum()
    elif pd.api.types.is_datetime64_any_dtype(series):
        return series.max()
    elif all(isinstance(item, date) for item in series.unique()):
        return series.max()
    else:
        return '<br>'.join(str(item) for item in series.unique())

stage_col_names = ["Mold", "Subcon", "Receive", "Count", "QA", "Pack", "WHS"]

def compute_rejects_row(row, stage_cols):
    mold_value = row['Mold'] if pd.notnull(row['Mold']) else 0
    last_val = 0
    for col in stage_cols[1:]:
        val = row[col] if pd.notnull(row[col]) else 0
        if val > 0:
            last_val = val
    diff = mold_value - last_val
    return diff if diff > 0 else 0

if __name__ == "__main__":
    config = fetch_config()
    Plan = config.get("Plan")
    Lot = config.get("Lot")
    targets = eval(config.get("targets"))
    df_orders = pd.read_excel(Plan, sheet_name="PurchaseOrder", header=1)
    df_orders = df_orders[['Sales Order No.','P/O DATE','PO#','PRODUCT CODE','P/O QTY','Target Del. Date']].dropna(subset=['Sales Order No.'])
    df_orders.columns = ['SO', 'PO_Date', 'PO', 'Prod_Code','Quantity','Delivery_Date']
    df_orders['PO_Date'] = pd.to_datetime(df_orders['PO_Date'], errors='coerce').dt.date
    df_orders['Delivery_Date'] = pd.to_datetime(df_orders['Delivery_Date'], errors='coerce').dt.date

    # --- CHANGED SECTION: Add Daily Output and Inventory ---
    df_compute = pd.read_excel(Plan, sheet_name="Computation", header=4)
    df_compute = df_compute[['OFFICIAL PRODUCT CODE         (Use by Production)',
                             'Delivery Date',
                             'Ordered Qty.',
                             "No. of Day's",
                             'Target Start',
                             'Daily Output',
                             'Finished Product Beg. Bal.']].dropna(subset=['Ordered Qty.'])
    df_compute.columns = ['Prod_Code', 'Delivery_Date', 'Quantity', 'Days', 'Target_Start', 
        'Daily_Output', 'Inventory']
    df_compute['Delivery_Date'] = pd.to_datetime(df_compute['Delivery_Date'], errors='coerce').dt.date
    df_compute['Target_Start'] = pd.to_datetime(df_compute['Target_Start'], errors='coerce').dt.date

    df_movements = pd.read_excel(Lot, sheet_name="Lot Monitoring", header=2)
    df_movements = df_movements[['Part Code', 'Lot No.', 'QTY', 'Actual Date', 'Qty', 'DR Date',
        'Qty Received', 'Actual Date.1', 'QTY.1', 'Actual Date.2', 'QTY.2','Actual Date.3', 'QTY.3',
        'Date', 'Qty.1']].dropna(subset=['Part Code'])
    df_movements.columns = ['Prod_Code', 'Mold_date', 'Mold_Qty', 'Subcon_Date', 'Subcon_Qty',
        'Receive_Date', 'Receive_Qty', 'Count_Date','Count_Qty','QC_Date','QC_Qty','Pack_Date','Pack_Qty',
        'WHS_Date','WHS_Qty' ]
    df_movements['Lot_Num'] = df_movements['Mold_date']
    df_movements['Mold_date'] = df_movements['Mold_date'].str.split('-').str[0]
    df_movements['Mold_date'] = pd.to_datetime(df_movements['Mold_date'], format='%y%m%d')
    df_movements['Mold_start'] = df_movements['Mold_date'].dt.date

    for col in ['Mold_date','Subcon_Date','Receive_Date','Count_Date','QC_Date','Pack_Date','WHS_Date']:
        try:
            df_movements[col] = df_movements[col].dt.date
        except:
            df_movements[col] = pd.to_datetime(df_movements[col], errors='coerce').dt.date

    # Merge Daily_Output and Inventory from df_compute into main
    df_main = pd.merge(df_orders, df_compute, left_on=['Prod_Code','Delivery_Date','Quantity'], right_on=['Prod_Code','Delivery_Date','Quantity'], how='left')
    df_main['dStart'] = np.where(df_main['Target_Start'].notna(), df_main['Target_Start'], df_main['PO_Date'])
    df_main['dEnd'] = df_main['Delivery_Date']
    # --- CHANGED SECTION: include Daily_Output and Inventory ---
    df_job = df_main[['SO','PO','dStart','dEnd','Prod_Code','Quantity','Days','Daily_Output','Inventory']]

    mold_df = df_movements[['Prod_Code', 'Lot_Num', 'Mold_start', 'Mold_date','Mold_Qty']].dropna(subset=['Mold_date'])
    subcon_df = df_movements[['Prod_Code', 'Lot_Num', 'Mold_start','Subcon_Date','Subcon_Qty']].dropna(subset=['Subcon_Date'])
    receive_df = df_movements[['Prod_Code', 'Lot_Num','Mold_start','Receive_Date','Receive_Qty']].dropna(subset=['Receive_Date'])
    count_df = df_movements[['Prod_Code', 'Lot_Num','Mold_start','Count_Date','Count_Qty']].dropna(subset=['Count_Date'])
    qc_df = df_movements[['Prod_Code', 'Lot_Num','Mold_start','QC_Date','Count_Qty']].dropna(subset=['QC_Date'])
    pack_df = df_movements[['Prod_Code', 'Lot_Num','Mold_start','Pack_Date','Count_Qty']].dropna(subset=['Pack_Date'])
    whs_df = df_movements[['Prod_Code', 'Lot_Num','Mold_start','WHS_Date','Count_Qty']].dropna(subset=['WHS_Date'])

    mold_df['Source'] = 'Mold'
    subcon_df['Source'] = 'Subcon'
    receive_df['Source'] = 'Receive'
    count_df['Source'] = 'Count'
    qc_df['Source'] = 'QA'
    pack_df['Source'] = 'Pack'
    whs_df['Source'] = 'WHS'

    column_val = ['Prod_Code', 'Lot_Num','Mold_start','Date','Qty','Source']
    mold_df.columns = column_val
    subcon_df.columns = column_val
    receive_df.columns = column_val
    count_df.columns = column_val
    qc_df.columns = column_val
    pack_df.columns = column_val
    whs_df.columns = column_val

    main_move_df = pd.concat([mold_df,subcon_df,receive_df,count_df,qc_df,pack_df,whs_df])
    main_move_df['Qty'] = np.ceil(main_move_df['Qty'])

    for target in targets:
        print("Processing: " + target)
        df_export = df_job[df_job['SO']==target]
        try:
            df_export = df_export.groupby(['SO', 'Prod_Code']).agg(custom_agg).reset_index()
        except:
            with open(f"./Output/{target}.txt", "w") as z:
                z.write("SO Does not Exist")
            continue

        key_merge = pd.merge(df_export, main_move_df, how='inner', on='Prod_Code')
        list_filter = key_merge[
            ((key_merge['Source'] == "Mold") &
             (key_merge['Mold_start'] >= key_merge['dStart']))][['Prod_Code','Lot_Num']]
        list_filter['merged'] = list_filter['Lot_Num']+list_filter['Prod_Code']
        key_merge['merged'] = key_merge['Lot_Num']+key_merge['Prod_Code']
        key_merge = key_merge[key_merge['merged'].isin(list_filter['merged'])]
        filtered = key_merge[(key_merge['Date'] >= key_merge['dStart'])]

        # --- CHANGED: propagate Daily_Output to filtered ---
        filtered = pd.merge(filtered, df_job[['Prod_Code', 'Daily_Output']], on='Prod_Code', how='left')

        # Pivot table for both reports
        pivot_table = pd.pivot_table(filtered,
                                    index=['PO','dEnd', 'Prod_Code', 'Quantity', 'Lot_Num', 'Mold_start'],
                                    columns='Source',
                                    values='Qty',
                                    aggfunc='sum',
                                    fill_value=0)
        desired_order = ['Mold', 'Subcon', 'Receive', 'Count', 'QA', 'Pack', 'WHS']
        for col in desired_order:
            if col not in pivot_table.columns:
                pivot_table[col] = 0
        pivot_table = pivot_table[desired_order]

        pivot_table['Rejects'] = pivot_table.apply(
            lambda row: compute_rejects_row(row, desired_order), axis=1
        )

        # --- CHANGED: add Daily_Output to pivot_table for reporting ---
        # We want to retain Daily_Output per (PO, dEnd, Prod_Code, Quantity, Lot_Num, Mold_start)
        # So, set index and merge in the value
        idx_cols = ['PO','dEnd', 'Prod_Code', 'Quantity', 'Lot_Num', 'Mold_start']
        pivot_table = pivot_table.reset_index()
        # Get unique Daily_Output per row
        daily_output_map = filtered.groupby(idx_cols)['Daily_Output'].first().reset_index()
        pivot_table = pd.merge(pivot_table, daily_output_map, on=idx_cols, how='left')
        pivot_table.set_index(idx_cols, inplace=True)

        sum_cols = desired_order

        print("Generating PROD HTML")
        html_prod = generate_prod_report(pivot_table, idx_cols, sum_cols, reject_col="Rejects", so_value=target)
        with open(f"./Output/PROD-{target}.html", "w") as f:
            f.write(html_prod)

        print("Generating BOSS HTML")
        html_boss = generate_boss_report(pivot_table, so_value=target)
        with open(f"./Output/BOSS-{target}.html", "w") as f:
            f.write(html_boss)
