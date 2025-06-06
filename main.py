import os
import pandas as pd
import numpy as np
import warnings, code
from datetime import date, timedelta
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
    df_orders['PO_Date'] = pd.to_datetime(df_orders['PO_Date'], errors='coerce', utc=True).dt.tz_convert('Asia/Hong_Kong').dt.date
    df_orders['Delivery_Date'] = pd.to_datetime(df_orders['Delivery_Date'], errors='coerce', utc=True).dt.tz_convert('Asia/Hong_Kong').dt.date

    # 1. Modify df_compute to fetch Daily Output and Finished Product Beg. Bal.
    df_compute = pd.read_excel(
        Plan,
        sheet_name="Computation",
        header=4
    )
    compute_cols = [
        'OFFICIAL PRODUCT CODE         (Use by Production)',
        'Delivery Date',
        'Ordered Qty.',
        "No. of Day's",
        'Target Start',
        'Daily Output',
        'Finished Product Beg. Bal.'
    ]
    df_compute = df_compute[compute_cols].dropna(subset=['Ordered Qty.'])
    df_compute.columns = [
        'Prod_Code',
        'Delivery_Date',
        'Quantity',
        'Days',
        'Target_Start',
        'Daily_Output',
        'Inventory'
    ]
    df_compute['Delivery_Date'] = pd.to_datetime(df_compute['Delivery_Date'], errors='coerce', utc=True).dt.tz_convert('Asia/Hong_Kong').dt.date
    df_compute['Target_Start'] = pd.to_datetime(df_compute['Target_Start'], errors='coerce', utc=True).dt.tz_convert('Asia/Hong_Kong').dt.date

    # 2. Modify merge to be case-insensitive for columns Prod_Code
    df_orders['Prod_Code_lower'] = df_orders['Prod_Code'].str.lower()
    df_orders['Delivery_Date_str'] = df_orders['Delivery_Date'].astype(str)
    df_orders['Quantity_str'] = df_orders['Quantity'].astype(str)

    df_compute['Prod_Code_lower'] = df_compute['Prod_Code'].str.lower()
    df_compute['Delivery_Date_str'] = df_compute['Delivery_Date'].astype(str)
    df_compute['Quantity_str'] = df_compute['Quantity'].astype(str)

    df_main = pd.merge(
        df_orders,
        df_compute,
        left_on=['Prod_Code_lower', 'Delivery_Date_str', 'Quantity_str'],
        right_on=['Prod_Code_lower', 'Delivery_Date_str', 'Quantity_str'],
        how='left',
        suffixes=('_order', '_compute')
    )

    # Restore original column names for downstream code
    df_main['Prod_Code'] = df_main['Prod_Code_order']
    df_main['Delivery_Date'] = pd.to_datetime(df_main['Delivery_Date_str']).dt.date
    df_main['Quantity'] = pd.to_numeric(df_main['Quantity_str'], errors='coerce')
    df_main['dStart'] = np.where(df_main['Target_Start'].notna(), df_main['Target_Start'], df_main['PO_Date'])
    df_main['dEnd'] = df_main['Delivery_Date']

    # 3. Fill Daily_Output nulls by searching df_compute by Prod_Code (case-insensitive)
    def fill_daily_output(row, df_compute):
        if pd.notnull(row['Daily_Output']):
            return row['Daily_Output']
        matches = df_compute[
            df_compute['Prod_Code_lower'] == str(row['Prod_Code']).lower()
        ]
        if not matches.empty:
            output = matches['Daily_Output'].dropna()
            if not output.empty:
                return output.iloc[0]
        return np.nan

    df_main['Daily_Output'] = df_main.apply(lambda row: fill_daily_output(row, df_compute), axis=1)

    # Inventory column is already in df_main, as merged

    # ========== Mold_End computation logic ==========
    def compute_mold_end(row):
        try:
            mold_start = pd.to_datetime(row['dStart'])
            quantity = row['Quantity']
            daily_output = row['Daily_Output']
            if pd.isnull(mold_start) or pd.isnull(quantity) or pd.isnull(daily_output) or daily_output == 0:
                return pd.NaT
            num_days = int(np.ceil(quantity / daily_output))
            return (mold_start + pd.Timedelta(days=num_days))
        except Exception:
            return pd.NaT

    df_main['Mold_End'] = df_main.apply(compute_mold_end, axis=1)

    # ========== Store Mold_End in df_job ==========
    df_job = df_main[['SO','PO','dStart','dEnd','Prod_Code','Quantity','Days','Daily_Output','Inventory','Mold_End']]

    df_movements = pd.read_excel(Lot, sheet_name="Lot Monitoring", header=2)
    df_movements = df_movements[['Part Code', 'Lot No.', 'QTY', 'Actual Date', 'Qty', 'DR Date', 'Qty Received', 'Actual Date.1', 'QTY.1', 'Actual Date.2', 'QTY.2','Actual Date.3', 'QTY.3', 'Date', 'Qty.1']].dropna(subset=['Part Code'])
    df_movements.columns = ['Prod_Code', 'Mold_date', 'Mold_Qty', 'Subcon_Date', 'Subcon_Qty', 'Receive_Date', 'Receive_Qty', 'Count_Date','Count_Qty','QC_Date','QC_Qty','Pack_Date','Pack_Qty','WHS_Date','WHS_Qty' ]
    df_movements['Lot_Num'] = df_movements['Mold_date']
    df_movements['Mold_date'] = df_movements['Mold_date'].str.split('-').str[0]
    df_movements['Mold_date'] = pd.to_datetime(df_movements['Mold_date'], format='%y%m%d')
    df_movements['Mold_start'] = df_movements['Mold_date'].dt.date

    for col in ['Mold_date','Subcon_Date','Receive_Date','Count_Date','QC_Date','Pack_Date','WHS_Date']:
        try:
            df_movements[col] = df_movements[col].dt.date
        except:
            df_movements[col] = pd.to_datetime(df_movements[col], errors='coerce', utc=True).dt.tz_convert('Asia/Hong_Kong').dt.date

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

        # Merge Daily_Output and Mold_End into main_move_df for this SO/prod code
        key_merge = pd.merge(df_export, main_move_df, how='inner', on='Prod_Code')

        # Convert to datetime for filtering
        key_merge['Mold_start_dt'] = pd.to_datetime(key_merge['Mold_start'])
        key_merge['dStart_dt'] = pd.to_datetime(key_merge['dStart'])
        key_merge['Mold_End_dt'] = pd.to_datetime(key_merge['Mold_End'])

        # --- EXTENDED LOGIC: Expand Mold_End if quantity > sum of Mold up to max 30 days ---
        # For each (SO, Prod_Code), adjust Mold_End if necessary
        updated_mold_ends = {}
        for idx, row in key_merge.iterrows():
            so = row['SO']
            prod_code = row['Prod_Code']
            quantity = row['Quantity']
            dstart = row['dStart_dt']
            orig_mold_end = row['Mold_End_dt']
            lot_num = row['Lot_Num']

            # Only check if values are valid
            if pd.isnull(dstart) or pd.isnull(orig_mold_end):
                updated_mold_ends[(so, prod_code)] = orig_mold_end
                continue

            # We want to increase Mold_End one day at a time
            # while sum of Mold < quantity, and maximum 30 days extension
            curr_end = orig_mold_end
            days_added = 0
            key_merge_mold = key_merge[key_merge['Source']=='Mold']
            while days_added < 30:
                # Select lots in the date window
                mask = (key_merge_mold['Prod_Code'] == prod_code) & \
                       (key_merge_mold['Mold_start_dt'] >= dstart) & \
                       (key_merge_mold['Mold_start_dt'] <= curr_end)
                sum_mold = key_merge_mold.loc[mask][['Lot_Num','Qty']].drop_duplicates()['Qty'].sum()
                if sum_mold >= quantity:
                    break
                # else, extend by 3 day
                curr_end += pd.Timedelta(days=1)
                days_added += 1
            updated_mold_ends[(so, prod_code)] = curr_end

        # Overwrite Mold_End_dt with the possibly extended value
        key_merge['Mold_End_dt'] = key_merge.apply(
            lambda row: updated_mold_ends.get((row['SO'], row['Prod_Code']), row['Mold_End_dt']),
            axis=1
        )

        #code.interact(local=locals())
        # Now use the extended Mold_End in list_filter
        list_filter = key_merge[
            (key_merge['Mold_start_dt'] >= key_merge['dStart_dt']) &
            (key_merge['Mold_start_dt'] <= key_merge['Mold_End_dt'])
        ][['Prod_Code','Lot_Num']]

        list_filter['merged'] = list_filter['Lot_Num']+list_filter['Prod_Code']
        key_merge['merged'] = key_merge['Lot_Num']+key_merge['Prod_Code']
        key_merge = key_merge[key_merge['merged'].isin(list_filter['merged'])]
        filtered = key_merge[(key_merge['Date'] >= key_merge['dStart'])]

        # Pivot table for both reports
        pivot_table = pd.pivot_table(
            filtered,
            index=['PO','dEnd', 'Prod_Code', 'Quantity', 'Lot_Num', 'Mold_start', 'Daily_Output'],
            columns='Source',
            values='Qty',
            aggfunc='sum',
            fill_value=0
        )
        desired_order = ['Mold', 'Subcon', 'Receive', 'Count', 'QA', 'Pack', 'WHS']
        for col in desired_order:
            if col not in pivot_table.columns:
                pivot_table[col] = 0
        pivot_table = pivot_table[desired_order]

        pivot_table['Rejects'] = pivot_table.apply(
            lambda row: compute_rejects_row(row, desired_order), axis=1
        )

        idx_cols = ['PO','dEnd', 'Prod_Code', 'Quantity', 'Lot_Num', 'Mold_start', 'Daily_Output']
        sum_cols = desired_order

        # Generate and save both reports
        print("Generating PROD HTML")
        html_prod = generate_prod_report(pivot_table, idx_cols, sum_cols, reject_col="Rejects", so_value=target)
        with open(f"./Output/PROD-{target}.html", "w") as f:
            f.write(html_prod)

        print("Generating BOSS HTML")
        html_boss = generate_boss_report(pivot_table, so_value=target)
        with open(f"./Output/BOSS-{target}.html", "w") as f:
            f.write(html_boss)
