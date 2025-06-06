import pandas as pd

def generate_prod_report(pivot_table, idx_cols, sum_cols, reject_col="Rejects", so_value=None):
    df = pivot_table.reset_index()
    # Drop SO column if it exists in index columns and dataframe
    if 'SO' in df.columns:
        df = df.drop(columns=['SO'])
    idx_cols_no_so = [col for col in idx_cols if col != 'SO']
    columns = idx_cols_no_so + sum_cols + [reject_col]

    group_cols = ['PO', 'dEnd', 'Prod_Code', 'Quantity']
    # Add helper for group merge
    df['_group_row'] = df.groupby(group_cols).cumcount()
    df['_group_rowspan'] = df.groupby(group_cols)[group_cols[0]].transform('count')

    html = []
    # Blue SO caption above headers, centered, bold
    if so_value is not None:
        html.append(
            f'<div style="text-align:center;margin-bottom:2px;background:#163D66;padding:10px 0;">'
            f'<span style="color:#fff;font-weight:bold;font-size:1.3em;">SO: {so_value}</span>'
            f'</div>'
        )

    html.append('<table id="report_table" class="report-table">')
    html.append('<thead><tr>')
    for col in columns:
        if col in sum_cols:
            html.append(f'<th class="stage-col-{col}">{col}</th>')
        elif col in ['dEnd', 'Mold_start']:
            html.append(f'<th class="date-col">{col}</th>')
        elif col == reject_col:
            html.append(f'<th class="rejects-col">{col}</th>')
        else:
            html.append(f'<th>{col}</th>')
    html.append('</tr></thead><tbody>')

    for idx, row in df.iterrows():
        html.append('<tr>')
        # PO, dEnd, Prod_Code, Quantity: only render at first of group
        for col in ['PO', 'dEnd', 'Prod_Code', 'Quantity']:
            if row['_group_row'] == 0:
                rowspan = int(row['_group_rowspan'])
                cell_value = row[col]
                if col == 'Prod_Code':
                    cell_value = f"<b>{cell_value}</b>"
                style_extra = ' date-col' if col == 'dEnd' else ''
                html.append(f'<td class="merge-group{style_extra}{ " prod-code" if col=="Prod_Code" else "" }" rowspan="{rowspan}">{cell_value}</td>')
        # Lot_Num, Mold_start (no merge)
        html.append(f'<td>{row["Lot_Num"]}</td>')
        html.append(f'<td class="date-col">{row["Mold_start"]}</td>')
        # Data columns
        for col in sum_cols:
            html.append(f'<td>{int(row[col]) if pd.notnull(row[col]) else 0}</td>')
        # Rejects column
        html.append(f'<td class="rejects-col">{int(row[reject_col]) if pd.notnull(row[reject_col]) else 0}</td>')
        html.append('</tr>')

        # Total row after group
        if row['_group_row'] == row['_group_rowspan'] - 1:
            group_mask = (
                (df['PO'] == row['PO']) &
                (df['dEnd'] == row['dEnd']) &
                (df['Prod_Code'] == row['Prod_Code']) &
                (df['Quantity'] == row['Quantity'])
            )
            group_rows = df[group_mask]
            totals = group_rows[sum_cols + [reject_col]].sum()
            total_row = '<tr style="background-color:#222;color:yellow;font-weight:bold;vertical-align:top;">'
            total_row += f'<td class="merge-group" colspan="4">Total</td>'
            total_row += '<td></td><td></td>'
            for col in sum_cols:
                total_row += f'<td>{int(totals[col]) if pd.notnull(totals[col]) else 0}</td>'
            total_row += f'<td class="rejects-col">{int(totals[reject_col]) if pd.notnull(totals[reject_col]) else 0}</td>'
            total_row += '</tr>'
            html.append(total_row)
    html.append('</tbody></table>')

    # CSS Styling
    style = """
    <style>
    html, body {
        height: 100%;
        margin: 0;
        padding: 0;
        width: 100vw;
        box-sizing: border-box;
        overflow-x: hidden;
    }
    body {
        font-family: sans-serif;
        width: 100vw;
        min-width: 0;
        margin: 0;
        padding: 0;
        box-sizing: border-box;
        font-size: 0.85em;
    }
    table {
        width: 90vw;
        max-width: 90vw;
        table-layout: fixed;
        border-collapse: collapse;
        margin-bottom: 20px;
        box-shadow: 3px 3px 7px rgba(0, 0, 0, 0.2);
        border: 2px solid #2c3e50;
        min-width: 0;
        font-size: 1em;
    }
    th, td {
        border: 1.5px solid #34495e;
        padding: 6px;
        text-align: center;
        vertical-align: top;
        overflow-wrap: break-word;
        word-break: break-word;
        text-overflow: unset;
        white-space: normal;
        font-size: 1em;
    }
    .stage-col {
        width: 36px;
        min-width: 28px;
        max-width: 42px;
        font-size: 1em;
    }
    .date-col {
        width: 148px;
        min-width: 110px;
        max-width: 200px;
        font-size: 1em;
    }
    .rejects-col {
        width: 64px;
        min-width: 58px;
        max-width: 80px;
        background-color: #ffeaea !important;
        color: #c00 !important;
        font-weight: bold !important;
    }
    thead {
        position: sticky;
        top: 0;
        background-color: #3498db;
        z-index: 1;
    }
    th {
        background-color: #3498db;
        color: yellow;
    }
    thead th:first-child {
        background-color: #3498db;
        color: yellow;
    }
    .merge-group {
        background-color: #ededed !important;
        color: #222 !important;
        vertical-align: top !important;
    }
    .prod-code {
        font-weight: bold !important;
    }
    tr[style*="background-color:#222"] td {
        background-color: #222 !important;
        color: yellow !important;
        font-weight: bold !important;
        border-top: 2px solid #ff0;
        vertical-align: top !important;
    }
    tr[style*="background-color:#222"] .rejects-col {
        background-color: #b80000 !important;
        color: #fff600 !important;
    }
    tr:hover td:not(.merge-group) {
        background-color: #333 !important;
        color: white !important;
    }
    caption {
        caption-side: top;
        font-size: 1.2em;
        font-weight: bold;
        margin-bottom: 10px;
        color: #333;
    }
    </style>
    """

    return f"<html><head>{style}</head><body>{''.join(html)}</body></html>"
