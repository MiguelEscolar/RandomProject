import pandas as pd

def generate_boss_report(pivot_table, so_value=None):
    group_cols = ['PO', 'Prod_Code', 'Quantity']
    sum_cols = ['Mold', 'Subcon', 'Receive', 'Count', 'QA', 'Pack', 'WHS', 'Rejects']

    grouped = (
        pivot_table
        .reset_index()
        .groupby(group_cols)[sum_cols]
        .sum()
        .reset_index()
    )
    # Find earliest (min) Mold_start for date calculations
    mold_start_min = (
        pivot_table
        .reset_index()
        .groupby(group_cols)['Mold_start']
        .min()
        .reset_index()
    )
    # Add Mold_start (as the new column)
    grouped = pd.merge(grouped, mold_start_min, on=group_cols, how='left')

    # Calculate date columns (all as "m/d" string)
    grouped['Mold_start_fmt'] = pd.to_datetime(grouped['Mold_start'], errors='coerce').apply(
        lambda val: f"{val.month}/{val.day}" if pd.notnull(val) else ""
    )
    grouped['Mold_end'] = pd.to_datetime(grouped['Mold_start'], errors='coerce')  # Mold End = Mold_start as datetime
    grouped['Subcon_target'] = grouped['Mold_end'] + pd.Timedelta(days=2)
    grouped['Receive_target'] = grouped['Subcon_target'] + pd.Timedelta(days=14)
    grouped['Count_target'] = grouped['Receive_target'] + pd.Timedelta(days=3)
    grouped['QC_target'] = grouped['Count_target'] + pd.Timedelta(days=3)
    grouped['Pack_target'] = grouped['QC_target'] + pd.Timedelta(days=3)

    def format_md(val):
        try:
            if pd.isnull(val):
                return ""
            val = pd.to_datetime(val)
            return f"{val.month}/{val.day}"
        except Exception:
            return ""
    date_cols = ['Mold_end', 'Subcon_target', 'Receive_target', 'Count_target', 'QC_target', 'Pack_target']
    for col in date_cols:
        grouped[col] = grouped[col].apply(format_md)

    # HTML columns (include Quantity after Prod_Code, Mold Start before Mold, Mold End)
    headers = [
        'PO', 'Product Code', 'Quantity',
        'Mold Start', 'Mold', 'Mold End',
        'Subcon', 'Subcon Target',
        'Receive', 'Receive Target',
        'Count', 'Count Target',
        'QA', 'QA Target',
        'Pack', 'Pack Target',
        'WHS', 'Rejects'
    ]

    col_classes = [
        'col-po', 'col-prodcode', 'col-qty',
        'col-moldstart', 'col-mold', 'col-molddate',
        'col-subcon', 'col-subcondate',
        'col-receive', 'col-receivedate',
        'col-count', 'col-countdate',
        'col-qa', 'col-qadate',
        'col-pack', 'col-packdate',
        'col-whs', 'col-rejects'
    ]

    # Columns that should share the same width: Mold Start, Mold, Mold End, Subcon, Subcon Target, ..., WHS
    process_width_class = "process-width"
    # All columns from 'Mold Start' (index 3) to 'WHS' (index 15), inclusive, EXCEPT "Rejects"
    process_indices = list(range(3, 16))

    # Columns that are date/target columns
    target_cols = ['col-moldstart', 'col-molddate', 'col-subcondate', 'col-receivedate',
                   'col-countdate', 'col-qadate', 'col-packdate']

    col_class_set = []
    for idx, c in enumerate(col_classes):
        class_str = c
        if idx in process_indices:
            class_str += f' {process_width_class}'
        if c in target_cols:
            class_str += ' target-bg'
        col_class_set.append(class_str)

    html = []
    if so_value is not None:
        html.append(
            f'<div style="text-align:center;margin-bottom:2px;background:#163D66;padding:10px 0;">'
            f'<span style="color:#fff;font-weight:bold;font-size:1.3em;">SO: {so_value} (BOSS)</span>'
            f'</div>'
        )
    html.append('<table id="boss_report_table" class="report-table">')
    html.append('<thead><tr>')
    for i, col in enumerate(headers):
        th_class = col_class_set[i]
        html.append(f'<th class="{th_class} boss-th"><div class="header-wrap">{col.replace("Date", "Target")}</div></th>')
    html.append('</tr></thead><tbody>')

    def format_num(val):
        try:
            return f"{int(val):,}"
        except Exception:
            return "0"

    for _, row in grouped.iterrows():
        html.append('<tr>')
        html.append(f'<td class="col-po blue-bg bold-cell">{row["PO"]}</td>')
        html.append(f'<td class="col-prodcode blue-bg bold-cell">{row["Prod_Code"]}</td>')
        html.append(f'<td class="col-qty blue-bg bold-cell">{format_num(row["Quantity"])}</td>')
        html.append(f'<td class="col-moldstart target-bg process-width">{row["Mold_start_fmt"]}</td>')
        html.append(f'<td class="col-mold process-width">{format_num(row["Mold"])}</td>')
        html.append(f'<td class="col-molddate target-bg process-width">{row["Mold_end"]}</td>')
        html.append(f'<td class="col-subcon process-width">{format_num(row["Subcon"])}</td>')
        html.append(f'<td class="col-subcondate target-bg process-width">{row["Subcon_target"]}</td>')
        html.append(f'<td class="col-receive process-width">{format_num(row["Receive"])}</td>')
        html.append(f'<td class="col-receivedate target-bg process-width">{row["Receive_target"]}</td>')
        html.append(f'<td class="col-count process-width">{format_num(row["Count"])}</td>')
        html.append(f'<td class="col-countdate target-bg process-width">{row["Count_target"]}</td>')
        html.append(f'<td class="col-qa process-width">{format_num(row["QA"])}</td>')
        html.append(f'<td class="col-qadate target-bg process-width">{row["QC_target"]}</td>')
        html.append(f'<td class="col-pack process-width">{format_num(row["Pack"])}</td>')
        html.append(f'<td class="col-packdate target-bg process-width">{row["Pack_target"]}</td>')
        html.append(f'<td class="col-whs process-width">{format_num(row["WHS"])}</td>')
        html.append(f'<td class="col-rejects rejects-col">{format_num(row["Rejects"])}</td>')
        html.append('</tr>')
    html.append('</tbody></table>')

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
        width: 95vw;
        max-width: 95vw;
        table-layout: fixed;
        border-collapse: collapse;
        margin-bottom: 20px;
        box-shadow: 3px 3px 7px rgba(0, 0, 0, 0.2);
        border: 2px solid #2c3e50;
        min-width: 0;
        font-size: 1em;
    }
    th.boss-th {
        background-color: #3498db !important;
        color: yellow !important;
        font-weight: bold !important;
        font-size: 0.85em !important; /* 15% smaller */
        word-break: break-word;
        white-space: normal;
        padding: 6px;
        vertical-align: top;
    }
    /* Wrap header text, avoid overlap */
    .header-wrap {
        word-break: break-word;
        white-space: normal;
        line-height: 1.14;
        display: block;
        width: 100%;
        overflow-wrap: break-word;
    }
    /* Prevent header hover effect */
    th.boss-th:hover, thead:hover th.boss-th {
        background-color: #3498db !important;
        color: yellow !important;
    }
    /* Text wrap for all td and th */
    td, th {
        word-break: break-word;
        white-space: normal;
        overflow-wrap: break-word;
    }
    td {
        border: 1.5px solid #34495e;
        padding: 6px;
        text-align: center;
        vertical-align: top;
        font-size: 1em;
    }
    .blue-bg, .col-po, .col-prodcode, .col-qty {
        /* 20% darker than #e2f0fb = #b4d1ed */
        background: #b4d1ed !important;
        color: #222 !important;
        font-weight: bold !important;
    }
    /* Width Tweaks */
    .col-po, th.col-po {
        min-width: 55.44px;  /* 50.4px * 1.1 */
        max-width: 83.16px;  /* 75.6px * 1.1 */
        width: 64.68px;      /* 58.8px * 1.1 */
    }
    .col-prodcode, th.col-prodcode {
        min-width: 187.2px;   /* 156px * 1.2 */
        max-width: 288px;     /* 240px * 1.2 */
        width: 216px;         /* 180px * 1.2 */
    }
    .col-qty, th.col-qty {
        min-width: 38.4px;  /* 64px * 0.6 */
        max-width: 57.6px;  /* 96px * 0.6 */
        width: 48px;        /* 80px * 0.6 */
    }
    /* Mold Start and all process columns (Mold, Mold End, Subcon, Subcon Target, ..., WHS) share the same width, 40% smaller */
    .process-width, th.process-width,
    .col-moldstart, th.col-moldstart,
    .col-mold, th.col-mold,
    .col-molddate, th.col-molddate,
    .col-subcon, th.col-subcon,
    .col-subcondate, th.col-subcondate,
    .col-receive, th.col-receive,
    .col-receivedate, th.col-receivedate,
    .col-count, th.col-count,
    .col-countdate, th.col-countdate,
    .col-qa, th.col-qa,
    .col-qadate, th.col-qadate,
    .col-pack, th.col-pack,
    .col-packdate, th.col-packdate,
    .col-whs, th.col-whs {
        min-width: 42px;   /* 70px * 0.6 */
        max-width: 66px;   /* 110px * 0.6 */
        width: 51px;       /* 85px * 0.6 */
    }
    /* 15% darker than #eee = #cfcfcf */
    .target-bg, .col-molddate, .col-subcondate, .col-receivedate, .col-countdate, .col-qadate, .col-packdate, .col-moldstart {
        background: #cfcfcf !important;
        color: #222 !important;
    }
    .bold-cell {
        font-weight: bold !important;
    }
    .rejects-col, .col-rejects {
        width: 57.6px;     /* 64px * 0.9 */
        min-width: 52.2px; /* 58px * 0.9 */
        max-width: 72px;   /* 80px * 0.9 */
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
    thead th {
        user-select: none;
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
    /* Hover effect for all cells except blue-bg columns */
    tr:hover td:not(.blue-bg) {
        background-color: #333 !important;
        color: white !important;
    }
    /* Don't highlight the date columns with hover, keep the target-bg color */
    tr:hover .target-bg,
    tr:hover .col-molddate,
    tr:hover .col-subcondate,
    tr:hover .col-receivedate,
    tr:hover .col-countdate,
    tr:hover .col-qadate,
    tr:hover .col-packdate,
    tr:hover .col-moldstart {
        background: #cfcfcf !important;
        color: #222 !important;
    }
    /* Don't highlight PO, Product Code, Quantity columns on hover */
    tr:hover .col-po,
    tr:hover .col-prodcode,
    tr:hover .col-qty {
        background: #b4d1ed !important;
        color: #222 !important;
        font-weight: bold !important;
    }
    tr:hover .rejects-col {
        background-color: #b80000 !important;
        color: #fff600 !important;
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
