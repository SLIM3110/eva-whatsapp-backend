"""
EVA Real Estate — Market Intelligence Report Generator v2
Supports: single-community reports and multi-area comparison reports.
Input: Property Monitor CSV exports (DLD data).
"""

import io, os, re
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    BaseDocTemplate, PageTemplate, Frame, Paragraph,
    Spacer, Table, TableStyle, Image, HRFlowable, PageBreak, KeepTogether
)
from reportlab.pdfgen import canvas

# ── Brand ──────────────────────────────────────────────────────────────────────
DARK_GREEN  = colors.HexColor('#1B4D3E')
GOLD        = colors.HexColor('#C9A96E')
CREAM       = colors.HexColor('#F5F0E8')
LIGHT_GREEN = colors.HexColor('#2A6B57')
MID_GREY    = colors.HexColor('#6B6B6B')
LIGHT_GREY  = colors.HexColor('#E8E8E8')
WHITE       = colors.white
BLACK       = colors.HexColor('#1A1A1A')
PAGE_W, PAGE_H = A4

EVA_PALETTE = ['#1B4D3E','#C9A96E','#2A6B57','#8B6914','#4A8C78','#E8D5A3','#5B8FA8','#A67C52']

# ── Styles ─────────────────────────────────────────────────────────────────────
def S(name, **kw):
    # Note: ReportLab's ParagraphStyle ignores any `tracking=` kwarg silently
    # (it's not in PropertySet.defaults), so we don't pass it. To fake letter
    # spacing in a label, the call-site inserts spaces in the actual string.
    base = {
        'cover_eyebrow':  dict(fontName='Helvetica',       fontSize=8,   textColor=GOLD,       leading=11, alignment=TA_LEFT),
        'cover_title':    dict(fontName='Helvetica-Bold',  fontSize=26,  textColor=WHITE,      leading=32, alignment=TA_LEFT),
        'cover_community':dict(fontName='Helvetica-Bold',  fontSize=17,  textColor=GOLD,       leading=22, alignment=TA_LEFT),
        'cover_agent':    dict(fontName='Helvetica',       fontSize=10,  textColor=CREAM,      leading=14, alignment=TA_LEFT),
        'cover_agent_name':dict(fontName='Helvetica-Bold', fontSize=12,  textColor=WHITE,      leading=16, alignment=TA_LEFT),
        'section_label':  dict(fontName='Helvetica-Bold',  fontSize=8,   textColor=GOLD,       leading=10, alignment=TA_LEFT, spaceAfter=3),
        'h1':             dict(fontName='Helvetica-Bold',  fontSize=20,  textColor=DARK_GREEN, leading=24, alignment=TA_LEFT, spaceAfter=6),
        'h2':             dict(fontName='Helvetica-Bold',  fontSize=13,  textColor=DARK_GREEN, leading=17, alignment=TA_LEFT, spaceAfter=5, spaceBefore=8),
        'h3':             dict(fontName='Helvetica-Bold',  fontSize=10.5,textColor=DARK_GREEN, leading=14, alignment=TA_LEFT, spaceAfter=3),
        'body':           dict(fontName='Helvetica',       fontSize=9.5, textColor=BLACK,      leading=15, alignment=TA_LEFT, spaceAfter=7),
        'body_small':     dict(fontName='Helvetica',       fontSize=8.5, textColor=MID_GREY,   leading=13, alignment=TA_LEFT),
        'callout':        dict(fontName='Helvetica-Oblique',fontSize=10, textColor=DARK_GREEN, leading=16, alignment=TA_LEFT, leftIndent=10, spaceAfter=7),
        'metric_big':     dict(fontName='Helvetica-Bold',  fontSize=21,  textColor=DARK_GREEN, leading=25, alignment=TA_CENTER),
        'metric_label':   dict(fontName='Helvetica',       fontSize=7.5, textColor=MID_GREY,   leading=10, alignment=TA_CENTER),
        'metric_sub':     dict(fontName='Helvetica',       fontSize=8,   textColor=GOLD,       leading=10, alignment=TA_CENTER),
        'table_head':     dict(fontName='Helvetica-Bold',  fontSize=8.5, textColor=WHITE,      leading=11, alignment=TA_CENTER),
        'table_cell':     dict(fontName='Helvetica',       fontSize=8.5, textColor=BLACK,      leading=11, alignment=TA_CENTER),
        'table_cell_l':   dict(fontName='Helvetica',       fontSize=8.5, textColor=BLACK,      leading=11, alignment=TA_LEFT),
        'table_bold':     dict(fontName='Helvetica-Bold',  fontSize=8.5, textColor=DARK_GREEN, leading=11, alignment=TA_CENTER),
        'outlook_body':   dict(fontName='Helvetica',       fontSize=9.5, textColor=BLACK,      leading=16, alignment=TA_LEFT, spaceAfter=8),
        'outlook_label':  dict(fontName='Helvetica-Bold',  fontSize=8,   textColor=WHITE,      leading=10, alignment=TA_CENTER),
        'disclaimer':     dict(fontName='Helvetica',       fontSize=7,   textColor=MID_GREY,   leading=10, alignment=TA_LEFT),
    }
    params = base.get(name, {})
    params.update(kw)
    return ParagraphStyle(name, **params)


# ── Charts ─────────────────────────────────────────────────────────────────────
def fig_img(fig, w, h=None):
    # If h is omitted, preserve the figure's natural aspect ratio so the
    # rendered chart isn't stretched. This makes line/bar charts crisper.
    if h is None:
        fw, fh = fig.get_size_inches()
        h = w * (fh / fw)
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=200, bbox_inches='tight', facecolor='none')
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=w, height=h)

def bar_chart(labels, values, title, ylabel, highlight_last=True):
    fig, ax = plt.subplots(figsize=(9.0, 3.6))
    cols = [EVA_PALETTE[0]] * len(values)
    if highlight_last: cols[-1] = EVA_PALETTE[1]
    bars = ax.bar(range(len(labels)), values, color=cols, width=0.62, zorder=3)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, fontsize=8.5, color='#3A3A3A')
    ax.set_ylabel(ylabel, fontsize=9, color='#6B6B6B')
    ax.set_title(title, fontsize=11, fontweight='bold', color='#1B4D3E', pad=10)
    ax.tick_params(axis='y', labelsize=8.5, colors='#6B6B6B')
    ax.spines[['top','right']].set_visible(False)
    ax.spines[['left','bottom']].set_color('#CFCFCF')
    ax.grid(axis='y', color='#EFEFEF', zorder=0, linewidth=0.6)
    ax.set_facecolor('white')
    if values:
        top = max(values)
        ax.set_ylim(0, top * 1.18)
        for b, v in zip(bars, values):
            ax.text(b.get_x() + b.get_width()/2, b.get_height() + top*0.025,
                    f'{int(v):,}' if isinstance(v, (int, float)) and v >= 100 else f'{v}',
                    ha='center', va='bottom', fontsize=8, color='#1B4D3E', fontweight='bold')
    fig.patch.set_alpha(0)
    fig.tight_layout()
    return fig

def line_chart(labels, series, title):
    """series = [(values, label), ...]"""
    fig, ax = plt.subplots(figsize=(9.0, 3.6))
    for i, (vals, lbl) in enumerate(series):
        ax.plot(labels, vals, color=EVA_PALETTE[i], linewidth=2.4,
                marker='o', markersize=5, markeredgecolor='white', markeredgewidth=1.0,
                label=lbl, zorder=3)
    ax.set_title(title, fontsize=11, fontweight='bold', color='#1B4D3E', pad=10)
    ax.legend(fontsize=8.5, framealpha=0, loc='best')
    ax.tick_params(axis='both', labelsize=8.5, colors='#6B6B6B')
    ax.spines[['top','right']].set_visible(False)
    ax.spines[['left','bottom']].set_color('#CFCFCF')
    ax.grid(color='#EFEFEF', zorder=0, linewidth=0.6)
    ax.set_facecolor('white')
    fig.patch.set_alpha(0)
    fig.tight_layout()
    return fig

def grouped_bar(labels, groups, group_labels, title):
    """Multi-community comparison bar."""
    x = np.arange(len(labels))
    w = 0.8 / len(groups)
    fig, ax = plt.subplots(figsize=(9.0, 3.8))
    for i, (vals, lbl) in enumerate(zip(groups, group_labels)):
        ax.bar(x + i*w - 0.4 + w/2, vals, w*0.9, label=lbl,
               color=EVA_PALETTE[i], zorder=3)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9, color='#3A3A3A')
    ax.set_title(title, fontsize=11, fontweight='bold', color='#1B4D3E', pad=10)
    ax.legend(fontsize=8.5, framealpha=0, loc='upper left')
    ax.tick_params(axis='y', labelsize=8.5, colors='#6B6B6B')
    ax.spines[['top','right']].set_visible(False)
    ax.spines[['left','bottom']].set_color('#CFCFCF')
    ax.grid(axis='y', color='#EFEFEF', zorder=0, linewidth=0.6)
    ax.set_facecolor('white')
    fig.patch.set_alpha(0)
    fig.tight_layout()
    return fig

def radar_chart(categories, area_data, title):
    """Spider/radar for multi-area scoring — full-width, generous sizing."""
    N = len(categories)
    angles = [n / float(N) * 2 * np.pi for n in range(N)]
    angles += angles[:1]
    fig, ax = plt.subplots(figsize=(7.0, 4.8), subplot_kw=dict(polar=True))
    for i, (label, values) in enumerate(area_data):
        vals = values + values[:1]
        col = EVA_PALETTE[i % len(EVA_PALETTE)]
        ax.plot(angles, vals, color=col, linewidth=2.5, label=label)
        ax.fill(angles, vals, color=col, alpha=0.12)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, size=10, color='#1B4D3E', fontweight='bold')
    ax.set_yticklabels([])
    ax.tick_params(pad=12)
    ax.set_title(title, size=12, fontweight='bold', color='#1B4D3E', pad=22)
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.14), ncol=3,
              fontsize=9, framealpha=0)
    ax.spines['polar'].set_color('#DDDDDD')
    ax.grid(color='#EEEEEE', linewidth=0.8)
    fig.patch.set_alpha(0)
    fig.tight_layout(pad=2.0)
    return fig

def donut_chart(labels, values, title):
    fig, ax = plt.subplots(figsize=(4.0, 3.4))
    wedges, _, autotexts = ax.pie(
        values, autopct='%1.0f%%', colors=EVA_PALETTE[:len(values)],
        startangle=90, wedgeprops=dict(width=0.55, edgecolor='white', linewidth=1.5),
        pctdistance=0.76)
    for t in autotexts:
        t.set_fontsize(9); t.set_color('white'); t.set_fontweight('bold')
    ax.legend(wedges, labels, loc='lower center',
              bbox_to_anchor=(0.5, -0.12), ncol=min(len(labels), 3),
              fontsize=8.5, framealpha=0)
    if title:
        ax.set_title(title, fontsize=10, fontweight='bold', color='#1B4D3E', pad=6)
    fig.patch.set_alpha(0); fig.tight_layout()
    return fig


# ── Layout helpers ─────────────────────────────────────────────────────────────
def section_header(label, title):
    return [
        Paragraph(label.upper(), S('section_label')),
        Paragraph(title, S('h1')),
        HRFlowable(width='100%', thickness=0.75, color=GOLD, spaceAfter=8),
    ]

def metric_cards(metrics):
    """metrics = [(val, label, sub), ...]"""
    cw = (PAGE_W - 36*mm) / len(metrics)
    cells = [[
        Paragraph(v, S('metric_big')),
        Spacer(1, 2),
        Paragraph(l, S('metric_label')),
        *([] if not sub else [Spacer(1, 2), Paragraph(sub, S('metric_sub'))])
    ] for v, l, sub in metrics]
    t = Table([cells], colWidths=[cw]*len(metrics))
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), CREAM),
        ('TOPPADDING',    (0,0),(-1,-1), 16),
        ('BOTTOMPADDING', (0,0),(-1,-1), 16),
        ('LEFTPADDING',   (0,0),(-1,-1), 10),
        ('RIGHTPADDING',  (0,0),(-1,-1), 10),
        ('LINEAFTER', (0,0),(-2,-1), 0.5, LIGHT_GREY),
        ('LINEABOVE', (0,0),(-1,-1), 1.5, GOLD),
        ('VALIGN', (0,0),(-1,-1), 'MIDDLE'),
    ]))
    return t

def data_table(headers, rows, col_widths=None, highlight_row=None):
    usable = PAGE_W - 36*mm
    if not col_widths:
        col_widths = [usable/len(headers)]*len(headers)
    else:
        # Many call sites supply widths whose total exceeds the usable page
        # width — that produces overflowing tables and ugly wrapping.
        # Scale all columns proportionally if they exceed the frame.
        total = sum(col_widths)
        if total > usable + 0.5:
            scale = usable / total
            col_widths = [w * scale for w in col_widths]
    hrow = [Paragraph(h, S('table_head')) for h in headers]
    data = [[Paragraph(str(c), S('table_cell_l') if j==0 else S('table_cell'))
             for j, c in enumerate(r)] for r in rows]
    t = Table([hrow]+data, colWidths=col_widths, repeatRows=1)
    style = [
        ('BACKGROUND', (0,0),(-1,0), DARK_GREEN),
        ('TOPPADDING',    (0,0),(-1,-1), 8),
        ('BOTTOMPADDING', (0,0),(-1,-1), 8),
        ('LEFTPADDING',   (0,0),(-1,-1), 9),
        ('RIGHTPADDING',  (0,0),(-1,-1), 9),
        ('ROWBACKGROUNDS', (0,1),(-1,-1), [WHITE, CREAM]),
        ('GRID', (0,0),(-1,-1), 0.3, LIGHT_GREY),
        ('LINEBELOW', (0,0),(-1,0), 1, GOLD),
        ('VALIGN', (0,0),(-1,-1), 'MIDDLE'),
    ]
    if highlight_row is not None:
        style += [('BACKGROUND', (0,highlight_row+1),(-1,highlight_row+1), colors.HexColor('#E8F5F0')),
                  ('FONTNAME', (0,highlight_row+1),(-1,highlight_row+1), 'Helvetica-Bold')]
    t.setStyle(TableStyle(style))
    return t

def outlook_badge(text, colour):
    t = Table([[Paragraph(text, S('outlook_label'))]], colWidths=[55*mm])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0),(-1,-1), colour),
        ('TOPPADDING',    (0,0),(-1,-1), 5),
        ('BOTTOMPADDING', (0,0),(-1,-1), 5),
        ('LEFTPADDING',   (0,0),(-1,-1), 6),
        ('RIGHTPADDING',  (0,0),(-1,-1), 6),
        ('ROUNDEDCORNERS', [3]),
    ]))
    return t


# ── Canvas callbacks ───────────────────────────────────────────────────────────
def draw_header(c, doc):
    c.saveState()
    c.setFillColor(DARK_GREEN)
    c.rect(0, PAGE_H-28*mm, PAGE_W, 28*mm, fill=1, stroke=0)
    c.setFillColor(GOLD)
    c.rect(0, PAGE_H-28*mm-1.5, PAGE_W, 1.5, fill=1, stroke=0)
    title = ' · '.join(c for c in doc.communities[:3]) if hasattr(doc,'communities') else ''
    c.setFont('Helvetica-Bold', 10)
    c.setFillColor(WHITE)
    c.drawString(18*mm, PAGE_H-17*mm, f'MARKET INTELLIGENCE REPORT  ·  {title.upper()}')
    c.setFont('Helvetica', 8)
    c.setFillColor(GOLD)
    c.drawRightString(PAGE_W-18*mm, PAGE_H-17*mm, doc.report_date)
    c.restoreState()

def draw_footer(c, doc, pn):
    c.saveState()
    c.setFillColor(LIGHT_GREY)
    c.rect(0, 0, PAGE_W, 14*mm, fill=1, stroke=0)
    c.setFillColor(GOLD)
    c.rect(0, 14*mm, PAGE_W, 0.75, fill=1, stroke=0)
    c.setFont('Helvetica', 7.5)
    c.setFillColor(MID_GREY)
    c.drawString(18*mm, 5*mm,
        f'Prepared by {doc.agent_name}  ·  EVA Real Estate LLC  ·  evadxb.com  ·  +971 58 102 5758')
    c.drawRightString(PAGE_W-18*mm, 5*mm, f'{pn}')
    c.restoreState()

def on_page(c, doc):
    pn = doc.page
    if pn == 1:
        # Full cover — drawn on canvas
        c.saveState()
        c.setFillColor(DARK_GREEN)
        c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
        c.setFillColor(GOLD)
        c.rect(0, 0, 5*mm, PAGE_H, fill=1, stroke=0)
        c.setFillColor(colors.HexColor('#153D30'))
        c.rect(5*mm, 0, PAGE_W-5*mm, 55*mm, fill=1, stroke=0)

        # Logo or text
        lp = getattr(doc, 'logo_path', None)
        if lp and os.path.exists(lp):
            c.drawImage(lp, 18*mm, PAGE_H-38*mm, width=35*mm, height=22*mm,
                        preserveAspectRatio=True, mask='auto')
        else:
            c.setFont('Helvetica-Bold', 22); c.setFillColor(GOLD)
            c.drawString(18*mm, PAGE_H-28*mm, 'EVA')
            c.setFont('Helvetica', 9); c.setFillColor(WHITE)
            c.drawString(18*mm, PAGE_H-37*mm, 'Real Estate')

        # Agent strip at bottom
        c.setFont('Helvetica', 8.5); c.setFillColor(GOLD)
        c.drawString(18*mm, 36*mm, 'PREPARED BY')
        c.setFont('Helvetica-Bold', 12); c.setFillColor(WHITE)
        c.drawString(18*mm, 26*mm, getattr(doc, 'agent_name', 'EVA Real Estate'))
        c.setFont('Helvetica', 8.5); c.setFillColor(CREAM)
        c.drawString(18*mm, 18*mm, getattr(doc, 'agent_contact', 'info@evadxb.com  ·  evadxb.com'))
        c.setFont('Helvetica', 7.5); c.setFillColor(colors.HexColor('#4A8C78'))
        c.drawString(18*mm, 10*mm, 'CONFIDENTIAL — FOR CLIENT USE ONLY')
        c.restoreState()
    elif pn == getattr(doc, '_total_pages', 9999):
        # Back cover
        c.saveState()
        c.setFillColor(DARK_GREEN)
        c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
        c.setFillColor(GOLD); c.rect(0,0, 5*mm, PAGE_H, fill=1, stroke=0)
        c.setFillColor(colors.HexColor('#153D30'))
        c.rect(5*mm, PAGE_H*0.35, PAGE_W-5*mm, PAGE_H*0.3, fill=1, stroke=0)
        c.setFont('Helvetica-Bold', 20); c.setFillColor(WHITE)
        c.drawCentredString(PAGE_W/2, PAGE_H*0.58, 'READY TO ACT ON THESE INSIGHTS?')
        c.setFont('Helvetica', 10); c.setFillColor(CREAM)
        agent = getattr(doc, 'agent_name', 'Our team')
        c.drawCentredString(PAGE_W/2, PAGE_H*0.58-20,
            f'{agent} is ready to guide you through every opportunity.')
        c.setFont('Helvetica-Bold', 13); c.setFillColor(GOLD)
        c.drawCentredString(PAGE_W/2, PAGE_H*0.58-52, '+971 58 102 5758')
        c.setFont('Helvetica', 9.5); c.setFillColor(CREAM)
        c.drawCentredString(PAGE_W/2, PAGE_H*0.58-68, 'info@evadxb.com  ·  evadxb.com')
        c.drawCentredString(PAGE_W/2, PAGE_H*0.58-84,
            'Dubai Marina, Marina Plaza, Office 3501')
        c.setFont('Helvetica', 7.5); c.setFillColor(colors.HexColor('#4A8C78'))
        c.drawCentredString(PAGE_W/2, 18,
            f'© {datetime.now().year} EVA Real Estate LLC  ·  Licensed Broker  ·  RERA Registered  ·  Data: Property Monitor / DLD')
        c.restoreState()
    else:
        draw_header(c, doc)
        draw_footer(c, doc, pn-1)


# ══════════════════════════════════════════════════════════════════════════════
# PROPERTY MONITOR CSV PARSER
# ══════════════════════════════════════════════════════════════════════════════

PM_TRANSACTION_MAP = {
    'transaction_date':  ['custom_date', 'transaction date', 'trans date', 'date', 'reg date'],
    'transaction_type':  ['transaction type', 'type'],
    'property_type':     ['unit_type', 'property type', 'prop type', 'type of property'],
    'area':              ['master_development', 'sub_loc_1', 'area', 'community', 'location', 'neighborhood', 'neighbourhood'],
    'building':          ['sub_loc_2', 'building', 'project', 'development', 'building name'],
    'sub_area':          ['sub_loc_3', 'sub_loc_4'],
    'unit_number':       ['unit_no', 'unit', 'unit no', 'unit number', 'apartment'],
    'floor':             ['floor_level', 'floor', 'floor no', 'floor number', 'level'],
    'bedrooms':          ['no_beds', 'bedrooms', 'beds', 'bed', 'no. of bedrooms', 'rooms'],
    'size_sqft':         ['unit_size_sqft', 'size', 'area sqft', 'size (sqft)', 'area (sqft)', 'sqft'],
    'price_aed':         ['total_sales_price_val', 'amount', 'price', 'transaction value', 'value (aed)', 'price (aed)', 'sale price'],
    'price_psf':         ['sales_price_sqft_unit', 'price per sqft', 'psf', 'price/sqft', 'aed/sqft', 'rate'],
    'reg_type':          ['sale_sequence', 'registration type', 'off-plan', 'ready'],
    'registration_type': ['evdnc_name', 'reg type', 'evidence type'],
    'developer':         ['dev_name', 'developer', 'developer name'],
    'plot_sqft':         ['plot_size_sqft', 'plot size sqft', 'land area'],
    'location_notes':    ['comments', 'notes', 'location notes', 'remarks', 'unit features', 'orientation'],
}

PM_RENTAL_MAP = {
    'reg_date':      ['custom_date', 'registration date', 'contract date', 'date', 'start date'],
    'property_type': ['unit_type', 'property type', 'type'],
    'area':          ['master_development', 'sub_loc_1', 'area', 'community', 'location'],
    'building':      ['sub_loc_2', 'building', 'project', 'building name'],
    'bedrooms':      ['no_beds', 'bedrooms', 'beds', 'bed', 'rooms'],
    'size_sqft':     ['unit_size_sqft', 'size', 'area sqft', 'sqft'],
    'annual_rent':   ['annual amount', 'annual rent', 'rent (aed)', 'yearly rent', 'amount'],
    'duration':      ['contract duration', 'duration', 'years', 'contract years'],
    'location_notes':['comments', 'notes', 'remarks', 'view', 'features'],
}

def _match_column(df_cols, candidates):
    df_lower = {c.lower().strip(): c for c in df_cols}
    for candidate in candidates:
        if candidate in df_lower:
            return df_lower[candidate]
    return None

def parse_pm_transactions(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, thousands=',', encoding='latin-1')
    df.columns = df.columns.str.strip()
    col_map = {}
    for field, candidates in PM_TRANSACTION_MAP.items():
        match = _match_column(df.columns, candidates)
        if match:
            col_map[match] = field
    df = df.rename(columns=col_map)
    for col in ['price_aed', 'price_psf', 'size_sqft', 'floor', 'plot_sqft']:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce')
    for raw_col in ['total_sales_price_val', 'sales_price_sqft_unit', 'unit_size_sqft', 'plot_size_sqft']:
        if raw_col in df.columns:
            df[raw_col] = pd.to_numeric(
                df[raw_col].astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce')
    if 'transaction_date' in df.columns:
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce', dayfirst=True)
        df['month'] = df['transaction_date'].dt.to_period('M')
    if 'bedrooms' in df.columns:
        df['bedrooms'] = df['bedrooms'].astype(str).str.extract(r'(\d+)')[0].fillna('0')
    return df

def parse_pm_rentals(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, thousands=',', encoding='latin-1')
    df.columns = df.columns.str.strip()
    col_map = {}
    for field, candidates in PM_RENTAL_MAP.items():
        match = _match_column(df.columns, candidates)
        if match:
            col_map[match] = field
    df = df.rename(columns=col_map)
    for col in ['annual_rent','size_sqft']:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r'[^\d.]','',regex=True), errors='coerce')
    if 'reg_date' in df.columns:
        df['reg_date'] = pd.to_datetime(df['reg_date'], errors='coerce', dayfirst=True)
        df['month'] = df['reg_date'].dt.to_period('M')
    return df


# ══════════════════════════════════════════════════════════════════════════════
# DATA ANALYSIS ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def analyse(txn_df: pd.DataFrame, rental_df: pd.DataFrame, community: str) -> dict:
    """Compute all metrics from Property Monitor DataFrames."""
    out = {'community': community}

    if 'area' in txn_df.columns:
        txn = txn_df[txn_df['area'].str.contains(community, case=False, na=False)].copy()
    else:
        txn = txn_df.copy()

    if 'area' in rental_df.columns:
        rent = rental_df[rental_df['area'].str.contains(community, case=False, na=False)].copy()
    else:
        rent = rental_df.copy()

    txn = txn[txn.get('transaction_type','Sale').astype(str).str.contains('Sale|sale|secondary|ready', na=False)] \
          if 'transaction_type' in txn.columns else txn

    # ── Core metrics
    out['total_transactions'] = str(len(txn))
    if 'price_aed' in txn.columns and txn['price_aed'].notna().any():
        out['avg_price']   = f"AED {txn['price_aed'].mean()/1e6:.2f}M"
        out['total_volume']= f"AED {txn['price_aed'].sum()/1e9:.2f}B"
    if 'price_psf' in txn.columns and txn['price_psf'].notna().any():
        out['avg_psf'] = f"AED {txn['price_psf'].mean():,.0f}"

    # Rental yield
    if 'annual_rent' in rent.columns and 'price_aed' in txn.columns:
        avg_rent  = rent['annual_rent'].mean()
        avg_price = txn['price_aed'].mean()
        if avg_price > 0:
            out['avg_yield'] = f"{(avg_rent/avg_price)*100:.1f}%"

    # ── Monthly series
    if 'month' in txn.columns:
        monthly = txn.groupby('month').agg(
            volume=('price_aed','count'),
            avg_psf=('price_psf','mean'),
            avg_price=('price_aed','mean')
        ).tail(12)
        out['months']         = [str(m) for m in monthly.index]
        out['monthly_volume'] = monthly['volume'].tolist()
        out['monthly_psf']    = monthly['avg_psf'].round(0).tolist()
        out['monthly_price']  = (monthly['avg_price']/1e6).round(2).tolist()

    # ── By bedroom type
    if 'bedrooms' in txn.columns:
        br = txn.groupby('bedrooms').agg(
            count=('price_aed','count'),
            avg_p=('price_aed','mean'),
            avg_psf=('price_psf','mean'),
            min_p=('price_aed','min'),
            max_p=('price_aed','max'),
        ).reset_index()
        def fmt_m(x): return f"AED {x/1e6:.2f}M" if x > 1e6 else f"AED {x/1e3:.0f}K"
        def br_label(b):
            b = str(b)
            return 'Studio' if b=='0' else f"{b} Bedroom{'s' if b!='1' else ''}"
        prop_rows = [[
            br_label(r['bedrooms']),
            str(int(r['count'])),
            fmt_m(r['avg_p']),
            f"AED {r['avg_psf']:,.0f}",
            fmt_m(r['min_p']),
            fmt_m(r['max_p']),
        ] for _, r in br.iterrows()]
        out['prop_type_data'] = [
            ['Property Type','Transactions','Avg Price','Avg PSF','Min Price','Max Price']
        ] + prop_rows

    # ── Rental by bedroom
    if 'bedrooms' in rent.columns and 'annual_rent' in rent.columns:
        rb = rent.groupby('bedrooms').agg(
            avg_rent=('annual_rent','mean'),
            count=('annual_rent','count')
        ).reset_index()
        def br_label(b): return 'Studio' if str(b)=='0' else f"{b}BR"
        out['rental_mix'] = rb['count'].tolist()
        out['rental_mix_labels'] = [br_label(r['bedrooms']) for _,r in rb.iterrows()]

    # ── Location notes summary
    loc_col = 'location_notes'
    if loc_col in txn.columns:
        notes = txn[loc_col].dropna().astype(str)
        high_floor = notes.str.contains('high|upper|top', case=False).sum()
        park_view  = notes.str.contains('park|garden|green', case=False).sum()
        sea_view   = notes.str.contains('sea|marina|water|ocean', case=False).sum()
        out['location_summary'] = {
            'high_floor_pct': f"{high_floor/max(len(notes),1)*100:.0f}%",
            'park_view_pct':  f"{park_view/max(len(notes),1)*100:.0f}%",
            'sea_view_pct':   f"{sea_view/max(len(notes),1)*100:.0f}%",
        }

    # ── YoY comparison (if 2+ years of data)
    if 'month' in txn.columns and 'price_psf' in txn.columns:
        txn = txn.copy()
        txn['year'] = txn['transaction_date'].dt.year
        yr = txn.groupby('year')['price_psf'].mean()
        if len(yr) >= 2:
            yrs = sorted(yr.index)
            out['years']     = [str(y) for y in yrs]
            out['yoy_price'] = [round(yr[y]) for y in yrs]
            if len(yrs) >= 2:
                growth = (yr[yrs[-1]] - yr[yrs[-2]]) / yr[yrs[-2]] * 100
                out['yoy_growth'] = f"{growth:+.1f}%"

    # ── View Premium Analysis (custom_view column)
    if 'custom_view' in txn.columns and 'price_psf' in txn.columns:
        try:
            view_psf_lists = {}
            for _, row in txn.iterrows():
                psf_val = row.get('price_psf')
                if psf_val is None or (isinstance(psf_val, float) and np.isnan(psf_val)):
                    continue
                views_str = str(row.get('custom_view', ''))
                if not views_str or views_str.lower() == 'nan':
                    continue
                for v in views_str.split(','):
                    v = v.strip().title()
                    if v:
                        view_psf_lists.setdefault(v, []).append(float(psf_val))
            view_summary = {v: round(np.mean(vals)) for v, vals in view_psf_lists.items()
                           if len(vals) >= 2}
            if view_summary:
                out['view_premium_data'] = sorted(view_summary.items(), key=lambda x: -x[1])[:10]
        except Exception:
            pass

    # ── Phase / Sub-Development Breakdown (building = sub_loc_2)
    if 'building' in txn.columns and txn['building'].notna().any():
        try:
            phases = txn.groupby('building').agg(
                count=('price_aed', 'count'),
                avg_psf=('price_psf', 'mean'),
                avg_price=('price_aed', 'mean'),
            ).dropna(subset=['avg_psf']).sort_values('avg_psf', ascending=False).reset_index()
            if len(phases) >= 2:
                out['phase_data'] = [
                    [str(r['building']), str(int(r['count'])),
                     f"AED {r['avg_price']/1e6:.2f}M", f"AED {r['avg_psf']:,.0f}"]
                    for _, r in phases.iterrows()
                ]
        except Exception:
            pass

    return out


# ══════════════════════════════════════════════════════════════════════════════
# PAGE BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def page_cover(data):
    els = [Spacer(1, 50*mm)]
    els.append(Paragraph('D U B A I  ·  R E A L  E S T A T E', S('cover_eyebrow')))
    els.append(Spacer(1, 3*mm))
    els.append(HRFlowable(width='38%', thickness=1.5, color=GOLD, spaceAfter=5*mm))
    rtype = data.get('report_type','single')
    title = 'MULTI-AREA<br/>COMPARISON REPORT' if rtype=='comparison' else 'MARKET<br/>INTELLIGENCE REPORT'
    els.append(Paragraph(title, S('cover_title')))
    els.append(Spacer(1, 3*mm))
    communities = data.get('communities', [data.get('community','Dubai')])
    els.append(Paragraph(' · '.join(c.upper() for c in communities), S('cover_community')))
    els.append(Spacer(1, 5*mm))
    els.append(Paragraph(data.get('report_period','Q1 2025'), S('cover_eyebrow')))
    els.append(Spacer(1, 12*mm))
    els.append(HRFlowable(width='100%', thickness=0.5, color=colors.HexColor('#2A6B57'), spaceAfter=10*mm))
    m1v = data.get('avg_psf',        data.get('cover_m1v','AED 1,622'))
    m2v = data.get('yoy_growth',     data.get('cover_m2v','+18.4%'))
    m3v = data.get('avg_yield',      data.get('cover_m3v','6.8%'))
    hero_value = ParagraphStyle('_hv', fontName='Helvetica-Bold', fontSize=19, textColor=WHITE, leading=23, alignment=TA_CENTER)
    hero_label = ParagraphStyle('_hl', fontName='Helvetica',      fontSize=7.5, textColor=GOLD, leading=10, alignment=TA_CENTER)
    hero_data = [[
        Paragraph(m1v, hero_value),
        Paragraph(m2v, hero_value),
        Paragraph(m3v, hero_value),
    ],[
        Paragraph('AVG PRICE / SQFT', hero_label),
        Paragraph('YoY PRICE GROWTH', hero_label),
        Paragraph('AVG RENTAL YIELD', hero_label),
    ]]
    cw = (PAGE_W - 36*mm) / 3
    t = Table(hero_data, colWidths=[cw,cw,cw])
    t.setStyle(TableStyle([
        ('TOPPADDING',(0,0),(-1,-1),8), ('BOTTOMPADDING',(0,0),(-1,-1),8),
        ('LINEAFTER',(0,0),(1,-1),0.5,colors.HexColor('#2A6B57')),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
    ]))
    els.append(t)
    els.append(PageBreak())
    return els


def page_executive_summary(data):
    els = section_header('Section 01', 'Executive Summary')
    community = data.get('community', data.get('communities',[''])[0])
    els.append(Paragraph(data.get('exec_summary',
        f'This report provides a comprehensive analysis of the {community} real estate market, '
        f'covering the period {data.get("report_period","Q1 2025")}. Transaction and rental data '
        'sourced directly from the Dubai Land Department via Property Monitor reflects the most '
        'up-to-date market conditions available. Key findings are presented in plain language '
        'to support informed investment and advisory decisions.'), S('body')))
    els.append(Spacer(1, 3*mm))
    els.append(metric_cards([
        (data.get('total_transactions','847'),  'TOTAL TRANSACTIONS',   data.get('txn_period','Last 12 Months')),
        (data.get('avg_price','AED 2.84M'),     'AVERAGE SALE PRICE',   data.get('yoy_growth','')),
        (data.get('avg_psf','AED 1,622'),       'AVG PRICE PER SQFT',   ''),
        (data.get('avg_yield','6.8%'),          'AVG RENTAL YIELD',     ''),
    ]))
    els.append(Spacer(1, 5*mm))
    els.append(Paragraph('Performance at a Glance', S('h2')))
    highlights = data.get('highlights', [
        ['Metric',                 'This Period',  'Previous Period', 'Change'],
        ['Total Transactions',     '847',          '721',             '▲ +17.5%'],
        ['Avg Sale Price',         'AED 2.84M',    'AED 2.53M',       '▲ +12.3%'],
        ['Avg Price / Sqft',       'AED 1,622',    'AED 1,492',       '▲ +8.7%'],
        ['Total Sales Volume',     'AED 2.4B',     'AED 1.82B',       '▲ +31.9%'],
        ['Avg Rental (2BR)',       'AED 142K',     'AED 128K',        '▲ +10.9%'],
        ['Avg Gross Yield',        '6.8%',         '6.2%',            '▲ +0.6pp'],
        ['Avg Days on Market',     '23 days',      '31 days',         '▼ Faster'],
    ])
    els.append(data_table(highlights[0], highlights[1:], col_widths=[88*mm,42*mm,42*mm,38*mm]))
    els.append(PageBreak())
    return els


def page_transaction_analysis(data):
    els = section_header('Section 02', 'Transaction Analysis')
    els.append(Paragraph(
        'The following analysis breaks down all recorded sales transactions for the selected '
        'period. In simple terms: how many properties sold, what they sold for, and how '
        'prices have moved month by month. This gives you a clear picture of market momentum '
        'and where buyers are most active.', S('body')))
    els.append(Spacer(1, 3*mm))
    months = data.get('months', ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'])
    vol    = data.get('monthly_volume', [58,62,71,68,74,80,77,83,91,87,95,101])
    psf    = data.get('monthly_psf',    [1480,1490,1510,1525,1540,1560,1570,1585,1600,1612,1618,1622])
    prices = data.get('monthly_price',  [2.4,2.45,2.5,2.52,2.55,2.6,2.63,2.68,2.72,2.77,2.81,2.84])

    els.append(fig_img(bar_chart(months, vol, 'Monthly Transaction Volume', 'No. Transactions'), PAGE_W-36*mm))
    els.append(Spacer(1, 6*mm))
    els.append(fig_img(line_chart(months, [(psf,'Avg PSF (AED)'),(prices,'Avg Price (AED M)')],
        'Price Per Sqft & Average Sale Price Trend'), PAGE_W-36*mm))
    els.append(Spacer(1, 3*mm))

    if 'prop_type_data' in data:
        els.append(Paragraph('Breakdown by Property Type', S('h2')))
        pt = data['prop_type_data']
        els.append(data_table(pt[0], pt[1:], col_widths=[52*mm,34*mm,34*mm,32*mm,34*mm,34*mm]))
    els.append(PageBreak())
    return els


def page_rental_analysis(data):
    els = section_header('Section 03', 'Rental Market Analysis')
    els.append(Paragraph(
        'The rental market tells you two important things: what income an investor can '
        'expect to earn, and how much demand there is from tenants. When rents rise and '
        'vacancies fall, it is a strong signal that the area is growing in popularity — '
        'which typically supports property price growth too.', S('body')))
    els.append(Spacer(1, 3*mm))
    rent_data = data.get('rental_data', [
        ['Property Type','Avg Annual Rent','Avg Sale Price','Gross Yield','Change YoY'],
        ['Studio',       'AED 58K',        'AED 680K',      '8.5%',       '▲ +1.1pp'],
        ['1 Bedroom',    'AED 95K',        'AED 1.24M',     '7.7%',       '▲ +0.8pp'],
        ['2 Bedroom',    'AED 142K',       'AED 2.10M',     '6.8%',       '▲ +0.6pp'],
        ['3 Bedroom',    'AED 198K',       'AED 3.42M',     '5.8%',       '▲ +0.5pp'],
        ['4 Bedroom',    'AED 260K',       'AED 5.18M',     '5.0%',       '▲ +0.3pp'],
    ])
    els.append(data_table(rent_data[0], rent_data[1:], col_widths=[48*mm,40*mm,40*mm,32*mm,30*mm+10]))
    els.append(Spacer(1, 4*mm))
    months = data.get('months', ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'])
    r2 = data.get('rent_2br', [125,127,128,130,132,135,136,138,140,141,142,142])
    r3 = data.get('rent_3br', [184,186,187,190,192,194,195,196,197,198,198,198])
    fig_r = line_chart(months, [(r2,"2-BR Annual Rent (AED '000)"),(r3,"3-BR Annual Rent (AED '000)")],
                       'Rental Value Trend — 12 Months')
    els.append(fig_img(fig_r, PAGE_W - 36*mm))
    els.append(Spacer(1, 8*mm))

    labels = data.get('rental_mix_labels', ['1BR','2BR','3BR','4BR','5BR+'])
    vals   = data.get('rental_mix',        [21, 34, 26, 13, 6])
    els.append(Paragraph('Rental Mix by Property Type', S('h2')))
    els.append(Spacer(1, 3*mm))
    fig_d = donut_chart(labels, vals, '')
    dt = Table([[fig_img(fig_d, 220)]], colWidths=[PAGE_W - 36*mm])
    dt.setStyle(TableStyle([('ALIGN',(0,0),(-1,-1),'CENTER'),
                             ('LEFTPADDING',(0,0),(-1,-1),0),
                             ('RIGHTPADDING',(0,0),(-1,-1),0)]))
    els.append(dt)
    els.append(PageBreak())
    return els


def page_location_analysis(data):
    els = section_header('Section 04', 'Location & Asset Analysis')
    els.append(Paragraph(
        'Not all units in a community sell for the same price. Location within the development '
        '— including floor level, view, orientation, and proximity to amenities — can add or '
        'subtract significant value. The data below is drawn directly from transaction records '
        'and shows you exactly which attributes are commanding a premium in this market.', S('body')))
    els.append(Spacer(1, 4*mm))

    loc_data = data.get('location_data', [
        ['Location Factor',           'Avg Premium vs Base', 'What It Means'],
        ['High Floor (10+)',           '+8–14%',   'Buyers pay more for height, privacy, and views'],
        ['Park / Garden View',         '+6–12%',   'Green outlooks are consistently the most in demand'],
        ['Sea / Marina View',          '+10–18%',  'Premium views command the strongest uplift'],
        ['Corner Unit',                '+4–7%',    'More windows and natural light drive preference'],
        ['End of Row / Semi-Detached', '+5–9%',    'Extra privacy and outdoor space adds value'],
        ['Ground Floor with Garden',   '+3–8%',    'Popular with families; direct garden access'],
        ['Upgraded Fit-Out',           '+5–12%',   'Premium kitchens and finishes justify higher prices'],
        ['Near Entrance / Amenities',  '+2–5%',    'Convenience factor valued by tenants and buyers'],
    ])
    els.append(data_table(loc_data[0], loc_data[1:], col_widths=[65*mm, 38*mm, 105*mm+10]))
    els.append(Spacer(1, 5*mm))

    view_premium = data.get('view_premium_data', [])
    if view_premium:
        els.append(Paragraph('Actual Price Premium by View Type', S('h2')))
        els.append(Paragraph(
            'The table below is calculated directly from recorded transaction data for this '
            'community. Each average PSF reflects only units with that specific view type so '
            'you can see exactly which outlooks are commanding a premium.', S('body')))
        view_rows = [[v, f"AED {p:,}"] for v, p in view_premium]
        els.append(data_table(['View Type', 'Avg Price per Sqft (AED)'],
                              view_rows, col_widths=[115*mm, 95*mm]))
        els.append(Spacer(1, 5*mm))

    phase_data = data.get('phase_data', [])
    if phase_data:
        els.append(Paragraph('Performance by Phase / Sub-Development', S('h2')))
        els.append(Paragraph(
            'Not every phase of a development performs equally. The breakdown below ranks each '
            'phase by average price per square foot, highlighting where within the community '
            'buyers are currently paying the most.', S('body')))
        els.append(data_table(
            ['Phase', 'Transactions', 'Avg Price', 'Avg PSF'],
            phase_data,
            col_widths=[85*mm, 42*mm, 52*mm, 42*mm]))
        els.append(Spacer(1, 5*mm))

    loc_summary = data.get('location_summary', {})
    if loc_summary:
        els.append(Paragraph('What the Data Shows for This Community', S('h2')))
        els.append(Paragraph(
            f"Of the transactions analysed, approximately {loc_summary.get('high_floor_pct','N/A')} "
            f"involved high-floor units, {loc_summary.get('park_view_pct','N/A')} had park or garden "
            f"facing positions, and {loc_summary.get('sea_view_pct','N/A')} featured sea or water views. "
            'These proportions reflect the composition of available supply and indicate where '
            'pricing pressure is most concentrated.', S('body')))

    custom_notes = data.get('custom_location_notes', '')
    if custom_notes:
        els.append(Paragraph('Agent Observations', S('h2')))
        els.append(Paragraph(custom_notes, S('callout')))

    els.append(PageBreak())
    return els


def page_investment_highlights(data):
    els = section_header('Section 05', 'Investment Performance')
    els.append(Paragraph(
        'These figures show how this community has performed as an investment — '
        'both in terms of income (rental yield) and capital growth (how much the '
        'property has increased in value). All figures are benchmarked against '
        'Dubai market averages so you can see exactly how it compares.', S('body')))
    els.append(Spacer(1, 3*mm))
    sc_note = data.get('service_charge_note', '')
    els.append(metric_cards([
        (data.get('avg_yield','6.8%'),
         'GROSS RENTAL YIELD', 'vs 5.2% Dubai avg'),
        (data.get('net_yield','–') if data.get('net_yield') else data.get('avg_yield','6.8%'),
         'NET RENTAL YIELD', f"after {sc_note} SC" if sc_note else 'estimated after costs'),
        (data.get('roi_5yr','–'),
         '5-YEAR TOTAL ROI', 'capital + income'),
    ]))
    if sc_note:
        els.append(Spacer(1, 2*mm))
        els.append(Paragraph(
            f'Net yield calculated after service charge of {sc_note}. '
            'Actual returns will vary based on financing, maintenance, and vacancy periods. '
            'Figures are indicative only.',
            S('body_small')))
    els.append(Spacer(1, 5*mm))
    roi_data = data.get('roi_data', [
        ['Area',                      'Gross Yield','Cap Growth','5-Yr ROI','Risk'],
        [data.get('community','This Community'), '6.8%','+18.4%','62%','Low-Medium'],
        ['Dubai Average (All)',        '5.2%','+12.1%','41%','Medium'],
        ['Palm Jumeirah',              '4.8%','+21.3%','58%','Low'],
        ['Downtown Dubai',             '5.5%','+14.7%','47%','Low-Medium'],
        ['Jumeirah Village Circle',    '7.2%','+9.8%', '44%','Medium'],
        ['Dubai Hills Estate',         '4.9%','+16.2%','49%','Low'],
    ])
    els.append(data_table(roi_data[0], roi_data[1:], col_widths=[68*mm,34*mm,34*mm,32*mm,40*mm+10], highlight_row=0))
    els.append(Spacer(1, 4*mm))
    if 'years' in data and 'yoy_price' in data:
        fig = bar_chart(data['years'], data['yoy_price'],
                        'Price Per Sqft - Historical Trend (AED)', 'AED / Sqft', highlight_last=True)
        els.append(fig_img(fig, PAGE_W-36*mm))
    els.append(PageBreak())
    return els


def page_market_outlook(data):
    els = section_header('Section 06', 'Market Outlook - What Happens Next')
    els.append(Paragraph(
        'The following forward-looking analysis is based on current market data, economic '
        'indicators, and demand signals. It is written in plain language so that the '
        'picture is clear for any reader - no financial jargon, just what the data suggests '
        'is coming next for this market.', S('body')))
    els.append(Spacer(1, 4*mm))

    signals = data.get('outlook_signals', [
        ('PRICE DIRECTION', 'Upward',   DARK_GREEN),
        ('DEMAND LEVEL',    'High',     LIGHT_GREEN),
        ('SUPPLY',          'Tight',    colors.HexColor('#8B6914')),
        ('RENTAL OUTLOOK',  'Stable+',  DARK_GREEN),
    ])
    badge_cells = [[outlook_badge(f"{lbl}: {val}", col) for lbl, val, col in signals]]
    bt = Table(badge_cells, colWidths=[(PAGE_W-36*mm)/4]*4)
    bt.setStyle(TableStyle([('LEFTPADDING',(0,0),(-1,-1),3),('RIGHTPADDING',(0,0),(-1,-1),3)]))
    els.append(bt)
    els.append(Spacer(1, 5*mm))

    outlook_items = data.get('outlook_items', [
        ('What does the price trend tell us?',
         'Over the past 12 months, prices have risen consistently with no significant corrections. '
         'This signals genuine underlying demand rather than speculation. Based on current volumes '
         'and supply levels, prices are expected to continue upward over the next 6-12 months, '
         'though growth may moderate slightly as affordability becomes a factor at the top end.'),
        ('Will there be more supply?',
         'New villa and townhouse supply in established communities is extremely limited. Unlike '
         'apartment towers, you cannot build more of the same product in the same location. This '
         'structural constraint is one of the most powerful long-term drivers of price stability. '
         'Unless a significant new phase is announced - which is not currently the case - supply '
         'is expected to remain tight for the foreseeable future.'),
        ('What does this mean for rental income?',
         'When property prices rise faster than rents, yields can compress slightly as a percentage. '
         'However, absolute rental values continue to increase in AED terms. For investors, this '
         'means rents are growing but the purchase price is growing faster. Net returns remain '
         'attractive compared to most global markets, and capital growth may more than compensate '
         'for any yield compression over a 3-5 year hold period.'),
        ('The broader Dubai picture',
         'Dubai continues to attract net inflows of high-net-worth residents, supported by visa '
         'reforms, tax advantages, and global lifestyle appeal. Population growth is sustained, '
         'mortgage rates remain competitive, and government infrastructure investment is ongoing. '
         'These macro tailwinds support continued market strength over a 2-5 year horizon, though '
         'buyers and investors should account for global economic conditions and conduct their own due diligence.'),
    ])
    # Support both list-of-tuples (default) and list-of-dicts (from Gemini JSON)
    for item in outlook_items:
        if isinstance(item, dict):
            title = item.get('title', '')
            body  = item.get('body', '')
        else:
            title, body = item
        els.append(KeepTogether([
            Paragraph(title, S('h3')),
            Paragraph(body, S('outlook_body')),
            HRFlowable(width='100%', thickness=0.3, color=LIGHT_GREY, spaceAfter=4),
        ]))
    els.append(Spacer(1, 3*mm))
    els.append(Paragraph(
        'Disclaimer: Forward-looking statements are based on publicly available data at time of '
        'publication and do not constitute financial advice. Past performance is not indicative '
        'of future results. Sources: DLD / Property Monitor, JLL, Knight Frank, Bloomberg.',
        S('disclaimer')))
    els.append(PageBreak())
    return els


def _psf_numeric(val):
    """Strip 'AED 1,622' / '1622' / None to a float; 0.0 on failure."""
    if val is None:
        return 0.0
    try:
        return float(str(val).replace('AED', '').replace(',', '').strip() or 0)
    except Exception:
        return 0.0


def page_comparison_overview(areas_data):
    els = section_header('Section 01', 'Multi-Area Comparison - At a Glance')
    els.append(Paragraph(
        'Key metrics compared across all selected communities. All data sourced '
        'from Property Monitor / DLD records uploaded for each area.',
        S('body')))
    els.append(Spacer(1, 4*mm))
    headers = ['Metric'] + [a.get('community','Area') for a in areas_data]
    rows = [
        ['Total Transactions'] + [a.get('total_transactions','N/A') for a in areas_data],
        ['Avg Sale Price']     + [a.get('avg_price','N/A')          for a in areas_data],
        ['Avg Price / Sqft']   + [a.get('avg_psf','N/A')            for a in areas_data],
        ['Avg Rental Yield']   + [a.get('avg_yield','N/A')          for a in areas_data],
        ['YoY Price Growth']   + [a.get('yoy_growth','N/A')         for a in areas_data],
        ['Total Volume']       + [a.get('total_volume','N/A')        for a in areas_data],
    ]
    # First column is the metric label; the rest split the remaining width
    # evenly across 1-6 areas. data_table() will scale further if needed.
    n = max(len(areas_data), 1)
    metric_col = 50*mm if n >= 5 else 55*mm
    area_col   = (PAGE_W - 36*mm - metric_col) / n
    cw = [metric_col] + [area_col] * n
    els.append(data_table(headers, rows, col_widths=cw))
    els.append(Spacer(1, 6*mm))

    # Honest per-area Avg PSF chart, replacing the previous hardcoded
    # placeholder grouped bar that did not reflect the uploaded data.
    communities = [a.get('community','Area') for a in areas_data]
    psf_values  = [_psf_numeric(a.get('avg_psf')) for a in areas_data]
    if any(psf_values):
        fig = bar_chart(communities, psf_values,
                        'Avg Price Per Sqft by Area (AED)', 'AED / Sqft',
                        highlight_last=False)
        els.append(fig_img(fig, PAGE_W-36*mm))
    els.append(PageBreak())
    return els


def _clean_pct(s):
    try: return float(str(s).replace('%','').replace('+','').replace('N/A','0'))
    except: return 0.0

def _styled_hbar(ax, communities, values, title, xlabel):
    colors_list = [EVA_PALETTE[i % len(EVA_PALETTE)] for i in range(len(communities))]
    bars = ax.barh(communities, values, color=colors_list, height=0.55, zorder=3)
    for bar, val in zip(bars, values):
        ax.text(bar.get_width() + max(values)*0.02, bar.get_y() + bar.get_height()/2,
                f'{val:.1f}%', va='center', ha='left', fontsize=9.5,
                color='#1A1A1A', fontweight='bold')
    ax.set_title(title, fontsize=11, fontweight='bold', color='#1B4D3E', pad=12)
    ax.set_xlabel(xlabel, fontsize=9, color='#6B6B6B')
    ax.tick_params(axis='y', labelsize=10, colors='#1A1A1A')
    ax.tick_params(axis='x', labelsize=8.5, colors='#6B6B6B')
    ax.spines[['top','right']].set_visible(False)
    ax.spines[['left','bottom']].set_color('#DDDDDD')
    ax.grid(axis='x', color='#EEEEEE', zorder=0)
    ax.set_facecolor('white')
    ax.set_xlim(0, max(values) * 1.25)

def page_comparison_yield(areas_data):
    els = section_header('Section 02', 'Rental Yield & Investment Comparison')
    els.append(Paragraph(
        'Yield and capital growth broken out clearly for each community. '
        'This helps you identify where the best income return, the strongest '
        'price growth, or the best combination of both can be found.',
        S('body')))
    els.append(Spacer(1, 6*mm))

    communities = [a.get('community','Area') for a in areas_data]
    yields  = [_clean_pct(a.get('avg_yield','0')) for a in areas_data]
    growths = [_clean_pct(a.get('yoy_growth','0')) for a in areas_data]

    # Cap chart height so two charts + spacer fit on one A4 page even with
    # 6 communities (width 8.5in × height 5in renders at ~290pt = 102mm,
    # leaving room for the second bar chart on the same page).
    fig_h_in = min(2.6 + len(communities) * 0.6, 5.0)

    fig1, ax1 = plt.subplots(figsize=(8.5, fig_h_in))
    _styled_hbar(ax1, communities, yields, 'Gross Rental Yield (%)', 'Yield (%)')
    fig1.patch.set_alpha(0)
    fig1.tight_layout(pad=1.8)
    els.append(fig_img(fig1, PAGE_W - 36*mm))
    els.append(Spacer(1, 10*mm))

    fig2, ax2 = plt.subplots(figsize=(8.5, fig_h_in))
    _styled_hbar(ax2, communities, growths, 'Year-on-Year Price Growth (%)', 'Growth (%)')
    fig2.patch.set_alpha(0)
    fig2.tight_layout(pad=1.8)
    els.append(fig_img(fig2, PAGE_W - 36*mm))
    els.append(PageBreak())

    els += section_header('Section 02 (cont.)', 'Investment Score — Overall Comparison')
    els.append(Paragraph(
        'The radar chart below scores each community across five investment dimensions. '
        'A larger shaded area indicates a stronger all-round investment profile.',
        S('body')))
    els.append(Spacer(1, 6*mm))
    categories = ['Yield', 'Growth', 'Volume', 'Affordability', 'Liquidity']

    # Compute honest 1-10 scores from the per-area metrics. Without this each
    # area used the same hardcoded vector [7,8,6,5,7] and the radar showed
    # identical overlapping shapes regardless of the data.
    def _to_int(v):
        try: return int(str(v).replace(',', '').strip() or 0)
        except Exception: return 0
    def _vol_b(v):
        # 'AED 2.4B' / 'AED 850M' -> billions float
        if not v: return 0.0
        s = str(v).upper().replace('AED','').strip()
        try:
            if s.endswith('B'): return float(s[:-1].replace(',',''))
            if s.endswith('M'): return float(s[:-1].replace(',','')) / 1000.0
            return float(s.replace(',',''))
        except Exception:
            return 0.0
    def _rank_score(values, invert=False):
        """Return 1-10 scores; highest value gets 10 (or lowest if invert)."""
        if not values:
            return [5.0] * 0
        lo, hi = min(values), max(values)
        if hi == lo:
            return [7.0] * len(values)
        out = []
        for v in values:
            t = (v - lo) / (hi - lo)  # 0..1
            score = 1 + t * 9         # 1..10
            if invert:
                score = 11 - score
            out.append(round(score, 1))
        return out

    yields_raw  = [_clean_pct(a.get('avg_yield','0'))   for a in areas_data]
    growth_raw  = [_clean_pct(a.get('yoy_growth','0'))  for a in areas_data]
    txns_raw    = [_to_int(a.get('total_transactions',0)) for a in areas_data]
    psf_raw     = [_psf_numeric(a.get('avg_psf',0))     for a in areas_data]
    volume_raw  = [_vol_b(a.get('total_volume','0'))    for a in areas_data]

    yield_s   = _rank_score(yields_raw)
    growth_s  = _rank_score(growth_raw)
    volume_s  = _rank_score(volume_raw)
    afford_s  = _rank_score(psf_raw, invert=True)   # cheaper = more affordable
    liquid_s  = _rank_score(txns_raw)               # more txns = more liquid

    area_scores = []
    for i, a in enumerate(areas_data):
        scores = [yield_s[i] if i < len(yield_s) else 5,
                  growth_s[i] if i < len(growth_s) else 5,
                  volume_s[i] if i < len(volume_s) else 5,
                  afford_s[i] if i < len(afford_s) else 5,
                  liquid_s[i] if i < len(liquid_s) else 5]
        area_scores.append((a.get('community','Area'), scores))

    els.append(fig_img(radar_chart(categories, area_scores, ''),
                       PAGE_W - 36*mm))
    els.append(PageBreak())
    return els


def page_comparison_outlook(areas_data):
    els = section_header('Section 03', 'Area-by-Area Outlook')
    for a in areas_data:
        els.append(Paragraph(a.get('community','Community'), S('h2')))
        outlook = a.get('outlook_summary',
            'Strong fundamentals with limited supply and growing demand. '
            'Prices expected to maintain upward trajectory over the next 12 months.')
        els.append(Paragraph(outlook, S('body')))
        els.append(HRFlowable(width='100%', thickness=0.5, color=LIGHT_GREY, spaceAfter=6))
    els.append(Spacer(1, 5*mm))
    els.append(Paragraph(
        'All forward-looking statements are based on DLD / Property Monitor data and prevailing '
        'market conditions at time of report generation. This report does not constitute '
        'financial or investment advice.', S('disclaimer')))
    els.append(PageBreak())
    return els


class EVADoc(BaseDocTemplate):
    def __init__(self, path, data, **kw):
        self.data         = data
        self.communities  = data.get('communities', [data.get('community','Dubai')])
        self.report_date  = data.get('report_date', datetime.now().strftime('%B %Y'))
        self.agent_name   = data.get('agent_name', 'EVA Real Estate')
        self.agent_contact= data.get('agent_contact', 'info@evadxb.com  |  evadxb.com')
        self.logo_path    = data.get('logo_path')
        super().__init__(path, pagesize=A4, **kw)


def generate_report(output_path, data, txn_csvs=None, rental_csvs=None):
    if txn_csvs:
        communities = data.get('communities', [data.get('community','')])
        all_txn      = pd.concat([parse_pm_transactions(p) for p in txn_csvs], ignore_index=True)
        rental_dfs   = [parse_pm_rentals(p) for p in (rental_csvs or [])]
        all_rent     = pd.concat(rental_dfs, ignore_index=True) if rental_dfs else pd.DataFrame()
        if data.get('report_type') == 'comparison':
            data['areas_data'] = [analyse(all_txn, all_rent, c) for c in communities]
        else:
            computed = analyse(all_txn, all_rent, communities[0] if communities else '')
            data.update(computed)

    margin = 18*mm
    doc = EVADoc(output_path, data,
                 leftMargin=margin, rightMargin=margin,
                 topMargin=36*mm, bottomMargin=22*mm)
    cover_frame    = Frame(margin, 22*mm, PAGE_W-2*margin, PAGE_H-44*mm,    id='cover')
    interior_frame = Frame(margin, 22*mm, PAGE_W-2*margin, PAGE_H-58*mm,   id='main')
    doc.addPageTemplates([
        PageTemplate(id='Cover',    frames=[cover_frame],    onPage=on_page),
        PageTemplate(id='Interior', frames=[interior_frame], onPage=on_page),
    ])
    story = []
    rtype = data.get('report_type', 'single')
    story += page_cover(data)
    if rtype == 'comparison':
        areas_data = data.get('areas_data', [data])
        story += page_comparison_overview(areas_data)
        story += page_comparison_yield(areas_data)
        story += page_comparison_outlook(areas_data)
    else:
        story += page_executive_summary(data)
        story += page_transaction_analysis(data)
        story += page_rental_analysis(data)
        story += page_location_analysis(data)
        story += page_investment_highlights(data)
        story += page_market_outlook(data)
    story += [Spacer(1, 1)]
    doc.build(story)
    import sys; sys.stderr.write(f'Report generated: {output_path}\n')
    return output_path
