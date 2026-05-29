"""
Script que genera el notebook de analytics del Gold layer.
Ejecutar dentro del contenedor: python3 create_notebooks.py
"""
import json
import os

def nb(cells):
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.8.10"}
        },
        "cells": cells
    }

def md(source):
    return {"cell_type": "markdown", "metadata": {}, "source": source, "id": os.urandom(4).hex()}

def code(source):
    return {
        "cell_type": "code", "metadata": {}, "source": source,
        "outputs": [], "execution_count": None, "id": os.urandom(4).hex()
    }

# ─────────────────────────────────────────────────────────────────
# NOTEBOOK 1 — Gold Layer Analytics
# ─────────────────────────────────────────────────────────────────

notebook_gold = nb([
    md("# 🏆 Enterprise Data Pipeline — Gold Layer Analytics\n\n"
       "> **Medallion Architecture**: Bronze → Silver → **Gold** ← estás aquí\n\n"
       "Este notebook explora los datasets analytics-ready del Gold layer:\n"
       "- 🎯 **Customer RFM Segmentation** — quiénes son nuestros mejores clientes\n"
       "- 📈 **Daily Revenue Trends** — evolución del negocio con moving averages\n"
       "- 📦 **Product Performance** — qué categorías generan más revenue\n"
       "- 🏪 **Seller Scorecard** — KPIs y tier ranking de vendedores"),

    md("## 0. Setup — Spark + MinIO Connection"),

    code("""\
import sys
sys.path.insert(0, '/opt/spark-apps')

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Matplotlib style ──────────────────────────────────────────────
plt.rcParams.update({
    'figure.facecolor': '#0f1117',
    'axes.facecolor': '#1a1d2e',
    'axes.edgecolor': '#2d3250',
    'axes.labelcolor': '#e0e0e0',
    'text.color': '#e0e0e0',
    'xtick.color': '#9e9e9e',
    'ytick.color': '#9e9e9e',
    'grid.color': '#2d3250',
    'grid.alpha': 0.5,
    'font.family': 'DejaVu Sans',
    'font.size': 11,
    'axes.titlesize': 14,
    'axes.titleweight': 'bold',
    'figure.titlesize': 16,
})

PALETTE = ['#6c63ff', '#f72585', '#4cc9f0', '#f8961e', '#43aa8b',
           '#90be6d', '#577590', '#f94144', '#277da1', '#f3722c']

# ── SparkSession ──────────────────────────────────────────────────
spark = (SparkSession.builder
    .appName("gold-analytics-notebook")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")
print(f"✅ SparkSession ready | version={spark.version}")
"""),

    code("""\
# ── Cargar Gold tables ────────────────────────────────────────────
rfm_df    = spark.read.parquet("s3a://gold/customer_rfm/")
revenue_df = spark.read.parquet("s3a://gold/daily_revenue_summary/")
product_df = spark.read.parquet("s3a://gold/product_performance/")
seller_df  = spark.read.parquet("s3a://gold/seller_scorecard/")

print(f"📊 customer_rfm:          {rfm_df.count():>8,} rows")
print(f"📊 daily_revenue_summary: {revenue_df.count():>8,} rows")
print(f"📊 product_performance:   {product_df.count():>8,} rows")
print(f"📊 seller_scorecard:      {seller_df.count():>8,} rows")
"""),

    md("## 1. 🎯 Customer RFM Segmentation\n\n"
       "RFM (Recency, Frequency, Monetary) es el modelo de segmentación de clientes "
       "más usado en CRM. Cada cliente recibe un score de 1–5 en cada dimensión."),

    code("""\
rfm = rfm_df.toPandas()

# ── Segment distribution ──────────────────────────────────────────
seg_counts = rfm['customer_segment'].value_counts().reset_index()
seg_counts.columns = ['segment', 'count']
seg_counts['pct'] = seg_counts['count'] / seg_counts['count'].sum() * 100

# ── RFM Score distribution ────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle('Customer RFM Segmentation', fontsize=18, fontweight='bold', color='white', y=1.02)

# Donut chart — segmentos
colors = PALETTE[:len(seg_counts)]
wedges, texts, autotexts = axes[0].pie(
    seg_counts['count'], labels=seg_counts['segment'],
    autopct='%1.1f%%', colors=colors,
    pctdistance=0.82, startangle=90,
    wedgeprops=dict(width=0.55, edgecolor='#0f1117', linewidth=2)
)
for t in texts: t.set_color('#e0e0e0'); t.set_fontsize(10)
for at in autotexts: at.set_color('white'); at.set_fontweight('bold'); at.set_fontsize(9)
centre = plt.Circle((0,0), 0.45, fc='#1a1d2e')
axes[0].add_artist(centre)
axes[0].text(0, 0, f"{len(rfm):,}\\ncustomers", ha='center', va='center',
             fontsize=13, fontweight='bold', color='white')
axes[0].set_title('Customer Segments', pad=15)

# Bar chart — RFM score distribution
rfm_counts = rfm['rfm_score'].value_counts().sort_index()
bars = axes[1].bar(rfm_counts.index, rfm_counts.values,
                   color=PALETTE[0], alpha=0.85, edgecolor='#6c63ff', linewidth=0.5)
axes[1].set_xlabel('RFM Score (3 = worst, 15 = best)')
axes[1].set_ylabel('Number of Customers')
axes[1].set_title('RFM Score Distribution')
axes[1].grid(axis='y', alpha=0.3)
for bar in bars:
    h = bar.get_height()
    axes[1].text(bar.get_x() + bar.get_width()/2., h + 5,
                 f'{int(h):,}', ha='center', va='bottom', fontsize=9, color='#9e9e9e')

plt.tight_layout()
plt.savefig('/tmp/rfm_segments.png', dpi=150, bbox_inches='tight', facecolor='#0f1117')
plt.show()
print(seg_counts.to_string(index=False))
"""),

    code("""\
# ── RFM Scatter — Recency vs Monetary, colored by segment ─────────
fig, ax = plt.subplots(figsize=(14, 7))

segments = rfm['customer_segment'].unique()
for i, seg in enumerate(sorted(segments)):
    mask = rfm['customer_segment'] == seg
    ax.scatter(rfm.loc[mask, 'recency_days'],
               rfm.loc[mask, 'monetary_value'],
               c=PALETTE[i % len(PALETTE)], label=seg,
               alpha=0.65, s=40, edgecolors='none')

ax.set_xlabel('Recency (days since last purchase) →  Lower = Better')
ax.set_ylabel('Monetary Value (Total Spend $)')
ax.set_title('RFM Landscape — Recency vs Monetary Value by Segment', pad=15)
ax.legend(loc='upper right', framealpha=0.2, labelcolor='white')
ax.grid(True, alpha=0.2)

# Annotations
ax.axvline(rfm['recency_days'].median(), color='#f72585', linestyle='--', alpha=0.4, linewidth=1)
ax.text(rfm['recency_days'].median() + 2, ax.get_ylim()[1]*0.95,
        'Median\\nRecency', color='#f72585', fontsize=9)

plt.tight_layout()
plt.savefig('/tmp/rfm_scatter.png', dpi=150, bbox_inches='tight', facecolor='#0f1117')
plt.show()
"""),

    md("## 2. 📈 Revenue Trends — Daily KPIs con Moving Average\n\n"
       "Análisis temporal del revenue diario con Adaptive Query Execution y Window functions en Spark SQL."),

    code("""\
rev = revenue_df.toPandas()
rev['order_date'] = pd.to_datetime(rev['order_date'])
rev = rev.sort_values('order_date')

fig, axes = plt.subplots(3, 1, figsize=(16, 14), sharex=True)
fig.suptitle('Daily Revenue & Business KPIs', fontsize=18, fontweight='bold', color='white')

# ── Revenue diario + 7d moving average ────────────────────────────
axes[0].fill_between(rev['order_date'], rev['total_revenue'],
                      alpha=0.25, color='#6c63ff')
axes[0].plot(rev['order_date'], rev['total_revenue'],
             color='#6c63ff', linewidth=1.2, alpha=0.8, label='Daily Revenue')
axes[0].plot(rev['order_date'], rev['revenue_7d_moving_avg'],
             color='#f72585', linewidth=2.5, label='7-Day Moving Avg')
axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))
axes[0].set_ylabel('Revenue')
axes[0].set_title('Daily Revenue with 7-Day Moving Average')
axes[0].legend(framealpha=0.2)
axes[0].grid(True, alpha=0.2)

# ── On-time delivery rate ─────────────────────────────────────────
axes[1].fill_between(rev['order_date'],
                      rev['on_time_delivery_rate_pct'].fillna(0),
                      alpha=0.3, color='#43aa8b')
axes[1].plot(rev['order_date'],
              rev['on_time_delivery_rate_pct'].fillna(0),
              color='#43aa8b', linewidth=1.8, label='On-Time Delivery %')
axes[1].axhline(rev['on_time_delivery_rate_pct'].mean(), color='#f8961e',
                 linestyle='--', alpha=0.7, linewidth=1.5, label='Average')
axes[1].set_ylabel('On-Time Rate %')
axes[1].set_title('Delivery On-Time Rate')
axes[1].set_ylim(0, 110)
axes[1].legend(framealpha=0.2)
axes[1].grid(True, alpha=0.2)

# ── Unique customers per day ──────────────────────────────────────
axes[2].bar(rev['order_date'], rev['unique_customers'],
             color='#4cc9f0', alpha=0.7, width=1)
axes[2].set_ylabel('Unique Customers')
axes[2].set_title('Daily Active Customers')
axes[2].grid(True, axis='y', alpha=0.2)
axes[2].set_xlabel('Date')

plt.tight_layout()
plt.savefig('/tmp/revenue_trends.png', dpi=150, bbox_inches='tight', facecolor='#0f1117')
plt.show()

# KPIs summary
print(f"\\n{'='*50}")
print(f"  BUSINESS KPIs SUMMARY")
print(f"{'='*50}")
print(f"  Total Revenue:          ${rev['total_revenue'].sum():>12,.2f}")
print(f"  Avg Daily Revenue:      ${rev['total_revenue'].mean():>12,.2f}")
print(f"  Total Orders:           {rev['total_orders'].sum():>12,}")
print(f"  Avg On-Time Delivery:   {rev['on_time_delivery_rate_pct'].mean():>11.1f}%")
print(f"  Avg Cancellation Rate:  {rev['cancellation_rate_pct'].mean():>11.1f}%")
print(f"{'='*50}")
"""),

    md("## 3. 📦 Product Performance by Category"),

    code("""\
prod = product_df.toPandas()
prod = prod.dropna(subset=['category', 'total_revenue'])

cat_perf = (prod.groupby('category')
    .agg(total_revenue=('total_revenue', 'sum'),
         total_orders=('total_orders', 'sum'),
         products=('product_id', 'count'))
    .sort_values('total_revenue', ascending=False)
    .head(12).reset_index())

fig, axes = plt.subplots(1, 2, figsize=(18, 7))
fig.suptitle('Product Performance by Category', fontsize=18, fontweight='bold', color='white')

# Horizontal bar — Revenue por categoría
colors_bar = [PALETTE[i % len(PALETTE)] for i in range(len(cat_perf))]
bars = axes[0].barh(cat_perf['category'][::-1],
                     cat_perf['total_revenue'][::-1],
                     color=colors_bar[::-1], alpha=0.85, edgecolor='none')
axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x/1000:.0f}K'))
axes[0].set_xlabel('Total Revenue')
axes[0].set_title('Revenue by Category (Top 12)')
axes[0].grid(axis='x', alpha=0.3)
for bar in bars:
    w = bar.get_width()
    axes[0].text(w * 1.01, bar.get_y() + bar.get_height()/2,
                 f'${w/1000:.1f}K', va='center', fontsize=9, color='#9e9e9e')

# Bubble chart — Revenue vs Orders
scatter = axes[1].scatter(
    cat_perf['total_orders'],
    cat_perf['total_revenue'],
    s=cat_perf['products'] * 15,
    c=range(len(cat_perf)),
    cmap='plasma', alpha=0.85, edgecolors='white', linewidth=0.8)
for _, row in cat_perf.iterrows():
    axes[1].annotate(row['category'],
                      (row['total_orders'], row['total_revenue']),
                      textcoords='offset points', xytext=(6, 4),
                      fontsize=8, color='#cccccc')
axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x/1000:.0f}K'))
axes[1].set_xlabel('Total Orders')
axes[1].set_ylabel('Total Revenue')
axes[1].set_title('Revenue vs Orders\\n(bubble size = # products)')
axes[1].grid(True, alpha=0.2)

plt.tight_layout()
plt.savefig('/tmp/product_performance.png', dpi=150, bbox_inches='tight', facecolor='#0f1117')
plt.show()
"""),

    md("## 4. 🏪 Seller Scorecard — KPIs & Tier Ranking\n\n"
       "Los sellers se clasifican en tiers (Platinum/Gold/Silver/Bronze) "
       "basado en su percentil de revenue."),

    code("""\
sellers = seller_df.toPandas()
sellers = sellers.dropna(subset=['seller_tier', 'total_revenue'])

tier_order = ['Platinum', 'Gold', 'Silver', 'Bronze']
tier_colors = {'Platinum': '#e5e4e2', 'Gold': '#ffd700',
               'Silver': '#c0c0c0', 'Bronze': '#cd7f32'}

fig, axes = plt.subplots(1, 3, figsize=(18, 6))
fig.suptitle('Seller Scorecard & Tier Analysis', fontsize=18, fontweight='bold', color='white')

# ── Tier distribution ─────────────────────────────────────────────
tier_counts = sellers['seller_tier'].value_counts().reindex(tier_order).fillna(0)
bars = axes[0].bar(tier_order, tier_counts.values,
                    color=[tier_colors[t] for t in tier_order],
                    edgecolor='#0f1117', linewidth=1.5, width=0.6)
axes[0].set_ylabel('Number of Sellers')
axes[0].set_title('Sellers by Tier')
axes[0].grid(axis='y', alpha=0.3)
for bar, tier in zip(bars, tier_order):
    h = bar.get_height()
    pct = h / len(sellers) * 100
    axes[0].text(bar.get_x() + bar.get_width()/2., h + 1,
                 f'{int(h)}\\n({pct:.0f}%)', ha='center', va='bottom',
                 fontsize=10, color='white', fontweight='bold')

# ── Revenue distribution per tier (box plot) ──────────────────────
tier_data = [sellers[sellers['seller_tier'] == t]['total_revenue'].dropna().values
             for t in tier_order]
bp = axes[1].boxplot(tier_data, labels=tier_order, patch_artist=True,
                      medianprops=dict(color='#0f1117', linewidth=2))
for patch, tier in zip(bp['boxes'], tier_order):
    patch.set_facecolor(tier_colors[tier])
    patch.set_alpha(0.85)
axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))
axes[1].set_ylabel('Total Revenue')
axes[1].set_title('Revenue Distribution by Tier')
axes[1].grid(axis='y', alpha=0.3)

# ── Review score vs Revenue scatter ──────────────────────────────
for tier in tier_order:
    mask = sellers['seller_tier'] == tier
    subset = sellers[mask].dropna(subset=['avg_review_score', 'total_revenue'])
    if len(subset) > 0:
        axes[2].scatter(subset['avg_review_score'], subset['total_revenue'],
                         c=tier_colors[tier], label=tier, alpha=0.75, s=50,
                         edgecolors='none')
axes[2].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}'))
axes[2].set_xlabel('Average Review Score (1–5)')
axes[2].set_ylabel('Total Revenue')
axes[2].set_title('Review Score vs Revenue')
axes[2].legend(framealpha=0.2, labelcolor='white')
axes[2].grid(True, alpha=0.2)
axes[2].set_xlim(0.5, 5.5)

plt.tight_layout()
plt.savefig('/tmp/seller_scorecard.png', dpi=150, bbox_inches='tight', facecolor='#0f1117')
plt.show()

# Summary stats
print(f"\\n{'='*55}")
print(f"  SELLER TIER SUMMARY")
print(f"{'='*55}")
for tier in tier_order:
    subset = sellers[sellers['seller_tier'] == tier]
    print(f"  {tier:<10}: {len(subset):>4} sellers | "
          f"Avg Revenue: ${subset['total_revenue'].mean():>10,.2f} | "
          f"Avg Score: {subset['avg_review_score'].mean():.2f}")
print(f"{'='*55}")
"""),

    md("## 5. 💡 Resumen Ejecutivo"),

    code("""\
fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('Executive Dashboard — Enterprise Data Pipeline',
             fontsize=20, fontweight='bold', color='white', y=1.02)

# ── KPI Cards ─────────────────────────────────────────────────────
kpis = [
    ('Total Revenue', f"${rev['total_revenue'].sum():,.0f}", '#6c63ff'),
    ('Total Customers', f"{len(rfm):,}", '#f72585'),
    ('On-Time Delivery', f"{rev['on_time_delivery_rate_pct'].mean():.1f}%", '#43aa8b'),
    ('Active Sellers', f"{len(sellers):,}", '#f8961e'),
]
for ax, (title, value, color) in zip(axes.flat, kpis):
    ax.set_facecolor('#1a1d2e')
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.axis('off')
    ax.add_patch(plt.Rectangle((0.05, 0.05), 0.9, 0.9,
                                 fill=True, facecolor='#22263d',
                                 edgecolor=color, linewidth=2,
                                 transform=ax.transAxes, clip_on=False))
    ax.text(0.5, 0.72, title, ha='center', va='center',
             transform=ax.transAxes, fontsize=14, color='#9e9e9e')
    ax.text(0.5, 0.42, value, ha='center', va='center',
             transform=ax.transAxes, fontsize=32, fontweight='bold', color=color)

    # Segmento RFM más frecuente como subtítulo
    if title == 'Total Customers':
        top_seg = rfm['customer_segment'].value_counts().index[0]
        ax.text(0.5, 0.22, f"Top segment: {top_seg}", ha='center', va='center',
                 transform=ax.transAxes, fontsize=10, color='#7e7e9e')
    elif title == 'Total Revenue':
        best_month = rev.loc[rev['total_revenue'].idxmax(), 'order_date']
        ax.text(0.5, 0.22, f"Peak: {best_month.strftime('%b %Y')}", ha='center', va='center',
                 transform=ax.transAxes, fontsize=10, color='#7e7e9e')

plt.tight_layout()
plt.savefig('/tmp/executive_dashboard.png', dpi=150, bbox_inches='tight', facecolor='#0f1117')
plt.show()

print("\\n✅ Análisis completo.")
print("   Imágenes exportadas a /tmp/*.png")
print("\\nPipeline ejecutado exitosamente:")
print("  Bronze (CSV raw) → Silver (Parquet limpio) → Gold (Analytics)")
print("  Tecnologías: Apache Spark 3.5 | MinIO | Airflow | Delta Lake")
"""),
])

# ── Guardar notebooks ──────────────────────────────────────────────
os.makedirs("/opt/spark-apps/notebooks", exist_ok=True)
nb_path = "/opt/spark-apps/notebooks/01_gold_analytics.ipynb"

with open(nb_path, "w") as f:
    json.dump(notebook_gold, f, indent=2)

print(f"✅ Notebook creado: {nb_path}")
print("   Abrí http://localhost:8888 y navegá a notebooks/01_gold_analytics.ipynb")
