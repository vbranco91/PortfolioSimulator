import streamlit as st
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from sklearn import linear_model

# 00. Settings
st.set_page_config(page_title="Portfolio Simulator | Actuarial Tool", layout="wide")

# Definição das cores oficiais (pág. 16 do PDF)
EA_BLUE = "#1A1446"   # Cor para Headlines (pág. 28)
EA_YELLOW = "#FFD000" # Cor primária/Destaque
EA_DARK_GRAY = "#343741" # Cor para Body Copy
# Cores secundarias
EA_TEAL = "#78E1E1"
EA_LIGHT_GRAY = "#BFBFBF"
EA_WHITE = "#FFFFFF"

# Import Main Dataset
dir = 'C:/Users/vinic/OneDrive/Desktop/Vini/02.Projetos/ExposicaoAtuarial/2025/Simulador/20.Template/04.Outputs/'
file_name = dir + 'Db_Cens_2025December_v1.parquet'
df = pd.read_parquet(file_name)
df_org = df.copy()

DataRef = pd.to_datetime('2025-12-01')

# Cenario ID Dictionary Arrangement
dim_Cenarios = pd.DataFrame({
    'CombinationID': df['CombinationID'].unique(),
    'Cenario': ['Actuals', 'CenBaseline'] + [f'Cen_{i}' for i in range(1, len(df['CombinationID'].unique()) - 1)]
})
dict_Cenarios = dict(zip(dim_Cenarios['CombinationID'], dim_Cenarios['Cenario']))

# Color Mappings
map_Colors = {
    dict_Cenarios[-5]: EA_DARK_GRAY,
    dict_Cenarios[0]: EA_YELLOW
}
map_Colors.update({c: EA_LIGHT_GRAY for c in list(dict_Cenarios.values())[2:]})

# Coverage Mapping
df_coverage = pd.DataFrame({
    'CoverageID': ['PhysDam Partial', 'PhysDam Total', 'Theft', 'All'],
    'ClaimType': ['ClaimsPP', 'ClaimsPT', 'ClaimsTheft', 'TotalClaims'],
    'AmountType': ['AmountPP', 'AmountPT', 'AmountTheft', 'TotalAmount']
})

# 1. Title (Headline)
# O manual recomenda EA Blue para manchetes e uso de pontuação (pág. 28).
st.markdown(f"<h1 style='color: {EA_BLUE};'>Portfolio Simulator | Actuarial Tool.</h1>", unsafe_allow_html=True)

# Aplicação de CSS para Tipografia e Cores
st.markdown(f"""
    <style>
    .main {{ background-color: {EA_WHITE}; font-family: 'Helvetica', sans-serif; }}
    h1 {{ color: {EA_BLUE}; border-bottom: 3px solid {EA_YELLOW}; }}
    h2, h3 {{ color: {EA_BLUE}; }}
    .stButton>button {{ background-color: {EA_TEAL}; color: {EA_BLUE}; border-radius: 5px; font-weight: bold; }}
    .sidebar .sidebar-content {{ background-color: {EA_DARK_GRAY}; }}
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR (Conteúdo) ---
st.sidebar.header("Contents")
st.sidebar.markdown("""
- [Used Tools](#used-tools)
- [The Data Set](#the-data-set)
- [Pipelines](#pipelines)
- [Visualizations](#visualizations)
- [Demo](#demo)
- [Conclusion](#conclusion)
""")



# Side bar for filtering
with st.sidebar:
#   st.header('Filter Cenario')
   with st.expander("📂 Cenários e Combinações"):
   # Select boxes for filtering
       CombinationID = st.multiselect("Select CombinationID", 
                                      options=df['CombinationID'].unique(),
                                      default=[-5, 0])
  
   with st.expander("📦 Produtos"):
   # Select boxes for filtering
       ProductCod = st.multiselect("Select Product ID", 
                                options=df['ProductCod'].unique(),
                                default=df['ProductCod'].unique())
   
   with st.expander("💼 Tipo de Negócio"):
   # Select boxes for filtering
       CodBusinessType = st.multiselect("Select Business Type ID", 
                                        options=df['BusinessTypeCod'].unique(),
                                        default=df['BusinessTypeCod'].unique())

   with st.expander("🚗 Tipo de Cobertura"):
   # Select boxes for filtering
       CoverageID = st.selectbox("Select Coverage Type", 
                                        options= ['PhysDam Partial', 'PhysDam Total', 'Theft', 'All'])

# Filter the DataFrame based on the selected values
df = df[(df['CombinationID'].isin(CombinationID)) &
        (df['ProductCod'].isin(ProductCod)) &
        (df['BusinessTypeCod'].isin(CodBusinessType))]


# --- HEADER PRINCIPAL ---
st.subheader("Actuarial Portfolio Assessment Tool")

# --- INTRODUCTION & GOALS ---
with st.expander("Introduction & Goals", expanded=True):
    st.write("""
    ### Executive Summary
    This project simulates actuarial loss trends and frequency/severity distributions. 
    Using **Python** and **Streamlit**, we process insurance datasets to evaluate risk exposure.
    - **Objective:** Provide a real-time dashboard for portfolio health monitoring.
    - **Tools:** Pandas for processing, Plotly for visualization.
    - **Data:** Synthetic actuarial premium and claims data.
    """)

# SEPARAÇÃO (Linha Horizontal - Amarela)
st.markdown(f"<hr style='border: 2px solid {EA_YELLOW};'>", unsafe_allow_html=True)

# --- USED TOOLS ---
st.header("Used Tools")
col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1: st.write("**Processing:** Pandas")
with col2: st.write("**Math:** NumPy")
with col3: st.write("**Regression:** Scikit-learn")
with col4: st.write("**Viz:** Plotly")
with col5: st.write("**UI:** Streamlit")
with col6: st.write("**Identity:** Brand Guide")

# --- THE DATA SET ---
st.header("The Data Set")
st.info("The dataset includes claim counts, incurred losses, and earned exposures per period.")
# Notas para análise futura:
# TODO: Integrar IBNR (Incurred But Not Reported) calculation logic here.
# TODO: Add seasonality factors for auto insurance lines.

# --- PIPELINES ---
st.header("Pipelines")

# 4. TEXTO DE CORPO (Body Copy)
# O guia sugere EA Dark Gray para textos longos (pág. 28).
st.markdown(f"""
<p style='color: {EA_DARK_GRAY};'>
Assumptions (PremParam & SinisParam)

**PremParam (Premium Parameters)**:

+ Cleans and imports premium and quotation data.
+ Establishes baselines for endorsements and cancellations using weighted averages (75% short-term, 25% long-term.
+ Models lags (delays) in endorsements/cancellations to capture timing effects.
+ Calculates conversion rates, average sum insured, and emission rates.
+ Defines retention/renewal ratios to project future portfolio behavior.


**SinisParam (Claims Parameters)**:

+ Builds a complete exposure grid to avoid missing data.
+ Applies pro-rata exposure factors to premiums, endorsements, and cancellations.
+ Calculates earned premium and exposure adjusted for time at risk.
+ Develops frequency and severity baselines using credibility weighting (70% recent, 30% historical).
+ Models salvage and recovery timing and percentages.
+ Applies regression (linear and polynomial) to identify trends in frequency/severity.
+ Introduces “Trended” adjustments to normalize historical costs to current levels.
+ Produces robust baselines for frequency, severity, and recovery.


**Projections**

+ Elasticity Modeling: Captures how conversion and retention rates react to price changes.
+ Scenario Loop (Monte Carlo): Tests tariff variations between -5% and +5% to measure sensitivity.
+ Renewal Dynamics: Projects future renewals based on past emissions and retention elasticity.
+ Macroeconomic Adjustments: Applies inflation factors to partial and total loss severities.
+ Anti-Selection Effects: Models how pricing changes alter risk composition (good vs. bad risks).
+ Final Outputs: Projects number and value of claims, adjusted for exposure, elasticity, and inflation.


💡 **Suggested Ideas for Future Improvements**

+ Automation: Develop automated ETL pipelines to reduce manual data imports.
+ Visualization: Add dashboards for scenario comparison (elasticity curves, renewal ratios, claim projections).
+ Machine Learning: Explore ML models for non-linear claim frequency/severity trends beyond polynomial regression.
+ Stress Testing: Incorporate extreme scenarios (economic shocks, regulatory changes) to test portfolio resilience.
+ Continuous Feedback Loop: Integrate analyst feedback directly into the simulator for iterative model refinement.
</p>
""", unsafe_allow_html=True)

# SEPARAÇÃO (Linha Horizontal - Amarela)
st.markdown(f"<hr style='border: 2px solid {EA_YELLOW};'>", unsafe_allow_html=True)


# --- VISUALIZATIONS ---
# SUBTÍTULO
st.header("Visualizations")
st.markdown(f"""
<p style='color: {EA_DARK_GRAY};'>
Summary of Key Performance Indicators.
</p>
""", unsafe_allow_html=True)

# SEPARAÇÃO NATIVA DO STREAMLIT (Linha Horizontal)
st.write("---") # Forma rápida de criar uma linha cinza sutil

# Section1. SUBTÍTULO
# O guia recomenda EA Blue para manchetes e uso de pontuação (pág. 28).
st.markdown(f"<h3 style='color: {EA_BLUE}; opacity: 0.8;'>Evolucao GWP e Comparativo com Target.</h3>", unsafe_allow_html=True)

# GWP 2026 vs Target
col1, = st.columns(1)

# Create First Chart - GWP 2026 Total
GWP_2026 = (df[(df['CalendarDate'] >= '2026-01-01') & 
                (df['CalendarDate'] <= '2026-12-31')
                & (df['CombinationID'].isin([-5, 0]))]
             ['GWP'].sum()
             )
GWPTarget_2026 = GWP_2026 / 0.97
df_chart2 = pd.DataFrame({
    'Category': ['GWP Projected 2026', 'GWP Target 2026'],
    'Value': [GWP_2026, GWPTarget_2026]
})
upper_limit = df_chart2['Value'].max() * 1.1
# lower_limit = df_chart2['Value'].min() * 0.9

fig_chart2 = (px
              .bar(df_chart2, x='Category', y='Value', 
                   color='Category',
                   color_discrete_map={'GWP Projected 2026': EA_YELLOW, 'GWP Target 2026': EA_DARK_GRAY},
                   title='GWP 2026 vs Target').update_layout(showlegend=False)
              .update_traces(texttemplate='%{y:,.0f}', textposition='outside')
              .update_yaxes(range=[0, upper_limit])
              .update_layout(
                         xaxis_title= None,
                         yaxis_title="GWP",
                         template="plotly_white",
                         uniformtext_minsize=10, 
                         uniformtext_mode='hide')
)
col1.plotly_chart(fig_chart2, width='stretch')

# SEPARAÇÃO NATIVA DO STREAMLIT (Linha Horizontal)
st.write("---") # Forma rápida de criar uma linha cinza sutil


# GWP Over Time
col2, = st.columns(1)

# Create First Chart - GWP Over Time
df_chart1 = (df[(df['CalendarDate'] >= '2025-01-01') & 
                (df['CalendarDate'] <= '2026-12-31') &
                (df['CombinationID'].isin(CombinationID)) ]
             .groupby(['CalendarDate', 'CombinationID'], as_index=False)
             ['GWP']
             .sum()             
             .reset_index()
             )
df_chart1['CombinationID'] = df_chart1['CombinationID'].map(dict_Cenarios)
upper_limit = df_chart1['GWP'].max() * 1.1
lower_limit = df_chart1['GWP'].min() * 0.9

fig_chart1 = (px.line(df_chart1, x='CalendarDate', y='GWP', 
                      color='CombinationID', 
                      color_discrete_map = map_Colors,
                      markers=True, title='GWP Over Time')
              .update_yaxes(range=[lower_limit, upper_limit])
              .update_layout(
                      xaxis_title="Date",
                      yaxis_title="GWP",
                      legend_title="Legend",
                      legend=dict(y=1, x=0.01),
                      template="plotly_white")
              )
col2.plotly_chart(fig_chart1, width='stretch')

# SEPARAÇÃO NATIVA DO STREAMLIT (Linha Horizontal)
st.write("---") # Forma rápida de criar uma linha cinza sutil


# GWP Yearly & AWP Over Time
col3, col4 = st.columns(2)

df_chart3 = df.copy()
df_chart3['Year'] = df_chart3['CalendarDate'].dt.year
df_chart3 = (df_chart3[(df_chart3['CalendarDate'] >= '2023-01-01') & 
                (df_chart3['CalendarDate'] <= '2026-12-31') &
                (df_chart3['CombinationID'].isin(CombinationID))]
             .groupby(['Year', 'CombinationID'], as_index=False)
             ['GWP']
             .sum()
             )

#Actuals_2026
# df_chart3['2026A'] = df_chart3[(df_chart3['Year'] == 2026) & (df_chart3['CombinationID'] == -5)]['GWP'].values[0]
df_chart3['2026A'] = 0
df_chart3.loc[df_chart3['CombinationID'] == -5, '2026A'] = 0
df_chart3['GWP'] = df_chart3['GWP'] + df_chart3['2026A']
df_chart3 = df_chart3[~((df_chart3['CombinationID'] == -5) & 
                      (df_chart3['Year'] == 2026))]

df_chart3['CombinationID'] = df_chart3['CombinationID'].map(dict_Cenarios)
upper_limit_gwp = df_chart3['GWP'].max() * 1.1
lower_limit_gwp = df_chart3['GWP'].min() * 0.9

fig_chart6 = (
    px.bar(df_chart3, x='Year', y='GWP', 
                     color='CombinationID', 
                     color_discrete_map = map_Colors,
                     barmode="group",
                     title='GWP by Year')
        .update_traces(texttemplate='%{y:,.0f}', textposition='outside')
        .update_yaxes(range=[lower_limit_gwp, upper_limit_gwp])
        .update_layout(xaxis_title= 'Year',
                         yaxis_title='GWP',
                         template='plotly_white',
                         uniformtext_minsize=10,
                         legend=dict(y=1, x=0.01),
                         uniformtext_mode='hide',
                         xaxis=dict(type="category"))
)
col3.plotly_chart(fig_chart6, width='stretch')

#
df_chart4 = (df[(df['CalendarDate'] >= '2025-01-01') & 
                (df['CalendarDate'] <= '2026-12-31') &
                (df['CombinationID'].isin(CombinationID))]
             .groupby(['CalendarDate', 'CombinationID'], as_index=False)
             [['WrittenPremium', 'WrittenItem']]
             .sum()
             )
df_chart4['AWP'] = df_chart4['WrittenPremium'] / df_chart4['WrittenItem']
df_chart4['CombinationID'] = df_chart4['CombinationID'].map(dict_Cenarios)
upper_limit_awp = df_chart4['AWP'].max() * 1.2
lower_limit_awp = df_chart4['AWP'].min() * 0.8

fig_chart4 = (px.line(df_chart4, x='CalendarDate', y='AWP', 
                      color='CombinationID', 
                      color_discrete_map = map_Colors,
                      markers=True, title='AWP Over Time')
              .update_yaxes(range=[lower_limit_awp, upper_limit_awp])
              .update_layout(
                      xaxis_title="Date",
                      yaxis_title="AWP",
                      legend_title="Legend",
                      legend=dict(y=1, x=0.01),
                      template="plotly_white")
              )
col4.plotly_chart(fig_chart4, width='stretch')

# SEPARAÇÃO NATIVA DO STREAMLIT (Linha Horizontal)
st.write("---") # Forma rápida de criar uma linha cinza sutil


# GWP vs Loss Ratio
col5, = st.columns(1)

# Section3. SUBTÍTULO
# O guia recomenda EA Blue para manchetes e uso de pontuação (pág. 28).
st.markdown(f"<h3 style='color: {EA_BLUE}; opacity: 0.8;'>GWP vs Loss Ratio (2026).</h3>", unsafe_allow_html=True)

# Create Fourth Chart - Scatter Plot of GWP vs LossRatio
df_actuals_chart4 = (df_org[(df_org['CalendarDate'] >= '2026-01-01') & 
                (df_org['CalendarDate'] <= '2026-12-31') & 
                (df_org['CombinationID'] == -5)]
             .groupby(['CombinationID'], as_index=False)
                [['GWP', 'EarnPrem', 'TotalAmount']]
                .sum()
)
# gwp_actuals = df_actuals_chart4["GWP"].values[0]
gwp_actuals = 0
EarnPrem_actuals = 0
TotalAmount_actuals = 0

df_chart4 = (df_org[(df_org['CalendarDate'] >= '2026-01-01') & 
                (df_org['CalendarDate'] <= '2026-12-31') & 
                (df_org['CombinationID'] != -5)]
             .groupby(['CombinationID'], as_index=False)
                [['GWP', 'EarnPrem', 'TotalAmount']]
                .sum()
)

# Somar os valores de GWP, EarnPrem e TotalAmount para cada combincao das projecoes de 2026, incluindo os actuals
df_chart4['GWP'] = df_chart4['GWP'] + gwp_actuals
df_chart4['EarnPrem'] = df_chart4['EarnPrem'] + EarnPrem_actuals
df_chart4['TotalAmount'] = df_chart4['TotalAmount'] + TotalAmount_actuals
df_chart4['LossRatio'] = df_chart4['TotalAmount'] / df_chart4['EarnPrem'] * 100

# 2. Identificação dos Pontos de Destaque
# Cenário Base (CombinationID 0)
baseline = df_chart4[df_chart4["CombinationID"] == 0]
baseline_id = 0
# Máximo GWP
max_gwp = df_chart4.loc[[df_chart4["GWP"].idxmax()]]
max_gwp_id = max_gwp['CombinationID'].values[0]
# Mínimo LossRatio
min_loss_ratio = df_chart4.loc[[df_chart4["LossRatio"].idxmin()]]
min_lr_id = min_loss_ratio['CombinationID'].values[0]

# 3. Construção do Gráfico
# Plotagem base com todos os pontos em cinza claro para destacar os alvos
fig_chart4 = px.scatter(
    df_chart4,
    x="LossRatio",
    y="GWP",
    hover_data=["CombinationID"],
    title="GWP vs Loss Ratio (2026)",
    labels={"LossRatio": "Loss Ratio (%)", "GWP": "GWP"},
)
fig_chart4.update_traces(
    marker=dict(color=EA_LIGHT_GRAY, size=6, opacity=0.7), name="Outros Cenários"
)

# Destaque: Baseline (ID: 0)
fig_chart4.add_trace(
    go.Scatter(
        x=baseline["LossRatio"],
        y=baseline["GWP"],
        mode="markers",
        marker=dict(color=EA_BLUE, size=12, symbol="circle"),
        name=f'Baseline (ID: {baseline["CombinationID"].values[0]})',
        hovertext=[f'ID: {baseline["CombinationID"].values[0]}'],
    )
)
# Destaque: Máximo GWP
fig_chart4.add_trace(
    go.Scatter(
        x=max_gwp["LossRatio"],
        y=max_gwp["GWP"],
        mode="markers",
        marker=dict(color=EA_YELLOW, size=12, symbol="diamond"),
        name=f'Máx GWP (ID: {max_gwp["CombinationID"].values[0]})',
        hovertext=[f'ID: {max_gwp["CombinationID"].values[0]}'],
    )
)
# Destaque: Mínimo Loss Ratio
fig_chart4.add_trace(
    go.Scatter(
        x=min_loss_ratio["LossRatio"],
        y=min_loss_ratio["GWP"],
        mode="markers",
        marker=dict(color=EA_YELLOW, size=12, symbol="square"),
        name=f'Mín Loss Ratio (ID: {min_loss_ratio["CombinationID"].values[0]})',
        hovertext=[f'ID: {min_loss_ratio["CombinationID"].values[0]}'],
    )
)

# Ajustes de Layout
fig_chart4.update_layout(
    xaxis_title="Loss Ratio (%)",
    yaxis_title="GWP",
    legend_title="Legend",
    legend=dict(yanchor='bottom', xanchor='right', y=0.05, x=1.15),
    template="plotly_white",
)
col5.plotly_chart(fig_chart4, width='stretch')

# Table Option 1
with st.expander("📊 Data Table - 2026 Projections"):
    # Configuração de colunas para melhor legibilidade
    # st.write("Insert Comment here")
    st.dataframe(
        df_chart4,
        column_config={
            "GWP": st.column_config.NumberColumn(
                "GWP",
                format="R$ %,d", # Adiciona prefixo e limita a 2 casas
            ),
            "EarnPrem": st.column_config.NumberColumn(
                "EarnPrem",
                format="%,d",
            ),
            "TotalAmount": st.column_config.NumberColumn(
                "TotalAmount",
                format="%,d",
            ),
            "LossRatio": st.column_config.NumberColumn(
                "Loss Ratio (%)",
                format="%.2f%%", # Adiciona o símbolo de porcentagem
            ),
        },
        hide_index=True, # Remove a coluna de índice da esquerda
        use_container_width=True # Faz a tabela ocupar toda a largura disponível
    )



# SEPARAÇÃO NATIVA DO STREAMLIT (Linha Horizontal)
st.write("---") # Forma rápida de criar uma linha cinza sutil


# Titles
col5, = st.columns(1)


# SEPARAÇÃO NATIVA DO STREAMLIT (Linha Horizontal)
st.write("---") # Forma rápida de criar uma linha cinza sutil


# Section2. SUBTÍTULO
# Frequency Over Time.
st.markdown(f"<h3 style='color: {EA_BLUE}; opacity: 0.8;'>Frequency Over Time.</h3>", unsafe_allow_html=True)

# 
col3, = st.columns(1)

# Create Third Chart - Frequency Over Time
indicators = df_coverage[df_coverage['CoverageID'] == CoverageID]['ClaimType'].values[0]
df_chart3 = (df[(df['CalendarDate'] >= '2023-01-01') & 
                (df['CalendarDate'] <= DataRef) & 
                (df['CombinationID'].isin(CombinationID))]
             .groupby(['CalendarDate', 'CombinationID'], as_index=False)
             [['Exposure'] + [indicators]]
             .sum().reset_index()
             )
df_chart3['CombinationID'] = df_chart3['CombinationID'].map({-5: 'Actuals', 0: 'CenBaseline'})
df_chart3.loc[df_chart3[indicators] == 0, indicators] = 1
df_chart3['Frequency'] = df_chart3[indicators] / df_chart3['Exposure']
upper_limit = df_chart3['Frequency'].max() * 1.2
lower_limit = df_chart3['Frequency'].min() * 0.8

df_chart3['Time'] = np.arange(len(df_chart3.index))
modelFreq = linear_model.LinearRegression()

X = df_chart3[['Time']]
fy = df_chart3['Frequency']   # Dependent variable
modelFreq.fit(X, fy)

# Create Polynomial Trend
# Trace Dependent variables
x = df_chart3['Time'].values
fy = df_chart3['Frequency'].values

# Build coefficients & Intercept
coeffs_2nd_degreeFreq = np.polyfit(x, fy, deg=2)
coeffs_3rd_degreeFreq = np.polyfit(x, fy, deg=3)

# Build Models
modelPoly2Freq = np.poly1d(coeffs_2nd_degreeFreq)
modelPoly3Freq = np.poly1d(coeffs_3rd_degreeFreq)

# Regression Frequency
df_chart3['LinearFreq'] = modelFreq.predict(X)
df_chart3['Poly2Freq'] = modelPoly2Freq(x)
df_chart3['Poly3Freq'] = modelPoly3Freq(x)

# Trending to Current Levels 
# Trended Frequency
df_chart3['TrendedFreqLinear'] = df_chart3['Frequency'] * df_chart3['LinearFreq'].iloc[-1] / df_chart3['LinearFreq']
df_chart3['TrendedFreqPoly2'] = df_chart3['Frequency'] * df_chart3['Poly2Freq'].iloc[-1] / df_chart3['Poly2Freq']
df_chart3['TrendedFreqPoly3'] = df_chart3['Frequency'] * df_chart3['Poly3Freq'].iloc[-1] / df_chart3['Poly3Freq']

# Calculate Baselines
# Baseline Frequency
df_chart3['BaselineFreq'] = df_chart3['TrendedFreqPoly3'].mean()
df_chart3 = df_chart3[['CalendarDate', 'CombinationID', 'Frequency', 'LinearFreq', 
                       'Poly2Freq', 'Poly3Freq', 'TrendedFreqPoly3', 'BaselineFreq']]

# Split Dataframes
df_chart3Actuals = df_chart3[df_chart3['CombinationID'] == 'Actuals'][['CalendarDate', 'CombinationID', 'Frequency']]
df_trends = df_chart3[df_chart3['CombinationID'] == 'Actuals'][['CalendarDate'] + ['LinearFreq', 'Poly2Freq', 'Poly3Freq', 'TrendedFreqPoly3', 'BaselineFreq']]
df_trends = df_trends.melt(
    id_vars=['CalendarDate'], 
    value_vars=['LinearFreq', 'Poly2Freq', 'Poly3Freq', 'TrendedFreqPoly3', 'BaselineFreq'],
    var_name='CombinationID', 
    value_name='Frequency'  
)

# Append DataFrames
df_chart3 = pd.concat([df_chart3Actuals, df_trends], ignore_index=True)

# Projected DataFrame
df_chart3proj = (df[(df['CalendarDate'] > DataRef) & 
                (df['CalendarDate'] <= '2026-12-01') & 
                (df['CombinationID'].isin(CombinationID))]
             .groupby(['CalendarDate', 'CombinationID'], as_index=False)
             [['Exposure'] + [indicators]]
             .sum().reset_index()
             )
df_chart3proj['CombinationID'] = df_chart3proj['CombinationID'].map(dict_Cenarios)
df_chart3proj['Frequency'] = df_chart3proj[indicators] / df_chart3proj['Exposure']
df_chart3proj = df_chart3proj[['CalendarDate', 'CombinationID', 'Frequency']]

# Append Projected DataFrame to Main DataFrame
df_chart3 = pd.concat([df_chart3, df_chart3proj], ignore_index=True)

# Chart with Projections + Trends
df_chart3 = df_chart3[~df_chart3['CombinationID'].isin(['LinearFreq', 'Poly2Freq'])]
fig_chart3 = (px.line(df_chart3, x='CalendarDate', y=['Frequency'], 
                     color='CombinationID', 
                     color_discrete_map= map_Colors,
#                     line_dash_map=map_Styles,
                     markers=True, title='Frequency Over Time')
                     .update_yaxes(range=[lower_limit, upper_limit])
                     .update_layout(
                         xaxis_title="Date",
                         yaxis_title="Frequency",
                         legend_title="Legend",
                         legend=dict(y=0.99, x=0.99),
                         template="plotly_white")
                     )
fig_chart3.update_traces(line_color= '#343741', line_dash='dot', line_width=2, selector=lambda trace: trace.name in ['LinearFreq', 'Poly2Freq', 'Poly3Freq'])
fig_chart3.update_traces(line_color= '#BE0000', line_dash='solid', selector=lambda trace: trace.name in ['TrendedFreqPoly3'])
fig_chart3.update_traces(line_color='#BE0000', line_dash='dashdot', line_width=3, selector=lambda trace: trace.name in ['BaselineFreq'])
col3.plotly_chart(fig_chart3, width='stretch')


# SEPARAÇÃO NATIVA DO STREAMLIT (Linha Horizontal)
st.write("---") # Forma rápida de criar uma linha cinza sutil

# Section2. SUBTÍTULO
# Severity Over Time.
st.markdown(f"<h3 style='color: {EA_BLUE}; opacity: 0.8;'>Severity Over Time.</h3>", unsafe_allow_html=True)

col7, = st.columns(1)

# Create 7th Chart - Severity Over Time
f_indicator = df_coverage[df_coverage['CoverageID'] == CoverageID]['ClaimType'].values[0]
s_indicator = df_coverage[df_coverage['CoverageID'] == CoverageID]['AmountType'].values[0]

df_chart7 = (df[(df['CalendarDate'] >= '2023-01-01') & 
                (df['CalendarDate'] <= DataRef) & 
                (df['CombinationID'].isin(CombinationID))]
             .groupby(['CalendarDate', 'CombinationID'], as_index=False)
             [[f_indicator] + [s_indicator]]
             .sum().reset_index()
             )
             
df_chart7['CombinationID'] = df_chart7['CombinationID'].map({-5: 'Actuals', 0: 'CenBaseline'})
#df_chart7.loc[df_chart7[s_indicator] == 0, indicators] = 1
df_chart7['Severity'] = df_chart7[s_indicator] / df_chart7[f_indicator]
upper_limit = df_chart7['Severity'].max() * 1.2
lower_limit = df_chart7['Severity'].min() * 0.8

df_chart7['Time'] = np.arange(len(df_chart7.index))
modelSev = linear_model.LinearRegression()

X = df_chart7[['Time']]
sy = df_chart7['Severity']   # Dependent variable
modelSev.fit(X, sy)

# Create Polynomial Trend
# Trace Dependent variables
x = df_chart7['Time'].values
sy = df_chart7['Severity'].values

# Build coefficients & Intercept
coeffs_2nd_degreeSev = np.polyfit(x, sy, deg=2)
coeffs_3rd_degreeSev = np.polyfit(x, sy, deg=3)

# Build Models
modelPoly2Sev = np.poly1d(coeffs_2nd_degreeSev)
modelPoly3Sev = np.poly1d(coeffs_3rd_degreeSev)

# Regression Severity
df_chart7['LinearSev'] = modelSev.predict(X)
df_chart7['Poly2Sev'] = modelPoly2Sev(x)
df_chart7['Poly3Sev'] = modelPoly3Sev(x)

# Trending to Current Levels 
# Trended Severity
df_chart7['TrendedSevLinear'] = df_chart7['Severity'] * df_chart7['LinearSev'].iloc[-1] / df_chart7['LinearSev']
df_chart7['TrendedSevPoly2'] = df_chart7['Severity'] * df_chart7['Poly2Sev'].iloc[-1] / df_chart7['Poly2Sev']
df_chart7['TrendedSevPoly3'] = df_chart7['Severity'] * df_chart7['Poly3Sev'].iloc[-1] / df_chart7['Poly3Sev']

# Calculate Baselines
# Baseline Severity
df_chart7['BaselineSev'] = df_chart7['TrendedSevPoly3'].mean()
df_chart7 = df_chart7[['CalendarDate', 'CombinationID', 'Severity', 'LinearSev', 
                       'Poly2Sev', 'Poly3Sev', 'TrendedSevPoly3', 'BaselineSev']]

# Split Dataframes
df_chart7Actuals = df_chart7[df_chart7['CombinationID'] == 'Actuals'][['CalendarDate', 'CombinationID', 'Severity']]
df_trends = df_chart7[df_chart7['CombinationID'] == 'Actuals'][['CalendarDate'] + ['LinearSev', 'Poly2Sev', 'Poly3Sev', 'TrendedSevPoly3', 'BaselineSev']]
df_trends = df_trends.melt(
    id_vars=['CalendarDate'], 
    value_vars=['LinearSev', 'Poly2Sev', 'Poly3Sev', 'TrendedSevPoly3', 'BaselineSev'],
    var_name='CombinationID', 
    value_name='Severity'  
)

# Append DataFrames
df_chart7 = pd.concat([df_chart7Actuals, df_trends], ignore_index=True)

# Projected DataFrame
df_chart7proj = (df[(df['CalendarDate'] > DataRef) & 
                (df['CalendarDate'] <= '2026-12-01') & 
                (df['CombinationID'].isin(CombinationID))]
             .groupby(['CalendarDate', 'CombinationID'], as_index=False)
             [[s_indicator] + [f_indicator]]
             .sum().reset_index()
             )
df_chart7proj['CombinationID'] = df_chart7proj['CombinationID'].map(dict_Cenarios)
df_chart7proj['Severity'] = df_chart7proj[s_indicator] / df_chart7proj[f_indicator]
df_chart7proj = df_chart7proj[['CalendarDate', 'CombinationID', 'Severity']]

# Append Projected DataFrame to Main DataFrame
df_chart7 = pd.concat([df_chart7, df_chart7proj], ignore_index=True)

# Chart with Projections + Trends
df_chart7 = df_chart7[~df_chart7['CombinationID'].isin(['LinearSev', 'Poly2Sev'])]
fig_chart7 = (px.line(df_chart7, x='CalendarDate', y=['Severity'], 
                     color='CombinationID', 
                     color_discrete_map= map_Colors,
#                     line_dash_map=map_Styles,
                     markers=True, title='Severity Over Time')
                     .update_yaxes(range=[lower_limit, upper_limit])
                     .update_layout(
                         xaxis_title="Date",
                         yaxis_title="Severity",
                         legend_title="Legend",
                         legend=dict(y=0.99, x=0.99),
                         template="plotly_white")
                     )
fig_chart7.update_traces(line_color= '#343741', line_dash='dot', line_width=2, selector=lambda trace: trace.name in ['LinearSev', 'Poly2Sev', 'Poly3Sev'])
fig_chart7.update_traces(line_color= '#BE0000', line_dash='solid', selector=lambda trace: trace.name in ['TrendedSevPoly3'])
fig_chart7.update_traces(line_color='#BE0000', line_dash='dashdot', line_width=3, selector=lambda trace: trace.name in ['BaselineSev'])
col7.plotly_chart(fig_chart7, width='stretch')

# SEPARAÇÃO NATIVA DO STREAMLIT (Linha Horizontal)
st.write("---") # Forma rápida de criar uma linha cinza sutil

# --- CONCLUSION ---
st.header("Conclusion")
st.success("""
The simulator successfully visualizes frequency and severity trends. 
The main challenge was aligning real-time visualizations with complex actuarial triangles. 
Future versions will include **Stochastic Modeling**.
""", icon = "✅")

# --- FOOTER ---
st.markdown(f"<hr style='border: 2px solid {EA_YELLOW};'>", unsafe_allow_html=True)
st.markdown(f"**Follow Me On:** [LinkedIn](https://www.linkedin.com/in/atuario-vinicius-almeida/)")

st.header("Appendix")
st.markdown("""
- [Loss Trends for Frequency & Severity](https://Exposureatuarial.wordpress.com/2026/02/16/loss-trends-for-frequency-severity/)
- [CAS Basic Ratemaking](https://Exposureatuarial.wordpress.com/2026/02/16/cas-basic-ratemaking-earned-premium-exposure/)
""")
st.divider()
