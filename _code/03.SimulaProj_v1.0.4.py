# Description--------------------------------------------------------------------
"""
#FileName: SimulaProj
#Author: Vinicius Almeida
#Contact: vinicius.almeida@hdi-yelum.com.br
#Copyright HDI, 2025
#Inception Date: 08.09.2025
#Version Date: 01.10.2025
#Version Control: 1.0.3
---
Goal

Calcular n Cenarios para Producao (GWP/Itens), aplicando diferentes tarifas e derivando atraves das elasticidades (eg. Pricing).


---
""" 
#0.0 Initial setting--------------------------------------------------------------------

import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import pyarrow as pa
import pyarrow.parquet as pq


#1.0 Import Dataframe (Parquet)--------------------------------------------------------------------
# Alterar diretorio para AWS S3
dir = '_data/_01Input/_01Prem/'
file_name = dir + 'Db_PremAct.parquet'
df = pd.read_parquet(file_name)

dir = '_data/_03Baselines/_01Prem/'
file_name = dir + 'BaselinePremium.parquet'
TbBslPrm = pd.read_parquet(file_name)
file_name = dir + 'PremiumDevelopment.parquet'
TbDsvPrm = pd.read_parquet(file_name)
file_name = dir + 'BaselineRatios.parquet'
TbTxs = pd.read_parquet(file_name)
file_name = dir + 'RenewalRatio.parquet'
TbBslRetRatio = pd.read_parquet(file_name)

# Criar Db Elasticidade em .parquet
dir = '_data/_01Input/_04ElasticityFigures/'
file_name = 'Db_Elast.parquet'
Db_Elast = pd.read_parquet(dir + file_name)

dir = '_data/_01Input/_03CotacaoVlrIS/'
file_name = dir + 'Db_QuotesSI.parquet'
df_ctc = pd.read_parquet(file_name)


#2.0 Parameters--------------------------------------------------------------------

DataRef = pd.to_datetime('2025-12-01')
attributes = ['BusinessTypeCod', 'ProductCod'
#              , 'CodEmp', 'CodCnl'
#              , 'CodReg', 'CodCol1', 'CodCol2'
              ]

TbDsvPrm.rename(columns = {'DsnvEndosPrem': 'LagEndosPrem',
                           'DsnvCancPrem': 'LagCancPrem',
                           'DsnvEndosItm': 'LagEndosItm',
                           'DsnvCancItm': 'LagCancItm'}, inplace = True)

# Incluir Desenvolvimento Acumulado para estimar o Triangulo inferior
# Desenvolvimento Acumulado
TbDsvPrm['CumLagCancItm'] = TbDsvPrm.groupby(['BusinessTypeCod', 'ProductCod'])['LagCancItm'].cumsum()
TbDsvPrm['LagsDif'] = TbDsvPrm['Lags']

# Select number of iterations
n_cen = 100

# Select Tariff Range
MinTarif = -0.05
MaxTarif = 0.05

#3.0 Funcoes--------------------------------------------------------------------

# Function to calculate month difference
def month_diff(d1, d2):
    rd = relativedelta(d1, d2)
    return rd.years * 12 + rd.months

# Create Classification Period
def classifica_periodo(date):
    if date <= DataRef:
        return 1
    return 2

# Funcao Fator Desconto/Agravo (Em construcao no motor de calculo)
# Funcao permite aplicar desconto/agravo diferentes em varios momentos e por diferentes atributos
def fatoraj_desconto(variavel):
    if variavel['BusinessTypeCod'] == 1 and variavel['PolicyEffDate'] == pd.to_datetime('2025-12-01'):
        return 1.1
    elif variavel['BusinessTypeCod'] == 2 and variavel['PolicyEffDate'] == pd.to_datetime('2025-12-01'):
        return 1.2
    elif variavel['BusinessTypeCod'] == 3 and variavel['PolicyEffDate'] == pd.to_datetime('2025-12-01'):
        return 1.05
    else:
        return 1

#4.0 Data Treatment Proj Premium & Item--------------------------------------------------------------------

# Identify Analysis Period
df['ActualsProj'] = 1

# Calculate GWP + Itens Liquidos
df['GWP'] = df['WrittenPremium'] + df['WritEndorsPremium'] + df['WritCancPremium']
df['WritCancItem'] = - df['WritCancItem']
df['Net_Item'] = df['WrittenItem'] + df['WritEndorsItem'] + df['WritCancItem']

# Filtro Data de Emissao 
Datefilter = (DataRef.to_period('M') - 27).to_timestamp()

# Lote Renovacao Itens Emitidos e Cancelados Observados Tipo Origem Renovacao
df_RBBatch = (df[(df['PolicyEffDate'] >= Datefilter) &
          (df['BusinessTypeCod'] == 2)]
          .groupby(['ActualsProj', 'BusinessTypeCod', 'ProductCod', 'PolicyEffDate'], as_index=False)
          [['WrittenItem', 'WritCancItem']]
          .sum()
          )

# Apply function to classify Actuals/Porjection
df_ctc['ActualsProj'] = df_ctc['PolicyEffDate'].apply(classifica_periodo)

# Join Main Dataframe + Baselines
df_ctc = pd.merge(df_ctc, TbTxs, how='left', on=['BusinessTypeCod', 'ProductCod'])
df_ctc = pd.merge(df_ctc, Db_Elast, how='left', on=['BusinessTypeCod', 'ProductCod'])

# Build Baseline AvgWrittenPremium
df_ctc['BslAvgAWP'] = df_ctc['SumInsuredAmount'] * df_ctc['TxEmit']

# Create Column Tarif Selection
TbTarif = TbTxs.copy()
TbTarif.drop(['TxEmit', 'IS', 'TxConv'], axis=1, inplace=True)
#TbTarif['FatTarifSel'] = 0
#TbTarif['CombinationID'] = 0

# DataFrame Append Cenarios Simulados
df_ProjCen = pd.DataFrame()
df_ProjCenAss = pd.DataFrame()

#5.0 Baseline Scenario and Simulations--------------------------------------------------------------------
######################################
######################################
####           Scenarios           ###
######################################
######################################

# Define the tarif range for FatTarifSel
#tarif_range = np.arange(-0.05, 0.051, 0.01)  # from -5% to +5% inclusive

# Generate tariff combinations
# Iterate Tarifs
tar = 0
for tar in range(n_cen):
#for tar in range(1):
    if tar == 0:
        # Scenario 0: Baseline
        TbTarif['FatTarifSel'] = 0
        TbTarif['CombinationID'] = tar
        df_tarcombinations = TbTarif.copy()
    else:
        # Build Random Seed for Simulation
        TbTarif['FatTarifSel'] = np.random.uniform(low=MinTarif, high=MaxTarif, size=len(TbTarif)) # Random Scenarios
        TbTarif['CombinationID'] = tar
        df_tarcombinations = pd.concat([df_tarcombinations, TbTarif], ignore_index=True)        

    ###########################
    # Aplly Simulation Tarifs #
    ###########################
    # Create New Business Proj Dataframe
    df_NBProj = df_ctc.copy()
    df_NBProj = (df_NBProj[(df_NBProj['PolicyEffDate'] > DataRef) &
                    (df_NBProj['BusinessTypeCod'] != 2)])
    df_NBProj = pd.merge(df_NBProj, TbTarif, how='left', on=attributes)
    # Apply Discount/Increase
    df_NBProj['ApplAWP'] = df_NBProj['BslAvgAWP'] * (1 + df_NBProj['FatTarifSel'])

    # Applied Conversion Column
    #df_NBProj['Intercept'] = df_NBProj['TxConv']
    df_NBProj['ApplConv'] = (1 + (df_NBProj['Coef3'] * df_NBProj['FatTarifSel'] ** 3 + 
                             df_NBProj['Coef2'] * df_NBProj['FatTarifSel'] ** 2 + 
                             df_NBProj['Coef1'] * df_NBProj['FatTarifSel'] + 
                             df_NBProj['Slope'])) * df_NBProj['TxConv']
    df_NBProj['AppliedRetention'] = 0

    # Written Premium & Item Columns
    df_NBProj['WrittenItem'] = np.floor(df_NBProj['Quotes'] * df_NBProj['ApplConv'])
    df_NBProj['WrittenPremium'] = df_NBProj['WrittenItem'] * df_NBProj['ApplAWP']

    # Create Lags
    df_NBProj['Lags'] = 0
    # Loop through lag values 1 to 12 
    df_NBProjaux = df_NBProj.copy()
    for lag in range(1,13):
        df_NBProjaux['Lags'] = lag
        df_NBProj = pd.concat([df_NBProj, df_NBProjaux], ignore_index=True)

    # Join Endorsements & Cancellation Baselines
######################################################################################    
    df_NBProj = pd.merge(df_NBProj, TbBslPrm, how='left', on=attributes)
    df_NBProj = pd.merge(df_NBProj, TbDsvPrm, how='left', on=['BusinessTypeCod', 'ProductCod', 'Lags'])

    # Calculate Dates
    df_NBProj['CalendarDate'] = df_NBProj['PolicyEffDate']
    df_NBProj['PolicyEffDate'] = (df_NBProj['CalendarDate'].dt.to_period('M') - df_NBProj['Lags']).dt.to_timestamp()

    # Calculate Proj Endorsements & Cancellations Developments
    df_NBProj['WritEndorsPremium'] = df_NBProj['WrittenPremium'] * df_NBProj['BaselEndosPrem'] * df_NBProj['LagEndosPrem']
    df_NBProj['WritCancPremium'] = -1 * (df_NBProj['WrittenPremium'] * df_NBProj['BaselCancPrem'] * df_NBProj['LagCancPrem'])
    df_NBProj['WritEndorsItem'] = np.floor(df_NBProj['WrittenItem'] * df_NBProj['BaselEndosItm'] * df_NBProj['LagEndosItm'])
    df_NBProj['WritCancItem'] = -1 * np.floor( (df_NBProj['WrittenItem'] * df_NBProj['BaselCancItm'] * df_NBProj['LagCancItm']))

    # Aplicar Valores Negativos nas Emissoes diferente de Lag0
    df_NBProj.loc[df_NBProj['Lags'] != 0, 'WrittenPremium'] = 0
    df_NBProj.loc[df_NBProj['Lags'] != 0, 'WrittenItem'] = 0

    # Calculate GWP + Itens Liquidos
    df_NBProj['GWP'] = df_NBProj['WrittenPremium'] + df_NBProj['WritEndorsPremium'] + df_NBProj['WritCancPremium']
    df_NBProj['Net_Item'] = df_NBProj['WrittenItem'] + df_NBProj['WritEndorsItem'] + df_NBProj['WritCancItem']

    # ActualsProj Column
    df_NBProj['ActualsProj'] = df_NBProj['CalendarDate'].apply(classifica_periodo)

    # Build New Business Batch DataFrame
    df_aux = df[['ActualsProj', 'BusinessTypeCod', 'ProductCod', 'PolicyEffDate', 'Lags', 'Net_Item']]
    df_NBProj_aux = df_NBProj[['ActualsProj', 'BusinessTypeCod', 'ProductCod', 'PolicyEffDate', 'Lags', 'Net_Item']]
    df_NBBatch = pd.concat([df_aux, df_NBProj_aux], ignore_index=True)

    ###########################################################
    # Inicio Etapas de Calculo dos Lotes de Renovacao Futuros #
    ###########################################################
    # Criar Dataframe Renovacao 
    df_RBProj = df_NBBatch.copy()
    df_RBProj = (df_RBProj[(df_RBProj['PolicyEffDate'] >= Datefilter)]
                        .groupby(['BusinessTypeCod', 'ProductCod', 'PolicyEffDate'], as_index=False)
                        [['Net_Item']]
                        .sum()
                    )

    # Apply function to classify Actuals/Porjection
    df_RBProj['ActualsProj'] = df_RBProj['PolicyEffDate'].apply(classifica_periodo)

    # Criar 2 Dataframes contendo Itens Liquidos de Novos Negocios
    df1 = df_RBProj[df_RBProj['BusinessTypeCod'] == 1].rename(columns={'Net_Item': 'NBItemLiq'})
    df2 = df_RBProj[df_RBProj['BusinessTypeCod'] == 3].rename(columns={'Net_Item': 'NBCItemLiq'})
    df_RBProj = (pd.merge(df1, df2, how='left', on=['ActualsProj', 'ProductCod', 'PolicyEffDate'])
                [['ActualsProj', 'ProductCod', 'PolicyEffDate', 'NBItemLiq', 'NBCItemLiq']])

    # Mapear Tipo Origem Negocio
    df_RBProj['BusinessTypeCod'] = 2
    df_RBProj = pd.merge(df_RBProj, df_RBBatch, how='left', on=['ActualsProj', 'BusinessTypeCod', 'ProductCod', 'PolicyEffDate'])

    # Identificar distancia entre DataRef e Data Emissao Serie Temporal
    df_RBProj['Lags'] = df_RBProj['PolicyEffDate'].apply(lambda x: -month_diff(x, DataRef))

    # Join com Tabela Desenvolvimento
    df_RBProj = pd.merge(df_RBProj, TbDsvPrm[['BusinessTypeCod', 'ProductCod', 'Lags', 'CumLagCancItm']], how='left', on=['BusinessTypeCod', 'ProductCod', 'Lags'])
    df_RBProj['CumLagCancItm'] = df_RBProj['CumLagCancItm'].fillna(1)
    df_RBProj['Lags'] = abs(df_RBProj['Lags'])

    # Calculo Ultimate CancItens
    df_RBProj['Net_Item'] = np.floor(df_RBProj['WrittenItem'] - abs((df_RBProj['WritCancItem'] / df_RBProj['CumLagCancItm'])))

    # Realizar Shift dos Lotes Novos à expirar (Lag 12)
    # Sort by attribute and date to ensure correct shifting
    df_RBProj = df_RBProj.sort_values(['ProductCod', 'PolicyEffDate']).reset_index(drop=True)

    # Criar shift da seria respeitando a serie temporal de cada atributo (Em construcao)
    df_RBProj['NBShift'] = df_RBProj.groupby('ProductCod')['NBItemLiq'].shift(12)
    df_RBProj['NBCShift'] = df_RBProj.groupby('ProductCod')['NBCItemLiq'].shift(12)

    #df_RBProj['NBShift'] = df_RBProj['NBItemLiq'].shift(12)
    #df_RBProj['NBCShift'] = df_RBProj['NBCItemLiq'].shift(12)

    # Join Baselines Cancelamento, IR (Indice de Retencao) e Coeficientes
    df_RBProj = pd.merge(df_RBProj, TbBslPrm[['BusinessTypeCod', 'ProductCod', 'BaselCancItm']], how='left', on=attributes)
    df_RBProj = pd.merge(df_RBProj, TbBslRetRatio, how='left', on=attributes)
    df_RBProj = pd.merge(df_RBProj, Db_Elast[['BusinessTypeCod', 'ProductCod', 'Coef3', 'Coef2', 'Coef1', 'Slope']], how='left', on=attributes)
    df_RBProj['Intercept'] = df_RBProj['RenewalRatio']

    df_RBProj = pd.merge(df_RBProj, TbTarif, how='left', on=attributes) # Tarifa futura definida Uniformente por enquanto


    # Iteracoes Lotes
    iteracao = 0
    #for iteracao in range(7): # Original
    for iteracao in range(2):
        # Somar iteracao
        iteracao = iteracao + 1
        
        if iteracao == 1:
            # 1a Renovacao possui emissoes a desenvolver.
            # Nas etapas anteriores foi foram aplicados os fatores para QtdItens chegar ao Ultimate atraves de 'CumLagCancItm'
            
            # Define date range
            date_interval1 =  pd.to_datetime(DataRef) + pd.DateOffset(months=12*iteracao-11)
            date_interval2 =  pd.to_datetime(DataRef) + pd.DateOffset(months=12*iteracao)

            # Sort by attribute and date to ensure correct shifting
            #df_RBProj = df_RBProj.sort_values(['ProductCod', 'PolicyEffDate']).reset_index(drop=True)
            # Inicio Loop Identificação dos lotes iterados
            df_RBProj['RBShift'] = df_RBProj.groupby('ProductCod')['Net_Item'].shift(12)

            #df_RBProj['RBShift'] = df_RBProj['Net_Item'].shift(12) 
            df_RBProj['LagLote'] = df_RBProj['RBShift'] + df_RBProj['NBShift'] + df_RBProj['NBCShift']

            # Aplicar Retencao Esperada
            df_RBProj['AppliedRetention'] = (1 + (df_RBProj['Coef3'] * df_RBProj['FatTarifSel'] ** 3 +
                                            df_RBProj['Coef2'] * df_RBProj['FatTarifSel'] ** 2 +
                                            df_RBProj['Coef1'] * df_RBProj['FatTarifSel'] +
                                            df_RBProj['Slope'])) * df_RBProj['RenewalRatio'] # Baseline (Intercept)

            # Lote Esperado
            df_RBProj['LoteEsperado'] = np.floor(df_RBProj['AppliedRetention'] * df_RBProj['LagLote'])

            # Lote Emitido = Lote Esperado
            df_RBProj.loc[(df_RBProj['PolicyEffDate'] >= date_interval1) & (df_RBProj['PolicyEffDate'] <= date_interval2), ['WrittenItem']] = (
                df_RBProj.loc[(df_RBProj['PolicyEffDate'] >= date_interval1) & (df_RBProj['PolicyEffDate'] <= date_interval2), ['LoteEsperado']].values
            )

            # Calculo Itens Liquidos Lotes Futuros
            df_RBProj.loc[(df_RBProj['PolicyEffDate'] >= date_interval1) & (df_RBProj['PolicyEffDate'] <= date_interval2), ['Net_Item']] = np.floor(
                df_RBProj.loc[(df_RBProj['PolicyEffDate'] >= date_interval1) & (df_RBProj['PolicyEffDate'] <= date_interval2), ['WrittenItem']].values *
                (1 - df_RBProj.loc[(df_RBProj['PolicyEffDate'] >= date_interval1) & (df_RBProj['PolicyEffDate'] <= date_interval2), ['BaselCancItm']].values)
            )

        else:

            # 2a Renovacao possui aplicao direta do Fator de Ultimate ou Baseline Cancelamento ('BaselCancItm')

            # Define date range
            date_interval1 =  pd.to_datetime(DataRef) + pd.DateOffset(months=12*iteracao-11)
            date_interval2 =  pd.to_datetime(DataRef) + pd.DateOffset(months=12*iteracao)

            # Inicio Loop Identificação dos lotes iterados
            #df_RBProj['RBShift'] = df_RBProj['Net_Item'].shift(12) 
            df_RBProj['RBShift'] = df_RBProj.groupby('ProductCod')['Net_Item'].shift(12) 
            df_RBProj['LagLote'] = df_RBProj['RBShift'] + df_RBProj['NBShift'] + df_RBProj['NBCShift']

            # Aplicar Retencao Esperada
            df_RBProj['AppliedRetention'] = (1 + (df_RBProj['Coef3'] * df_RBProj['FatTarifSel'] ** 3 +
                                            df_RBProj['Coef2'] * df_RBProj['FatTarifSel'] ** 2 +
                                            df_RBProj['Coef1'] * df_RBProj['FatTarifSel'] +
                                            df_RBProj['Slope'])) * df_RBProj['RenewalRatio'] # Baseline (Intercept)

            # Lote Esperado
            df_RBProj['LoteEsperado'] = np.floor(df_RBProj['AppliedRetention'] * df_RBProj['LagLote'])

            # Lote Emitido = Lote Esperado 
            # (Talvez tenha que aplicar esse filtro em todos os calculos da iteracao > 2)
            df_RBProj.loc[(df_RBProj['PolicyEffDate'] >= date_interval1) & (df_RBProj['PolicyEffDate'] <= date_interval2), ['WrittenItem']] = (
                df_RBProj.loc[(df_RBProj['PolicyEffDate'] >= date_interval1) & (df_RBProj['PolicyEffDate'] <= date_interval2), ['LoteEsperado']].values
            )

            # Calculo Itens Liquidos Lotes Futuros
            df_RBProj.loc[(df_RBProj['PolicyEffDate'] >= date_interval1) & (df_RBProj['PolicyEffDate'] <= date_interval2), ['Net_Item']] = np.floor(
                df_RBProj.loc[(df_RBProj['PolicyEffDate'] >= date_interval1) & (df_RBProj['PolicyEffDate'] <= date_interval2), ['WrittenItem']].values *
                (1 - df_RBProj.loc[(df_RBProj['PolicyEffDate'] >= date_interval1) & (df_RBProj['PolicyEffDate'] <= date_interval2), ['BaselCancItm']].values)
            )
        
            # End of Loop (Iteracoes Lotes)

    # Join Baseline Taxas/Premissas
    df_RBProj = pd.merge(df_RBProj, TbTxs[['BusinessTypeCod', 'ProductCod', 'TxEmit']], how='left', on=attributes)
    df_RBProj = pd.merge(df_RBProj, df_ctc[['BusinessTypeCod', 'ProductCod', 'PolicyEffDate', 'SumInsuredAmount']], how='left', on=['BusinessTypeCod', 'ProductCod', 'PolicyEffDate']) 

    # Calculo Premio Emitido Medio
    df_RBProj['BslAvgAWP'] = df_RBProj['SumInsuredAmount'] * df_RBProj['TxEmit']
    df_RBProj['ApplAWP'] = df_RBProj['BslAvgAWP'] * (1 + df_RBProj['FatTarifSel'])

    # Premio Emitido
    df_RBProj['WrittenPremium'] = df_RBProj['ApplAWP'] * df_RBProj['WrittenItem']

    # Dataframe Simulacao Renovacao (Cenarios)
    # Criar Lags
    df_RBProj['ApplConv'] = 0
    df_RBProj = df_RBProj[['ActualsProj', 'ProductCod', 'BusinessTypeCod', 
                        'PolicyEffDate', 'Lags', 
                        'WrittenPremium', 
                        'WrittenItem',
                        'CombinationID', 'ApplConv', 'AppliedRetention', 
                        'FatTarifSel', 'Coef3', 'Coef2', 'Coef1', 'Slope']]
    df_RBProj['Lags'] = 0


    # Loop through lag values 1 to 12
    df_RBProjaux = df_RBProj.copy()
    for lag in range(1,13):
        df_RBProjaux['Lags'] = lag
        df_RBProj = pd.concat([df_RBProj, df_RBProjaux], ignore_index=True)

    # Join Endorsements & Cancellation Baselines
    df_RBProj = pd.merge(df_RBProj, TbBslPrm, how='left', on=attributes)
    df_RBProj = pd.merge(df_RBProj, TbDsvPrm, how='left', on=['BusinessTypeCod', 'ProductCod', 'Lags'])

    # Calculate Dates
    df_RBProj['CalendarDate'] = df_RBProj['PolicyEffDate']
    df_RBProj['PolicyEffDate'] = (df_RBProj['CalendarDate'].dt.to_period('M') - df_RBProj['Lags']).dt.to_timestamp()

    # Calculate Proj Endorsements & Cancellations Developments
    df_RBProj['WritEndorsPremium'] = df_RBProj['WrittenPremium'] * df_RBProj['BaselEndosPrem'] * df_RBProj['LagEndosPrem']
    df_RBProj['WritCancPremium'] = -1 * (df_RBProj['WrittenPremium'] * df_RBProj['BaselCancPrem'] * df_RBProj['LagCancPrem'])
    df_RBProj['WritEndorsItem'] = np.floor(df_RBProj['WrittenItem'] * df_RBProj['BaselEndosItm'] * df_RBProj['LagEndosItm'])
    df_RBProj['WritCancItem'] = -1 * np.floor( (df_RBProj['WrittenItem'] * df_RBProj['BaselCancItm'] * df_RBProj['LagCancItm']))

    # Aplicar Valores Negativos nas Emissoes diferente de Lag0
    df_RBProj.loc[df_RBProj['Lags'] != 0, 'WrittenPremium'] = 0
    df_RBProj.loc[df_RBProj['Lags'] != 0, 'WrittenItem'] = 0

    # Calculate GWP + Itens Liquidos
    df_RBProj['GWP'] = df_RBProj['WrittenPremium'] + df_RBProj['WritEndorsPremium'] + df_RBProj['WritCancPremium']
    df_RBProj['Net_Item'] = df_RBProj['WrittenItem'] + df_RBProj['WritEndorsItem'] + df_RBProj['WritCancItem']

    # ActualsProj Column
    df_RBProj['ActualsProj'] = df_RBProj['CalendarDate'].apply(classifica_periodo)

    # Concatenar New Business & Renewal Business
    df_NBProj = df_NBProj[df_NBProj['CalendarDate'] > DataRef][['ActualsProj', 'ProductCod', 'BusinessTypeCod', 
                                                        'PolicyEffDate', 'Lags', 'CalendarDate',
                                                        'WrittenPremium', 'WritEndorsPremium', 'WritCancPremium', 'GWP',
                                                        'WrittenItem', 'WritEndorsItem', 'WritCancItem', 'Net_Item',
                                                        'CombinationID', 'ApplConv', 'AppliedRetention', 
                                                        'FatTarifSel', 'Coef3', 'Coef2', 'Coef1', 'Slope']]
    df_RBProj = df_RBProj[df_RBProj['CalendarDate'] > DataRef][['ActualsProj', 'ProductCod', 'BusinessTypeCod', 
                                                        'PolicyEffDate', 'Lags', 'CalendarDate',
                                                        'WrittenPremium', 'WritEndorsPremium', 'WritCancPremium', 'GWP',
                                                        'WrittenItem', 'WritEndorsItem', 'WritCancItem', 'Net_Item',
                                                        'CombinationID', 'ApplConv', 'AppliedRetention', 
                                                        'FatTarifSel', 'Coef3', 'Coef2', 'Coef1', 'Slope']]
    df_ProjCen_aux = pd.concat([df_NBProj, df_RBProj], ignore_index=True)

    # Concatenar Cenarios
    df_ProjCen = pd.concat([df_ProjCen, df_ProjCen_aux], ignore_index=True)

    # Loop Simulation
    tar += 1


#8.0 Export Dataframe ProjCenarios (Parquet)--------------------------------------------------------------------

# Disable scientific notation for large numbers
pd.options.display.float_format = '{:.0f}'.format

##################################
# Create an ParquetWriter object #
##################################
# Alterar diretorio para AWS S3
diroutput = '_data/_01Input/_06ProjCenOutputs/'
outputfile = 'Db_PremProjCen.parquet'
df_ProjCen.reset_index(drop=True, inplace=True)
df_ProjCen.to_parquet(diroutput + outputfile)


diroutput = '_data/_01Input/_06ProjCenOutputs/Excel/'
outputfile = 'Db_PremProjCen.xlsx'
df_ProjCen.reset_index(drop=True, inplace=True)
df_ProjCen.to_excel(diroutput + outputfile, index=False)




