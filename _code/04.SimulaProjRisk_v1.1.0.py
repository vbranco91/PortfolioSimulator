# Description--------------------------------------------------------------------
"""
#FileName: SimulaProjRisk
#Author: Vinicius Almeida
#Contact: vinicius.almeida@hdi-yelum.com.br
#Copyright HDI, 2025
#Inception Date: 19.02.2026
#Version Date: 19.02.2026
#Version Control: 1.0.0
---
Goal

Calcular n Cenarios para Risco (Claims/Amount), aplicando diferentes tarifas e derivando atraves das elasticidades (eg. Pricing).

---
Backlog

Feito
- Criar Fator de cauda

Em desenvolvimento
- Criar Fator de cauda
- Aplicar Calendarizacao para qtde de eventos

---
"""

#0.0 Initial setting--------------------------------------------------------------------

import pandas as pd
import numpy as np
from itertools import product
import matplotlib.pyplot as plt


#1.0 Import Dataframe--------------------------------------------------------------------
dir = '_data/_01Input/_01Prem/'
file_name = dir + 'Db_PremAct.parquet'
df = pd.read_parquet(file_name)

dir = '_data/_01Input/'
file_name = dir + 'Db_Exp.xlsx'
df_exp = pd.read_excel(file_name)

dir = '_data/_01Input/_02Sns/'
file_name = dir + 'Db_Sns.parquet'
df_sns = pd.read_parquet(file_name)

dir = '_data/_01Input/_03ProjCenOutputs/'
file_name = dir + 'Db_PremProjCen.parquet'
df_ProjCen = pd.read_parquet(file_name)

dir = '_data/_03Baselines/_02Sns/'
file_name = dir + 'BaselineSns.parquet'
TbBslSns = pd.read_parquet(file_name)

file_name = dir + 'BaselineDevSns.parquet'
TbDsvSns = pd.read_parquet(file_name)
TbDsvSns = TbDsvSns[['ProductCod', 'BusinessTypeCod', 'Lags', 'FreqPPClmDevFact', 'FreqPTClmDevFact', 'FreqRbftClmDevFact']]

# Baseline Salvage & Subrogation
file_name = dir + 'BaselineSS.parquet'
TbBslSS = pd.read_parquet(file_name)

file_name = dir + 'BaselineDsvSS.parquet'
TbDsvSS = pd.read_parquet(file_name)

# Criar Db Elasticidade
dir = '_data/_01Input/_04ElasticityFigures/'
file_name = 'Db_ElastRisk.xlsx'
Db_ElastRisk = pd.read_excel(dir + file_name)

# Inflation Figures
dir = '_data/_03Baselines/_05Macro/'
file_name = dir + 'TbInflation.xlsx'
TbInfl = pd.read_excel(file_name)


#2.0 Parameters--------------------------------------------------------------------

pd.options.display.float_format = '{:.0f}'.format
DataRef = pd.to_datetime('2025-12-01')
attributes = ['CodTipNeg', 'CodProd'
#              , 'CodEmp', 'CodCnl'
#              , 'CodReg', 'CodCol1', 'CodCol2'
              ]

# Brand Mappings
brand_map = {
    31041: 6,
    31120: 6,
    31100: 6,
    31111: 6,
    31112: 6,
    31113: 6,
    31115: 6,
    31018: 6,
    31020: 6,
    431: 1,
    432: 1,
    833: 1,
    131: 1,
    133: 1,
    139: 1
}


# Filter Business Type w/o definition
df = df[df['CodTipNeg'] > 0]
df_sns = df_sns[df_sns['CodTipNeg'] > 0]

# Utilizar zero para lags < 12 sem movimentacao no periodo de analise
ProductKey = df['CodProd'].unique()
BusinessKey = df['CodTipNeg'].unique()

df_aux = pd.DataFrame(list(product(ProductKey, 
                                   BusinessKey,
                                   pd.date_range(start='2022-01-01', end=DataRef, freq='MS')
                                   , list(range(13)))),
                  columns=['CodProd', 'CodTipNeg', 'DtEmissao', 'Lags'])

df_aux[['PremEmit', 'PremEndos', 'PremCanc', 'GWP',
        'ItensEmit', 'ItensEndos', 'ItensCanc', 'ItensLiq',
        'VlrIS', 'VlrComm']] = 0

df_aux['DtRefCtb'] = (df_aux['DtEmissao'].dt.to_period('M') + df_aux['Lags']).dt.to_timestamp()
df_aux = df_aux[df_aux['DtRefCtb'] <= DataRef]

df = pd.concat([df, df_aux], ignore_index=True)
df = (df
      .groupby(['CodProd', 'CodTipNeg', 'DtEmissao', 'Lags', 'DtRefCtb'], as_index=False)
      [['PremEmit', 'PremEndos', 'PremCanc', 'GWP',
        'ItensEmit', 'ItensEndos', 'ItensCanc', 'ItensLiq',
        'VlrIS', 'VlrComm']]
        .sum())

df_aux = pd.DataFrame(list(product(ProductKey, 
                                   BusinessKey,
                                   pd.date_range(start='2022-01-01', end=DataRef, freq='MS')
                                   , list(range(50)))),
                  columns=['CodProd', 'CodTipNeg', 'DtEmissao', 'Lags'])

df_aux[['QtdPP', 'QtdPT', 'QtdRbft', 'Qtd',
        'VlrIndPP', 'VlrIndPT', 'VlrIndRbft', 'VlrInd',
        'VlrSalvados', 'QtdSalvados', 'VlrRess', 'QtdRess']] = 0

df_aux['DtRefCtb'] = (df_aux['DtEmissao'].dt.to_period('M') + df_aux['Lags']).dt.to_timestamp()
df_aux = df_aux[df_aux['DtRefCtb'] <= DataRef]
df_sns = pd.concat([df_sns, df_aux], ignore_index=True)
df_sns = (df_sns
      .groupby(['CodProd', 'CodTipNeg', 'DtEmissao', 'Lags', 'DtRefCtb'], as_index=False)
      [['QtdPP', 'QtdPT', 'QtdRbft', 'Qtd',
        'VlrIndPP', 'VlrIndPT', 'VlrIndRbft', 'VlrInd',
        'VlrSalvados', 'QtdSalvados', 'VlrRess', 'QtdRess']]
        .sum())

#4.0 Merge Exposure Table--------------------------------------------------------------------

# Classify Actuals/Projection
df['ActualsProj'] = 1
df['CombinationID'] = 0
df['FatTarifSel'] = 0

# Keep Columns DataFrame Actuals
df = df[['ActualsProj', 'CodProd', 'CodTipNeg', 
            'DtEmissao', 'Lags', 'DtRefCtb',
            'PremEmit', 'PremEndos', 'PremCanc', 'GWP',
            'ItensEmit', 'ItensEndos', 'ItensCanc', 'ItensLiq',
            'CombinationID', 'FatTarifSel']]

# Keep Columns DataFrame ProjCen
df_ProjCen = df_ProjCen[['ActualsProj', 'CodProd', 'CodTipNeg', 
            'DtEmissao', 'Lags', 'DtRefCtb',
            'PremEmit', 'PremEndos', 'PremCanc', 'GWP',
            'ItensEmit', 'ItensEndos', 'ItensCanc', 'ItensLiq',
            'CombinationID', 'FatTarifSel']]

# Qtde de Simulacoes
simulacoes = df_ProjCen['CombinationID'].max()

# Create n Actuals to calculate exposure for every scenario
df_aux = df.copy()
for i in range(simulacoes):
    i += 1
    df_aux['CombinationID'] = i
    df = pd.concat([df, df_aux], ignore_index=True)


# Append DataFrame Actuals + DataFrame Cenarios
#df_ProjCen[df_ProjCen['CombinationID'] == -5]['DtRefCtb'].sort_values().unique()
df_ProjCen = df_ProjCen[df_ProjCen['DtRefCtb'] > DataRef]
df = pd.concat([df, df_ProjCen], ignore_index=True)

# Perform left join on columns 'Lags' and 'DtEmissao'
df_exp = df_exp[['Lags', 'DtEmissao', 
                 'Exp0', 'Exp1', 'Exp2', 'Exp3', 
                 'Exp4', 'Exp5', 'Exp6', 'Exp7', 
                 'Exp8', 'Exp9', 'Exp10', 'Exp11', 'Exp12',
                 'ExpI0', 'ExpI1', 'ExpI2', 'ExpI3', 
                 'ExpI4', 'ExpI5', 'ExpI6', 'ExpI7', 
                 'ExpI8', 'ExpI9', 'ExpI10', 'ExpI11', 'ExpI12',
                 ]]
df = pd.merge(df, df_exp, how='left', on=['Lags', 'DtEmissao'])


#5.0 Calculate Exposure--------------------------------------------------------------------

# PremEmit aggregation
df['PremEmitaux'] = (df.groupby(['CodTipNeg', 'CodProd', 'DtEmissao', 'CombinationID'], as_index=False)['PremEmit'].transform('sum'))
df['PremEmitaux'].fillna(0, inplace=True)

# ItemEmit aggregation
df['ItensEmitaux'] = (df.groupby(['CodTipNeg', 'CodProd', 'DtEmissao', 'CombinationID'], as_index=False)['ItensEmit'].transform('sum'))
df['ItensEmitaux'].fillna(0, inplace=True)

# PremEndos, PremCanc, ItensCanc aggregation
# Step 3: Loop through lag values 0 to 12
for lag in range(13):
    # PremEndos aggregation
    df_endos = (
        df[df['Lags'] == lag]
        .groupby(['CodTipNeg', 'CodProd', 'DtEmissao', 'CombinationID'], as_index=False)['PremEndos']
        .sum()
        .rename(columns={'PremEndos': f'EmitEnd{lag}'})
    )
    
    # PremCanc aggregation
    df_canc = (
        df[df['Lags'] == lag]
        .groupby(['CodTipNeg', 'CodProd', 'DtEmissao', 'CombinationID'], as_index=False)['PremCanc']
        .sum()
        .rename(columns={'PremCanc': f'CancEnd{lag}'})
    )
    
    # ItensCanc aggregation
    df_cancexp = (
        df[df['Lags'] == lag]
        .groupby(['CodTipNeg', 'CodProd', 'DtEmissao', 'CombinationID'], as_index=False)['ItensCanc']
        .sum()
        .rename(columns={'ItensCanc': f'ExpCanc{lag}'})
    )
    
    # Fill NA values
    df_endos.fillna(0, inplace=True)
    df_canc.fillna(0, inplace=True)
    df_cancexp.fillna(0, inplace=True)

    # Merge both into main DataFrame
    df = pd.merge(df, df_endos, how='left', on=['CodTipNeg', 'CodProd', 'DtEmissao', 'CombinationID'])
    df = pd.merge(df, df_canc, how='left', on=['CodTipNeg', 'CodProd', 'DtEmissao', 'CombinationID'])
    df = pd.merge(df, df_cancexp, how='left', on=['CodTipNeg', 'CodProd', 'DtEmissao', 'CombinationID'])


# Fill NA values
df['EmitEnd0'].fillna(0, inplace=True)
df['EmitEnd1'].fillna(0, inplace=True)
df['EmitEnd2'].fillna(0, inplace=True)
df['EmitEnd3'].fillna(0, inplace=True)
df['EmitEnd4'].fillna(0, inplace=True)
df['EmitEnd5'].fillna(0, inplace=True)
df['EmitEnd6'].fillna(0, inplace=True)
df['EmitEnd7'].fillna(0, inplace=True)
df['EmitEnd8'].fillna(0, inplace=True)
df['EmitEnd9'].fillna(0, inplace=True)
df['EmitEnd10'].fillna(0, inplace=True)
df['EmitEnd11'].fillna(0, inplace=True)
df['EmitEnd12'].fillna(0, inplace=True)

df['CancEnd0'].fillna(0, inplace=True)
df['CancEnd1'].fillna(0, inplace=True)
df['CancEnd2'].fillna(0, inplace=True)
df['CancEnd3'].fillna(0, inplace=True)
df['CancEnd4'].fillna(0, inplace=True)
df['CancEnd5'].fillna(0, inplace=True)
df['CancEnd6'].fillna(0, inplace=True)
df['CancEnd7'].fillna(0, inplace=True)
df['CancEnd8'].fillna(0, inplace=True)
df['CancEnd9'].fillna(0, inplace=True)
df['CancEnd10'].fillna(0, inplace=True)
df['CancEnd11'].fillna(0, inplace=True)
df['CancEnd12'].fillna(0, inplace=True)

df['ExpCanc0'].fillna(0, inplace=True)
df['ExpCanc1'].fillna(0, inplace=True)
df['ExpCanc2'].fillna(0, inplace=True)
df['ExpCanc3'].fillna(0, inplace=True)
df['ExpCanc4'].fillna(0, inplace=True)
df['ExpCanc5'].fillna(0, inplace=True)
df['ExpCanc6'].fillna(0, inplace=True)
df['ExpCanc7'].fillna(0, inplace=True)
df['ExpCanc8'].fillna(0, inplace=True)
df['ExpCanc9'].fillna(0, inplace=True)
df['ExpCanc10'].fillna(0, inplace=True)
df['ExpCanc11'].fillna(0, inplace=True)
df['ExpCanc12'].fillna(0, inplace=True)


# Calculate Earned Premium, Endorsements, Cancellation 
df['EmitExp'] = df['PremEmitaux'] * df['Exp0'] 

df['EndosExp'] = (df['EmitEnd0'] * df['Exp0'] +
                           df['EmitEnd1'] * df['Exp1'] +
                           df['EmitEnd2'] * df['Exp2'] +
                           df['EmitEnd3'] * df['Exp3'] +
                           df['EmitEnd4'] * df['Exp4'] +
                           df['EmitEnd5'] * df['Exp5'] +
                           df['EmitEnd6'] * df['Exp6'] +
                           df['EmitEnd7'] * df['Exp7'] +
                           df['EmitEnd8'] * df['Exp8'] +
                           df['EmitEnd9'] * df['Exp9'] +
                           df['EmitEnd10'] * df['Exp10'] +
                           df['EmitEnd11'] * df['Exp11'] +
                           df['EmitEnd12'] * df['Exp12'])

df['CancExp'] = (df['CancEnd0'] * df['Exp0'] +
                           df['CancEnd1'] * df['Exp1'] +
                           df['CancEnd2'] * df['Exp2'] +
                           df['CancEnd3'] * df['Exp3'] +
                           df['CancEnd4'] * df['Exp4'] +
                           df['CancEnd5'] * df['Exp5'] +
                           df['CancEnd6'] * df['Exp6'] +
                           df['CancEnd7'] * df['Exp7'] +
                           df['CancEnd8'] * df['Exp8'] +
                           df['CancEnd9'] * df['Exp9'] +
                           df['CancEnd10'] * df['Exp10'] +
                           df['CancEnd11'] * df['Exp11'] +
                           df['CancEnd12'] * df['Exp12'])

df['ExposicaoEmit'] = df['ItensEmitaux'] * df['Exp0'] 

df['ExposicaoEndos'] = 0

df['ExposicaoCanc'] = (df['ExpCanc0'] * df['Exp0'] +
                           df['ExpCanc1'] * df['ExpI1'] +
                           df['ExpCanc2'] * df['ExpI2'] +
                           df['ExpCanc3'] * df['ExpI3'] +
                           df['ExpCanc4'] * df['ExpI4'] +
                           df['ExpCanc5'] * df['ExpI5'] +
                           df['ExpCanc6'] * df['ExpI6'] +
                           df['ExpCanc7'] * df['ExpI7'] +
                           df['ExpCanc8'] * df['ExpI8'] +
                           df['ExpCanc9'] * df['ExpI9'] +
                           df['ExpCanc10'] * df['ExpI10'] +
                           df['ExpCanc11'] * df['ExpI11'] +
                           df['ExpCanc12'] * df['ExpI12'])

# Earned Premium
df['PGCalc'] = df['EmitExp'] + df['EndosExp'] - abs(df['CancExp'])

# Earned Exposure
df['Exposicao'] = df['ExposicaoEmit'] + df['ExposicaoEndos'] - abs(df['ExposicaoCanc'])

# Sort columns
df.sort_values(by=['CodTipNeg', 'DtRefCtb', 'Lags'], ascending=[True, True, False], inplace=True)

# Drop auxiliar Columns
df.drop([
    'EmitEnd0', 'EmitEnd1', 'EmitEnd2',
    'EmitEnd3', 'EmitEnd4', 'EmitEnd5',
    'EmitEnd6', 'EmitEnd7', 'EmitEnd8',
    'EmitEnd9', 'EmitEnd10', 'EmitEnd11', 'EmitEnd12',
    
    'CancEnd0', 'CancEnd1', 'CancEnd2',
    'CancEnd3', 'CancEnd4', 'CancEnd5',
    'CancEnd6', 'CancEnd7', 'CancEnd8',
    'CancEnd9', 'CancEnd10', 'CancEnd11', 'CancEnd12',
    
    'Exp0', 'Exp1', 'Exp2', 'Exp3',
    'Exp4', 'Exp5',  'Exp6',
    'Exp7', 'Exp8', 'Exp9',
    'Exp10', 'Exp11', 'Exp12',
    
    'ExpI0', 'ExpI1', 'ExpI2', 'ExpI3',
    'ExpI4', 'ExpI5',  'ExpI6',
    'ExpI7', 'ExpI8', 'ExpI9',
    'ExpI10', 'ExpI11', 'ExpI12',
    
    'EmitEnd0', 'EmitEnd1', 'EmitEnd2', 'EmitEnd3',
    'EmitEnd4', 'EmitEnd5',  'EmitEnd6',
    'EmitEnd7', 'EmitEnd8', 'EmitEnd9',
    'EmitEnd10', 'EmitEnd11', 'EmitEnd12',
    
    'CancEnd0', 'CancEnd1', 'CancEnd2', 'CancEnd3',
    'CancEnd4', 'CancEnd5',  'CancEnd6',
    'CancEnd7', 'CancEnd8', 'CancEnd9',
    'CancEnd10', 'CancEnd11', 'CancEnd12',
    
    'ExpCanc0', 'ExpCanc1', 'ExpCanc2', 'ExpCanc3',
    'ExpCanc4', 'ExpCanc5',  'ExpCanc6',
    'ExpCanc7', 'ExpCanc8', 'ExpCanc9',
    'ExpCanc10', 'ExpCanc11', 'ExpCanc12',
    
    'PremEmitaux', 'ItensEmitaux'

               ], axis=1, inplace=True)

# Rearrange the columns
df = df[['ActualsProj', 'CodProd', 'CodTipNeg',
         'DtEmissao', 'Lags', 'DtRefCtb', 'CombinationID',
         'PremEmit', 'PremEndos', 'PremCanc', 'GWP',
         'EmitExp', 'EndosExp', 'CancExp', 'PGCalc',
         'ItensEmit', 'ItensEndos', 'ItensCanc', 'ItensLiq',
         'ExposicaoEmit', 'ExposicaoEndos', 'ExposicaoCanc', 'Exposicao', 'FatTarifSel']]


# Create Actuals Dataframe
# Join Actuals Data
df_actuals = pd.merge(df_sns, df, how='left', on=(['CodProd', 'CodTipNeg', 'DtEmissao', 'Lags', 'DtRefCtb']))
df_actuals = df_actuals[(df_actuals['DtRefCtb'] <= DataRef) &
                        df_actuals['CombinationID'] == 0]

# Fill NA values 
df_actuals.fillna(0, inplace=True)

# Join Baselines Claims Table
df = pd.merge(df, TbBslSns, how='left', on=['CodProd', 'CodTipNeg'])
# Join Baselines Claims Develop Table
df = pd.merge(df, TbDsvSns, how='left', on=['CodProd', 'CodTipNeg', 'Lags'])
# Join Baselines SalSub Table
# Insuficiencia Db de Salvados
TbBslSS['CodBrand'] = TbBslSS['CodProd'].map(brand_map).astype(int)
#TbBslSS.loc[TbBslSS['CodBrand'] == 6, ['BaselSalv', 'BaselSub']] = TbBslSS.loc[TbBslSS['CodBrand'] == 6, ['BaselSalv', 'BaselSub']] / 0.985
df = pd.merge(df, TbBslSS, how='left', on=['CodProd', 'CodTipNeg'])
# Join Baselines SalSub Develop Table
df = pd.merge(df, TbDsvSS, how='left', on=['CodProd', 'CodTipNeg', 'Lags'])
# Create SalSub Tail Factor Table
TbTailSS = (TbDsvSS[TbDsvSS['Lags'] <= 12]
         .groupby(['CodProd', 'CodTipNeg'], as_index=False)
         [['BaselSalvDev', 'BaselSubDev']]
         .sum()
         .rename(columns={'BaselSalvDev': 'BaselSalvTail',
                          'BaselSubDev': 'BaselSubTail'}))
TbTailSS[['BaselSalvTail', 'BaselSubTail']] = 1 / TbTailSS[['BaselSalvTail', 'BaselSubTail']]
# Join SalSub Tail Factor Table
df = pd.merge(df, TbTailSS, how='left', on=attributes)

# Join Risk Elast Table
df = pd.merge(df, Db_ElastRisk, how='left', on=attributes)

# Join Inflation Table
df = pd.merge(df, TbInfl, how='left', on=['CodProd', 'CodTipNeg'])

# Create Dif Month Column
df['DateDifCtb'] = (df['DtRefCtb'].dt.to_period('M') - DataRef.to_period('M')).apply(lambda x: x.n)

# Create Inflationary Factors
df['PPInflFct'] = (1 + df['PPInfl']) ** (df['DateDifCtb'] / 12)
df['PTInflFct'] = (1 + df['PTInfl']) ** (df['DateDifCtb'] / 12)
df['RbftInflFct'] = (1 + df['RbftInfl']) ** (df['DateDifCtb'] / 12)


# Claims & Amount Calculation {Baseline}
# Create Column ExposicaoAux Baseline
df_ExpAux = (df[df['CombinationID'] == 0]
 .groupby(['CodProd', 'CodTipNeg', 'DtEmissao', 'Lags', 'CombinationID'], as_index=False)
 ['Exposicao']
 .sum()
 )
df_ExpAux.drop(['CombinationID'], axis=1, inplace=True)
df_ExpAux.rename(columns={'Exposicao': 'ExposicaoAux'}, inplace=True)
df = pd.merge(df, df_ExpAux, how='left', on=['CodProd', 'CodTipNeg', 'DtEmissao', 'Lags']) 

# Calculate #N Claims (Alteracao para Exposicao)
df['QtdPP'] = round(df['BaselineFreqPP'] * (1 + df['Coef3_2'] * df['FatTarifSel'] ** 3 +
                                            df['Coef2_2'] * df['FatTarifSel'] ** 2 +
                                            df['Coef1_2'] * df['FatTarifSel'] +
                                            df['Slope_2']) *
                                            df['Exposicao'] * df['FreqPPClmDevFact'], 0)

df['QtdPT'] = round(df['BaselineFreqPT'] * (1 + df['Coef3_2'] * df['FatTarifSel'] ** 3 +
                                            df['Coef2_2'] * df['FatTarifSel'] ** 2 +
                                            df['Coef1_2'] * df['FatTarifSel'] +
                                            df['Slope_2']) *
                                            df['Exposicao'] * df['FreqPPClmDevFact'], 0)

df['QtdRbft'] = round(df['BaselineFreqRbft'] * (1 + df['Coef3_2'] * df['FatTarifSel'] ** 3 +
                                            df['Coef2_2'] * df['FatTarifSel'] ** 2 +
                                            df['Coef1_2'] * df['FatTarifSel'] +
                                            df['Slope_2']) *
                                            df['Exposicao'] * df['FreqPPClmDevFact'], 0)
df['Qtd'] = df['QtdPP'] + df['QtdPT'] + df['QtdRbft']

# Calculate IncLoss Amount
df['VlrIndPP'] = df['QtdPP'] * df['BaselineSevPP'] * df['PPInflFct']
df['VlrIndPT'] = df['QtdPT'] * df['BaselineSevPT'] * df['PTInflFct']
df['VlrIndRbft'] = df['QtdRbft'] * df['BaselineSevRbft'] * df['RbftInflFct']
df['VlrInd'] = df['VlrIndPP'] + df['VlrIndPT'] + df['VlrIndRbft']

# Salvage & Subrogation Calculation
# Calculate Auxiliar Column IncLoss PhysDamTotal
df['VlrIndPTaux'] = (df.groupby(['CodProd', 'CodTipNeg', 'DtRefCtb', 'CombinationID'], as_index=False)['VlrIndPT'].transform('sum'))
# Calculate Projected Salvage & Subrogation
df['VlrSalvados'] = (df['VlrIndPTaux'] * df['BaselSalv']  * df['BaselSalvDev']  * df['BaselSalvTail']  + # * 1.0308  SalSub TailFactor
                     df['VlrIndPTaux'] * df['BaselSub']  * df['BaselSubDev'] * df['BaselSubTail'] # * 1.0509
                     )
# Ajuste para compensar valores com Despesas de Salvados (Revisitar numero por produto)
df['VlrSalvados'] = np.where(df['CodProd'].isin([31111, 31041, 31018, 31020]), df['VlrSalvados'] * 0.95, df['VlrSalvados'])

df['VlrIndTot'] = df['VlrIndPP'] + df['VlrIndPT'] + df['VlrIndRbft'] - df['VlrSalvados']


# Rearrange Actuals Columns
df_actuals['ActualsProj'] = 1
df_actuals['CombinationID'] = -5
# Ajuste para compensar valores com Despesas de Salvados (Revisitar numero por produto)
df_actuals['VlrSalvados'] = np.where(df_actuals['CodProd'].isin([31111, 31041, 31018, 31020]), df_actuals['VlrSalvados'] * 0.95, df_actuals['VlrSalvados'])

df_actuals['VlrIndTot'] = df_actuals['VlrIndPP'] + df_actuals['VlrIndPT'] + df_actuals['VlrIndRbft'] - df_actuals['VlrSalvados']
#df_actuals['VlrSalvados'] = 0
#df_actuals['QtdSalvados'] = 0

df_actuals = df_actuals[['ActualsProj', 'CodProd', 'CodTipNeg', 'DtEmissao', 'Lags', 'DtRefCtb', 'CombinationID',
                         'PremEmit', 'PremEndos', 'PremCanc', 'GWP',
                         'EmitExp', 'EndosExp', 'CancExp', 'PGCalc', 
                         'ItensEmit', 'ItensEndos', 'ItensCanc', 'ItensLiq', 
                         'ExposicaoEmit', 'ExposicaoEndos', 'ExposicaoCanc', 'Exposicao',
#                         'BaselineFreqPP', 'BaselineFreqPT', 'BaselineFreqRbft',
#                         'BaselineSevPP', 'BaselineSevPT', 'BaselineSevRbft', 
                         'QtdPP', 'QtdPT', 'QtdRbft', 'Qtd', 
                         'VlrIndPP', 'VlrIndPT', 'VlrIndRbft', 'VlrInd'
                         ,'VlrSalvados', 'VlrIndTot'
#                         , 'QtdSalvados'
#                         ,'VlrIS', 'VlrComm'
       ]]

# Rearrange Projected Columns
df_ProjCen = df.copy()
df_ProjCen = df_ProjCen[df_ProjCen['DtRefCtb'] > DataRef]
#df_ProjCen['VlrSalvados'] = 0
#df_ProjCen.loc['QtdSalvados'] = 0
df_ProjCen = df_ProjCen[['ActualsProj', 'CodProd', 'CodTipNeg', 'DtEmissao', 'Lags', 'DtRefCtb', 'CombinationID',
                         'PremEmit', 'PremEndos', 'PremCanc', 'GWP',
                         'EmitExp', 'EndosExp', 'CancExp', 'PGCalc', 
                         'ItensEmit', 'ItensEndos', 'ItensCanc', 'ItensLiq', 
                         'ExposicaoEmit', 'ExposicaoEndos', 'ExposicaoCanc', 'Exposicao',
#                         'BaselineFreqPP', 'BaselineFreqPT', 'BaselineFreqRbft',
#                         'BaselineSevPP', 'BaselineSevPT', 'BaselineSevRbft', 
                         'QtdPP', 'QtdPT', 'QtdRbft', 'Qtd', 
                         'VlrIndPP', 'VlrIndPT', 'VlrIndRbft', 'VlrInd'
                         ,'VlrSalvados', 'VlrIndTot'
#                         , 'QtdSalvados'
#                         ,'VlrIS', 'VlrComm'
       ]]


# Append Actuals & Projected Data
df_ActProj = pd.concat([df_actuals, df_ProjCen], ignore_index=True)


#7.0 Export Dataframe Cenario Selecionado (parquet)--------------------------------------------------------------------
# Alterar diretorio para AWS S3
diroutput = '_data/_04Outputs/'

version = '_v1'
date = pd.to_datetime('2026-04-01')
month_per = date.month_name()
year_per = date.year
outputfile = f"Db_Cens_{year_per}{month_per}{version}.parquet"
#outputfile = 'Db_Cens_v0.parquet'

df_ActProj.reset_index(drop=True, inplace=True)
df_ActProj.to_parquet(diroutput + outputfile)


#7.1 Export Dataframe Cenario Selecionado (Excel)--------------------------------------------------------------------
df_ActProj = (
    df_ActProj[(df_ActProj['CombinationID'] <= 2) &
             (df_ActProj['DtRefCtb'] < pd.to_datetime('2027-01-01')) &
             (df_ActProj['DtRefCtb'] >= pd.to_datetime('2024-01-01'))]
             .groupby(['ActualsProj', 'CodProd', 'CodTipNeg','DtEmissao', 'Lags',  'DtRefCtb', 'CombinationID'], as_index=False)
             [['PremEmit', 'PremEndos', 'PremCanc', 'GWP',
              'EmitExp', 'EndosExp', 'CancExp', 'PGCalc',
              'ItensEmit', 'ItensEndos', 'ItensCanc', 'ItensLiq',
              'ExposicaoEmit', 'ExposicaoEndos', 'ExposicaoCanc', 'Exposicao',
              'QtdPP', 'QtdPT', 'QtdRbft', 'Qtd',
              'VlrIndPP', 'VlrIndPT', 'VlrIndRbft', 'VlrInd'
              ,'VlrSalvados', 'VlrIndTot']]
              .sum()
)

# Alterar diretorio para AWS S3
diroutput = '_data/_04Outputs/Excel/'

outputfile = 'Db_Cens_202604v0.csv'
df_ActProj.reset_index(drop=True, inplace=True)
df_ActProj.to_csv(diroutput + outputfile)

outputfile = 'Db_Cens_202604v0.xlsx'
df_ActProj.reset_index(drop=True, inplace=True)
df_ActProj.to_excel(diroutput + outputfile)


