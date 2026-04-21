# Description--------------------------------------------------------------------
"""
#FileName: PremParam
#Author: Vinicius Almeida
#Contact: v.branco91@gmail.com
#Copyright Exposure Atuarial, 2025
#Inception Date: 08.09.2025
#Version Date: 01.10.2025
#Version Control: 1.0.0
---
Goal

Calculate Frequency and Severity Assumptions for Motor Lines


---
DRAFT

"""
#0.0 Initial setting--------------------------------------------------------------------

import pandas as pd
import numpy as np
from itertools import product
import matplotlib.pyplot as plt
#from sklearn.linear_model import LinearRegression
from sklearn import linear_model

import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from matplotlib import patches

#1.0 Import Dataframe--------------------------------------------------------------------
# Alterar diretorio de importacao de dados
dir = '_data/_01Input/_01Prem/'
file_name = dir + 'Db_PremAct.parquet'
df = pd.read_parquet(file_name)

dir = '_data/_01Input/'
file_name = dir + 'Db_Exp.xlsx'
df_exp = pd.read_excel(file_name)


dir = '_data/_01Input/_01Sns/'
file_name = dir + 'Db_Sns.parquet'
df_sns = pd.read_parquet(file_name)

#2.0 Parameters--------------------------------------------------------------------

DataRef = pd.to_datetime('2025-12-01')
CutOffDate = pd.to_datetime('2022-01-01')

# Iteration Objects
attributes = ['ProductCod', 'BusinessTypeCod']
# BrandKeys = df['CodMarca'].unique()
ProductKeys = df['ProductCod'].unique()
BusinessKeys = df[df['BusinessTypeCod'] > 0]['BusinessTypeCod'].unique()

# Filter Date
df = df[(df['CalendarDate'] <= DataRef) &
        (df['CalendarDate'] > CutOffDate)]

# Auxiliar Dataframe for segmentations w/o Lags movements
df_aux = pd.DataFrame(list(product(ProductKeys, 
                                   BusinessKeys,
                                   pd.date_range(start='2022-01-01', end=DataRef, freq='MS')
                                   , list(range(13)))),
                  columns=['ProductCod', 'BusinessTypeCod', 'PolicyEffDate', 'Lags'])

df_aux[['WrittenPremium', 'WritEndorsPremium', 'WritCancPremium', 'GWP',
        'WrittenItem', 'WritEndorsItem', 'WritCancItem', 'Net_Item',
        'SumInsuredAmount', 'VlrComm']] = 0

df_aux['CalendarDate'] = (df_aux['PolicyEffDate'].dt.to_period('M') + df_aux['Lags']).dt.to_timestamp()
df_aux = df_aux[df_aux['CalendarDate'] <= DataRef]

# Append Premium Dataframe w/ Auxiliar
df = pd.concat([df, df_aux], ignore_index=True)

# Filter Claims Dataframe
df = (df
      .groupby(['ProductCod', 'BusinessTypeCod', 'PolicyEffDate', 'Lags', 'CalendarDate'], as_index=False)
      [['WrittenPremium', 'WritEndorsPremium', 'WritCancPremium', 'GWP',
        'WrittenItem', 'WritEndorsItem', 'WritCancItem', 'Net_Item',
        'SumInsuredAmount', 'VlrComm']]
        .sum())


# Filter Claims Dataframe
df_sns = df_sns[(df_sns['CalendarDate'] <= DataRef) &
                (df_sns['CalendarDate'] > pd.to_datetime(CutOffDate)+pd.DateOffset(months=12)
)]


# Dataframe for Baseline Selections
TbBslSns = (df.groupby(attributes
                       , as_index=False)
                       .size()
                       [attributes]
                       )

TbBslSns['BaselineFreqPP'] = 0
TbBslSns['BaselineFreqPT'] = 0
TbBslSns['BaselineFreqTheft'] = 0
TbBslSns['BaselineFreqPP'] = TbBslSns['BaselineFreqPP'].astype(float)
TbBslSns['BaselineFreqPT'] = TbBslSns['BaselineFreqPT'].astype(float)
TbBslSns['BaselineFreqTheft'] = TbBslSns['BaselineFreqTheft'].astype(float)

TbBslSns['BaselineSevPP'] = 0
TbBslSns['BaselineSevPT'] = 0
TbBslSns['BaselineSevTheft'] = 0
TbBslSns['BaselineSevPP'] = TbBslSns['BaselineSevPP'].astype(float)
TbBslSns['BaselineSevPT'] = TbBslSns['BaselineSevPT'].astype(float)
TbBslSns['BaselineSevTheft'] = TbBslSns['BaselineSevTheft'].astype(float)

# Credibility Weight For Claims Development
CredWeigthClmDsv = 0.7


# 3.0 Merge Exposure Table--------------------------------------------------------------------

# Filter Date
df = df[(df['CalendarDate'] <= DataRef) &
        (df['Lags'] <= 12)]


# Keep Columns DataFrame Actuals
# Alterar colunas (Acrescentar/Retirar atributos)
df = df[['ProductCod', 'BusinessTypeCod', 
            'PolicyEffDate', 'Lags', 'CalendarDate',
            'WrittenPremium', 'WritEndorsPremium', 'WritCancPremium', 'GWP',
            'WrittenItem', 'WritEndorsItem', 'WritCancItem', 'Net_Item'
            ]]

# Perform left join on columns 'Lags' and 'PolicyEffDate'
df_exp = df_exp[['Lags', 'PolicyEffDate', 
                 'Exp0', 'Exp1', 'Exp2', 'Exp3', 
                 'Exp4', 'Exp5', 'Exp6', 'Exp7', 
                 'Exp8', 'Exp9', 'Exp10', 'Exp11', 'Exp12',
                 'ExpI0', 'ExpI1', 'ExpI2', 'ExpI3', 
                 'ExpI4', 'ExpI5', 'ExpI6', 'ExpI7', 
                 'ExpI8', 'ExpI9', 'ExpI10', 'ExpI11', 'ExpI12',
                 ]]
df = pd.merge(df, df_exp, how='left', on=['Lags', 'PolicyEffDate'])


# 3.1 Calculate Exposure--------------------------------------------------------------------
# WrittenPremium aggregation
df['WrittenPremiumaux'] = (df.groupby(['BusinessTypeCod', 'ProductCod', 'PolicyEffDate'], as_index=False)['WrittenPremium'].transform('sum'))
df['WrittenPremiumaux'] = df['WrittenPremiumaux'].fillna(0)

# ItemEmit aggregation
df['WrittenItemaux'] = (df.groupby(['BusinessTypeCod', 'ProductCod', 'PolicyEffDate'], as_index=False)['WrittenItem'].transform('sum'))
df['WrittenItemaux'] = df['WrittenItemaux'].fillna(0)

# WritEndorsPremium, WritCancPremium, WritCancItem aggregation
# Step 3: Loop through lag values 0 to 12
for lag in range(13):
    # WritEndorsPremium aggregation
    df_endos = (
        df[df['Lags'] == lag]
        .groupby(['BusinessTypeCod', 'ProductCod', 'PolicyEffDate'], as_index=False)['WritEndorsPremium']
        .sum()
        .rename(columns={'WritEndorsPremium': f'EmitEnd{lag}'})
    )
    
    # WritCancPremium aggregation
    df_canc = (
        df[df['Lags'] == lag]
        .groupby(['BusinessTypeCod', 'ProductCod', 'PolicyEffDate'], as_index=False)['WritCancPremium']
        .sum()
        .rename(columns={'WritCancPremium': f'CancEnd{lag}'})
    )
    
    # WritCancItem aggregation
    df_cancexp = (
        df[df['Lags'] == lag]
        .groupby(['BusinessTypeCod', 'ProductCod', 'PolicyEffDate'], as_index=False)['WritCancItem']
        .sum()
        .rename(columns={'WritCancItem': f'ExpCanc{lag}'})
    )
    
    # Merge both into main DataFrame
    df = pd.merge(df, df_endos, how='left', on=['BusinessTypeCod', 'ProductCod', 'PolicyEffDate'])
    df = pd.merge(df, df_canc, how='left', on=['BusinessTypeCod', 'ProductCod', 'PolicyEffDate'])
    df = pd.merge(df, df_cancexp, how='left', on=['BusinessTypeCod', 'ProductCod', 'PolicyEffDate'])


# Fill NA 
df = df.fillna(0)

# Calculate Earned Premium, Endorsements, Cancellation 
df['EmitExp'] = df['WrittenPremiumaux'] * df['Exp0'] 

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

df['ExposureEmit'] = df['WrittenItemaux'] * df['Exp0'] 

df['ExposureEndos'] = 0

df['ExposureCanc'] = (df['ExpCanc0'] * df['Exp0'] +
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
df['EarnPrem'] = df['EmitExp'] + df['EndosExp'] - abs(df['CancExp'])

# Earned Exposure
df['Exposure'] = df['ExposureEmit'] + df['ExposureEndos'] - abs(df['ExposureCanc'])


# Sort columns
df.sort_values(by=['BusinessTypeCod', 'CalendarDate', 'Lags'], ascending=[True, True, False], inplace=True)

# Drop auxiliar Columns
df.drop(['WrittenPremiumaux', 
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
               'Exp10', 'Exp11', 'Exp12'], axis=1, inplace=True)

# Rearrange the columns
df = df[['ProductCod', 'BusinessTypeCod',
         'PolicyEffDate', 'Lags', 'CalendarDate',
         'WrittenPremium', 'WritEndorsPremium', 'WritCancPremium', 'GWP',
         'EmitExp', 'EndosExp', 'CancExp', 'EarnPrem',
         'WrittenItem', 'WritEndorsItem', 'WritCancItem', 'Net_Item',
         'ExposureEmit', 'ExposureEndos', 'ExposureCanc', 'Exposure']]

# 4.0 Baseline Claims Development--------------------------------------------------------------------

# Join Datasets
TbDsvSns = pd.merge(df_sns, df, how='left', on=['ProductCod', 'BusinessTypeCod', 'PolicyEffDate', 'Lags', 'CalendarDate'])

TbDsvSns = TbDsvSns[TbDsvSns['Lags'] <= 12]

# Calc Freq
TbDsvSns['FreqPP'] = TbDsvSns['ClaimsPP'] / TbDsvSns['Exposure']
TbDsvSns['FreqPT'] = TbDsvSns['ClaimsPT'] / TbDsvSns['Exposure']
TbDsvSns['FreqTheft'] = TbDsvSns['ClaimsTheft'] / TbDsvSns['Exposure']

# Group DataFrame
TbDsvSns = (TbDsvSns[(TbDsvSns['CalendarDate'] > DataRef - pd.DateOffset(months=12)) &
                   (TbDsvSns['CalendarDate'] <= DataRef)]
           .groupby(['ProductCod', 'BusinessTypeCod', 'Lags', 'CalendarDate'], as_index=False)
           [['FreqPP', 'FreqPT', 'FreqTheft']]
           .sum()
           )

# Mean R12
TbDsvSns_R12 = (TbDsvSns
                .groupby(['ProductCod', 'BusinessTypeCod', 'Lags'], as_index=False)
                [['FreqPP', 'FreqPT', 'FreqTheft']]
                .mean()
                .rename(columns={'FreqPP': 'FreqPPR12',
                                 'FreqPT': 'FreqPTR12',
                                 'FreqTheft': 'FreqTheftR12'
                                 }))

# Mean R3
TbDsvSns_R3 = (TbDsvSns[(TbDsvSns['CalendarDate'] > DataRef - pd.DateOffset(months=3))]
                .groupby(['ProductCod', 'BusinessTypeCod', 'Lags'], as_index=False)
                [['FreqPP', 'FreqPT', 'FreqTheft']]
                .mean()
                .rename(columns={'FreqPP': 'FreqPPR3',
                                 'FreqPT': 'FreqPTR3',
                                 'FreqTheft': 'FreqTheftR3'
                                 }))

# Join Clm Dev Tables
TbDsvSns = pd.merge(TbDsvSns_R12, TbDsvSns_R3, how='left', on=['ProductCod', 'BusinessTypeCod', 'Lags'])

# Baseline Claims Development
TbDsvSns['FreqPPDsv'] = TbDsvSns['FreqPPR12'] * (1 - CredWeigthClmDsv) + CredWeigthClmDsv * TbDsvSns['FreqPPR3']
TbDsvSns['FreqPTDsv'] = TbDsvSns['FreqPTR12'] * (1 - CredWeigthClmDsv) + CredWeigthClmDsv * TbDsvSns['FreqPTR3']
TbDsvSns['FreqTheftDsv'] = TbDsvSns['FreqTheftR12'] * (1 - CredWeigthClmDsv) + CredWeigthClmDsv * TbDsvSns['FreqTheftR3']

# Baseline Claims Development Avg
TbDsvSns['FreqPPDsvAvg'] = TbDsvSns.groupby(['ProductCod', 'BusinessTypeCod'], as_index=False)['FreqPPDsv'].transform('mean')
TbDsvSns['FreqPTDsvAvg'] = TbDsvSns.groupby(['ProductCod', 'BusinessTypeCod'], as_index=False)['FreqPTDsv'].transform('mean')
TbDsvSns['FreqTheftDsvAvg'] = TbDsvSns.groupby(['ProductCod', 'BusinessTypeCod'], as_index=False)['FreqTheftDsv'].transform('mean')

# Baseline Claims Development Factor
TbDsvSns['FreqPPClmDevFact'] = TbDsvSns['FreqPPDsv'] / TbDsvSns['FreqPPDsvAvg']
TbDsvSns['FreqPTClmDevFact'] = TbDsvSns['FreqPTDsv'] / TbDsvSns['FreqPTDsvAvg']
TbDsvSns['FreqTheftClmDevFact'] = TbDsvSns['FreqTheftDsv'] / TbDsvSns['FreqTheftDsvAvg']


#5.0 Salvage & Subrogation Baselines--------------------------------------------------------------------
# Create 2 Dataframes with Salvage, Subrogation & Inc Loss (PhysDamTotal) metrics
TbBslSS12 = (df_sns[(df_sns['CalendarDate'] > DataRef - pd.DateOffset(months=12)) &
                   (df_sns['CalendarDate'] <= DataRef)]
           .groupby(['ProductCod', 'BusinessTypeCod'], as_index=False)
           [['AmountPT', 'SalvAmount', 'SubrogAmount']]
           .sum()
           )

TbBslSS12['BaselSalv12'] = TbBslSS12['SalvAmount'] / TbBslSS12['AmountPT']
TbBslSS12['BaselSub12'] = TbBslSS12['SubrogAmount'] / TbBslSS12['AmountPT']


TbBslSS3 = (df_sns[(df_sns['CalendarDate'] > DataRef - pd.DateOffset(months=3)) &
                   (df_sns['CalendarDate'] <= DataRef)]
           .groupby(['ProductCod', 'BusinessTypeCod'], as_index=False)
           [['AmountPT', 'SalvAmount', 'SubrogAmount']]
           .sum()
           )

TbBslSS3['BaselSalv3'] = TbBslSS3['SalvAmount'] / TbBslSS3['AmountPT']
TbBslSS3['BaselSub3'] = TbBslSS3['SubrogAmount'] / TbBslSS3['AmountPT']

# Set Salvage & Subrogation Baselines
TbBslSS3['BaselSalv'] = TbBslSS3['BaselSalv3'] * 0.75 + TbBslSS12['BaselSalv12'] * 0.25
TbBslSS3['BaselSub'] = TbBslSS3['BaselSub3'] * 0.75 + TbBslSS12['BaselSub12'] * 0.25

# Keep Baseline Dataframe 
TbBslSS = TbBslSS3[['ProductCod', 'BusinessTypeCod',
                    'BaselSalv', 'BaselSub']]

#6.0 Salvage & Subrogation Development--------------------------------------------------------------------
# Group DataFrame
TbDsvSS = (df_sns[(df_sns['CalendarDate'] > DataRef - pd.DateOffset(months=12)) &
                   (df_sns['CalendarDate'] <= DataRef)]
           .groupby(['ProductCod', 'BusinessTypeCod', 'Lags'], as_index=False)
           [['SalvAmount', 'SubrogAmount']]
           .sum()
           )

# Total Salvage
TbDsvSS['SalvAmountTot'] = TbDsvSS.groupby(['ProductCod', 'BusinessTypeCod'])['SalvAmount'].transform('sum')
TbDsvSS['SubrogAmountTot'] = TbDsvSS.groupby(['ProductCod', 'BusinessTypeCod'])['SubrogAmount'].transform('sum')

TbDsvSS['BaselSalvDev'] = TbDsvSS['SalvAmount'] / TbDsvSS['SalvAmountTot']
TbDsvSS['BaselSubDev'] = TbDsvSS['SubrogAmount'] / TbDsvSS['SubrogAmountTot']

TbDsvSS = TbDsvSS[['ProductCod', 'BusinessTypeCod', 'Lags', 'BaselSalvDev', 'BaselSubDev']]


#7.0 Merge DataFrames--------------------------------------------------------------------
# Join DF Sinistro w/ DF Exposure
df_sns = pd.merge(df_sns, df, how='left', on=['ProductCod', 'BusinessTypeCod', 'PolicyEffDate', 'Lags', 'CalendarDate'])

# Fill NA 
df_sns = df_sns.fillna(0)

# Grouping Data
df_calc = (df_sns
            [(df_sns['CalendarDate'] <= DataRef) &
             (df_sns['CalendarDate'] >= pd.to_datetime(CutOffDate)+pd.DateOffset(months=12))]
          .groupby(['ProductCod', 'BusinessTypeCod', 'CalendarDate'], as_index=False)
          [['Exposure',
           'ClaimsPP', 'ClaimsPT', 'ClaimsTheft',
           'AmountPP', 'AmountPT', 'AmountTheft']]
          .sum())


#7.0 Calculate Indicators--------------------------------------------------------------------
# Frequency
df_calc['FreqPP'] = df_calc['ClaimsPP'] / df_calc['Exposure']
df_calc['FreqPT'] = df_calc['ClaimsPT'] / df_calc['Exposure']
df_calc['FreqTheft'] = df_calc['ClaimsTheft'] / df_calc['Exposure']

# Severity
df_calc['SevPP'] = df_calc['AmountPP'] / df_calc['ClaimsPP']
df_calc['SevPT'] = df_calc['AmountPT'] / df_calc['ClaimsPT']
df_calc['SevTheft'] = df_calc['AmountTheft'] / df_calc['ClaimsTheft']

# Setar Indice
df_calc = df_calc.set_index('CalendarDate')
# Identificar pontos no tempo
#df_calc['Time'] = np.arange(len(df_calc.index))


#8.0 Calculate Regressions--------------------------------------------------------------------
# Iterations to segment Baselines by Brand, Product & Business Type
# Loop
# Product_It = 31111
# Business_It = 2

for Product_It in ProductKeys:
            
            for Business_It in BusinessKeys:
                
                df_group = (df_calc
                            [#(df_calc['CodMarca'] == 6) &
                             (df_calc['ProductCod'] == Product_It) &
                             (df_calc['BusinessTypeCod'] == Business_It)]
                               )
                df_group['Time'] = np.arange(len(df_group.index))
                df_group.loc[df_group['ClaimsPP'] == 0, 'ClaimsPP'] = 3
                df_group.loc[df_group['ClaimsPT'] == 0, 'ClaimsPT'] = 3
                df_group.loc[df_group['ClaimsTheft'] == 0, 'ClaimsTheft'] = 3

                df_group['SevPP'] = df_group['AmountPP'] / df_group['ClaimsPP']
                df_group['SevPT'] = df_group['AmountPT'] / df_group['ClaimsPT']
                df_group['SevTheft'] = df_group['AmountTheft'] / df_group['ClaimsTheft']

                df_group.loc[df_group['SevPP'] <= 0, 'SevPP'] = df_group['SevPP'].mean()
                df_group.loc[df_group['SevPT'] <= 0, 'SevPT'] = df_group['SevPT'].mean()
                df_group.loc[df_group['SevTheft'] <= 0, 'SevTheft'] = df_group['SevTheft'].mean()

                # Create Linear Trend
                modelFreqPP = linear_model.LinearRegression()
                modelFreqPT = linear_model.LinearRegression()
                modelFreqTheft = linear_model.LinearRegression()
                modelSevPP = linear_model.LinearRegression()
                modelSevPT = linear_model.LinearRegression()
                modelSevTheft = linear_model.LinearRegression()
                X = df_group[['Time']]  # Independent variable (reshaped)
                fy = df_group['FreqPP']   # Dependent variable
                fz = df_group['FreqPT']   # Dependent variable
                fw = df_group['FreqTheft']   # Dependent variable
                sy = df_group['SevPP']   # Dependent variable
                sz = df_group['SevPT']   # Dependent variable
                sw = df_group['SevTheft']   # Dependent variable
                # Freq
                modelFreqPP.fit(X, fy)
                modelFreqPT.fit(X, fz)
                modelFreqTheft.fit(X, fw)
                # Severity
                modelSevPP.fit(X, sy)
                modelSevPT.fit(X, sz)
                modelSevTheft.fit(X, sw)

                # Create Polynomial Trend
                # Trace Dependent variables
                x = df_group['Time'].values
                fy = df_group['FreqPP'].values
                fz = df_group['FreqPT'].values
                fw = df_group['FreqTheft'].values

                # Build coefficients & Intercept
                coeffs_2nd_degreeFreqPP = np.polyfit(x, fy, deg=2)
                coeffs_2nd_degreeFreqPT = np.polyfit(x, fz, deg=2)
                coeffs_2nd_degreeFreqTheft = np.polyfit(x, fw, deg=2)

                coeffs_3rd_degreeFreqPP = np.polyfit(x, fy, deg=3)
                coeffs_3rd_degreeFreqPT = np.polyfit(x, fz, deg=3)
                coeffs_3rd_degreeFreqTheft = np.polyfit(x, fw, deg=3)

                # Build Models
                modelPoly2FreqPP = np.poly1d(coeffs_2nd_degreeFreqPP)
                modelPoly2FreqPT = np.poly1d(coeffs_2nd_degreeFreqPT)
                modelPoly2FreqTheft = np.poly1d(coeffs_2nd_degreeFreqTheft)

                modelPoly3FreqPP = np.poly1d(coeffs_3rd_degreeFreqPP)
                modelPoly3FreqPT = np.poly1d(coeffs_3rd_degreeFreqPT)
                modelPoly3FreqTheft = np.poly1d(coeffs_3rd_degreeFreqTheft)


                # Regression Frequency
                df_group['LinearFreqPP'] = modelFreqPP.predict(X)
                df_group['LinearFreqPT'] = modelFreqPT.predict(X)
                df_group['LinearFreqTheft'] = modelFreqTheft.predict(X)

                df_group['Poly2FreqPP'] = modelPoly2FreqPP(x)
                df_group['Poly2FreqPT'] = modelPoly2FreqPT(x)
                df_group['Poly2FreqTheft'] = modelPoly2FreqTheft(x)

                df_group['Poly3FreqPP'] = modelPoly3FreqPP(x)
                df_group['Poly3FreqPT'] = modelPoly3FreqPT(x)
                df_group['Poly3FreqTheft'] = modelPoly3FreqTheft(x)



                # Trace Dependent variables
                sy = df_group['SevPP'].values
                sz = df_group['SevPT'].values
                sw = df_group['SevTheft'].values

                # Build coefficients & Intercept
                coeffs_2nd_degreeSevPP = np.polyfit(x, sy, deg=2)
                coeffs_2nd_degreeSevPT = np.polyfit(x, sz, deg=2)
                coeffs_2nd_degreeSevTheft = np.polyfit(x, sw, deg=2)

                coeffs_3rd_degreeSevPP = np.polyfit(x, sy, deg=3)
                coeffs_3rd_degreeSevPT = np.polyfit(x, sz, deg=3)
                coeffs_3rd_degreeSevTheft = np.polyfit(x, sw, deg=3)

                # Build Models
                modelPoly2SevPP = np.poly1d(coeffs_2nd_degreeSevPP)
                modelPoly2SevPT = np.poly1d(coeffs_2nd_degreeSevPT)
                modelPoly2SevTheft = np.poly1d(coeffs_2nd_degreeSevTheft)

                modelPoly3SevPP = np.poly1d(coeffs_3rd_degreeSevPP)
                modelPoly3SevPT = np.poly1d(coeffs_3rd_degreeSevPT)
                modelPoly3SevTheft = np.poly1d(coeffs_3rd_degreeSevTheft)

                # Regression Severity
                df_group['LinearSevPP'] = modelSevPP.predict(X)
                df_group['LinearSevPT'] = modelSevPT.predict(X)
                df_group['LinearSevTheft'] = modelSevTheft.predict(X)

                df_group['Poly2SevPP'] = modelPoly2SevPP(x)
                df_group['Poly2SevPT'] = modelPoly2SevPT(x)
                df_group['Poly2SevTheft'] = modelPoly2SevTheft(x)

                df_group['Poly3SevPP'] = modelPoly3SevPP(x)
                df_group['Poly3SevPT'] = modelPoly3SevPT(x)
                df_group['Poly3SevTheft'] = modelPoly3SevTheft(x)



                # Trending to Current Levels 
                # Trended Frequency
                df_group['TrendedFreqPP'] = df_group['FreqPP'] * df_group['Poly3FreqPP'].iloc[-1] / df_group['Poly3FreqPP']
                df_group['TrendedFreqPT'] = df_group['FreqPT'] * df_group['Poly3FreqPT'].iloc[-1] / df_group['Poly3FreqPT']
                df_group['TrendedFreqTheft'] = df_group['FreqTheft'] * df_group['Poly3FreqTheft'].iloc[-1] / df_group['Poly3FreqTheft']

                # Trended Severity
                df_group['TrendedSevPP'] = df_group['SevPP'] * df_group['Poly3SevPP'].iloc[-1] / df_group['Poly3SevPP']
                df_group['TrendedSevPT'] = df_group['SevPT'] * df_group['Poly3SevPT'].iloc[-1] / df_group['Poly3SevPT']
                df_group['TrendedSevTheft'] = df_group['SevTheft'] * df_group['Poly3SevTheft'].iloc[-1] / df_group['Poly3SevTheft']


                # Calculate Baselines
                # Baseline Frequency
                df_group['BaselineFreqPP'] = df_group['TrendedFreqPP'].mean()
                df_group['BaselineFreqPT'] = df_group['TrendedFreqPT'].mean()
                df_group['BaselineFreqTheft'] = df_group['TrendedFreqTheft'].mean()

                # Baseline Severity
                df_group['BaselineSevPP'] = df_group['TrendedSevPP'].mean()
                df_group['BaselineSevPT'] = df_group['TrendedSevPT'].mean()
                df_group['BaselineSevTheft'] = df_group['TrendedSevTheft'].mean()


                # Set Baselines to DataFrame
                TbBslSns.loc[(TbBslSns['ProductCod'] == Product_It) & (TbBslSns['BusinessTypeCod'] == Business_It), 'BaselineFreqPP'] = round(df_group['TrendedFreqPP'].mean(), 6)
                TbBslSns.loc[(TbBslSns['ProductCod'] == Product_It) & (TbBslSns['BusinessTypeCod'] == Business_It), 'BaselineFreqPT'] = round(df_group['TrendedFreqPT'].mean(), 6)
                TbBslSns.loc[(TbBslSns['ProductCod'] == Product_It) & (TbBslSns['BusinessTypeCod'] == Business_It), 'BaselineFreqTheft'] = round(df_group['TrendedFreqTheft'].mean(), 6)

                TbBslSns.loc[(TbBslSns['ProductCod'] == Product_It) & (TbBslSns['BusinessTypeCod'] == Business_It), 'BaselineSevPP'] = round(df_group['TrendedSevPP'].mean(), 4)
                TbBslSns.loc[(TbBslSns['ProductCod'] == Product_It) & (TbBslSns['BusinessTypeCod'] == Business_It), 'BaselineSevPT'] = round(df_group['TrendedSevPT'].mean(), 4)
                TbBslSns.loc[(TbBslSns['ProductCod'] == Product_It) & (TbBslSns['BusinessTypeCod'] == Business_It), 'BaselineSevTheft'] = round(df_group['TrendedSevTheft'].mean(), 4)


#9.0 Export Params--------------------------------------------------------------------

##################################
# Create an ParquetWriter object #
##################################
# Alterar diretorio para AWS S3
diroutput = 'C:/Users/vinic/OneDrive/Desktop/Vini/02.Projetos/ExposicaoAtuarial/2025/Simulador/20.Template/03.Baselines/02.Sns/'

outputfile = 'BaselineSns.parquet'
TbBslSns.reset_index(drop=True, inplace=True)
TbBslSns.to_parquet(diroutput + outputfile)

outputfile = 'BaselineDevSns.parquet'
TbDsvSns.reset_index(drop=True, inplace=True)
TbDsvSns.to_parquet(diroutput + outputfile)


outputfile = 'BaselineSS.parquet'
TbBslSS.reset_index(drop=True, inplace=True)
TbBslSS.to_parquet(diroutput + outputfile)

outputfile = 'BaselineDsvSS.parquet'
TbDsvSS.reset_index(drop=True, inplace=True)
TbDsvSS.to_parquet(diroutput + outputfile)


# Excel File
# Alterar diretorio para AWS S3
diroutput = 'C:/Users/vinic/OneDrive/Desktop/Vini/02.Projetos/ExposicaoAtuarial/2025/Simulador/20.Template/03.Baselines/02.Sns/Excel/'

outputfile = 'BaselineSns.xlsx'
TbBslSns.reset_index(drop=True, inplace=True)
TbBslSns.to_excel(diroutput + outputfile)

outputfile = 'BaselineDevSns.xlsx'
TbDsvSns.reset_index(drop=True, inplace=True)
TbDsvSns.to_excel(diroutput + outputfile)

outputfile = 'BaselineSS.xlsx'
TbBslSS.reset_index(drop=True, inplace=True)
TbBslSS.to_excel(diroutput + outputfile)

outputfile = 'BaselineDsvSS.xlsx'
TbDsvSS.reset_index(drop=True, inplace=True)
TbDsvSS.to_excel(diroutput + outputfile)

