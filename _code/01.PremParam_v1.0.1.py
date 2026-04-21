# Description--------------------------------------------------------------------
"""
#FileName: PremParam
#Author: Vinicius Almeida
#Contact: v.branco91@gmail.com
#Copyright Exposicao Atuarial, 2025
#Inception Date: 08.09.2025
#Version Date: 01.10.2025
#Version Control: 1.0.0
---
Goal

Define premium/item baselines for simulator calculations.

---
"""
#0.0 Initial setting--------------------------------------------------------------------

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

#1.0 Import Dataframe (Parquet)--------------------------------------------------------------------
dir = '_data/_01Input/_01Prem/'
file_name = dir + 'Db_PremAct.parquet'
df = pd.read_parquet(file_name)
df['PolicyEffDate'].sort_values().unique()

dir = '_data/_01Input/_03CotacaoVlrIS/'
file_name = dir + 'Db_QuotesSI.parquet'
df_ctc = pd.read_parquet(file_name)


#2.0 Parameters--------------------------------------------------------------------

DateRef = pd.to_datetime('2025-12-01')
attributes = ['BusinessTypeCod', 'ProductCod'
#              , 'BrandCod', 'CodCnl'
#              , 'CodReg', 'CodCol1', 'CodCol2'
              ]

# Filter Date
df = df[df['CalendarDate'] <= DateRef]
df_ctc = df_ctc[df_ctc['PolicyEffDate'] <= DateRef]

#3.0 Baseline Endorsement/Cancellation--------------------------------------------------------------------

df_group = (
    df
    .groupby(['BusinessTypeCod', 'ProductCod', 'CalendarDate'], as_index=False)
    [['WrittenPremium', 'WritEndorsPremium', 'WritCancPremium',
      'WrittenItem', 'WritCancItem', 'SumInsuredAmount']]
    .sum()
)

# Define date range
date_interval1 =  pd.to_datetime(DateRef) + pd.DateOffset(months=-2)
date_interval2 =  pd.to_datetime(DateRef) + pd.DateOffset(months=-12)

# Filter by date range
df1 = df_group[(df_group['CalendarDate'] >= date_interval1) & (df_group['CalendarDate'] <= DateRef)]
df2 = df_group[(df_group['CalendarDate'] >= date_interval2) & (df_group['CalendarDate'] <= DateRef)]

# Group by attribute(s) and sum metrics
# For flexibility, we keep a list of attributes to group by
df1 = df1.groupby(attributes).sum(numeric_only=True).reset_index()
df1['BaselEndosPrem'] = df1['WritEndorsPremium'] / df1['WrittenPremium']
df1['BaselCancPrem'] = - df1['WritCancPremium'] / df1['WrittenPremium']
df1['BaselCancItm'] = df1['WritCancItem'] / df1['WrittenItem']

df2 = df2.groupby(attributes).sum(numeric_only=True).reset_index()
df2['BaselEndosPrem'] = df2['WritEndorsPremium'] / df2['WrittenPremium']
df2['BaselCancPrem'] = - df2['WritCancPremium'] / df2['WrittenPremium']
df2['BaselCancItm'] = df2['WritCancItem'] / df2['WrittenItem']

TbBslPrm = df1.copy()
weigth = 0.75
TbBslPrm['BaselEndosPrem'] = (TbBslPrm['BaselEndosPrem'] * weigth) + (df2['BaselEndosPrem'] * (1 - weigth))
TbBslPrm['BaselCancPrem'] = (TbBslPrm['BaselCancPrem'] * weigth) + (df2['BaselCancPrem'] * (1 - weigth))
TbBslPrm['BaselEndosItm'] = 0
TbBslPrm['BaselCancItm'] = (TbBslPrm['BaselCancItm'] * weigth) + (df2['BaselCancItm'] * (1 - weigth))
TbBslPrm = TbBslPrm[['BusinessTypeCod', 'ProductCod', 
                     'BaselEndosPrem', 'BaselCancPrem',
                     'BaselEndosItm', 'BaselCancItm']]


#4.0 Endorsement/Cancellation Development--------------------------------------------------------------------
# Define date range
date_interval1 =  pd.to_datetime(DateRef) + pd.DateOffset(months=-12)
date_interval2 =  pd.to_datetime(DateRef) + pd.DateOffset(months=-36)

# Filter by date range
df1 = df[(df['CalendarDate'] >= date_interval2) & (df['CalendarDate'] <= date_interval1)]
df2 = df1.copy()

# Calculate
df1 = df1.groupby(['BusinessTypeCod', 'ProductCod', 'Lags']).sum(numeric_only=True).reset_index()

df2 = df2.groupby(attributes).sum(numeric_only=True).reset_index()
df2 = (df2
       .rename(columns = {'WritEndorsPremium': 'WritEndorsPremiumAux',
                          'WritCancPremium': 'WritCancPremiumAux',
                          'WritCancItem': 'WritCancItemAux'})
                          [['BusinessTypeCod', 'ProductCod', 'WritEndorsPremiumAux', 'WritCancPremiumAux', 'WritCancItemAux']]
                          )

df1.drop(['WrittenPremium', 'WrittenItem'], axis=1, inplace=True)

TbDsvPrm = pd.merge(df1, df2, how='left', on=attributes)
TbDsvPrm['DsnvEndosPrem'] = TbDsvPrm['WritEndorsPremium'] / TbDsvPrm['WritEndorsPremiumAux']
TbDsvPrm['DsnvCancPrem'] = TbDsvPrm['WritCancPremium'] / TbDsvPrm['WritCancPremiumAux']
TbDsvPrm['DsnvEndosItm'] = 0
TbDsvPrm['DsnvCancItm'] = TbDsvPrm['WritCancItem'] / TbDsvPrm['WritCancItemAux']

TbDsvPrm = TbDsvPrm[['BusinessTypeCod', 'ProductCod', 'Lags', 
                     'DsnvEndosPrem', 'DsnvCancPrem',
                     'DsnvEndosItm', 'DsnvCancItm']]


#5.0 Baseline SI + Conversion Ratio--------------------------------------------------------------------

# Merge Dataframe with DbQuotes
df_ctc = (df_ctc
#          .groupby(['BusinessTypeCod', 'ProductCod', 'PolicyEffDate'], as_index=False)
#          .sum(['WrittenPremium', 'WritEndorsPremium', 'WritCancPremium', 
#                'WrittenItem', 'WritCancItem', 'SumInsuredAmount', 'Quotes'])
          .rename(columns = {'PolicyEffDate': 'CalendarDate'})
          .drop(['SumInsuredAmount'], axis=1))
df_group = pd.merge(df_group, df_ctc, how='left', on=['BusinessTypeCod', 'ProductCod', 'CalendarDate'])

# Optional fill for missing values
df_group.fillna(0, inplace=True)

# Define date range
date_interval1 =  pd.to_datetime(DateRef) + pd.DateOffset(months=-2)
#date_interval2 =  pd.to_datetime(DateRef) + pd.DateOffset(months=-12)
date_interval2 =  pd.to_datetime(DateRef) + pd.DateOffset(months=-6)

# Filter by date range
df1 = df_group[(df_group['CalendarDate'] >= date_interval1) & (df_group['CalendarDate'] <= DateRef)]
df2 = df_group[(df_group['CalendarDate'] >= date_interval2) & (df_group['CalendarDate'] <= DateRef)]

# Group by attribute(s) and sum metrics
# For flexibility, we keep a list of attributes to group by
df1 = df1.groupby(attributes).sum(numeric_only=True).reset_index()
df1['IS'] = df1['SumInsuredAmount'] / df1['WrittenItem']
df1['TxEmit'] = df1['WrittenPremium'] / df1['SumInsuredAmount']
df1['TxConv'] = df1['WrittenItem'] / df1['Quotes']

df2 = df2.groupby(attributes).sum(numeric_only=True).reset_index()
df2['IS'] = df2['SumInsuredAmount'] / df2['WrittenItem']
df2['TxEmit'] = df2['WrittenPremium'] / df2['SumInsuredAmount']
df2['TxConv'] = df2['WrittenItem'] / df2['Quotes']

TbTxs = df1.copy()
weigth = 0.75
TbTxs['TxEmit'] = (TbTxs['TxEmit'] * weigth) + (df2['TxEmit'] * (1 - weigth))
TbTxs['IS'] = (TbTxs['IS'] * weigth) + (df2['IS'] * (1 - weigth))
TbTxs['TxConv'] = (TbTxs['TxConv'] * weigth) + (df2['TxConv'] * (1 - weigth))

# Tabela Baseline IS + Conversao 
TbTxs = TbTxs[['BusinessTypeCod', 'ProductCod', 
               'TxEmit', 'IS', 'TxConv']]

#6.0 Baseline Retention Ratio--------------------------------------------------------------------

# Db Renovacao
df_renov = (
    df
    .groupby(['ProductCod', 'PolicyEffDate'], as_index=False)[['WrittenItem', 'WritCancItem']]
    .sum()
)

# Lote a Renovar
df_renov['LoteRenov'] = (df_renov['WrittenItem'] - df_renov['WritCancItem']).shift(12)

# Written Renewal
df_aux = (df[df['BusinessTypeCod'] == 2]
          .groupby(['BusinessTypeCod', 'ProductCod', 'PolicyEffDate'], as_index=False)[['WrittenItem']]
          .sum()
          )
df_aux.rename(columns = {'WrittenItem': 'WritRenewal'}, inplace = True)

df_renov = pd.merge(df_renov, df_aux, how = 'left', on=['ProductCod', 'PolicyEffDate'])

# Renewal Ratio (Excluir)
df_renov['RenewalRatio'] = df_renov['WritRenewal'] / df_renov['LoteRenov']

# Define date range
date_interval1 =  pd.to_datetime(DateRef) + pd.DateOffset(months=-3)
date_interval2 =  pd.to_datetime(DateRef) + pd.DateOffset(months=-13)
date_interval3 =  pd.to_datetime(DateRef) + pd.DateOffset(months=-1)

# Filter by date range
df1 = df_renov[(df_renov['PolicyEffDate'] >= date_interval1) & (df_renov['PolicyEffDate'] <= date_interval3)]
df2 = df_renov[(df_renov['PolicyEffDate'] >= date_interval2) & (df_renov['PolicyEffDate'] <= date_interval3)]

# Group by attribute(s) and sum metrics
# For flexibility, we keep a list of attributes to group by
df1 = df1.groupby(attributes).sum(numeric_only=True).reset_index()
df1['RenewalRatio'] = df1['WritRenewal'] / df1['LoteRenov']

df2 = df2.groupby(attributes).sum(numeric_only=True).reset_index()
df2['RenewalRatio'] = df2['WritRenewal'] / df2['LoteRenov']

TbBslRetRatio = df1.copy()
weigth = 0.75
TbBslRetRatio['RenewalRatio'] = (TbBslRetRatio['RenewalRatio'] * weigth) + (df2['RenewalRatio'] * (1 - weigth))
TbBslRetRatio = (TbBslRetRatio
                 .groupby(attributes, as_index=False)['RenewalRatio']
                 .sum(numeric_only=True)
                 )


#8.0 Export Params--------------------------------------------------------------------

##################################
# Create an ParquetWriter object #
##################################
# Alterar diretorio para AWS S3
diroutput = '_data/_03Baselines/_01Prem/'

outputfile = 'BaselinePremium.parquet'
TbBslPrm.reset_index(drop=True, inplace=True)
TbBslPrm.to_parquet(diroutput + outputfile)

outputfile = 'PremiumDevelopment.parquet'
TbDsvPrm.reset_index(drop=True, inplace=True)
TbDsvPrm.to_parquet(diroutput + outputfile)

outputfile = 'BaselineRatios.parquet'
TbTxs.reset_index(drop=True, inplace=True)
TbTxs.to_parquet(diroutput + outputfile)

outputfile = 'RenewalRatio.parquet'
TbBslRetRatio.reset_index(drop=True, inplace=True)
TbBslRetRatio.to_parquet(diroutput + outputfile)

print(f"DataFrames exported to '{outputfile}' successfully.")


diroutput = '_data/_03Baselines/_01Prem/Excel/'
outputfile = 'BaselinePremium.xlsx'
TbBslPrm.reset_index(drop=True, inplace=True)
TbBslPrm.to_excel(diroutput + outputfile)

outputfile = 'PremiumDevelopment.xlsx'
TbDsvPrm.reset_index(drop=True, inplace=True)
TbDsvPrm.to_excel(diroutput + outputfile)

outputfile = 'BaselineRatios.xlsx'
TbTxs.reset_index(drop=True, inplace=True)
TbTxs.to_excel(diroutput + outputfile)

outputfile = 'RenewalRatio.xlsx'
TbBslRetRatio.reset_index(drop=True, inplace=True)
TbBslRetRatio.to_excel(diroutput + outputfile)
