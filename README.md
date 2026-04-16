# Portfolio Simulator
Actuarial Portfolio Assesment Tool

# Introduction & Goals
This project simulates actuarial loss trends and frequency/severity distributions.

Using **Python** and **Streamlit**, we process insurance datasets to evaluate risk exposure and forecast future outcomes.
+ **Objective:** Provide a real-time dashboard for portfolio health monitoring.
    + **Data:** Synthetic actuarial claims and earned premium data.
    + **Tools:** Pandas for processing, Plotly for visualization.
    + **Conclusion:**
        + This is an end-to-end project. From data gathering up to live visualizations on streamlit.
        + The biggest challenges was to replicate renewal batches coming from open business in the books (some w probability to face cancellations).
        + Applying Polynomial Regessions was a huge boost in development and better stabelizes Frequency and Severity for all coverages.
        + Overall this project turn out to be a good decision tool for stakeholders the way it's currently built.

# Contents

- [The Data Set](#the-data-set)
- [Used Tools](#used-tools)
- [Pipelines](#pipelines)
  - [Assumptions]
  - [Projections]
  - [Suggested Ideas for Future Improvements]
  - [Visualizations](#visualizations)
- [Conclusion](#conclusion)
- [Follow Me On](#follow-me-on)
- [Appendix](#appendix)


# The Data Set
**Db_Prem** is a long-format actuarial accounting / production dataset combining:
- Premium accounting flows (issued, endorsedm cancelled)
- Earned / accounting premiums (exposed-adjusted)
- Item counts and exposure measures
- Time development (lags) between issue and accounting reference
- Breakdown by product and business type

This dataset is well suited for: premium triangles, earned premium calculations, exposure-based frequency and severity analysis and LTV/pricing/portfolio analysis

**Raw Premium movements**
- **WrittenPremium**: Issued premium
- **WritEndorsPremium**: Endorsement premium (positive or negative)
- **WritCancPremium**: Cancelled premium
- **GWP (Gross Written Premium)**: GWP = PremEmit + PremEndos + PremCanc

These values are transactional, aligned with policy lifecycle events.


**Earned or exposure-adjusted premium recognition**
- **EmitExp**
- **EndorsExp**
- **CancExp**
- **EarnPrem**: Calculated Earned Premium

These are consistent with **pro-rata temporis earning or accounting rules**, distributing written premium over coverage duration.
Allowing Earned Premium triangles, ratio analysis (loss / expense over earned), etc.


**Item counts (policy / exposure units)**
- **WrittenItem**: Issued items (policies, risks)
- **WritEndorsItem**: Endorsements counts
- **WritCancItem**: Cancelled itens
- **Net_Item**: Net Items

These metrics are critical for Frequency modeling, policy retention, average premium per item.


**Exposure measures (actuarial core)**
- **ExposicaoEmit**: Issued items (policies, risks)
- **ExposicaoEndors**: Endorsements counts
- **ExposicaoCanc**: Cancelled itens
- **Exposure**: Net Exposure

These metrics are fractional, indicating partial-year exposure, midterm=endorsements and cancellations, time-weigthed risk contribution.

This is ideal for, Pure Premium modeling, rate adequacy studies.


**Db_Sns** is a long format triangle with botjh claims-type and monetary-type measures.
This table is a cohort-by-development (issue x lag) transactional ledger, typicel of insurance claim development datasets used for pricing, reserving and profitability analysis.

**Claims measures**
- **QtdPP**: Phhysical Damage Partial Loss Claims
- **QtdPT**: Phhysical Damage Total Loss Claims
- **QtdRbft**: Theft Loss Claims
- **Qtd**: Total Claims

These fields represents materialization of risk in the portfolio.


**Financial movements (claim cashflows)**
- **VlrIndPP**: Phhysical Damage Partial Loss Amounts
- **VlrIndPT**: Phhysical Damage Total Loss Amounts
- **VlrIndRbft**: Theft Loss Amounts
- **VlrInd**: Total Loss Amounts

These fields represents **monetary flows by coverage type**, recorded at each lag.
Values can be positive and negative according to refunds/corrections. 


**Recovery / offset components**
- **VlrSalvados**: Salvage value recovered
- **QtdSalvados**: Salvage count
- **VlrRess**: Subrogation value recovered
- **QtdRess**: Subrogation count

These fields appears **only when losses exists**.



**Data structure and granularity**
Each row represents (Product x Business Type x Issued Period x Accounting Period). 
This is a **multi-valuation long table**.

**Characteristics**
- Positive and negative premiums -> endorsements & cancellations
- Exposure can be negative at sub-component level but balances to net exposure
- Item counts align consistently with exposure direction

**Dimensions**
a) **Attributes segmentation**
- **ProductID**: Product code.
- **BusinessID**: Business Type code.

b) **Time Dimensions** 
This dataset uses multiple time axes, which is typical of actuarial datasets
- **DtEmissao**: Issued Date, stored as serial date. Represents the **origin period** (underwriting month).
- **DtRefCtb**: Accounting reference date. Represents the **valuation/accounting period**.

# 🛠️ Used Tools
+ Python
    + The core programming language for the simulator. It provides flexibility, readability, and a huge ecosystem of libraries for data analysis and visualization.

+ Pandas
    + Used for handling tabular data (CSV, Excel, Parquet). It makes grouping, filtering, and aggregating insurance data straightforward.
    + Reads and structures data into DataFrames, enabling operations like groupby, merge, and sum.
    + Essential for actuarial work where large datasets (premiums, claims, exposures) need efficient manipulation.

+ Numpy
    + Provides efficient numerical operations, especially for vectorized calculations and handling large datasets.
    + Handles arrays and mathematical operations behind the scenes, making calculations faster.

+ Plotly
    + Used for plotting trends, frequencies, severities, and other actuarial metrics. It allows customization of charts for professional reporting.
    + Translates numerical results into visual graphs (line charts, bar charts, scatter plots).
    + Provides precise control over actuarial charts, which are often regulatory or board‑facing.

+ Streamlit
    + Powers the interactive dashboard/web app. It lets you share results and insights with stakeholders in a user‑friendly way without needing complex web development.
    + Wraps your Python scripts into a web interface, displaying tables, charts, and interactive widgets directly in the browser.
    + Quick deployment of dashboards without needing HTML/JS — perfect for sharing projections with non‑technical stakeholders.


# Pipelines
## Assumptions (PremParam & SinisParam)
+ **PremParam (Premium Parameters)**:
    + Cleans and imports premium and quotation data.
    + Establishes baselines for endorsements and cancellations using weighted averages (75% short-term, 25% long-term.
    + Models lags (delays) in endorsements/cancellations to capture timing effects.
    + Calculates conversion rates, average sum insured, and emission rates.
    + Defines retention/renewal ratios to project future portfolio behavior.

+ **SinisParam (Claims Parameters)**:
    + Builds a complete exposure grid to avoid missing data.
    + Applies pro-rata exposure factors to premiums, endorsements, and cancellations.
    + Calculates earned premium and exposure adjusted for time at risk.
    + Develops frequency and severity baselines using credibility weighting (70% recent, 30% historical).
    + Models salvage and recovery timing and percentages.
    + Applies regression (linear and polynomial) to identify trends in frequency/severity.
    + Introduces “Trended” adjustments to normalize historical costs to current levels.
    + Produces robust baselines for frequency, severity, and recovery.


## Projections
- **Elasticity Modeling**: Captures how conversion and retention rates react to price changes.
- **Scenario Loop (Monte Carlo)**: Tests tariff variations between -5% and +5% to measure sensitivity.
- **Renewal Dynamics**: Projects future renewals based on past emissions and retention elasticity.
- **Macroeconomic Adjustments**: Applies inflation factors to partial and total loss severities.
- **Anti-Selection Effects**: Models how pricing changes alter risk composition (good vs. bad risks).
- **Final Outputs**: Projects number and value of claims, adjusted for exposure, elasticity, and inflation.


## 💡 **Suggested Ideas for Future Improvements**
- **Automation**: Develop automated ETL pipelines to reduce manual data imports (especially HDI inputs).
- **Visualization**: Add dashboards for scenario comparison (elasticity curves, renewal ratios, claim projections).
- **Machine Learning**: Explore ML models for non-linear claim frequency/severity trends beyond polynomial regression.
- **Stress Testing**: Incorporate extreme scenarios (economic shocks, regulatory changes) to test portfolio resilience.
- **Continuous Feedback Loop**: Integrate analyst feedback directly into the simulator for iterative model refinement.


## Visualizations
- **Link**: [Streamlit](https://actuarialportfoliosimulator.streamlit.app/).


# Conclusion
- This is an end-to-end project. From data gathering up to live visualizations on streamlit.
- The biggest challenges was to replicate renewal batches coming from open business in the books (some w probability to face cancellations) + future expected new business coming from applied elasticity conversion.
- Applying Polynomial Regessions was a huge boost in development and better stabelizes Frequency and Severity for all coverages.
- There is still room to grow. Input roadside assistance and apply baselines, project commision values and add others expenses to build up a future Combined Ratio.
- Overall this project turn out to be a good decision tool for stakeholders the way it's currently built.
- It took different packages and applied actuarial/economic logics, that made me grew in terms of business knowledge and coding skills. 

# Follow Me On
[LinkedIn Profile](https://www.linkedin.com/in/atuario-vinicius-almeida/?locale=en)

# Appendix
- Exposicao Atuarrial. [Loss Trends for Frequency & Severity](https://exposicaoatuarial.wordpress.com/2026/02/16/loss-trends-for-frequency-severity/)
- Exposicao Atuarrial. [CAS Basic Ratemaking Earned Premium & Exposure](https://exposicaoatuarial.wordpress.com/2026/02/16/cas-basic-ratemaking-earned-premium-exposure/)
