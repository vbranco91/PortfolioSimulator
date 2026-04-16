# Portfolio Simulator
Actuarial Portfolio Assesment Tool

# Introduction & Goals
This project simulates actuarial loss trends and frequency/severity distributions.

Using **Python** and **Streamlit**, we process insurance datasets to evaluate risk exposure and forecast future outcomes.
+ **Objective:** Provide a real-time dashboard for portfolio health monitoring.
    + **Data:** Synthetic actuarial claims and earned premium data.
    + **Tools:** Pandas for processing, Plotly for visualization.
    + **Conclusion:** In Progress.  

# Contents

- [The Data Set](#the-data-set)
- [Used Tools](#used-tools)
  - [Connect](#connect)
  - [Buffer](#buffer)
  - [Processing](#processing)
  - [Storage](#storage)
- [Pipelines](#pipelines)
  - [Assumptions](## Assumptions (PremParam & SinisParam))
  - [Projections](## Projections)
  - [Projections](## Projections)
  - [Visualizations](#visualizations)
- [Demo](#demo)
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
- **PremEmit**: Issued premium
- **PremEndos**: Endorsement premium (positive or negative)
- **PremCanc**: Cancelled premium
- **GWP (Gross Written Premium)**: GWP = PremEmit + PremEndos + PremCanc
These values are transactional, aligned with policy lifecycle events.

**Earned or exposure-adjusted premium recognition**
- **EmitExp**
- **EndorsExp**
- **CancExp**
- **PGCalc**: Calculated Earned Premium
These are consistent with **pro-rata temporis earning or accounting rules**, distributing written premium over coverage duration.
Allowing Earned Premium triangles, ratio analysis (loss / expense over earned), etc.

**Item counts (policy / exposure units)**
- **ItensEmit**: Issued items (policies, risks)
- **ItensEndors**: Endorsements counts
- **ItensCanc**: Cancelled itens
- **Itens Net**: Net Items
These metrics are critical for Frequency modeling, policy retention, average premium per item.

**Exposure measures (actuarial core)**
- **ExposicaoEmit**: Issued items (policies, risks)
- **ExposicaoEndors**: Endorsements counts
- **ExposicaoCanc**: Cancelled itens
- **Exposicao**: Net Exposure
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
- Explain which tools do you use and why
- Python (Pandas, Numpy, Matplotlib, Streamlit)
- How do they work (don't go too deep into details, but add links)
- Why did you choose them
- How did you set them up


# Pipelines
## Assumptions (PremParam & SinisParam)
- **PremParam (Premium Parameters)**:
-     Cleans and imports premium and quotation data.
-     Establishes baselines for endorsements and cancellations using weighted averages (75% short-term, 25% long-term).
-     Models lags (delays) in endorsements/cancellations to capture timing effects.
-     Calculates conversion rates, average sum insured, and emission rates.
-     Defines retention/renewal ratios to project future portfolio behavior.

- **SinisParam (Claims Parameters)**:
-     Builds a complete exposure grid to avoid missing data.
-     Applies pro-rata exposure factors to premiums, endorsements, and cancellations.
-     Calculates earned premium and exposure adjusted for time at risk.
-     Develops frequency and severity baselines using credibility weighting (70% recent, 30% historical).
-     Models salvage and recovery timing and percentages.
-     Applies regression (linear and polynomial) to identify trends in frequency/severity.
-     Introduces “Trended” adjustments to normalize historical costs to current levels.
-     Produces robust baselines for frequency, severity, and recovery.


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
- **Link**: Streamlit.


# Conclusion
Write a comprehensive conclusion.
- How did this project turn out
- What major things have you learned
- What were the biggest challenges

# Follow Me On
[LinkedIn Profile](https://www.linkedin.com/in/atuario-vinicius-almeida/?locale=en)

# Appendix
- Exposicao Atuarrial. [Loss Trends for Frequency & Severity](https://exposicaoatuarial.wordpress.com/2026/02/16/loss-trends-for-frequency-severity/)
- Exposicao Atuarrial. [CAS Basic Ratemaking Earned Premium & Exposure](https://exposicaoatuarial.wordpress.com/2026/02/16/cas-basic-ratemaking-earned-premium-exposure/)
