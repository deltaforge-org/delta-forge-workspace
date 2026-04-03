# Nonprofit Donations Pipeline

## Scenario

A nonprofit organization tracks donations from 15 donors (individuals, corporations, foundations) across 5 campaigns (annual giving, spring gala, year-end appeal, education program, capital campaign) directed to 4 designated funds (general operating, education programs, building fund, emergency relief). The pipeline calculates donor tiers based on lifetime giving (Bronze < $500, Silver $500-$2K, Gold $2K-$10K, Platinum > $10K), tracks tier promotions and demotions via Change Data Feed (CDF), identifies retained vs lapsed donors, and measures campaign effectiveness including goal attainment and cost per dollar raised. Time travel enables viewing donor tier status at any historical point.

## Star Schema

```
                    +----------------+
                    | dim_donor      |
                    |----------------|
                    | donor_key      |<------+
                    | name           |       |
                    | donor_type     |       |
                    | current_tier   |       |
                    | lifetime_amount|       |
                    +----------------+       |
                                             |
+----------------+  +-------------------+    |    +----------------+
| dim_campaign   |  | fact_donations    |    |    | dim_fund       |
|----------------|  |-------------------|    |    |----------------|
| campaign_key   |<-| donation_key      |    +--->| fund_key       |
| campaign_name  |  | donor_key      FK |         | fund_name      |
| campaign_type  |  | campaign_key   FK |         | fund_type      |
| goal_amount    |  | fund_key       FK |         | restricted_flag|
| channel        |  | donation_date     |         | purpose        |
+----------------+  | amount            |         +----------------+
                    | payment_method    |
                    | is_recurring      |
                    | tax_deductible    |
                    +-------------------+
                              |
                    +---------+-----------+
                    | kpi_fundraising     |
                    |---------------------|
                    | campaign_id         |
                    | fund_name, period   |
                    | total_donations     |
                    | goal_attainment_pct |
                    | donor_retention_rate|
                    | cost_per_dollar     |
                    | major_gift_count    |
                    +---------------------+
```

## Medallion Flow

```
BRONZE                      SILVER                          GOLD
+-----------------+    +-------------------------+    +---------------------+
| raw_donations   |--->| donors_tiered (CDF)     |--->| fact_donations      |
| (60+ donations) |    | (tier calc, retention)  |    | dim_donor           |
+-----------------+    +-------------------------+    | dim_campaign        |
| raw_donors      |--->| donations_enriched      |--->| dim_fund            |
+-----------------+    | (tier join, major gift)  |    | kpi_fundraising    |
| raw_campaigns   |    +-------------------------+    +---------------------+
+-----------------+
| raw_funds       |
+-----------------+
```

## Features

- **Change Data Feed (CDF)**: Enabled on silver.donors_tiered to track tier promotions/demotions
- **Donor tier calculation**: Bronze/Silver/Gold/Platinum based on lifetime giving thresholds
- **CHECK constraints**: Amount > 0, valid campaign date ranges
- **Time travel**: View donor tier status at any historical version
- **Campaign effectiveness**: Goal attainment %, cost per dollar raised
- **Donor retention**: Year-over-year retention using LAG window function
- **Recurring vs one-time**: Revenue mix analysis by period
- **Major gift tracking**: Donations >= $10,000 flagged and counted
- **Lapse detection**: Donors with no activity in 12+ months identified
- **Pseudonymisation**: MASK on donor name and email

## Seed Data

- **15 donors**: 9 individuals, 3 foundations, 3 corporations
- **5 campaigns**: Annual Giving, Spring Gala, Year-End Appeal, Education Access, Capital Campaign
- **4 funds**: General Operating (unrestricted), Education Programs, Building Fund, Emergency Relief (all restricted)
- **60 donations** spanning Jan 2023 - Jun 2024 with recurring monthly gifts, major gifts (>$10K), and lapsed donors

## Verification Checklist

- [ ] All 15 donors in dim_donor with calculated tiers
- [ ] All 5 campaigns in dim_campaign with goal amounts
- [ ] All 4 funds in dim_fund with restriction flags
- [ ] Tier distribution: mix of Bronze, Silver, Gold, Platinum
- [ ] Campaign goal attainment calculated (Capital Campaign should be highest total)
- [ ] Donor retention rate computed year-over-year
- [ ] Recurring vs one-time revenue trends visible
- [ ] Major gifts (>= $10K) identified and counted
- [ ] CDF captures tier changes on donors_tiered
- [ ] Time travel shows previous tier status
- [ ] Incremental load adds July donations via INCREMENTAL_FILTER
