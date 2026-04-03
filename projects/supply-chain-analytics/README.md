# Supply Chain Analytics

End-to-end supply chain visibility pipeline for a global FMCG company.

## Data Sources
- **ERP**: Purchase orders from 6 suppliers
- **WMS**: Warehouse movements (receipts, picks, shipments)
- **TMS**: Transport/shipment tracking events
- **POS**: Point-of-sale demand signals from 8 stores

## Architecture
Split-file medallion pattern — each transformation is its own SQL file.

### Bronze (Raw Ingestion)
- Purchase orders, warehouse movements, transport events, POS demand, reference data

### Silver (Cleansed & Enriched)
- Inventory positions (MERGE on warehouse x SKU)
- Order fulfillment (fill rate, lead time)
- Shipment tracking (dedup, transit status)
- Demand signals (7-day moving avg, stockout risk)

### Gold (Star Schema + KPIs)
- Dimensions: supplier, product, warehouse, store
- Facts: inventory snapshot, order lifecycle
- KPIs: supplier scorecard, inventory health, demand forecast

## Running
Execute `pipeline.sql` to run the full DAG, or run any individual file for isolated testing.

## Schedule
Daily at 04:00 UTC with 2 retries and a 2-hour SLA.
