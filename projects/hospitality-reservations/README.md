# Hospitality Reservations Pipeline

## Scenario

A hotel management company operates 4 properties across the US (Miami 5-star resort, Aspen lodge, Chicago city center, San Diego coastal inn) under 3 brands. Reservations flow from 4 channels: direct website, OTA (BookNow.com), corporate program, and travel agents -- each with different commission structures. The pipeline handles duplicate booking attempts (MERGE dedup), soft deletes for cancellations (is_cancelled flag preserves row), no-shows, extended stays, and loyalty member segmentation. Revenue management KPIs include RevPAR, ADR, occupancy rates, cancellation analysis by lead time and channel, and loyalty program impact.

## Star Schema

```
                    +----------------+
                    | dim_guest      |
                    |----------------|
                    | guest_key      |<------+
                    | name           |       |
                    | loyalty_tier   |       |
                    | lifetime_stays |       |
                    +----------------+       |
                                             |
+----------------+  +---------------------+  |    +----------------+
| dim_room_type  |  | fact_reservations   |  |    | dim_channel    |
|----------------|  |---------------------|  |    |----------------|
| room_type_key  |<-| reservation_key     |  +--->| channel_key    |
| room_type      |  | property_key     FK |       | channel_name   |
| base_rate      |  | room_type_key    FK |       | commission_pct |
| amenities      |  | guest_key        FK |       +----------------+
| view_type      |  | channel_key      FK |
+----------------+  | check_in_date      |
                    | check_out_date     |    +------------------+
                    | nights             |    | dim_property     |
                    | room_rate          |    |------------------|
                    | total_amount       |--->| property_key     |
                    | status             |    | property_name    |
                    | booking_lead_days  |    | brand            |
                    | is_cancelled       |    | total_rooms      |
                    +---------------------+   | avg_rack_rate    |
                                              +------------------+
                    +---------------------------+
                    | kpi_revenue_management    |
                    |---------------------------|
                    | property_id, month        |
                    | occupancy_rate, adr       |
                    | revpar, total_revenue     |
                    | cancellation_rate         |
                    | direct_booking_pct        |
                    | loyalty_revenue_pct       |
                    +---------------------------+
```

## Medallion Flow

```
BRONZE                      SILVER                          GOLD
+-----------------+    +-------------------------+    +---------------------+
| raw_bookings    |--->| bookings_deduped        |--->| fact_reservations   |
| (65+ bookings)  |    | (dedup, soft delete)    |    | dim_property        |
+-----------------+    | (nights, lead days)     |    | dim_room_type       |
| raw_properties  |    +-------------------------+    | dim_guest           |
+-----------------+                                   | dim_channel         |
| raw_room_types  |                                   | kpi_revenue_mgmt   |
+-----------------+                                   +---------------------+
| raw_guests      |
+-----------------+
| raw_channels    |
+-----------------+
```

## Features

- **Soft delete**: Cancelled reservations marked is_cancelled=true, row preserved for analysis
- **MERGE dedup**: Duplicate booking attempts (is_duplicate=true) filtered during silver processing
- **RevPAR**: Revenue Per Available Room = Total Revenue / (Rooms * Days)
- **ADR**: Average Daily Rate = Revenue / Room Nights Sold
- **Window functions**: Running occupancy trends, cumulative revenue analysis
- **Cancellation analysis**: By lead time bucket and channel for yield management
- **Loyalty impact**: Revenue contribution from Gold/Platinum tier members
- **Channel mix**: Commission cost analysis and direct booking percentage
- **Pseudonymisation**: MASK on guest name, REDACT on credit_card_last4

## Seed Data

- **4 properties**: Miami (5-star), Aspen (4-star), Chicago (4-star), San Diego (3-star)
- **5 room types**: Standard King, Deluxe Double, Executive Suite, Penthouse Suite, Family Room
- **18 guests**: 3 Platinum, 4 Gold, 4 Silver, 5 Bronze tier across 8 countries
- **4 channels**: Direct (0% commission), OTA (18%), Corporate (5%), Travel Agent (12%)
- **65 bookings** across Jan-Apr 2024 with cancellations, no-shows, extended stays, duplicates

## Verification Checklist

- [ ] All 4 properties in dim_property
- [ ] All 18 guests in dim_guest with loyalty tiers
- [ ] 5 room types in dim_room_type
- [ ] Duplicate bookings (BK-009-DUP, BK-029-DUP) excluded from silver
- [ ] Cancelled bookings preserved with is_cancelled=true (soft delete)
- [ ] RevPAR and ADR calculated in kpi_revenue_management
- [ ] Channel commission impact visible in channel mix analysis
- [ ] Loyalty tier revenue contribution tracked
- [ ] Incremental load adds May bookings via INCREMENTAL_FILTER
