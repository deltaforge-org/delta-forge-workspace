-- =============================================================================
-- Logistics Shipments Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================
-- Event sourcing pipeline with idempotent composite-key deduplication,
-- shipment timeline reconstruction, SLA violation detection, and
-- Z-ordered route optimization.
-- =============================================================================

SCHEDULE logistics_6hr_schedule
    CRON '0 */6 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE logistics_shipments_pipeline
    DESCRIPTION 'Global logistics event sourcing pipeline: idempotent dedup, timeline reconstruction, SLA compliance, route optimization'
    SCHEDULE 'logistics_6hr_schedule'
    TAGS 'logistics,event-sourcing,SLA,Z-order'
    SLA 60
    FAIL_FAST true
    LIFECYCLE production;

-- ===================== STEP 1: validate_bronze =====================
-- Assert that all bronze reference tables have expected row counts
-- before beginning any transformations.

STEP validate_bronze
  TIMEOUT '2m'
AS
  ASSERT ROW_COUNT = 8
  SELECT COUNT(*) AS carrier_count FROM logi.bronze.raw_carriers;

  ASSERT ROW_COUNT = 15
  SELECT COUNT(*) AS location_count FROM logi.bronze.raw_locations;

  ASSERT ROW_COUNT = 20
  SELECT COUNT(*) AS customer_count FROM logi.bronze.raw_customers;

  ASSERT ROW_COUNT = 8
  SELECT COUNT(*) AS sla_count FROM logi.bronze.raw_sla_contracts;

  ASSERT ROW_COUNT = 80
  SELECT COUNT(*) AS event_count FROM logi.bronze.raw_tracking_events;

-- ===================== STEP 2: dedup_events =====================
-- Idempotent composite-key MERGE: same (shipment_id + event_type +
-- event_timestamp) arriving multiple times produces exactly one row.
-- WHEN MATCHED: do nothing (idempotent — no UPDATE needed).
-- This is the core event sourcing dedup pattern.

STEP dedup_events
  DEPENDS ON (validate_bronze)
  TIMEOUT '5m'
AS
  MERGE INTO logi.silver.events_deduped AS target
  USING (
      SELECT
          event_id,
          shipment_id,
          customer_id,
          carrier_id,
          origin_id,
          destination_id,
          service_level,
          event_type,
          event_timestamp,
          ship_date,
          promised_date,
          delivery_date,
          weight_kg,
          volume_m3,
          cost,
          revenue
      FROM logi.bronze.raw_tracking_events
  ) AS source
  ON  target.shipment_id     = source.shipment_id
  AND target.event_type      = source.event_type
  AND target.event_timestamp = source.event_timestamp
  WHEN NOT MATCHED THEN INSERT (
      event_id, shipment_id, customer_id, carrier_id, origin_id,
      destination_id, service_level, event_type, event_timestamp,
      ship_date, promised_date, delivery_date, weight_kg, volume_m3,
      cost, revenue, deduped_at
  ) VALUES (
      source.event_id, source.shipment_id, source.customer_id,
      source.carrier_id, source.origin_id, source.destination_id,
      source.service_level, source.event_type, source.event_timestamp,
      source.ship_date, source.promised_date, source.delivery_date,
      source.weight_kg, source.volume_m3, source.cost, source.revenue,
      CURRENT_TIMESTAMP
  );

-- ===================== STEP 3: reconstruct_timelines =====================
-- Reconstruct shipment status from deduplicated events using window
-- functions: ROW_NUMBER for latest status, LAG for previous status
-- and time-between-events, full lifecycle metrics per shipment.

STEP reconstruct_timelines
  DEPENDS ON (dedup_events)
  TIMEOUT '5m'
AS
  MERGE INTO logi.silver.shipment_status AS tgt
  USING (
      WITH ordered_events AS (
          SELECT
              shipment_id,
              customer_id,
              carrier_id,
              origin_id,
              destination_id,
              service_level,
              event_type,
              event_timestamp,
              ship_date,
              promised_date,
              delivery_date,
              weight_kg,
              volume_m3,
              cost,
              revenue,
              ROW_NUMBER() OVER (
                  PARTITION BY shipment_id ORDER BY event_timestamp DESC
              ) AS rn_desc,
              ROW_NUMBER() OVER (
                  PARTITION BY shipment_id ORDER BY event_timestamp ASC
              ) AS rn_asc,
              LAG(event_type) OVER (
                  PARTITION BY shipment_id ORDER BY event_timestamp DESC
              ) AS next_newer_status,
              LAG(event_timestamp) OVER (
                  PARTITION BY shipment_id ORDER BY event_timestamp
              ) AS prev_event_time,
              COUNT(*) OVER (PARTITION BY shipment_id) AS event_count,
              MIN(event_timestamp) OVER (PARTITION BY shipment_id) AS first_event_time,
              MAX(event_timestamp) OVER (PARTITION BY shipment_id) AS last_event_time,
              MAX(delivery_date) OVER (PARTITION BY shipment_id) AS final_delivery_date
          FROM logi.silver.events_deduped
      ),
      latest AS (
          SELECT
              shipment_id,
              customer_id,
              carrier_id,
              origin_id,
              destination_id,
              service_level,
              ship_date,
              promised_date,
              final_delivery_date AS delivery_date,
              event_type AS latest_status,
              next_newer_status AS previous_status,
              event_count,
              CAST(DATEDIFF(event_timestamp, prev_event_time) * 24 AS DECIMAL(8,2)) AS hours_in_last_stage,
              CAST(DATEDIFF(last_event_time, first_event_time) * 24 AS DECIMAL(8,2)) AS total_transit_hours,
              CASE
                  WHEN final_delivery_date IS NOT NULL
                  THEN CAST(DATEDIFF(final_delivery_date, ship_date) AS INT)
                  ELSE NULL
              END AS transit_days,
              CASE
                  WHEN final_delivery_date IS NOT NULL AND final_delivery_date <= promised_date THEN true
                  WHEN final_delivery_date IS NOT NULL AND final_delivery_date > promised_date  THEN false
                  ELSE NULL
              END AS on_time_flag,
              weight_kg,
              volume_m3,
              cost,
              revenue,
              first_event_time,
              last_event_time
          FROM ordered_events
          WHERE rn_desc = 1
      )
      SELECT * FROM latest
  ) AS src
  ON tgt.shipment_id = src.shipment_id
  WHEN MATCHED AND src.last_event_time > tgt.last_event_time THEN UPDATE SET
      tgt.latest_status       = src.latest_status,
      tgt.previous_status     = src.previous_status,
      tgt.delivery_date       = src.delivery_date,
      tgt.transit_days        = src.transit_days,
      tgt.on_time_flag        = src.on_time_flag,
      tgt.event_count         = src.event_count,
      tgt.hours_in_last_stage = src.hours_in_last_stage,
      tgt.total_transit_hours = src.total_transit_hours,
      tgt.last_event_time     = src.last_event_time,
      tgt.reconstructed_at    = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      shipment_id, customer_id, carrier_id, origin_id, destination_id,
      service_level, ship_date, promised_date, delivery_date, latest_status,
      previous_status, event_count, hours_in_last_stage, total_transit_hours,
      transit_days, on_time_flag, weight_kg, volume_m3, cost, revenue,
      first_event_time, last_event_time, reconstructed_at
  ) VALUES (
      src.shipment_id, src.customer_id, src.carrier_id, src.origin_id,
      src.destination_id, src.service_level, src.ship_date, src.promised_date,
      src.delivery_date, src.latest_status, src.previous_status, src.event_count,
      src.hours_in_last_stage, src.total_transit_hours, src.transit_days,
      src.on_time_flag, src.weight_kg, src.volume_m3, src.cost, src.revenue,
      src.first_event_time, src.last_event_time, CURRENT_TIMESTAMP
  );

-- ===================== STEP 4: detect_sla_violations =====================
-- Compare actual transit days against SLA contract max_transit_days.
-- Compute penalty exposure. Runs parallel with reconstruct_timelines.

STEP detect_sla_violations
  DEPENDS ON (dedup_events)
  TIMEOUT '3m'
AS
  MERGE INTO logi.silver.sla_violations AS tgt
  USING (
      WITH shipment_transit AS (
          SELECT
              shipment_id,
              customer_id,
              carrier_id,
              origin_id,
              destination_id,
              service_level,
              ship_date,
              MAX(delivery_date) AS delivery_date,
              CASE
                  WHEN MAX(delivery_date) IS NOT NULL
                  THEN CAST(DATEDIFF(MAX(delivery_date), ship_date) AS INT)
                  ELSE NULL
              END AS actual_transit_days
          FROM logi.silver.events_deduped
          GROUP BY shipment_id, customer_id, carrier_id, origin_id,
                   destination_id, service_level, ship_date
      )
      SELECT
          CONCAT(st.shipment_id, '-', st.carrier_id) AS violation_id,
          st.shipment_id,
          st.carrier_id,
          st.service_level,
          sla.max_transit_days AS sla_max_days,
          st.actual_transit_days,
          CASE
              WHEN st.actual_transit_days > sla.max_transit_days
              THEN st.actual_transit_days - sla.max_transit_days
              ELSE 0
          END AS days_over_sla,
          CASE
              WHEN st.actual_transit_days > sla.max_transit_days
              THEN (st.actual_transit_days - sla.max_transit_days) * sla.penalty_per_day
              ELSE 0
          END AS penalty_amount,
          st.customer_id,
          st.origin_id,
          st.destination_id,
          CASE
              WHEN st.actual_transit_days > sla.max_transit_days THEN true
              ELSE false
          END AS sla_violated
      FROM shipment_transit st
      JOIN logi.bronze.raw_sla_contracts sla
          ON st.carrier_id = sla.carrier_id
          AND st.service_level = sla.service_level
      WHERE st.actual_transit_days IS NOT NULL
  ) AS src
  ON tgt.violation_id = src.violation_id
  WHEN MATCHED THEN UPDATE SET
      tgt.actual_transit_days = src.actual_transit_days,
      tgt.days_over_sla       = src.days_over_sla,
      tgt.penalty_amount      = src.penalty_amount,
      tgt.sla_violated        = src.sla_violated,
      tgt.detected_at         = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      violation_id, shipment_id, carrier_id, service_level, sla_max_days,
      actual_transit_days, days_over_sla, penalty_amount, customer_id,
      origin_id, destination_id, sla_violated, detected_at
  ) VALUES (
      src.violation_id, src.shipment_id, src.carrier_id, src.service_level,
      src.sla_max_days, src.actual_transit_days, src.days_over_sla,
      src.penalty_amount, src.customer_id, src.origin_id, src.destination_id,
      src.sla_violated, CURRENT_TIMESTAMP
  );

-- ===================== STEP 5: build_dim_carrier =====================

STEP build_dim_carrier
  DEPENDS ON (reconstruct_timelines, detect_sla_violations)
AS
  MERGE INTO logi.gold.dim_carrier AS tgt
  USING (
      SELECT
          carrier_id     AS carrier_key,
          carrier_name,
          carrier_type,
          fleet_size,
          headquarters,
          on_time_rating,
          cost_per_kg
      FROM logi.bronze.raw_carriers
  ) AS src
  ON tgt.carrier_key = src.carrier_key
  WHEN MATCHED THEN UPDATE SET
      tgt.on_time_rating = src.on_time_rating,
      tgt.cost_per_kg    = src.cost_per_kg,
      tgt.headquarters   = src.headquarters,
      tgt.loaded_at      = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      carrier_key, carrier_name, carrier_type, fleet_size, headquarters,
      on_time_rating, cost_per_kg, loaded_at
  ) VALUES (
      src.carrier_key, src.carrier_name, src.carrier_type, src.fleet_size,
      src.headquarters, src.on_time_rating, src.cost_per_kg, CURRENT_TIMESTAMP
  );

-- ===================== STEP 6: build_dim_location =====================

STEP build_dim_location
  DEPENDS ON (reconstruct_timelines, detect_sla_violations)
AS
  MERGE INTO logi.gold.dim_location AS tgt
  USING (
      SELECT
          location_id    AS location_key,
          hub_name,
          city,
          state,
          country,
          region,
          hub_type,
          latitude,
          longitude
      FROM logi.bronze.raw_locations
  ) AS src
  ON tgt.location_key = src.location_key
  WHEN MATCHED THEN UPDATE SET
      tgt.hub_name  = src.hub_name,
      tgt.hub_type  = src.hub_type,
      tgt.loaded_at = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      location_key, hub_name, city, state, country, region, hub_type,
      latitude, longitude, loaded_at
  ) VALUES (
      src.location_key, src.hub_name, src.city, src.state, src.country,
      src.region, src.hub_type, src.latitude, src.longitude, CURRENT_TIMESTAMP
  );

-- ===================== STEP 7: build_dim_customer =====================

STEP build_dim_customer
  DEPENDS ON (reconstruct_timelines, detect_sla_violations)
AS
  MERGE INTO logi.gold.dim_customer AS tgt
  USING (
      SELECT
          customer_id    AS customer_key,
          customer_name,
          tier,
          industry,
          city,
          country,
          account_manager
      FROM logi.bronze.raw_customers
  ) AS src
  ON tgt.customer_key = src.customer_key
  WHEN MATCHED THEN UPDATE SET
      tgt.tier            = src.tier,
      tgt.account_manager = src.account_manager,
      tgt.loaded_at       = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      customer_key, customer_name, tier, industry, city, country,
      account_manager, loaded_at
  ) VALUES (
      src.customer_key, src.customer_name, src.tier, src.industry, src.city,
      src.country, src.account_manager, CURRENT_TIMESTAMP
  );

-- ===================== STEP 8: build_dim_route =====================
-- Derived dimension: origin->dest pairs with Haversine distance,
-- average transit time, shipment volume, and primary transport mode.

STEP build_dim_route
  DEPENDS ON (build_dim_location)
  TIMEOUT '3m'
AS
  MERGE INTO logi.gold.dim_route AS tgt
  USING (
      SELECT
          s.origin_id || '->' || s.destination_id AS route_key,
          ol.hub_name AS origin_hub,
          ol.city AS origin_city,
          dl.hub_name AS destination_hub,
          dl.city AS destination_city,
          CAST(
              111.0 * SQRT(
                  POWER(ol.latitude - dl.latitude, 2) +
                  POWER((ol.longitude - dl.longitude) * COS(RADIANS((ol.latitude + dl.latitude) / 2)), 2)
              )
          AS INT) AS distance_km,
          CAST(AVG(s.transit_days) AS DECIMAL(5,1)) AS avg_transit_days,
          COUNT(*) AS shipment_count,
          CASE
              WHEN c.carrier_type = 'Air' THEN 'Air'
              WHEN c.carrier_type IN ('Ocean', 'ocean') THEN 'Ocean'
              WHEN c.carrier_type = 'Rail' THEN 'Rail'
              ELSE 'Road'
          END AS primary_mode
      FROM logi.silver.shipment_status s
      JOIN logi.bronze.raw_locations ol ON s.origin_id = ol.location_id
      JOIN logi.bronze.raw_locations dl ON s.destination_id = dl.location_id
      JOIN logi.bronze.raw_carriers c ON s.carrier_id = c.carrier_id
      WHERE s.transit_days IS NOT NULL
      GROUP BY s.origin_id || '->' || s.destination_id,
               ol.hub_name, ol.city, dl.hub_name, dl.city,
               ol.latitude, ol.longitude, dl.latitude, dl.longitude,
               c.carrier_type
  ) AS src
  ON tgt.route_key = src.route_key
  WHEN MATCHED THEN UPDATE SET
      tgt.avg_transit_days = src.avg_transit_days,
      tgt.shipment_count   = src.shipment_count,
      tgt.distance_km      = src.distance_km,
      tgt.loaded_at        = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      route_key, origin_hub, origin_city, destination_hub, destination_city,
      distance_km, avg_transit_days, shipment_count, primary_mode, loaded_at
  ) VALUES (
      src.route_key, src.origin_hub, src.origin_city, src.destination_hub,
      src.destination_city, src.distance_km, src.avg_transit_days,
      src.shipment_count, src.primary_mode, CURRENT_TIMESTAMP
  );

-- ===================== STEP 9: build_fact_shipments =====================
-- Star schema fact table: one row per delivered shipment with full
-- lifecycle metrics, SLA violation status, and margin calculation.

STEP build_fact_shipments
  DEPENDS ON (build_dim_carrier, build_dim_location, build_dim_customer, build_dim_route)
  TIMEOUT '5m'
AS
  MERGE INTO logi.gold.fact_shipments AS tgt
  USING (
      SELECT
          ss.shipment_id     AS shipment_key,
          ss.carrier_id      AS carrier_key,
          ss.origin_id       AS origin_key,
          ss.destination_id  AS destination_key,
          ss.customer_id     AS customer_key,
          ss.origin_id || '->' || ss.destination_id AS route_key,
          ss.service_level,
          ss.ship_date,
          ss.delivery_date,
          ss.promised_date,
          ss.weight_kg,
          ss.volume_m3,
          ss.cost,
          ss.revenue,
          CAST(ss.revenue - ss.cost AS DECIMAL(10,2)) AS margin,
          ss.on_time_flag,
          ss.transit_days,
          ss.event_count,
          COALESCE(sv.sla_violated, false) AS sla_violated,
          COALESCE(sv.penalty_amount, 0) AS penalty_amount
      FROM logi.silver.shipment_status ss
      LEFT JOIN logi.silver.sla_violations sv
          ON ss.shipment_id = sv.shipment_id
      WHERE ss.latest_status = 'delivered'
  ) AS src
  ON tgt.shipment_key = src.shipment_key
  WHEN MATCHED THEN UPDATE SET
      tgt.delivery_date   = src.delivery_date,
      tgt.on_time_flag    = src.on_time_flag,
      tgt.transit_days    = src.transit_days,
      tgt.event_count     = src.event_count,
      tgt.margin          = src.margin,
      tgt.sla_violated    = src.sla_violated,
      tgt.penalty_amount  = src.penalty_amount,
      tgt.loaded_at       = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      shipment_key, carrier_key, origin_key, destination_key, customer_key,
      route_key, service_level, ship_date, delivery_date, promised_date,
      weight_kg, volume_m3, cost, revenue, margin, on_time_flag, transit_days,
      event_count, sla_violated, penalty_amount, loaded_at
  ) VALUES (
      src.shipment_key, src.carrier_key, src.origin_key, src.destination_key,
      src.customer_key, src.route_key, src.service_level, src.ship_date,
      src.delivery_date, src.promised_date, src.weight_kg, src.volume_m3,
      src.cost, src.revenue, src.margin, src.on_time_flag, src.transit_days,
      src.event_count, src.sla_violated, src.penalty_amount, CURRENT_TIMESTAMP
  );

-- ===================== STEP 10: kpi_delivery_performance =====================
-- Carrier x route x month performance: on-time %, avg transit, cost efficiency.

STEP kpi_delivery_performance
  DEPENDS ON (build_fact_shipments)
AS
  MERGE INTO logi.gold.kpi_delivery_performance AS tgt
  USING (
      SELECT
          dc.carrier_name,
          ol.city || ' -> ' || dl.city AS route,
          DATE_FORMAT(f.ship_date, 'yyyy-MM') AS month,
          COUNT(*) AS total_shipments,
          COUNT(CASE WHEN f.on_time_flag = true THEN 1 END) AS on_time_count,
          CAST(
              COUNT(CASE WHEN f.on_time_flag = true THEN 1 END) * 100.0 / COUNT(*)
          AS DECIMAL(5,2)) AS on_time_pct,
          CAST(AVG(f.transit_days) AS DECIMAL(5,1)) AS avg_transit_days,
          CAST(AVG(f.cost / NULLIF(f.weight_kg, 0)) AS DECIMAL(8,2)) AS avg_cost_per_kg,
          SUM(f.weight_kg) AS total_weight,
          SUM(f.revenue) AS total_revenue,
          SUM(f.margin) AS total_margin
      FROM logi.gold.fact_shipments f
      JOIN logi.gold.dim_carrier dc ON f.carrier_key = dc.carrier_key
      JOIN logi.gold.dim_location ol ON f.origin_key = ol.location_key
      JOIN logi.gold.dim_location dl ON f.destination_key = dl.location_key
      GROUP BY dc.carrier_name, ol.city || ' -> ' || dl.city,
               DATE_FORMAT(f.ship_date, 'yyyy-MM')
  ) AS src
  ON tgt.carrier_name = src.carrier_name AND tgt.route = src.route AND tgt.month = src.month
  WHEN MATCHED THEN UPDATE SET
      tgt.total_shipments  = src.total_shipments,
      tgt.on_time_count    = src.on_time_count,
      tgt.on_time_pct      = src.on_time_pct,
      tgt.avg_transit_days = src.avg_transit_days,
      tgt.avg_cost_per_kg  = src.avg_cost_per_kg,
      tgt.total_weight     = src.total_weight,
      tgt.total_revenue    = src.total_revenue,
      tgt.total_margin     = src.total_margin,
      tgt.loaded_at        = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      carrier_name, route, month, total_shipments, on_time_count, on_time_pct,
      avg_transit_days, avg_cost_per_kg, total_weight, total_revenue, total_margin,
      loaded_at
  ) VALUES (
      src.carrier_name, src.route, src.month, src.total_shipments, src.on_time_count,
      src.on_time_pct, src.avg_transit_days, src.avg_cost_per_kg,
      src.total_weight, src.total_revenue, src.total_margin, CURRENT_TIMESTAMP
  );

-- ===================== STEP 11: kpi_sla_compliance =====================
-- SLA violation rate, total penalty exposure, worst violation per carrier x service level.

STEP kpi_sla_compliance
  DEPENDS ON (build_fact_shipments)
AS
  MERGE INTO logi.gold.kpi_sla_compliance AS tgt
  USING (
      SELECT
          dc.carrier_name,
          f.service_level,
          DATE_FORMAT(f.ship_date, 'yyyy-MM') AS month,
          COUNT(*) AS total_shipments,
          COUNT(CASE WHEN f.sla_violated = true THEN 1 END) AS violated_count,
          CAST(
              COUNT(CASE WHEN f.sla_violated = true THEN 1 END) * 100.0 / COUNT(*)
          AS DECIMAL(5,2)) AS violation_rate,
          COALESCE(SUM(f.penalty_amount), 0) AS total_penalty,
          CAST(AVG(CASE WHEN f.sla_violated = true
              THEN f.transit_days - sv.sla_max_days ELSE NULL END) AS DECIMAL(5,1)) AS avg_days_over,
          MAX(CASE WHEN f.sla_violated = true
              THEN f.transit_days - sv.sla_max_days ELSE 0 END) AS worst_violation
      FROM logi.gold.fact_shipments f
      JOIN logi.gold.dim_carrier dc ON f.carrier_key = dc.carrier_key
      LEFT JOIN logi.silver.sla_violations sv ON f.shipment_key = sv.shipment_id
      GROUP BY dc.carrier_name, f.service_level, DATE_FORMAT(f.ship_date, 'yyyy-MM')
  ) AS src
  ON tgt.carrier_name = src.carrier_name AND tgt.service_level = src.service_level AND tgt.month = src.month
  WHEN MATCHED THEN UPDATE SET
      tgt.total_shipments  = src.total_shipments,
      tgt.violated_count   = src.violated_count,
      tgt.violation_rate   = src.violation_rate,
      tgt.total_penalty    = src.total_penalty,
      tgt.avg_days_over    = src.avg_days_over,
      tgt.worst_violation  = src.worst_violation,
      tgt.loaded_at        = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      carrier_name, service_level, month, total_shipments, violated_count,
      violation_rate, total_penalty, avg_days_over, worst_violation, loaded_at
  ) VALUES (
      src.carrier_name, src.service_level, src.month, src.total_shipments,
      src.violated_count, src.violation_rate, src.total_penalty,
      src.avg_days_over, src.worst_violation, CURRENT_TIMESTAMP
  );

-- ===================== STEP 12: optimize_zorder =====================
-- Z-ORDER fact_shipments by (origin_key, destination_key) for efficient
-- route-based query patterns. CONTINUE ON FAILURE so the pipeline
-- succeeds even if OPTIMIZE is not supported.

STEP optimize_zorder
  DEPENDS ON (kpi_delivery_performance, kpi_sla_compliance)
  CONTINUE ON FAILURE
AS
  OPTIMIZE logi.silver.events_deduped;
  OPTIMIZE logi.silver.shipment_status;
  OPTIMIZE logi.gold.fact_shipments
      ZORDER BY (origin_key, destination_key);
