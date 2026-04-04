-- =============================================================================
-- Legal Case Management Pipeline - Full Load Transformation
-- =============================================================================
-- 13-step DAG: validate_bronze -> enrich_cases + validate_billings (parallel)
-- -> build_party_profiles -> dim_case + dim_attorney + dim_party (parallel)
-- -> build_fact_billings -> compute_firm_kpi -> build_legal_graph
-- -> run_graph_analytics -> optimize (CONTINUE ON FAILURE)
-- =============================================================================

-- ===================== SCHEDULE & PIPELINE =====================

SCHEDULE legal_daily_schedule CRON '0 7 * * *' TIMEZONE 'America/New_York' RETRIES 2 TIMEOUT 3600 MAX_CONCURRENT 1 INACTIVE;

PIPELINE legal_billing_pipeline DESCRIPTION 'Daily legal billing pipeline: case profitability, attorney influence via PageRank, conflict-of-interest via Cypher, practice group community detection' SCHEDULE 'legal_daily_schedule' TAGS 'legal,billing,graph,pagerank,cypher' SLA 3600 FAIL_FAST true LIFECYCLE production;

-- ===================== STEP 0: create_objects =====================

STEP create_objects
  TIMEOUT '2m'
AS
  CREATE ZONE IF NOT EXISTS legal TYPE EXTERNAL
      COMMENT 'Litigation analytics pipeline zone';

  CREATE SCHEMA IF NOT EXISTS legal.bronze COMMENT 'Raw cases, parties, attorneys, billings, and relationship edges';
  CREATE SCHEMA IF NOT EXISTS legal.silver COMMENT 'Enriched cases with complexity scores, validated billings, party profiles';
  CREATE SCHEMA IF NOT EXISTS legal.gold COMMENT 'Star schema, firm KPIs, and legal network property graph';

  CREATE DELTA TABLE IF NOT EXISTS legal.bronze.raw_cases (
      case_id             STRING      NOT NULL,
      case_number         STRING      NOT NULL,
      case_type           STRING      NOT NULL,
      court               STRING,
      filing_date         DATE,
      close_date          DATE,
      status              STRING,
      priority            STRING,
      ingested_at         TIMESTAMP   NOT NULL
  ) LOCATION 'legal/bronze/legal/raw_cases';

  CREATE DELTA TABLE IF NOT EXISTS legal.bronze.raw_parties (
      party_id            STRING      NOT NULL,
      party_name          STRING      NOT NULL,
      party_type          STRING      NOT NULL,
      ssn                 STRING,
      contact_email       STRING,
      contact_phone       STRING,
      organization        STRING,
      jurisdiction        STRING,
      ingested_at         TIMESTAMP   NOT NULL
  ) LOCATION 'legal/bronze/legal/raw_parties';

  CREATE DELTA TABLE IF NOT EXISTS legal.bronze.raw_attorneys (
      attorney_id         STRING      NOT NULL,
      attorney_name       STRING      NOT NULL,
      bar_number          STRING      NOT NULL,
      practice_group      STRING,
      partner_flag        BOOLEAN,
      years_experience    INT,
      hourly_rate         DECIMAL(8,2),
      ingested_at         TIMESTAMP   NOT NULL
  ) LOCATION 'legal/bronze/legal/raw_attorneys';

  CREATE DELTA TABLE IF NOT EXISTS legal.bronze.raw_billings (
      billing_id          STRING      NOT NULL,
      case_id             STRING      NOT NULL,
      attorney_id         STRING      NOT NULL,
      billing_date        DATE        NOT NULL,
      hours               DECIMAL(5,2) NOT NULL,
      hourly_rate         DECIMAL(8,2) NOT NULL,
      amount              DECIMAL(10,2),
      billable_flag       BOOLEAN     NOT NULL,
      billing_type        STRING,
      description         STRING,
      ingested_at         TIMESTAMP   NOT NULL
  ) LOCATION 'legal/bronze/legal/raw_billings';

  CREATE DELTA TABLE IF NOT EXISTS legal.bronze.raw_relationships (
      relationship_id     STRING      NOT NULL,
      source_id           STRING      NOT NULL,
      target_id           STRING      NOT NULL,
      source_type         STRING      NOT NULL,
      target_type         STRING      NOT NULL,
      relationship_type   STRING      NOT NULL,
      weight              DECIMAL(5,2),
      effective_date      DATE,
      ingested_at         TIMESTAMP   NOT NULL
  ) LOCATION 'legal/bronze/legal/raw_relationships';

  CREATE DELTA TABLE IF NOT EXISTS legal.silver.cases_enriched (
      case_id             STRING      NOT NULL,
      case_number         STRING      NOT NULL,
      case_type           STRING      NOT NULL,
      court               STRING,
      filing_date         DATE,
      close_date          DATE,
      status              STRING,
      priority            STRING,
      duration_days       INT,
      attorney_count      INT,
      party_count         INT,
      total_billed_hours  DECIMAL(8,2),
      total_billed_amount DECIMAL(12,2),
      complexity_score    DECIMAL(5,2),
      processed_at        TIMESTAMP   NOT NULL
  ) LOCATION 'legal/silver/legal/cases_enriched';

  CREATE DELTA TABLE IF NOT EXISTS legal.silver.billings_validated (
      billing_id          STRING      NOT NULL,
      case_id             STRING      NOT NULL,
      attorney_id         STRING      NOT NULL,
      billing_date        DATE        NOT NULL,
      hours               DECIMAL(5,2) NOT NULL CHECK (hours > 0),
      hourly_rate         DECIMAL(8,2) NOT NULL CHECK (hourly_rate > 0),
      amount              DECIMAL(10,2),
      billable_flag       BOOLEAN     NOT NULL,
      billing_type        STRING,
      case_type           STRING,
      practice_group      STRING,
      partner_flag        BOOLEAN,
      complexity_score    DECIMAL(5,2),
      processed_at        TIMESTAMP   NOT NULL
  ) LOCATION 'legal/silver/legal/billings_validated';

  CREATE DELTA TABLE IF NOT EXISTS legal.silver.party_profiles (
      party_id            STRING      NOT NULL,
      party_name          STRING      NOT NULL,
      party_type          STRING      NOT NULL,
      cases_as_plaintiff  INT,
      cases_as_defendant  INT,
      cases_as_witness    INT,
      cases_as_expert     INT,
      total_involvement   INT,
      processed_at        TIMESTAMP   NOT NULL
  ) LOCATION 'legal/silver/legal/party_profiles';

  CREATE DELTA TABLE IF NOT EXISTS legal.gold.dim_case (
      case_key            INT         NOT NULL,
      case_id             STRING      NOT NULL,
      case_number         STRING      NOT NULL,
      case_type           STRING      NOT NULL,
      court               STRING,
      filing_date         DATE,
      close_date          DATE,
      status              STRING,
      priority            STRING,
      complexity_score    DECIMAL(5,2),
      loaded_at           TIMESTAMP   NOT NULL
  ) LOCATION 'legal/gold/legal/dim_case';

  CREATE DELTA TABLE IF NOT EXISTS legal.gold.dim_attorney (
      attorney_key        INT         NOT NULL,
      attorney_id         STRING      NOT NULL,
      attorney_name       STRING      NOT NULL,
      bar_number          STRING,
      practice_group      STRING,
      partner_flag        BOOLEAN,
      years_experience    INT,
      hourly_rate         DECIMAL(8,2),
      loaded_at           TIMESTAMP   NOT NULL
  ) LOCATION 'legal/gold/legal/dim_attorney';

  CREATE DELTA TABLE IF NOT EXISTS legal.gold.dim_party (
      party_key           INT         NOT NULL,
      party_id            STRING      NOT NULL,
      party_name          STRING      NOT NULL,
      party_type          STRING      NOT NULL,
      organization        STRING,
      jurisdiction        STRING,
      loaded_at           TIMESTAMP   NOT NULL
  ) LOCATION 'legal/gold/legal/dim_party';

  CREATE DELTA TABLE IF NOT EXISTS legal.gold.fact_billings (
      billing_key         INT         NOT NULL,
      case_key            INT         NOT NULL,
      attorney_key        INT         NOT NULL,
      billing_date        DATE        NOT NULL,
      hours               DECIMAL(5,2) NOT NULL,
      hourly_rate         DECIMAL(8,2) NOT NULL,
      amount              DECIMAL(10,2),
      billable_flag       BOOLEAN     NOT NULL,
      case_complexity     DECIMAL(5,2),
      loaded_at           TIMESTAMP   NOT NULL
  ) LOCATION 'legal/gold/legal/fact_billings';

  CREATE DELTA TABLE IF NOT EXISTS legal.gold.kpi_firm_performance (
      attorney_id         STRING      NOT NULL,
      attorney_name       STRING      NOT NULL,
      practice_group      STRING,
      partner_flag        BOOLEAN,
      total_hours         DECIMAL(8,2),
      billable_hours      DECIMAL(8,2),
      utilization_pct     DECIMAL(5,2),
      total_revenue       DECIMAL(12,2),
      case_count          INT,
      avg_case_complexity DECIMAL(5,2),
      revenue_per_hour    DECIMAL(8,2),
      loaded_at           TIMESTAMP   NOT NULL
  ) LOCATION 'legal/gold/legal/kpi_firm_performance';

-- ===================== STEP 1: validate_bronze =====================

STEP validate_bronze
  DEPENDS ON (create_objects)
  TIMEOUT '2m'
AS
  ASSERT ROW_COUNT = 15
  SELECT COUNT(*) AS row_count FROM legal.bronze.raw_cases;

  ASSERT ROW_COUNT = 25
  SELECT COUNT(*) AS row_count FROM legal.bronze.raw_parties;

  ASSERT ROW_COUNT = 10
  SELECT COUNT(*) AS row_count FROM legal.bronze.raw_attorneys;

  ASSERT ROW_COUNT = 70
  SELECT COUNT(*) AS row_count FROM legal.bronze.raw_billings;

  ASSERT ROW_COUNT = 40
  SELECT COUNT(*) AS row_count FROM legal.bronze.raw_relationships;

-- ===================== STEP 2: enrich_cases =====================
-- Compute duration, attorney count, party count, billing totals, complexity score

STEP enrich_cases
  DEPENDS ON (validate_bronze)
  TIMEOUT '5m'
AS
  INSERT INTO legal.silver.cases_enriched
  SELECT
      c.case_id,
      c.case_number,
      c.case_type,
      c.court,
      c.filing_date,
      c.close_date,
      c.status,
      c.priority,
      DATEDIFF(COALESCE(c.close_date, CAST('2024-06-01' AS DATE)), c.filing_date) AS duration_days,
      agg.attorney_count,
      agg.party_count,
      agg.total_billed_hours,
      agg.total_billed_amount,
      -- Complexity: log(hours+1) * sqrt(attorney_count) * ln(party_count+1) normalized 1-10
      LEAST(10.0, GREATEST(1.0, ROUND(
          LN(GREATEST(1, agg.total_billed_hours + 1))
          * SQRT(CAST(agg.attorney_count AS DOUBLE))
          * LN(CAST(agg.party_count + 1 AS DOUBLE))
          / 2.0
      , 2))) AS complexity_score,
      CURRENT_TIMESTAMP AS processed_at
  FROM legal.bronze.raw_cases c
  LEFT JOIN (
      SELECT
          b.case_id,
          COUNT(DISTINCT b.attorney_id) AS attorney_count,
          COUNT(DISTINCT r.source_id)
              + COUNT(DISTINCT r.target_id) AS party_count,
          SUM(b.hours)  AS total_billed_hours,
          SUM(CASE WHEN b.billable_flag = true THEN b.amount ELSE 0 END) AS total_billed_amount
      FROM legal.bronze.raw_billings b
      LEFT JOIN legal.bronze.raw_relationships r
          ON r.relationship_type = 'represents'
          AND r.target_id IN (
              SELECT p.party_id FROM legal.bronze.raw_parties p
          )
          AND r.source_id = b.attorney_id
      GROUP BY b.case_id
  ) agg ON c.case_id = agg.case_id;

  ASSERT ROW_COUNT = 15
  SELECT COUNT(*) AS row_count FROM legal.silver.cases_enriched;

-- ===================== STEP 3: validate_billings =====================
-- Join billings with case/attorney metadata; enforce CHECK constraints via query

STEP validate_billings
  DEPENDS ON (validate_bronze)
  TIMEOUT '5m'
AS
  INSERT INTO legal.silver.billings_validated
  SELECT
      b.billing_id,
      b.case_id,
      b.attorney_id,
      b.billing_date,
      b.hours,
      b.hourly_rate,
      b.amount,
      b.billable_flag,
      b.billing_type,
      c.case_type,
      a.practice_group,
      a.partner_flag,
      ce.complexity_score,
      CURRENT_TIMESTAMP AS processed_at
  FROM legal.bronze.raw_billings b
  JOIN legal.bronze.raw_cases c ON b.case_id = c.case_id
  JOIN legal.bronze.raw_attorneys a ON b.attorney_id = a.attorney_id
  LEFT JOIN legal.silver.cases_enriched ce ON b.case_id = ce.case_id
  WHERE b.hours > 0
    AND b.hourly_rate > 0;

  ASSERT ROW_COUNT = 70
  SELECT COUNT(*) AS row_count FROM legal.silver.billings_validated;

-- ===================== STEP 4: build_party_profiles =====================

STEP build_party_profiles
  DEPENDS ON (enrich_cases, validate_billings)
  TIMEOUT '3m'
AS
  INSERT INTO legal.silver.party_profiles
  SELECT
      p.party_id,
      p.party_name,
      p.party_type,
      COALESCE(SUM(CASE WHEN r.relationship_type = 'represents' AND p.party_type = 'plaintiff' THEN 1 ELSE 0 END), 0) AS cases_as_plaintiff,
      COALESCE(SUM(CASE WHEN r.relationship_type = 'represents' AND p.party_type = 'defendant' THEN 1 ELSE 0 END), 0) AS cases_as_defendant,
      COALESCE(SUM(CASE WHEN r.relationship_type = 'witnesses_for' AND p.party_type = 'witness' THEN 1 ELSE 0 END), 0) AS cases_as_witness,
      COALESCE(SUM(CASE WHEN r.relationship_type = 'witnesses_for' AND p.party_type = 'expert' THEN 1 ELSE 0 END), 0) AS cases_as_expert,
      COUNT(DISTINCT r.relationship_id) AS total_involvement,
      CURRENT_TIMESTAMP AS processed_at
  FROM legal.bronze.raw_parties p
  LEFT JOIN legal.bronze.raw_relationships r
      ON (r.source_id = p.party_id OR r.target_id = p.party_id)
  GROUP BY p.party_id, p.party_name, p.party_type;

  ASSERT ROW_COUNT = 25
  SELECT COUNT(*) AS row_count FROM legal.silver.party_profiles;

-- ===================== STEP 5: dim_case =====================

STEP dim_case
  DEPENDS ON (build_party_profiles)
  TIMEOUT '3m'
AS
  MERGE INTO legal.gold.dim_case AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY ce.case_id) AS case_key,
          ce.case_id,
          ce.case_number,
          ce.case_type,
          ce.court,
          ce.filing_date,
          ce.close_date,
          ce.status,
          ce.priority,
          ce.complexity_score
      FROM legal.silver.cases_enriched ce
  ) AS source
  ON target.case_id = source.case_id
  WHEN MATCHED THEN UPDATE SET
      case_type        = source.case_type,
      status           = source.status,
      close_date       = source.close_date,
      complexity_score = source.complexity_score,
      loaded_at        = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      case_key, case_id, case_number, case_type, court, filing_date,
      close_date, status, priority, complexity_score, loaded_at
  ) VALUES (
      source.case_key, source.case_id, source.case_number, source.case_type,
      source.court, source.filing_date, source.close_date, source.status,
      source.priority, source.complexity_score, CURRENT_TIMESTAMP
  );

-- ===================== STEP 6: dim_attorney =====================

STEP dim_attorney
  DEPENDS ON (build_party_profiles)
AS
  MERGE INTO legal.gold.dim_attorney AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY a.attorney_id) AS attorney_key,
          a.attorney_id,
          a.attorney_name,
          a.bar_number,
          a.practice_group,
          a.partner_flag,
          a.years_experience,
          a.hourly_rate
      FROM legal.bronze.raw_attorneys a
  ) AS source
  ON target.attorney_id = source.attorney_id
  WHEN MATCHED THEN UPDATE SET
      attorney_name    = source.attorney_name,
      practice_group   = source.practice_group,
      partner_flag     = source.partner_flag,
      years_experience = source.years_experience,
      hourly_rate      = source.hourly_rate,
      loaded_at        = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      attorney_key, attorney_id, attorney_name, bar_number, practice_group,
      partner_flag, years_experience, hourly_rate, loaded_at
  ) VALUES (
      source.attorney_key, source.attorney_id, source.attorney_name,
      source.bar_number, source.practice_group, source.partner_flag,
      source.years_experience, source.hourly_rate, CURRENT_TIMESTAMP
  );

-- ===================== STEP 7: dim_party =====================

STEP dim_party
  DEPENDS ON (build_party_profiles)
AS
  MERGE INTO legal.gold.dim_party AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY p.party_id) AS party_key,
          p.party_id,
          p.party_name,
          p.party_type,
          p.organization,
          p.jurisdiction
      FROM legal.bronze.raw_parties p
  ) AS source
  ON target.party_id = source.party_id
  WHEN MATCHED THEN UPDATE SET
      party_name   = source.party_name,
      party_type   = source.party_type,
      organization = source.organization,
      jurisdiction = source.jurisdiction,
      loaded_at    = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      party_key, party_id, party_name, party_type, organization, jurisdiction, loaded_at
  ) VALUES (
      source.party_key, source.party_id, source.party_name, source.party_type,
      source.organization, source.jurisdiction, CURRENT_TIMESTAMP
  );

-- ===================== STEP 8: build_fact_billings =====================

STEP build_fact_billings
  DEPENDS ON (dim_case, dim_attorney, dim_party)
  TIMEOUT '5m'
AS
  MERGE INTO legal.gold.fact_billings AS target
  USING (
      SELECT
          ROW_NUMBER() OVER (ORDER BY bv.billing_date, bv.billing_id) AS billing_key,
          dc.case_key,
          da.attorney_key,
          bv.billing_date,
          bv.hours,
          bv.hourly_rate,
          bv.amount,
          bv.billable_flag,
          bv.complexity_score AS case_complexity
      FROM legal.silver.billings_validated bv
      JOIN legal.gold.dim_case dc ON bv.case_id = dc.case_id
      JOIN legal.gold.dim_attorney da ON bv.attorney_id = da.attorney_id
  ) AS source
  ON target.case_key = source.case_key
     AND target.attorney_key = source.attorney_key
     AND target.billing_date = source.billing_date
     AND target.hours = source.hours
  WHEN MATCHED THEN UPDATE SET
      amount         = source.amount,
      case_complexity = source.case_complexity,
      loaded_at      = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN INSERT (
      billing_key, case_key, attorney_key, billing_date, hours,
      hourly_rate, amount, billable_flag, case_complexity, loaded_at
  ) VALUES (
      source.billing_key, source.case_key, source.attorney_key,
      source.billing_date, source.hours, source.hourly_rate,
      source.amount, source.billable_flag, source.case_complexity,
      CURRENT_TIMESTAMP
  );

  ASSERT ROW_COUNT = 70
  SELECT COUNT(*) AS row_count FROM legal.gold.fact_billings;

-- ===================== STEP 9: compute_firm_kpi =====================
-- Utilization rates, revenue per attorney, case profitability

STEP compute_firm_kpi
  DEPENDS ON (build_fact_billings)
  TIMEOUT '3m'
AS
  INSERT INTO legal.gold.kpi_firm_performance
  SELECT
      da.attorney_id,
      da.attorney_name,
      da.practice_group,
      da.partner_flag,
      SUM(fb.hours) AS total_hours,
      SUM(CASE WHEN fb.billable_flag = true THEN fb.hours ELSE 0 END) AS billable_hours,
      ROUND(
          100.0 * SUM(CASE WHEN fb.billable_flag = true THEN fb.hours ELSE 0 END)
          / NULLIF(SUM(fb.hours), 0),
          2
      ) AS utilization_pct,
      ROUND(SUM(fb.amount), 2) AS total_revenue,
      COUNT(DISTINCT fb.case_key) AS case_count,
      ROUND(AVG(fb.case_complexity), 2) AS avg_case_complexity,
      ROUND(
          SUM(fb.amount) / NULLIF(SUM(CASE WHEN fb.billable_flag = true THEN fb.hours ELSE 0 END), 0),
          2
      ) AS revenue_per_hour,
      CURRENT_TIMESTAMP AS loaded_at
  FROM legal.gold.fact_billings fb
  JOIN legal.gold.dim_attorney da ON fb.attorney_key = da.attorney_key
  GROUP BY da.attorney_id, da.attorney_name, da.practice_group, da.partner_flag;

  ASSERT ROW_COUNT = 10
  SELECT COUNT(*) AS row_count FROM legal.gold.kpi_firm_performance;

-- ===================== STEP 10: build_legal_graph =====================
-- Property graph with parties as vertices, relationships as typed/weighted edges

STEP build_legal_graph
  DEPENDS ON (compute_firm_kpi)
AS
  CREATE GRAPH IF NOT EXISTS legal.gold.legal_network
      VERTEX TABLE legal.bronze.raw_parties
          ID COLUMN party_id
          NODE TYPE COLUMN party_type
          NODE NAME COLUMN party_name
      EDGE TABLE legal.bronze.raw_relationships
          SOURCE COLUMN source_id
          TARGET COLUMN target_id
          WEIGHT COLUMN weight
          EDGE TYPE COLUMN relationship_type
      DIRECTED;

-- ===================== STEP 11: run_graph_analytics =====================
-- PageRank, community detection, multi-hop conflict check, adversarial pairs

STEP run_graph_analytics
  DEPENDS ON (build_legal_graph)
  TIMEOUT '5m'
AS
  -- PageRank: most influential attorneys (connected across many cases/parties)
  USE legal.gold.legal_network
  CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
  YIELD node_id, score, rank
  RETURN node_id, score, rank
  ORDER BY rank ASC
  LIMIT 15;

  -- Community detection (Louvain): practice groups from co_counsel edges
  USE legal.gold.legal_network
  CALL algo.louvain({relationshipTypes: ['co_counsel'], maxIterations: 10})
  YIELD node_id, community_id
  RETURN community_id, COLLECT(node_id) AS members, COUNT(*) AS size
  ORDER BY size DESC;

  -- Multi-hop: all parties within 3 hops of P008 (conflict-of-interest check)
  USE legal.gold.legal_network
  MATCH path = (start {party_id: 'P008'})-[*1..3]-(connected)
  RETURN DISTINCT connected.party_id AS connected_party,
         connected.party_name AS name,
         connected.party_type AS type,
         LENGTH(path) AS hops
  ORDER BY hops ASC, connected_party;

  -- Betweenness centrality: attorneys bridging practice groups
  USE legal.gold.legal_network
  CALL algo.betweennessCentrality({nodeLabels: ['attorney']})
  YIELD node_id, centrality
  RETURN node_id, centrality
  ORDER BY centrality DESC
  LIMIT 10;

  -- Adversarial pattern: (attorney)-[:represents]->(party)-[:opposes]->(opponent)<-[:represents]-(opponent_attorney)
  USE legal.gold.legal_network
  MATCH (a1)-[:represents]->(p1)-[:opposes]->(p2)<-[:represents]-(a2)
  WHERE a1.party_id <> a2.party_id
  RETURN a1.party_name AS attorney_1,
         p1.party_name AS plaintiff,
         p2.party_name AS defendant,
         a2.party_name AS attorney_2
  ORDER BY attorney_1;

-- ===================== STEP 12: restore_demo =====================

STEP restore_demo
  DEPENDS ON (run_graph_analytics)
AS
  -- Demonstrate RESTORE: revert dim_case to version 0 then back
  RESTORE legal.gold.dim_case TO VERSION 0;
  RESTORE legal.gold.dim_case TO VERSION 1;

-- ===================== STEP 13: optimize =====================

STEP optimize
  DEPENDS ON (restore_demo)
  CONTINUE ON FAILURE
AS
  OPTIMIZE legal.silver.cases_enriched;
  OPTIMIZE legal.silver.billings_validated;
  OPTIMIZE legal.silver.party_profiles;
  OPTIMIZE legal.gold.dim_case;
  OPTIMIZE legal.gold.dim_attorney;
  OPTIMIZE legal.gold.dim_party;
  OPTIMIZE legal.gold.fact_billings;
  OPTIMIZE legal.gold.kpi_firm_performance;
