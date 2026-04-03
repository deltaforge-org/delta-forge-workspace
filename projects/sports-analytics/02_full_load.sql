-- =============================================================================
-- Sports Analytics Pipeline: Full Load (Bronze -> Silver -> Gold)
-- =============================================================================

SCHEDULE sports_nightly_schedule
    CRON '0 23 * * *'
    TIMEZONE 'UTC'
    RETRIES 2
    TIMEOUT 3600
    MAX_CONCURRENT 1
    ACTIVE;

PIPELINE sports_analytics_pipeline
    DESCRIPTION 'Sports analytics with player performance, graph network, composite scoring, and position leaderboards'
    SCHEDULE 'sports_nightly_schedule'
    TAGS 'sports,football,analytics,graph,medallion'
    SLA 45
    FAIL_FAST true
    LIFECYCLE production;

-- =============================================================================
-- STEP 1: SILVER - Upsert rolling player aggregates with per-90 stats
-- =============================================================================
-- Composite score weighted by position:
--   Forwards: 40% goals_per_90 * 10 + 25% assists_per_90 * 10 + 20% rating + 15% shots_on_target_per_90 * 5
--   Midfielders: 20% goals_per_90 * 10 + 30% assists_per_90 * 10 + 30% pass_accuracy/10 + 20% rating
--   Defenders: 10% goals_per_90 * 10 + 10% assists_per_90 * 10 + 30% (tackles+interceptions)_per_90 * 3 + 30% rating + 20% pass_accuracy/10

MERGE INTO {{zone_prefix}}.silver.player_rolling_stats AS tgt
USING (
    WITH player_agg AS (
        SELECT
            ms.player_id,
            ms.season,
            p.position,
            ms.team_id,
            COUNT(*) AS matches_played,
            SUM(ms.minutes_played) AS total_minutes,
            SUM(ms.goals) AS total_goals,
            SUM(ms.assists) AS total_assists,
            SUM(ms.passes_completed) AS total_passes_completed,
            SUM(ms.tackles) AS total_tackles,
            SUM(ms.interceptions) AS total_interceptions,
            SUM(ms.shots_on_target) AS total_shots_on_target,
            CAST(AVG(ms.rating) AS DECIMAL(4,2)) AS avg_rating,
            CASE WHEN SUM(ms.minutes_played) > 0
                THEN CAST(SUM(ms.goals) * 90.0 / SUM(ms.minutes_played) AS DECIMAL(6,3))
                ELSE 0.000 END AS goals_per_90,
            CASE WHEN SUM(ms.minutes_played) > 0
                THEN CAST(SUM(ms.assists) * 90.0 / SUM(ms.minutes_played) AS DECIMAL(6,3))
                ELSE 0.000 END AS assists_per_90,
            CASE WHEN SUM(ms.passes_attempted) > 0
                THEN CAST(SUM(ms.passes_completed) * 100.0 / SUM(ms.passes_attempted) AS DECIMAL(5,2))
                ELSE 0.00 END AS pass_accuracy,
            CASE WHEN SUM(ms.minutes_played) > 0
                THEN CAST(SUM(ms.shots_on_target) * 90.0 / SUM(ms.minutes_played) AS DECIMAL(6,3))
                ELSE 0.000 END AS shots_per_90,
            CASE WHEN SUM(ms.minutes_played) > 0
                THEN CAST((SUM(ms.tackles) + SUM(ms.interceptions)) * 90.0 / SUM(ms.minutes_played) AS DECIMAL(6,3))
                ELSE 0.000 END AS def_per_90
        FROM {{zone_prefix}}.bronze.raw_match_stats ms
        JOIN {{zone_prefix}}.bronze.raw_players p ON ms.player_id = p.player_id
        GROUP BY ms.player_id, ms.season, p.position, ms.team_id
    )
    SELECT
        player_id,
        season,
        position,
        team_id,
        matches_played,
        total_minutes,
        total_goals,
        total_assists,
        total_passes_completed,
        total_tackles,
        total_interceptions,
        total_shots_on_target,
        avg_rating,
        goals_per_90,
        assists_per_90,
        pass_accuracy,
        CAST(CASE position
            WHEN 'Forward' THEN
                0.40 * goals_per_90 * 10.0 + 0.25 * assists_per_90 * 10.0 + 0.20 * avg_rating + 0.15 * shots_per_90 * 5.0
            WHEN 'Midfielder' THEN
                0.20 * goals_per_90 * 10.0 + 0.30 * assists_per_90 * 10.0 + 0.30 * pass_accuracy / 10.0 + 0.20 * avg_rating
            WHEN 'Defender' THEN
                0.10 * goals_per_90 * 10.0 + 0.10 * assists_per_90 * 10.0 + 0.30 * def_per_90 * 3.0 + 0.30 * avg_rating + 0.20 * pass_accuracy / 10.0
            ELSE avg_rating
        END AS DECIMAL(6,2)) AS composite_score,
        CURRENT_TIMESTAMP AS updated_at
    FROM player_agg
) AS src
ON tgt.player_id = src.player_id AND tgt.season = src.season
WHEN MATCHED THEN UPDATE SET
    tgt.matches_played          = src.matches_played,
    tgt.total_minutes           = src.total_minutes,
    tgt.total_goals             = src.total_goals,
    tgt.total_assists           = src.total_assists,
    tgt.total_passes_completed  = src.total_passes_completed,
    tgt.total_tackles           = src.total_tackles,
    tgt.total_interceptions     = src.total_interceptions,
    tgt.total_shots_on_target   = src.total_shots_on_target,
    tgt.avg_rating              = src.avg_rating,
    tgt.goals_per_90            = src.goals_per_90,
    tgt.assists_per_90          = src.assists_per_90,
    tgt.pass_accuracy           = src.pass_accuracy,
    tgt.composite_score         = src.composite_score,
    tgt.updated_at              = src.updated_at
WHEN NOT MATCHED THEN INSERT (
    player_id, season, position, team_id, matches_played, total_minutes,
    total_goals, total_assists, total_passes_completed, total_tackles,
    total_interceptions, total_shots_on_target, avg_rating, goals_per_90,
    assists_per_90, pass_accuracy, composite_score, updated_at
) VALUES (
    src.player_id, src.season, src.position, src.team_id, src.matches_played,
    src.total_minutes, src.total_goals, src.total_assists, src.total_passes_completed,
    src.total_tackles, src.total_interceptions, src.total_shots_on_target, src.avg_rating,
    src.goals_per_90, src.assists_per_90, src.pass_accuracy, src.composite_score,
    src.updated_at
);

-- =============================================================================
-- STEP 2: GOLD - Dimensions
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.dim_player AS tgt
USING (
    SELECT player_id AS player_key, player_id, name, position, nationality, age,
           height_cm, weight_kg, foot, market_value_m, contract_end_date
    FROM {{zone_prefix}}.bronze.raw_players
) AS src
ON tgt.player_key = src.player_key
WHEN MATCHED THEN UPDATE SET tgt.market_value_m = src.market_value_m, tgt.age = src.age
WHEN NOT MATCHED THEN INSERT (player_key, player_id, name, position, nationality, age, height_cm, weight_kg, foot, market_value_m, contract_end_date)
VALUES (src.player_key, src.player_id, src.name, src.position, src.nationality, src.age, src.height_cm, src.weight_kg, src.foot, src.market_value_m, src.contract_end_date);

MERGE INTO {{zone_prefix}}.gold.dim_team AS tgt
USING (
    SELECT team_id AS team_key, team_id, team_name, league, city, country, stadium, capacity, manager
    FROM {{zone_prefix}}.bronze.raw_teams
) AS src
ON tgt.team_key = src.team_key
WHEN MATCHED THEN UPDATE SET tgt.manager = src.manager
WHEN NOT MATCHED THEN INSERT (team_key, team_id, team_name, league, city, country, stadium, capacity, manager)
VALUES (src.team_key, src.team_id, src.team_name, src.league, src.city, src.country, src.stadium, src.capacity, src.manager);

MERGE INTO {{zone_prefix}}.gold.dim_match AS tgt
USING (
    SELECT
        m.match_id AS match_key, m.match_id,
        ht.team_name AS home_team, at.team_name AS away_team,
        m.match_date, m.competition, m.venue, m.attendance,
        m.home_score, m.away_score
    FROM {{zone_prefix}}.bronze.raw_matches m
    JOIN {{zone_prefix}}.bronze.raw_teams ht ON m.home_team_id = ht.team_id
    JOIN {{zone_prefix}}.bronze.raw_teams at ON m.away_team_id = at.team_id
) AS src
ON tgt.match_key = src.match_key
WHEN MATCHED THEN UPDATE SET tgt.home_score = src.home_score, tgt.away_score = src.away_score
WHEN NOT MATCHED THEN INSERT (match_key, match_id, home_team, away_team, match_date, competition, venue, attendance, home_score, away_score)
VALUES (src.match_key, src.match_id, src.home_team, src.away_team, src.match_date, src.competition, src.venue, src.attendance, src.home_score, src.away_score);

MERGE INTO {{zone_prefix}}.gold.dim_season AS tgt
USING (
    SELECT DISTINCT
        season AS season_key, season AS season_name,
        MIN(match_date) AS start_date, MAX(match_date) AS end_date,
        COUNT(DISTINCT match_id) AS total_matchdays
    FROM {{zone_prefix}}.bronze.raw_matches
    GROUP BY season
) AS src
ON tgt.season_key = src.season_key
WHEN MATCHED THEN UPDATE SET tgt.total_matchdays = src.total_matchdays
WHEN NOT MATCHED THEN INSERT (season_key, season_name, start_date, end_date, total_matchdays)
VALUES (src.season_key, src.season_name, src.start_date, src.end_date, src.total_matchdays);

-- =============================================================================
-- STEP 3: GOLD - Fact: fact_match_performance
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.fact_match_performance AS tgt
USING (
    SELECT
        ms.stat_id AS performance_key,
        ms.player_id AS player_key,
        ms.team_id AS team_key,
        ms.match_id AS match_key,
        ms.season AS season_key,
        ms.minutes_played,
        ms.goals,
        ms.assists,
        ms.passes_completed,
        CAST(ms.passes_completed * 100.0 / NULLIF(ms.passes_attempted, 0) AS DECIMAL(5,2)) AS pass_pct,
        ms.tackles,
        ms.interceptions,
        ms.shots_on_target,
        ms.rating
    FROM {{zone_prefix}}.bronze.raw_match_stats ms
) AS src
ON tgt.performance_key = src.performance_key
WHEN MATCHED THEN UPDATE SET tgt.rating = src.rating
WHEN NOT MATCHED THEN INSERT (
    performance_key, player_key, team_key, match_key, season_key,
    minutes_played, goals, assists, passes_completed, pass_pct,
    tackles, interceptions, shots_on_target, rating
) VALUES (
    src.performance_key, src.player_key, src.team_key, src.match_key, src.season_key,
    src.minutes_played, src.goals, src.assists, src.passes_completed, src.pass_pct,
    src.tackles, src.interceptions, src.shots_on_target, src.rating
);

-- =============================================================================
-- STEP 4: GOLD - KPI: kpi_player_rankings with league and position ranks
-- =============================================================================

MERGE INTO {{zone_prefix}}.gold.kpi_player_rankings AS tgt
USING (
    SELECT
        player_id,
        season,
        position,
        total_goals,
        total_assists,
        avg_rating,
        CASE WHEN total_goals > 0
            THEN CAST(total_minutes * 1.0 / total_goals AS DECIMAL(8,2))
            ELSE 9999.00
        END AS minutes_per_goal,
        pass_accuracy,
        total_tackles + total_interceptions AS defensive_contribution,
        composite_score,
        ROW_NUMBER() OVER (PARTITION BY season ORDER BY composite_score DESC) AS league_rank,
        ROW_NUMBER() OVER (PARTITION BY season, position ORDER BY composite_score DESC) AS position_rank
    FROM {{zone_prefix}}.silver.player_rolling_stats
) AS src
ON tgt.player_id = src.player_id AND tgt.season = src.season
WHEN MATCHED THEN UPDATE SET
    tgt.total_goals             = src.total_goals,
    tgt.total_assists           = src.total_assists,
    tgt.avg_rating              = src.avg_rating,
    tgt.minutes_per_goal        = src.minutes_per_goal,
    tgt.composite_score         = src.composite_score,
    tgt.league_rank             = src.league_rank,
    tgt.position_rank           = src.position_rank
WHEN NOT MATCHED THEN INSERT (
    player_id, season, position, total_goals, total_assists, avg_rating,
    minutes_per_goal, pass_accuracy, defensive_contribution, composite_score,
    league_rank, position_rank
) VALUES (
    src.player_id, src.season, src.position, src.total_goals, src.total_assists,
    src.avg_rating, src.minutes_per_goal, src.pass_accuracy, src.defensive_contribution,
    src.composite_score, src.league_rank, src.position_rank
);

-- =============================================================================
-- STEP 5: GOLD - Graph: Build player network vertices and edges
-- =============================================================================

-- Vertices: All players
MERGE INTO {{zone_prefix}}.gold.player_network_vertices AS tgt
USING (
    SELECT
        player_id AS id,
        'Player' AS type,
        name,
        position,
        current_team_id AS team
    FROM {{zone_prefix}}.bronze.raw_players
) AS src
ON tgt.id = src.id
WHEN MATCHED THEN UPDATE SET tgt.team = src.team
WHEN NOT MATCHED THEN INSERT (id, type, name, position, team)
VALUES (src.id, src.type, src.name, src.position, src.team);

-- Edges: TEAMMATE (same team, same season, weight = mutual matches)
MERGE INTO {{zone_prefix}}.gold.player_network_edges AS tgt
USING (
    SELECT DISTINCT
        a.player_id AS src,
        b.player_id AS dst,
        CAST(COUNT(DISTINCT a.match_id) AS DECIMAL(6,2)) AS weight,
        'TEAMMATE' AS type,
        a.season
    FROM {{zone_prefix}}.bronze.raw_match_stats a
    JOIN {{zone_prefix}}.bronze.raw_match_stats b
        ON a.match_id = b.match_id AND a.team_id = b.team_id AND a.player_id < b.player_id
    GROUP BY a.player_id, b.player_id, a.season
) AS src
ON tgt.src = src.src AND tgt.dst = src.dst AND tgt.type = src.type AND tgt.season = src.season
WHEN MATCHED THEN UPDATE SET tgt.weight = src.weight
WHEN NOT MATCHED THEN INSERT (src, dst, weight, type, season)
VALUES (src.src, src.dst, src.weight, src.type, src.season);

-- Edges: OPPONENT (faced each other in a match)
MERGE INTO {{zone_prefix}}.gold.player_network_edges AS tgt
USING (
    SELECT DISTINCT
        a.player_id AS src,
        b.player_id AS dst,
        CAST(COUNT(DISTINCT a.match_id) AS DECIMAL(6,2)) AS weight,
        'OPPONENT' AS type,
        a.season
    FROM {{zone_prefix}}.bronze.raw_match_stats a
    JOIN {{zone_prefix}}.bronze.raw_match_stats b
        ON a.match_id = b.match_id AND a.team_id != b.team_id AND a.player_id < b.player_id
    GROUP BY a.player_id, b.player_id, a.season
) AS src
ON tgt.src = src.src AND tgt.dst = src.dst AND tgt.type = src.type AND tgt.season = src.season
WHEN MATCHED THEN UPDATE SET tgt.weight = src.weight
WHEN NOT MATCHED THEN INSERT (src, dst, weight, type, season)
VALUES (src.src, src.dst, src.weight, src.type, src.season);

-- Edges: TRANSFER
MERGE INTO {{zone_prefix}}.gold.player_network_edges AS tgt
USING (
    SELECT
        t.player_id || '-' || t.from_team_id AS src,
        t.player_id || '-' || t.to_team_id AS dst,
        COALESCE(t.fee_m, 0.00) AS weight,
        'TRANSFER' AS type,
        CAST(NULL AS STRING) AS season
    FROM {{zone_prefix}}.bronze.raw_transfers t
) AS src
ON tgt.src = src.src AND tgt.dst = src.dst AND tgt.type = src.type
WHEN NOT MATCHED THEN INSERT (src, dst, weight, type, season)
VALUES (src.src, src.dst, src.weight, src.type, src.season);

-- =============================================================================
-- STEP 6: Graph queries - Transfer network analysis
-- =============================================================================

-- PageRank: Most connected players in the network
USE {{zone_prefix}}.gold.player_network
CALL algo.pageRank({'maxIterations': 20, 'dampingFactor': 0.85}) YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 10;

-- Community detection for playing style clusters
USE {{zone_prefix}}.gold.player_network
MATCH (n)-[:TEAMMATE]->(m)
RETURN n.name, m.name, n.position, m.position
ORDER BY n.name;

-- =============================================================================
-- OPTIMIZE with Z-ordering for fast player+season lookups
-- =============================================================================

OPTIMIZE {{zone_prefix}}.silver.player_rolling_stats;
OPTIMIZE {{zone_prefix}}.gold.fact_match_performance;
OPTIMIZE {{zone_prefix}}.gold.kpi_player_rankings;
