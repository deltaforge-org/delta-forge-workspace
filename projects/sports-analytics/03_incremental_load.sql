-- =============================================================================
-- Sports Analytics Pipeline: Incremental Load
-- =============================================================================
-- Demonstrates incremental processing using the INCREMENTAL_FILTER macro.
-- New match results and stats from late-season 2023-24 are added.

-- Show current state before incremental load
SELECT 'silver.player_rolling_stats' AS table_name, COUNT(*) AS row_count
FROM {{zone_prefix}}.silver.player_rolling_stats
UNION ALL
SELECT 'gold.fact_match_performance', COUNT(*)
FROM {{zone_prefix}}.gold.fact_match_performance;

-- =============================================================================
-- Insert new match + stats (May 2024 - end of season)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_matches VALUES
('M013', 'TM-002', 'TM-003', '2024-05-01', '2023-24', 'Champions League', 'Etihad Stadium', 53300, 2, 1, '2024-05-02T00:00:00');

ASSERT ROW_COUNT = 1

INSERT INTO {{zone_prefix}}.bronze.raw_match_stats VALUES
('S076', 'M013', 'P006', 'TM-002', '2023-24', 90, 2, 0, 14, 18, 0, 0, 6, 9.50, '2024-05-02T00:00:00'),
('S077', 'M013', 'P007', 'TM-002', '2023-24', 88, 0, 2, 68, 78, 1, 2, 1, 9.00, '2024-05-02T00:00:00'),
('S078', 'M013', 'P008', 'TM-002', '2023-24', 90, 0, 0, 72, 80, 5, 3, 0, 8.00, '2024-05-02T00:00:00'),
('S079', 'M013', 'P011', 'TM-003', '2023-24', 90, 1, 0, 16, 22, 0, 0, 4, 7.50, '2024-05-02T00:00:00'),
('S080', 'M013', 'P012', 'TM-003', '2023-24', 90, 0, 1, 65, 76, 2, 2, 1, 7.20, '2024-05-02T00:00:00'),
('S081', 'M013', 'P015', 'TM-003', '2023-24', 85, 0, 0, 30, 38, 1, 0, 2, 6.50, '2024-05-02T00:00:00');

ASSERT ROW_COUNT = 6
SELECT 'row count check' AS status;


-- =============================================================================
-- Incremental MERGE for fact_match_performance using INCREMENTAL_FILTER
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
    JOIN {{zone_prefix}}.bronze.raw_matches m ON ms.match_id = m.match_id
    WHERE {{INCREMENTAL_FILTER({{zone_prefix}}.gold.fact_match_performance, performance_key, null)}}
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

-- Refresh rolling stats (full recompute since it is an upsert aggregate)
MERGE INTO {{zone_prefix}}.silver.player_rolling_stats AS tgt
USING (
    WITH player_agg AS (
        SELECT
            ms.player_id, ms.season, p.position, ms.team_id,
            COUNT(*) AS matches_played,
            SUM(ms.minutes_played) AS total_minutes,
            SUM(ms.goals) AS total_goals,
            SUM(ms.assists) AS total_assists,
            SUM(ms.passes_completed) AS total_passes_completed,
            SUM(ms.tackles) AS total_tackles,
            SUM(ms.interceptions) AS total_interceptions,
            SUM(ms.shots_on_target) AS total_shots_on_target,
            CAST(AVG(ms.rating) AS DECIMAL(4,2)) AS avg_rating,
            CASE WHEN SUM(ms.minutes_played) > 0 THEN CAST(SUM(ms.goals) * 90.0 / SUM(ms.minutes_played) AS DECIMAL(6,3)) ELSE 0.000 END AS goals_per_90,
            CASE WHEN SUM(ms.minutes_played) > 0 THEN CAST(SUM(ms.assists) * 90.0 / SUM(ms.minutes_played) AS DECIMAL(6,3)) ELSE 0.000 END AS assists_per_90,
            CASE WHEN SUM(ms.passes_attempted) > 0 THEN CAST(SUM(ms.passes_completed) * 100.0 / SUM(ms.passes_attempted) AS DECIMAL(5,2)) ELSE 0.00 END AS pass_accuracy,
            CASE WHEN SUM(ms.minutes_played) > 0 THEN CAST(SUM(ms.shots_on_target) * 90.0 / SUM(ms.minutes_played) AS DECIMAL(6,3)) ELSE 0.000 END AS shots_per_90,
            CASE WHEN SUM(ms.minutes_played) > 0 THEN CAST((SUM(ms.tackles) + SUM(ms.interceptions)) * 90.0 / SUM(ms.minutes_played) AS DECIMAL(6,3)) ELSE 0.000 END AS def_per_90
        FROM {{zone_prefix}}.bronze.raw_match_stats ms
        JOIN {{zone_prefix}}.bronze.raw_players p ON ms.player_id = p.player_id
        GROUP BY ms.player_id, ms.season, p.position, ms.team_id
    )
    SELECT *, CAST(CASE position
        WHEN 'Forward' THEN 0.40 * goals_per_90 * 10.0 + 0.25 * assists_per_90 * 10.0 + 0.20 * avg_rating + 0.15 * shots_per_90 * 5.0
        WHEN 'Midfielder' THEN 0.20 * goals_per_90 * 10.0 + 0.30 * assists_per_90 * 10.0 + 0.30 * pass_accuracy / 10.0 + 0.20 * avg_rating
        WHEN 'Defender' THEN 0.10 * goals_per_90 * 10.0 + 0.10 * assists_per_90 * 10.0 + 0.30 * def_per_90 * 3.0 + 0.30 * avg_rating + 0.20 * pass_accuracy / 10.0
        ELSE avg_rating END AS DECIMAL(6,2)) AS composite_score,
        CURRENT_TIMESTAMP AS updated_at
    FROM player_agg
) AS src
ON tgt.player_id = src.player_id AND tgt.season = src.season
WHEN MATCHED THEN UPDATE SET
    tgt.total_goals = src.total_goals, tgt.total_assists = src.total_assists,
    tgt.avg_rating = src.avg_rating, tgt.composite_score = src.composite_score,
    tgt.updated_at = src.updated_at
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
-- Verify incremental processing
-- =============================================================================

-- Fact should now have 75 + 6 = 81 performance records
SELECT COUNT(*) AS fact_total FROM {{zone_prefix}}.gold.fact_match_performance;
-- Verify match M013 stats appeared
ASSERT VALUE fact_total = 81
SELECT COUNT(*) AS m013_stats
FROM {{zone_prefix}}.gold.fact_match_performance
WHERE match_key = 'M013';

ASSERT VALUE m013_stats = 6
SELECT 'm013_stats check passed' AS m013_stats_status;

