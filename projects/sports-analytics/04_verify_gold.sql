-- =============================================================================
-- Sports Analytics Pipeline: Gold Layer Verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Verify fact_match_performance row count
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS fact_performance_count
FROM {{zone_prefix}}.gold.fact_match_performance;

-- -----------------------------------------------------------------------------
-- 2. Verify all 20 players loaded into dim_player
-- -----------------------------------------------------------------------------
ASSERT VALUE fact_performance_count >= 75
SELECT COUNT(*) AS player_count
FROM {{zone_prefix}}.gold.dim_player;

-- -----------------------------------------------------------------------------
-- 3. Verify all 4 teams loaded into dim_team
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS team_count
FROM {{zone_prefix}}.gold.dim_team;

-- -----------------------------------------------------------------------------
-- 4. Top scorers across both seasons with goals per 90
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 4
SELECT
    dp.name,
    dp.position,
    dp.nationality,
    SUM(fp.goals) AS total_goals,
    SUM(fp.assists) AS total_assists,
    CAST(SUM(fp.goals) * 90.0 / NULLIF(SUM(fp.minutes_played), 0) AS DECIMAL(6,3)) AS goals_per_90,
    CAST(AVG(fp.rating) AS DECIMAL(4,2)) AS avg_rating,
    ROW_NUMBER() OVER (ORDER BY SUM(fp.goals) DESC) AS goal_rank
FROM {{zone_prefix}}.gold.fact_match_performance fp
JOIN {{zone_prefix}}.gold.dim_player dp ON fp.player_key = dp.player_key
GROUP BY dp.name, dp.position, dp.nationality
ORDER BY total_goals DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 5. Position-specific leaderboards (composite score)
-- -----------------------------------------------------------------------------
ASSERT VALUE total_goals > 0
SELECT
    kpr.position,
    dp.name,
    kpr.season,
    kpr.composite_score,
    kpr.total_goals,
    kpr.total_assists,
    kpr.pass_accuracy,
    kpr.defensive_contribution,
    kpr.position_rank
FROM {{zone_prefix}}.gold.kpi_player_rankings kpr
JOIN {{zone_prefix}}.gold.dim_player dp ON kpr.player_id = dp.player_key
WHERE kpr.position_rank <= 3
ORDER BY kpr.position, kpr.season, kpr.position_rank;

-- -----------------------------------------------------------------------------
-- 6. Team performance comparison by season
-- -----------------------------------------------------------------------------
ASSERT VALUE composite_score > 0
SELECT
    dt.team_name,
    fp.season_key AS season,
    COUNT(DISTINCT fp.match_key) AS matches_played,
    SUM(fp.goals) AS total_goals,
    SUM(fp.assists) AS total_assists,
    CAST(AVG(fp.pass_pct) AS DECIMAL(5,2)) AS avg_pass_accuracy,
    CAST(AVG(fp.rating) AS DECIMAL(4,2)) AS avg_rating
FROM {{zone_prefix}}.gold.fact_match_performance fp
JOIN {{zone_prefix}}.gold.dim_team dt ON fp.team_key = dt.team_key
GROUP BY dt.team_name, fp.season_key
ORDER BY dt.team_name, season;

-- -----------------------------------------------------------------------------
-- 7. Player rating trend across seasons using LAG
-- -----------------------------------------------------------------------------
ASSERT VALUE matches_played > 0
SELECT
    dp.name,
    dp.position,
    kpr.season,
    kpr.avg_rating,
    LAG(kpr.avg_rating) OVER (PARTITION BY kpr.player_id ORDER BY kpr.season) AS prev_season_rating,
    CAST(
        kpr.avg_rating - LAG(kpr.avg_rating) OVER (PARTITION BY kpr.player_id ORDER BY kpr.season)
    AS DECIMAL(4,2)) AS rating_change
FROM {{zone_prefix}}.gold.kpi_player_rankings kpr
JOIN {{zone_prefix}}.gold.dim_player dp ON kpr.player_id = dp.player_key
ORDER BY dp.name, kpr.season;

-- -----------------------------------------------------------------------------
-- 8. Match outcome analysis with attendance
-- -----------------------------------------------------------------------------
ASSERT VALUE avg_rating > 0
SELECT
    dm.competition,
    dm.venue,
    dm.home_team,
    dm.away_team,
    dm.home_score || '-' || dm.away_score AS score,
    dm.attendance,
    CASE
        WHEN dm.home_score > dm.away_score THEN 'Home Win'
        WHEN dm.home_score < dm.away_score THEN 'Away Win'
        ELSE 'Draw'
    END AS result
FROM {{zone_prefix}}.gold.dim_match dm
ORDER BY dm.match_date;

-- -----------------------------------------------------------------------------
-- 9. Referential integrity: all fact foreign keys exist in dimensions
-- -----------------------------------------------------------------------------
ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS orphaned_players
FROM {{zone_prefix}}.gold.fact_match_performance fp
LEFT JOIN {{zone_prefix}}.gold.dim_player dp ON fp.player_key = dp.player_key
WHERE dp.player_key IS NULL;

ASSERT VALUE orphaned_players = 0

SELECT COUNT(*) AS orphaned_teams
FROM {{zone_prefix}}.gold.fact_match_performance fp
LEFT JOIN {{zone_prefix}}.gold.dim_team dt ON fp.team_key = dt.team_key
WHERE dt.team_key IS NULL;

ASSERT VALUE orphaned_teams = 0
SELECT 'orphaned_teams check passed' AS orphaned_teams_status;


-- -----------------------------------------------------------------------------
-- 10. Graph queries: Player network - teammate connectivity and transfers
-- -----------------------------------------------------------------------------
USE {{zone_prefix}}.gold.player_network
MATCH (n)-[:TEAMMATE]->(m)
RETURN n.name AS player1, m.name AS player2, n.position AS pos1, m.position AS pos2
ORDER BY player1
LIMIT 20;

USE {{zone_prefix}}.gold.player_network
CALL algo.pageRank({'maxIterations': 20, 'dampingFactor': 0.85}) YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 5;
