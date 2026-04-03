-- =============================================================================
-- Sports Analytics Pipeline: Create Objects & Seed Data
-- =============================================================================

-- =============================================================================
-- ZONES
-- =============================================================================


-- =============================================================================
-- SCHEMAS
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw player, team, match, and transfer data';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Rolling player aggregates and per-90 stats';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold COMMENT 'Star schema, graph, and KPI player rankings';

-- =============================================================================
-- BRONZE TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_players (
    player_id           STRING      NOT NULL,
    name                STRING      NOT NULL,
    position            STRING      NOT NULL,
    nationality         STRING      NOT NULL,
    age                 INT         NOT NULL,
    height_cm           INT,
    weight_kg           INT,
    foot                STRING,
    market_value_m      DECIMAL(8,2),
    contract_end_date   DATE,
    current_team_id     STRING      NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/sports/bronze/raw_players';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_players TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_teams (
    team_id             STRING      NOT NULL,
    team_name           STRING      NOT NULL,
    league              STRING      NOT NULL,
    city                STRING      NOT NULL,
    country             STRING      NOT NULL,
    stadium             STRING      NOT NULL,
    capacity            INT,
    manager             STRING      NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/sports/bronze/raw_teams';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_teams TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_matches (
    match_id            STRING      NOT NULL,
    home_team_id        STRING      NOT NULL,
    away_team_id        STRING      NOT NULL,
    match_date          DATE        NOT NULL,
    season              STRING      NOT NULL,
    competition         STRING      NOT NULL,
    venue               STRING      NOT NULL,
    attendance          INT,
    home_score          INT         NOT NULL,
    away_score          INT         NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/sports/bronze/raw_matches';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_matches TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_match_stats (
    stat_id             STRING      NOT NULL,
    match_id            STRING      NOT NULL,
    player_id           STRING      NOT NULL,
    team_id             STRING      NOT NULL,
    season              STRING      NOT NULL,
    minutes_played      INT         NOT NULL,
    goals               INT         NOT NULL,
    assists             INT         NOT NULL,
    passes_completed    INT         NOT NULL,
    passes_attempted    INT         NOT NULL,
    tackles             INT         NOT NULL,
    interceptions       INT         NOT NULL,
    shots_on_target     INT         NOT NULL,
    rating              DECIMAL(4,2) NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/sports/bronze/raw_match_stats';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_match_stats TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_transfers (
    transfer_id         STRING      NOT NULL,
    player_id           STRING      NOT NULL,
    from_team_id        STRING      NOT NULL,
    to_team_id          STRING      NOT NULL,
    transfer_date       DATE        NOT NULL,
    fee_m               DECIMAL(8,2),
    ingested_at         TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/sports/bronze/raw_transfers';

GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_transfers TO USER {{current_user}};

-- =============================================================================
-- SILVER TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.player_rolling_stats (
    player_id               STRING      NOT NULL,
    season                  STRING      NOT NULL,
    position                STRING,
    team_id                 STRING,
    matches_played          INT,
    total_minutes           INT,
    total_goals             INT,
    total_assists           INT,
    total_passes_completed  INT,
    total_tackles           INT,
    total_interceptions     INT,
    total_shots_on_target   INT,
    avg_rating              DECIMAL(4,2),
    goals_per_90            DECIMAL(6,3),
    assists_per_90          DECIMAL(6,3),
    pass_accuracy           DECIMAL(5,2),
    composite_score         DECIMAL(6,2),
    updated_at              TIMESTAMP   NOT NULL
) LOCATION '{{data_path}}/sports/silver/player_rolling_stats'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT ADMIN ON TABLE {{zone_prefix}}.silver.player_rolling_stats TO USER {{current_user}};

-- =============================================================================
-- GOLD TABLES
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_player (
    player_key          STRING      NOT NULL,
    player_id           STRING      NOT NULL,
    name                STRING      NOT NULL,
    position            STRING,
    nationality         STRING,
    age                 INT,
    height_cm           INT,
    weight_kg           INT,
    foot                STRING,
    market_value_m      DECIMAL(8,2),
    contract_end_date   DATE
) LOCATION '{{data_path}}/sports/gold/dim_player';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_player TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_team (
    team_key            STRING      NOT NULL,
    team_id             STRING      NOT NULL,
    team_name           STRING      NOT NULL,
    league              STRING,
    city                STRING,
    country             STRING,
    stadium             STRING,
    capacity            INT,
    manager             STRING
) LOCATION '{{data_path}}/sports/gold/dim_team';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_team TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_match (
    match_key           STRING      NOT NULL,
    match_id            STRING      NOT NULL,
    home_team           STRING      NOT NULL,
    away_team           STRING      NOT NULL,
    match_date          DATE,
    competition         STRING,
    venue               STRING,
    attendance          INT,
    home_score          INT,
    away_score          INT
) LOCATION '{{data_path}}/sports/gold/dim_match';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_match TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_season (
    season_key          STRING      NOT NULL,
    season_name         STRING      NOT NULL,
    start_date          DATE,
    end_date            DATE,
    total_matchdays     INT
) LOCATION '{{data_path}}/sports/gold/dim_season';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_season TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_match_performance (
    performance_key     STRING      NOT NULL,
    player_key          STRING      NOT NULL,
    team_key            STRING      NOT NULL,
    match_key           STRING      NOT NULL,
    season_key          STRING      NOT NULL,
    minutes_played      INT,
    goals               INT,
    assists             INT,
    passes_completed    INT,
    pass_pct            DECIMAL(5,2),
    tackles             INT,
    interceptions       INT,
    shots_on_target     INT,
    rating              DECIMAL(4,2)
) LOCATION '{{data_path}}/sports/gold/fact_match_performance';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_match_performance TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_player_rankings (
    player_id           STRING      NOT NULL,
    season              STRING      NOT NULL,
    position            STRING,
    total_goals         INT,
    total_assists       INT,
    avg_rating          DECIMAL(4,2),
    minutes_per_goal    DECIMAL(8,2),
    pass_accuracy       DECIMAL(5,2),
    defensive_contribution INT,
    composite_score     DECIMAL(6,2),
    league_rank         INT,
    position_rank       INT
) LOCATION '{{data_path}}/sports/gold/kpi_player_rankings';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_player_rankings TO USER {{current_user}};

-- =============================================================================
-- GRAPH: Player Network (vertices and edges tables)
-- =============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.player_network_vertices (
    id                  STRING      NOT NULL,
    type                STRING      NOT NULL,
    name                STRING      NOT NULL,
    position            STRING,
    team                STRING
) LOCATION '{{data_path}}/sports/gold/player_network_vertices';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.player_network_vertices TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.player_network_edges (
    src                 STRING      NOT NULL,
    dst                 STRING      NOT NULL,
    weight              DECIMAL(6,2) NOT NULL,
    type                STRING      NOT NULL,
    season              STRING
) LOCATION '{{data_path}}/sports/gold/player_network_edges';

GRANT ADMIN ON TABLE {{zone_prefix}}.gold.player_network_edges TO USER {{current_user}};

CREATE GRAPH IF NOT EXISTS {{zone_prefix}}.gold.player_network
    VERTEX TABLE {{zone_prefix}}.gold.player_network_vertices
        ID COLUMN id
        NODE TYPE COLUMN type
        NODE NAME COLUMN name
    EDGE TABLE {{zone_prefix}}.gold.player_network_edges
        SOURCE COLUMN src
        TARGET COLUMN dst
        WEIGHT COLUMN weight
        EDGE TYPE COLUMN type
    DIRECTED;

-- =============================================================================
-- SEED DATA: raw_teams (4 teams)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_teams VALUES
('TM-001', 'Arsenal FC',       'Premier League',  'London',      'England',  'Emirates Stadium',     60704,  'Mikel Arteta',     '2024-01-01T00:00:00'),
('TM-002', 'Manchester City',  'Premier League',  'Manchester',  'England',  'Etihad Stadium',       53400,  'Pep Guardiola',    '2024-01-01T00:00:00'),
('TM-003', 'FC Barcelona',     'La Liga',         'Barcelona',   'Spain',    'Camp Nou',             99354,  'Xavi Hernandez',   '2024-01-01T00:00:00'),
('TM-004', 'Bayern Munich',    'Bundesliga',      'Munich',      'Germany',  'Allianz Arena',        75024,  'Thomas Tuchel',    '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 4
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_teams;


-- =============================================================================
-- SEED DATA: raw_players (20 players across 4 teams)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_players VALUES
-- Arsenal (5 players)
('P001', 'Bukayo Saka',        'Forward',    'England',   22, 178, 72, 'Left',  120.00, '2027-06-30', 'TM-001', '2024-01-01T00:00:00'),
('P002', 'Martin Odegaard',    'Midfielder', 'Norway',    25, 178, 68, 'Left',   90.00, '2028-06-30', 'TM-001', '2024-01-01T00:00:00'),
('P003', 'William Saliba',     'Defender',   'France',    23, 192, 86, 'Right',  80.00, '2027-06-30', 'TM-001', '2024-01-01T00:00:00'),
('P004', 'Declan Rice',        'Midfielder', 'England',   25, 185, 80, 'Right', 100.00, '2028-06-30', 'TM-001', '2024-01-01T00:00:00'),
('P005', 'Gabriel Jesus',      'Forward',    'Brazil',    27, 175, 73, 'Right',  45.00, '2027-06-30', 'TM-001', '2024-01-01T00:00:00'),
-- Manchester City (5 players)
('P006', 'Erling Haaland',     'Forward',    'Norway',    23, 194, 88, 'Left',  180.00, '2027-06-30', 'TM-002', '2024-01-01T00:00:00'),
('P007', 'Kevin De Bruyne',    'Midfielder', 'Belgium',   32, 181, 76, 'Right', 75.00,  '2025-06-30', 'TM-002', '2024-01-01T00:00:00'),
('P008', 'Rodri',              'Midfielder', 'Spain',     27, 191, 82, 'Right', 110.00, '2027-06-30', 'TM-002', '2024-01-01T00:00:00'),
('P009', 'Phil Foden',         'Forward',    'England',   23, 171, 69, 'Left',  110.00, '2027-06-30', 'TM-002', '2024-01-01T00:00:00'),
('P010', 'Ruben Dias',         'Defender',   'Portugal',  26, 186, 82, 'Right', 75.00,  '2027-06-30', 'TM-002', '2024-01-01T00:00:00'),
-- FC Barcelona (5 players)
('P011', 'Robert Lewandowski', 'Forward',    'Poland',    35, 185, 81, 'Right', 25.00,  '2025-06-30', 'TM-003', '2024-01-01T00:00:00'),
('P012', 'Pedri',              'Midfielder', 'Spain',     21, 174, 63, 'Right', 100.00, '2026-06-30', 'TM-003', '2024-01-01T00:00:00'),
('P013', 'Ronald Araujo',      'Defender',   'Uruguay',   25, 188, 86, 'Right', 60.00,  '2026-06-30', 'TM-003', '2024-01-01T00:00:00'),
('P014', 'Gavi',               'Midfielder', 'Spain',     19, 173, 68, 'Right', 90.00,  '2026-06-30', 'TM-003', '2024-01-01T00:00:00'),
('P015', 'Lamine Yamal',       'Forward',    'Spain',     16, 180, 71, 'Left',  60.00,  '2026-06-30', 'TM-003', '2024-01-01T00:00:00'),
-- Bayern Munich (5 players)
('P016', 'Harry Kane',         'Forward',    'England',   30, 188, 86, 'Right', 100.00, '2027-06-30', 'TM-004', '2024-01-01T00:00:00'),
('P017', 'Jamal Musiala',      'Midfielder', 'Germany',   21, 183, 72, 'Right', 110.00, '2026-06-30', 'TM-004', '2024-01-01T00:00:00'),
('P018', 'Kim Min-jae',        'Defender',   'South Korea',27, 190, 88, 'Right', 55.00, '2028-06-30', 'TM-004', '2024-01-01T00:00:00'),
('P019', 'Leroy Sane',         'Forward',    'Germany',   28, 183, 80, 'Left',  55.00,  '2025-06-30', 'TM-004', '2024-01-01T00:00:00'),
('P020', 'Joshua Kimmich',     'Midfielder', 'Germany',   29, 177, 75, 'Right', 70.00,  '2025-06-30', 'TM-004', '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 20
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_players;


-- =============================================================================
-- SEED DATA: raw_transfers (3 transfers between seasons)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_transfers VALUES
('TR-001', 'P016', 'TM-002', 'TM-004', '2023-08-01', 100.00, '2024-01-01T00:00:00'),
('TR-002', 'P004', 'TM-003', 'TM-001', '2023-07-15',  90.00, '2024-01-01T00:00:00'),
('TR-003', 'P019', 'TM-002', 'TM-004', '2022-07-01',  40.00, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 3
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_transfers;


-- =============================================================================
-- SEED DATA: raw_matches (12 matches across 2 seasons)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_matches VALUES
-- Season 2022-23 (6 matches)
('M001', 'TM-001', 'TM-002', '2022-10-15', '2022-23', 'Premier League',  'Emirates Stadium',  60500, 1, 3, '2024-01-01T00:00:00'),
('M002', 'TM-003', 'TM-004', '2022-10-26', '2022-23', 'Champions League','Camp Nou',           88000, 3, 0, '2024-01-01T00:00:00'),
('M003', 'TM-002', 'TM-001', '2023-01-14', '2022-23', 'Premier League',  'Etihad Stadium',     53200, 4, 1, '2024-01-01T00:00:00'),
('M004', 'TM-004', 'TM-003', '2023-02-14', '2022-23', 'Champions League','Allianz Arena',      75000, 2, 0, '2024-01-01T00:00:00'),
('M005', 'TM-001', 'TM-003', '2023-03-08', '2022-23', 'Champions League','Emirates Stadium',   60200, 1, 1, '2024-01-01T00:00:00'),
('M006', 'TM-002', 'TM-004', '2023-04-11', '2022-23', 'Champions League','Etihad Stadium',     52800, 3, 1, '2024-01-01T00:00:00'),
-- Season 2023-24 (6 matches)
('M007', 'TM-002', 'TM-001', '2023-10-08', '2023-24', 'Premier League',  'Etihad Stadium',     53400, 1, 0, '2024-01-01T00:00:00'),
('M008', 'TM-003', 'TM-004', '2023-10-25', '2023-24', 'Champions League','Camp Nou',           91000, 1, 2, '2024-01-01T00:00:00'),
('M009', 'TM-001', 'TM-002', '2024-01-13', '2023-24', 'Premier League',  'Emirates Stadium',   60700, 0, 1, '2024-01-01T00:00:00'),
('M010', 'TM-004', 'TM-003', '2024-02-20', '2023-24', 'Champions League','Allianz Arena',      74500, 3, 2, '2024-01-01T00:00:00'),
('M011', 'TM-003', 'TM-001', '2024-03-12', '2023-24', 'Champions League','Camp Nou',           88500, 2, 1, '2024-01-01T00:00:00'),
('M012', 'TM-001', 'TM-004', '2024-04-09', '2023-24', 'Champions League','Emirates Stadium',   60400, 2, 2, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_matches;


-- =============================================================================
-- SEED DATA: raw_match_stats (75 rows - player stats per match)
-- =============================================================================

INSERT INTO {{zone_prefix}}.bronze.raw_match_stats VALUES
-- M001: Arsenal 1-3 Man City (2022-23)
('S001', 'M001', 'P001', 'TM-001', '2022-23', 90, 1, 0, 38, 52, 2, 1, 4, 7.20, '2024-01-01T00:00:00'),
('S002', 'M001', 'P002', 'TM-001', '2022-23', 90, 0, 1, 62, 75, 1, 2, 1, 7.00, '2024-01-01T00:00:00'),
('S003', 'M001', 'P003', 'TM-001', '2022-23', 90, 0, 0, 55, 60, 5, 3, 0, 6.50, '2024-01-01T00:00:00'),
('S004', 'M001', 'P006', 'TM-002', '2022-23', 90, 2, 0, 18, 22, 0, 0, 5, 9.00, '2024-01-01T00:00:00'),
('S005', 'M001', 'P007', 'TM-002', '2022-23', 85, 0, 2, 72, 82, 2, 1, 2, 8.50, '2024-01-01T00:00:00'),
('S006', 'M001', 'P008', 'TM-002', '2022-23', 90, 1, 0, 68, 74, 4, 3, 1, 8.00, '2024-01-01T00:00:00'),
('S007', 'M001', 'P009', 'TM-002', '2022-23', 78, 0, 1, 35, 42, 1, 0, 3, 7.50, '2024-01-01T00:00:00'),
-- M002: Barcelona 3-0 Bayern (2022-23)
('S008', 'M002', 'P011', 'TM-003', '2022-23', 90, 2, 0, 22, 28, 0, 0, 6, 9.20, '2024-01-01T00:00:00'),
('S009', 'M002', 'P012', 'TM-003', '2022-23', 90, 0, 2, 75, 85, 2, 3, 1, 8.80, '2024-01-01T00:00:00'),
('S010', 'M002', 'P014', 'TM-003', '2022-23', 82, 1, 0, 58, 68, 3, 2, 2, 7.80, '2024-01-01T00:00:00'),
('S011', 'M002', 'P013', 'TM-003', '2022-23', 90, 0, 0, 42, 48, 6, 4, 0, 7.50, '2024-01-01T00:00:00'),
('S012', 'M002', 'P016', 'TM-004', '2022-23', 90, 0, 0, 15, 20, 1, 0, 2, 5.50, '2024-01-01T00:00:00'),
('S013', 'M002', 'P017', 'TM-004', '2022-23', 86, 0, 0, 48, 58, 2, 1, 1, 6.00, '2024-01-01T00:00:00'),
('S014', 'M002', 'P020', 'TM-004', '2022-23', 90, 0, 0, 55, 62, 3, 2, 0, 6.20, '2024-01-01T00:00:00'),
-- M003: Man City 4-1 Arsenal (2022-23)
('S015', 'M003', 'P006', 'TM-002', '2022-23', 90, 3, 0, 20, 25, 0, 0, 7, 9.80, '2024-01-01T00:00:00'),
('S016', 'M003', 'P007', 'TM-002', '2022-23', 90, 0, 3, 78, 88, 1, 2, 3, 9.20, '2024-01-01T00:00:00'),
('S017', 'M003', 'P009', 'TM-002', '2022-23', 90, 1, 0, 42, 50, 1, 0, 4, 8.00, '2024-01-01T00:00:00'),
('S018', 'M003', 'P010', 'TM-002', '2022-23', 90, 0, 0, 52, 56, 7, 4, 0, 7.80, '2024-01-01T00:00:00'),
('S019', 'M003', 'P001', 'TM-001', '2022-23', 90, 1, 0, 35, 48, 1, 1, 3, 6.80, '2024-01-01T00:00:00'),
('S020', 'M003', 'P002', 'TM-001', '2022-23', 90, 0, 1, 58, 72, 2, 1, 1, 6.50, '2024-01-01T00:00:00'),
('S021', 'M003', 'P005', 'TM-001', '2022-23', 75, 0, 0, 12, 18, 0, 0, 2, 5.80, '2024-01-01T00:00:00'),
-- M004: Bayern 2-0 Barcelona (2022-23)
('S022', 'M004', 'P016', 'TM-004', '2022-23', 90, 1, 0, 18, 24, 0, 0, 5, 8.00, '2024-01-01T00:00:00'),
('S023', 'M004', 'P017', 'TM-004', '2022-23', 90, 1, 1, 52, 60, 1, 1, 3, 8.50, '2024-01-01T00:00:00'),
('S024', 'M004', 'P018', 'TM-004', '2022-23', 90, 0, 0, 48, 52, 8, 5, 0, 8.20, '2024-01-01T00:00:00'),
('S025', 'M004', 'P011', 'TM-003', '2022-23', 90, 0, 0, 14, 20, 0, 0, 3, 5.80, '2024-01-01T00:00:00'),
('S026', 'M004', 'P012', 'TM-003', '2022-23', 88, 0, 0, 65, 78, 1, 2, 0, 6.20, '2024-01-01T00:00:00'),
-- M005: Arsenal 1-1 Barcelona (2022-23)
('S027', 'M005', 'P001', 'TM-001', '2022-23', 90, 0, 0, 32, 45, 2, 1, 3, 6.50, '2024-01-01T00:00:00'),
('S028', 'M005', 'P002', 'TM-001', '2022-23', 90, 1, 0, 65, 78, 1, 2, 2, 7.80, '2024-01-01T00:00:00'),
('S029', 'M005', 'P003', 'TM-001', '2022-23', 90, 0, 0, 50, 55, 6, 3, 0, 7.20, '2024-01-01T00:00:00'),
('S030', 'M005', 'P011', 'TM-003', '2022-23', 90, 1, 0, 16, 22, 0, 0, 4, 7.50, '2024-01-01T00:00:00'),
('S031', 'M005', 'P015', 'TM-003', '2022-23', 68, 0, 1, 28, 35, 0, 0, 2, 7.00, '2024-01-01T00:00:00'),
-- M006: Man City 3-1 Bayern (2022-23)
('S032', 'M006', 'P006', 'TM-002', '2022-23', 90, 2, 0, 16, 20, 0, 0, 6, 9.50, '2024-01-01T00:00:00'),
('S033', 'M006', 'P007', 'TM-002', '2022-23', 90, 0, 2, 70, 80, 2, 1, 1, 8.80, '2024-01-01T00:00:00'),
('S034', 'M006', 'P008', 'TM-002', '2022-23', 90, 1, 0, 65, 72, 5, 3, 1, 8.20, '2024-01-01T00:00:00'),
('S035', 'M006', 'P016', 'TM-004', '2022-23', 90, 1, 0, 14, 19, 0, 0, 4, 7.00, '2024-01-01T00:00:00'),
('S036', 'M006', 'P019', 'TM-004', '2022-23', 85, 0, 1, 30, 38, 1, 0, 3, 6.80, '2024-01-01T00:00:00'),
('S037', 'M006', 'P020', 'TM-004', '2022-23', 90, 0, 0, 58, 65, 4, 2, 0, 6.50, '2024-01-01T00:00:00'),
-- M007: Man City 1-0 Arsenal (2023-24)
('S038', 'M007', 'P006', 'TM-002', '2023-24', 90, 1, 0, 15, 20, 0, 0, 4, 8.00, '2024-01-01T00:00:00'),
('S039', 'M007', 'P007', 'TM-002', '2023-24', 75, 0, 1, 55, 65, 1, 1, 1, 7.50, '2024-01-01T00:00:00'),
('S040', 'M007', 'P010', 'TM-002', '2023-24', 90, 0, 0, 48, 52, 6, 3, 0, 7.80, '2024-01-01T00:00:00'),
('S041', 'M007', 'P001', 'TM-001', '2023-24', 90, 0, 0, 30, 42, 1, 1, 2, 6.20, '2024-01-01T00:00:00'),
('S042', 'M007', 'P004', 'TM-001', '2023-24', 90, 0, 0, 52, 60, 5, 4, 0, 7.00, '2024-01-01T00:00:00'),
-- M008: Barcelona 1-2 Bayern (2023-24)
('S043', 'M008', 'P011', 'TM-003', '2023-24', 90, 1, 0, 18, 25, 0, 0, 5, 7.80, '2024-01-01T00:00:00'),
('S044', 'M008', 'P012', 'TM-003', '2023-24', 90, 0, 1, 68, 80, 2, 2, 1, 7.50, '2024-01-01T00:00:00'),
('S045', 'M008', 'P015', 'TM-003', '2023-24', 80, 0, 0, 25, 32, 1, 0, 3, 6.80, '2024-01-01T00:00:00'),
('S046', 'M008', 'P016', 'TM-004', '2023-24', 90, 1, 0, 16, 22, 0, 0, 6, 8.50, '2024-01-01T00:00:00'),
('S047', 'M008', 'P017', 'TM-004', '2023-24', 90, 1, 1, 55, 64, 2, 1, 3, 8.80, '2024-01-01T00:00:00'),
('S048', 'M008', 'P018', 'TM-004', '2023-24', 90, 0, 0, 45, 50, 7, 4, 0, 7.50, '2024-01-01T00:00:00'),
-- M009: Arsenal 0-1 Man City (2023-24)
('S049', 'M009', 'P001', 'TM-001', '2023-24', 90, 0, 0, 28, 40, 2, 0, 2, 6.00, '2024-01-01T00:00:00'),
('S050', 'M009', 'P002', 'TM-001', '2023-24', 90, 0, 0, 60, 72, 1, 2, 1, 6.50, '2024-01-01T00:00:00'),
('S051', 'M009', 'P004', 'TM-001', '2023-24', 90, 0, 0, 55, 62, 6, 3, 0, 7.20, '2024-01-01T00:00:00'),
('S052', 'M009', 'P006', 'TM-002', '2023-24', 90, 1, 0, 12, 16, 0, 0, 5, 8.50, '2024-01-01T00:00:00'),
('S053', 'M009', 'P008', 'TM-002', '2023-24', 90, 0, 0, 70, 78, 4, 3, 0, 7.80, '2024-01-01T00:00:00'),
-- M010: Bayern 3-2 Barcelona (2023-24)
('S054', 'M010', 'P016', 'TM-004', '2023-24', 90, 2, 0, 20, 26, 0, 0, 7, 9.50, '2024-01-01T00:00:00'),
('S055', 'M010', 'P017', 'TM-004', '2023-24', 88, 1, 2, 48, 56, 1, 1, 2, 9.00, '2024-01-01T00:00:00'),
('S056', 'M010', 'P019', 'TM-004', '2023-24', 90, 0, 1, 32, 40, 1, 0, 3, 7.20, '2024-01-01T00:00:00'),
('S057', 'M010', 'P011', 'TM-003', '2023-24', 90, 2, 0, 20, 28, 0, 0, 6, 8.80, '2024-01-01T00:00:00'),
('S058', 'M010', 'P012', 'TM-003', '2023-24', 90, 0, 2, 72, 84, 2, 3, 1, 8.50, '2024-01-01T00:00:00'),
('S059', 'M010', 'P014', 'TM-003', '2023-24', 82, 0, 0, 50, 62, 3, 2, 1, 6.80, '2024-01-01T00:00:00'),
-- M011: Barcelona 2-1 Arsenal (2023-24)
('S060', 'M011', 'P011', 'TM-003', '2023-24', 90, 1, 0, 15, 22, 0, 0, 4, 8.00, '2024-01-01T00:00:00'),
('S061', 'M011', 'P015', 'TM-003', '2023-24', 90, 1, 1, 35, 42, 0, 0, 4, 8.50, '2024-01-01T00:00:00'),
('S062', 'M011', 'P013', 'TM-003', '2023-24', 90, 0, 0, 40, 45, 7, 5, 0, 7.80, '2024-01-01T00:00:00'),
('S063', 'M011', 'P001', 'TM-001', '2023-24', 90, 1, 0, 32, 45, 1, 1, 3, 7.50, '2024-01-01T00:00:00'),
('S064', 'M011', 'P002', 'TM-001', '2023-24', 90, 0, 1, 58, 70, 2, 2, 1, 7.20, '2024-01-01T00:00:00'),
('S065', 'M011', 'P003', 'TM-001', '2023-24', 90, 0, 0, 48, 52, 5, 3, 0, 7.00, '2024-01-01T00:00:00'),
-- M012: Arsenal 2-2 Bayern (2023-24)
('S066', 'M012', 'P001', 'TM-001', '2023-24', 90, 1, 1, 35, 48, 1, 0, 5, 8.50, '2024-01-01T00:00:00'),
('S067', 'M012', 'P004', 'TM-001', '2023-24', 90, 1, 0, 50, 58, 4, 3, 1, 8.00, '2024-01-01T00:00:00'),
('S068', 'M012', 'P005', 'TM-001', '2023-24', 70, 0, 1, 18, 24, 0, 0, 2, 6.80, '2024-01-01T00:00:00'),
('S069', 'M012', 'P016', 'TM-004', '2023-24', 90, 1, 0, 14, 20, 0, 0, 4, 8.00, '2024-01-01T00:00:00'),
('S070', 'M012', 'P017', 'TM-004', '2023-24', 90, 0, 1, 45, 55, 2, 1, 2, 7.50, '2024-01-01T00:00:00'),
('S071', 'M012', 'P020', 'TM-004', '2023-24', 90, 1, 0, 52, 60, 5, 3, 1, 7.80, '2024-01-01T00:00:00'),
-- Extra stats for underrepresented players
('S072', 'M003', 'P003', 'TM-001', '2022-23', 90, 0, 0, 48, 54, 5, 4, 0, 6.80, '2024-01-01T00:00:00'),
('S073', 'M006', 'P010', 'TM-002', '2022-23', 90, 0, 0, 50, 55, 6, 4, 0, 7.50, '2024-01-01T00:00:00'),
('S074', 'M007', 'P009', 'TM-002', '2023-24', 82, 0, 0, 38, 45, 1, 0, 3, 6.80, '2024-01-01T00:00:00'),
('S075', 'M009', 'P005', 'TM-001', '2023-24', 65, 0, 0, 10, 14, 0, 0, 1, 5.50, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 75
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_match_stats;

