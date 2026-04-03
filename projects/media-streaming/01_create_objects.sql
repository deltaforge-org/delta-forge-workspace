-- =============================================================================
-- Media Streaming Analytics Pipeline: Object Creation & Seed Data
-- =============================================================================

-- ===================== ZONES =====================

CREATE ZONE IF NOT EXISTS {{zone_prefix}} TYPE EXTERNAL
    COMMENT 'Workshop project zone';

-- ===================== SCHEMAS =====================
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.bronze COMMENT 'Raw viewing event feeds';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.silver COMMENT 'Reconstructed sessions and engagement scores';
CREATE SCHEMA IF NOT EXISTS {{zone_prefix}}.gold   COMMENT 'Star schema for content and engagement analytics';

-- ===================== BRONZE TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_users (
    user_id            STRING      NOT NULL,
    subscription_tier  STRING,
    signup_date        DATE,
    country            STRING,
    age_band           STRING,
    preferred_genre    STRING,
    ingested_at        TIMESTAMP
) LOCATION '{{data_path}}/media/bronze/raw_users';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_content (
    content_id         STRING      NOT NULL,
    title              STRING      NOT NULL,
    genre              STRING,
    release_year       INT,
    duration_min       INT,
    rating             DECIMAL(2,1),
    content_type       STRING,
    production_cost    DECIMAL(12,2),
    ingested_at        TIMESTAMP
) LOCATION '{{data_path}}/media/bronze/raw_content';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_devices (
    device_id          STRING      NOT NULL,
    device_type        STRING,
    os                 STRING,
    app_version        STRING,
    screen_resolution  STRING,
    ingested_at        TIMESTAMP
) LOCATION '{{data_path}}/media/bronze/raw_devices';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.bronze.raw_viewing_events (
    event_id           BIGINT      NOT NULL,
    user_id            STRING      NOT NULL,
    content_id         STRING      NOT NULL,
    device_id          STRING      NOT NULL,
    event_timestamp    TIMESTAMP   NOT NULL,
    event_type         STRING      NOT NULL,
    watch_duration_sec INT,
    position_pct       DECIMAL(5,2),
    quality_level      STRING,
    ingested_at        TIMESTAMP
) LOCATION '{{data_path}}/media/bronze/raw_viewing_events';

-- ===================== SILVER TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.sessions_reconstructed (
    session_id         STRING      NOT NULL,
    user_id            STRING      NOT NULL,
    device_id          STRING,
    start_time         TIMESTAMP,
    end_time           TIMESTAMP,
    total_watch_sec    INT,
    content_count      INT,
    avg_completion_pct DECIMAL(5,2),
    is_binge           BOOLEAN,
    enriched_at        TIMESTAMP
) LOCATION '{{data_path}}/media/silver/sessions_reconstructed';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.silver.engagement_scored (
    event_id           BIGINT      NOT NULL,
    user_id            STRING      NOT NULL,
    content_id         STRING      NOT NULL,
    device_id          STRING,
    session_id         STRING,
    event_timestamp    TIMESTAMP,
    event_type         STRING,
    watch_duration_sec INT,
    position_pct       DECIMAL(5,2),
    quality_level      STRING,
    completion_pct     DECIMAL(5,2),
    is_binge_event     BOOLEAN,
    churn_signal       BOOLEAN,
    enriched_at        TIMESTAMP
) LOCATION '{{data_path}}/media/silver/engagement_scored';

-- ===================== GOLD TABLES =====================

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_user (
    user_key           STRING      NOT NULL,
    user_id            STRING      NOT NULL,
    subscription_tier  STRING,
    signup_date        DATE,
    country            STRING,
    age_band           STRING,
    preferred_genre    STRING
) LOCATION '{{data_path}}/media/gold/dim_user';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_content (
    content_key        STRING      NOT NULL,
    content_id         STRING      NOT NULL,
    title              STRING,
    genre              STRING,
    release_year       INT,
    duration_min       INT,
    rating             DECIMAL(2,1),
    content_type       STRING,
    production_cost    DECIMAL(12,2)
) LOCATION '{{data_path}}/media/gold/dim_content';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_device (
    device_key         STRING      NOT NULL,
    device_type        STRING,
    os                 STRING,
    app_version        STRING,
    screen_resolution  STRING
) LOCATION '{{data_path}}/media/gold/dim_device';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.dim_session (
    session_key        STRING      NOT NULL,
    session_id         STRING      NOT NULL,
    user_key           STRING,
    start_time         TIMESTAMP,
    end_time           TIMESTAMP,
    total_watch_sec    INT,
    content_count      INT,
    avg_completion_pct DECIMAL(5,2)
) LOCATION '{{data_path}}/media/gold/dim_session';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.fact_viewing_events (
    event_key          BIGINT      NOT NULL,
    user_key           STRING      NOT NULL,
    content_key        STRING      NOT NULL,
    device_key         STRING,
    session_key        STRING,
    event_timestamp    TIMESTAMP,
    event_type         STRING,
    watch_duration_sec INT,
    position_pct       DECIMAL(5,2),
    quality_level      STRING
) LOCATION '{{data_path}}/media/gold/fact_viewing_events';

CREATE DELTA TABLE IF NOT EXISTS {{zone_prefix}}.gold.kpi_engagement (
    content_id         STRING      NOT NULL,
    genre              STRING,
    period             STRING      NOT NULL,
    total_views        INT,
    unique_viewers     INT,
    avg_completion_pct DECIMAL(5,2),
    avg_watch_duration_min DECIMAL(7,2),
    binge_rate         DECIMAL(5,2),
    churn_signal_pct   DECIMAL(5,2),
    content_roi        DECIMAL(10,4)
) LOCATION '{{data_path}}/media/gold/kpi_engagement';

-- ===================== GRANTS =====================
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_viewing_events TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_users TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_content TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.bronze.raw_devices TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.sessions_reconstructed TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.silver.engagement_scored TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.fact_viewing_events TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_user TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_content TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_device TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.dim_session TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_prefix}}.gold.kpi_engagement TO USER {{current_user}};

-- ===================== PSEUDONYMISATION =====================
CREATE PSEUDONYMISATION RULE ON {{zone_prefix}}.bronze.raw_users (user_id) TRANSFORM keyed_hash PARAMS (salt = 'media_user_hash_key_2024');

-- ===================== SEED DATA: USERS (12 users) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_users VALUES
('USR-001', 'premium',  '2022-01-15', 'US', '25-34', 'sci-fi',     '2024-01-01T00:00:00'),
('USR-002', 'basic',    '2022-06-20', 'US', '18-24', 'comedy',     '2024-01-01T00:00:00'),
('USR-003', 'premium',  '2021-03-10', 'UK', '35-44', 'drama',      '2024-01-01T00:00:00'),
('USR-004', 'standard', '2023-01-05', 'CA', '25-34', 'action',     '2024-01-01T00:00:00'),
('USR-005', 'premium',  '2020-11-18', 'US', '45-54', 'documentary','2024-01-01T00:00:00'),
('USR-006', 'basic',    '2023-08-30', 'DE', '18-24', 'horror',     '2024-01-01T00:00:00'),
('USR-007', 'standard', '2022-04-12', 'US', '25-34', 'sci-fi',     '2024-01-01T00:00:00'),
('USR-008', 'premium',  '2021-09-01', 'FR', '35-44', 'drama',      '2024-01-01T00:00:00'),
('USR-009', 'basic',    '2023-12-15', 'US', '18-24', 'comedy',     '2024-01-01T00:00:00'),
('USR-010', 'standard', '2022-07-25', 'AU', '25-34', 'action',     '2024-01-01T00:00:00'),
('USR-011', 'premium',  '2020-02-14', 'US', '55-64', 'documentary','2024-01-01T00:00:00'),
('USR-012', 'basic',    '2023-10-01', 'JP', '18-24', 'anime',      '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 12
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_users;


-- ===================== SEED DATA: CONTENT (15 items) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_content VALUES
('CNT-001', 'Galactic Frontier',        'sci-fi',      2024, 148, 8.2, 'movie',        45000000.00, '2024-01-01T00:00:00'),
('CNT-002', 'The Last Detective S1',    'drama',       2023,  55, 8.7, 'series',       30000000.00, '2024-01-01T00:00:00'),
('CNT-003', 'The Last Detective S1E2',  'drama',       2023,  52, 8.5, 'series',              0.00, '2024-01-01T00:00:00'),
('CNT-004', 'The Last Detective S1E3',  'drama',       2023,  58, 8.9, 'series',              0.00, '2024-01-01T00:00:00'),
('CNT-005', 'Laugh Track Live',         'comedy',      2024,  95, 7.1, 'movie',        12000000.00, '2024-01-01T00:00:00'),
('CNT-006', 'Ocean Mysteries',          'documentary', 2023,  90, 9.0, 'documentary',   8000000.00, '2024-01-01T00:00:00'),
('CNT-007', 'Neon Nights',              'action',      2024, 125, 7.8, 'movie',        60000000.00, '2024-01-01T00:00:00'),
('CNT-008', 'Code Breakers S2',         'sci-fi',      2024,  48, 8.4, 'series',       25000000.00, '2024-01-01T00:00:00'),
('CNT-009', 'Code Breakers S2E2',       'sci-fi',      2024,  50, 8.3, 'series',              0.00, '2024-01-01T00:00:00'),
('CNT-010', 'Code Breakers S2E3',       'sci-fi',      2024,  47, 8.6, 'series',              0.00, '2024-01-01T00:00:00'),
('CNT-011', 'Haunted Manor',            'horror',      2023, 102, 6.5, 'movie',        18000000.00, '2024-01-01T00:00:00'),
('CNT-012', 'Climate Rising',           'documentary', 2024,  85, 8.8, 'documentary',   5000000.00, '2024-01-01T00:00:00'),
('CNT-013', 'Tokyo Drift Anime',        'anime',       2024,  24, 7.9, 'series',        3000000.00, '2024-01-01T00:00:00'),
('CNT-014', 'Summer Romance',           'drama',       2024, 110, 6.8, 'movie',        20000000.00, '2024-01-01T00:00:00'),
('CNT-015', 'Wild Expeditions',         'documentary', 2023,  78, 8.1, 'documentary',   6000000.00, '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 15
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_content;


-- ===================== SEED DATA: DEVICES (8 device configs) =====================
INSERT INTO {{zone_prefix}}.bronze.raw_devices VALUES
('DEV-TV-001',  'smart_tv',   'tvOS',    '4.2.1', '3840x2160', '2024-01-01T00:00:00'),
('DEV-TV-002',  'smart_tv',   'Roku',    '3.8.0', '1920x1080', '2024-01-01T00:00:00'),
('DEV-MOB-001', 'mobile',     'iOS',     '4.2.1', '1170x2532', '2024-01-01T00:00:00'),
('DEV-MOB-002', 'mobile',     'Android', '4.1.9', '1080x2400', '2024-01-01T00:00:00'),
('DEV-TAB-001', 'tablet',     'iPadOS',  '4.2.1', '2048x2732', '2024-01-01T00:00:00'),
('DEV-WEB-001', 'web',        'Chrome',  '4.2.0', '1920x1080', '2024-01-01T00:00:00'),
('DEV-WEB-002', 'web',        'Firefox', '4.2.0', '2560x1440', '2024-01-01T00:00:00'),
('DEV-TV-003',  'smart_tv',   'FireTV',  '3.9.2', '3840x2160', '2024-01-01T00:00:00');

ASSERT ROW_COUNT = 8
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_devices;


-- ===================== SEED DATA: VIEWING EVENTS (72 rows) =====================
-- 12 users, varying engagement: bingers (USR-001, USR-003), casual (USR-004, USR-010),
-- churning (USR-006, USR-009 - no recent activity), active (USR-005, USR-007, USR-008)
INSERT INTO {{zone_prefix}}.bronze.raw_viewing_events VALUES
-- USR-001: Binger - watches Code Breakers S2 series back-to-back
(5001, 'USR-001', 'CNT-008', 'DEV-TV-001',  '2024-02-01T19:00:00', 'play',     0,    0.00, '4K',    '2024-02-01T19:00:01'),
(5002, 'USR-001', 'CNT-008', 'DEV-TV-001',  '2024-02-01T19:48:00', 'complete', 2880, 100.00, '4K',   '2024-02-01T19:48:01'),
(5003, 'USR-001', 'CNT-009', 'DEV-TV-001',  '2024-02-01T19:50:00', 'play',     0,    0.00, '4K',    '2024-02-01T19:50:01'),
(5004, 'USR-001', 'CNT-009', 'DEV-TV-001',  '2024-02-01T20:40:00', 'complete', 3000, 100.00, '4K',   '2024-02-01T20:40:01'),
(5005, 'USR-001', 'CNT-010', 'DEV-TV-001',  '2024-02-01T20:42:00', 'play',     0,    0.00, '4K',    '2024-02-01T20:42:01'),
(5006, 'USR-001', 'CNT-010', 'DEV-TV-001',  '2024-02-01T21:29:00', 'complete', 2820, 100.00, '4K',   '2024-02-01T21:29:01'),
(5007, 'USR-001', 'CNT-001', 'DEV-TV-001',  '2024-02-05T20:00:00', 'play',     0,    0.00, '4K',    '2024-02-05T20:00:01'),
(5008, 'USR-001', 'CNT-001', 'DEV-TV-001',  '2024-02-05T22:28:00', 'complete', 8880, 100.00, '4K',   '2024-02-05T22:28:01'),
-- USR-002: Casual comedy watcher
(5009, 'USR-002', 'CNT-005', 'DEV-MOB-001', '2024-02-03T12:00:00', 'play',     0,    0.00, '720p',  '2024-02-03T12:00:01'),
(5010, 'USR-002', 'CNT-005', 'DEV-MOB-001', '2024-02-03T12:45:00', 'pause',   2700, 47.37, '720p',  '2024-02-03T12:45:01'),
(5011, 'USR-002', 'CNT-005', 'DEV-MOB-001', '2024-02-03T14:00:00', 'play',     0,   47.37, '720p',  '2024-02-03T14:00:01'),
(5012, 'USR-002', 'CNT-005', 'DEV-MOB-001', '2024-02-03T14:50:00', 'complete', 3000, 100.00, '720p', '2024-02-03T14:50:01'),
(5013, 'USR-002', 'CNT-014', 'DEV-WEB-001', '2024-02-10T21:00:00', 'play',     0,    0.00, '1080p', '2024-02-10T21:00:01'),
(5014, 'USR-002', 'CNT-014', 'DEV-WEB-001', '2024-02-10T21:35:00', 'abandon',  2100, 31.82, '1080p', '2024-02-10T21:35:01'),
-- USR-003: Binger - drama series
(5015, 'USR-003', 'CNT-002', 'DEV-TV-002',  '2024-02-02T20:00:00', 'play',     0,    0.00, '1080p', '2024-02-02T20:00:01'),
(5016, 'USR-003', 'CNT-002', 'DEV-TV-002',  '2024-02-02T20:55:00', 'complete', 3300, 100.00, '1080p','2024-02-02T20:55:01'),
(5017, 'USR-003', 'CNT-003', 'DEV-TV-002',  '2024-02-02T20:57:00', 'play',     0,    0.00, '1080p', '2024-02-02T20:57:01'),
(5018, 'USR-003', 'CNT-003', 'DEV-TV-002',  '2024-02-02T21:49:00', 'complete', 3120, 100.00, '1080p','2024-02-02T21:49:01'),
(5019, 'USR-003', 'CNT-004', 'DEV-TV-002',  '2024-02-02T21:51:00', 'play',     0,    0.00, '1080p', '2024-02-02T21:51:01'),
(5020, 'USR-003', 'CNT-004', 'DEV-TV-002',  '2024-02-02T22:49:00', 'complete', 3480, 100.00, '1080p','2024-02-02T22:49:01'),
(5021, 'USR-003', 'CNT-006', 'DEV-TAB-001', '2024-02-15T10:00:00', 'play',     0,    0.00, '1080p', '2024-02-15T10:00:01'),
(5022, 'USR-003', 'CNT-006', 'DEV-TAB-001', '2024-02-15T11:30:00', 'complete', 5400, 100.00, '1080p','2024-02-15T11:30:01'),
-- USR-004: Casual action viewer
(5023, 'USR-004', 'CNT-007', 'DEV-MOB-002', '2024-02-08T22:00:00', 'play',     0,    0.00, '1080p', '2024-02-08T22:00:01'),
(5024, 'USR-004', 'CNT-007', 'DEV-MOB-002', '2024-02-08T22:45:00', 'pause',   2700, 36.00, '1080p', '2024-02-08T22:45:01'),
(5025, 'USR-004', 'CNT-007', 'DEV-MOB-002', '2024-02-09T22:00:00', 'play',     0,   36.00, '1080p', '2024-02-09T22:00:01'),
(5026, 'USR-004', 'CNT-007', 'DEV-MOB-002', '2024-02-09T23:20:00', 'complete', 4800, 100.00, '1080p','2024-02-09T23:20:01'),
-- USR-005: Documentary enthusiast
(5027, 'USR-005', 'CNT-006', 'DEV-TV-003',  '2024-02-04T09:00:00', 'play',     0,    0.00, '4K',    '2024-02-04T09:00:01'),
(5028, 'USR-005', 'CNT-006', 'DEV-TV-003',  '2024-02-04T10:30:00', 'complete', 5400, 100.00, '4K',   '2024-02-04T10:30:01'),
(5029, 'USR-005', 'CNT-012', 'DEV-TV-003',  '2024-02-10T09:00:00', 'play',     0,    0.00, '4K',    '2024-02-10T09:00:01'),
(5030, 'USR-005', 'CNT-012', 'DEV-TV-003',  '2024-02-10T10:25:00', 'complete', 5100, 100.00, '4K',   '2024-02-10T10:25:01'),
(5031, 'USR-005', 'CNT-015', 'DEV-TV-003',  '2024-02-17T09:00:00', 'play',     0,    0.00, '4K',    '2024-02-17T09:00:01'),
(5032, 'USR-005', 'CNT-015', 'DEV-TV-003',  '2024-02-17T10:18:00', 'complete', 4680, 100.00, '4K',   '2024-02-17T10:18:01'),
-- USR-006: Churning user - last activity was Jan, none since
(5033, 'USR-006', 'CNT-011', 'DEV-WEB-002', '2024-01-05T23:00:00', 'play',     0,    0.00, '1080p', '2024-01-05T23:00:01'),
(5034, 'USR-006', 'CNT-011', 'DEV-WEB-002', '2024-01-05T23:40:00', 'abandon',  2400, 39.22, '1080p', '2024-01-05T23:40:01'),
(5035, 'USR-006', 'CNT-005', 'DEV-WEB-002', '2024-01-12T20:00:00', 'play',     0,    0.00, '1080p', '2024-01-12T20:00:01'),
(5036, 'USR-006', 'CNT-005', 'DEV-WEB-002', '2024-01-12T20:20:00', 'abandon',  1200, 21.05, '1080p', '2024-01-12T20:20:01'),
-- USR-007: Active sci-fi fan
(5037, 'USR-007', 'CNT-001', 'DEV-TV-001',  '2024-02-06T21:00:00', 'play',     0,    0.00, '4K',    '2024-02-06T21:00:01'),
(5038, 'USR-007', 'CNT-001', 'DEV-TV-001',  '2024-02-06T23:28:00', 'complete', 8880, 100.00, '4K',   '2024-02-06T23:28:01'),
(5039, 'USR-007', 'CNT-008', 'DEV-MOB-001', '2024-02-12T08:00:00', 'play',     0,    0.00, '720p',  '2024-02-12T08:00:01'),
(5040, 'USR-007', 'CNT-008', 'DEV-MOB-001', '2024-02-12T08:48:00', 'complete', 2880, 100.00, '720p', '2024-02-12T08:48:01'),
(5041, 'USR-007', 'CNT-009', 'DEV-MOB-001', '2024-02-12T12:00:00', 'play',     0,    0.00, '720p',  '2024-02-12T12:00:01'),
(5042, 'USR-007', 'CNT-009', 'DEV-MOB-001', '2024-02-12T12:50:00', 'complete', 3000, 100.00, '720p', '2024-02-12T12:50:01'),
-- USR-008: Drama lover, premium
(5043, 'USR-008', 'CNT-002', 'DEV-TV-002',  '2024-02-09T20:00:00', 'play',     0,    0.00, '1080p', '2024-02-09T20:00:01'),
(5044, 'USR-008', 'CNT-002', 'DEV-TV-002',  '2024-02-09T20:55:00', 'complete', 3300, 100.00, '1080p','2024-02-09T20:55:01'),
(5045, 'USR-008', 'CNT-014', 'DEV-TV-002',  '2024-02-16T20:00:00', 'play',     0,    0.00, '1080p', '2024-02-16T20:00:01'),
(5046, 'USR-008', 'CNT-014', 'DEV-TV-002',  '2024-02-16T21:50:00', 'complete', 6600, 100.00, '1080p','2024-02-16T21:50:01'),
-- USR-009: Churning user - signed up Dec, watched once in Jan
(5047, 'USR-009', 'CNT-005', 'DEV-MOB-002', '2024-01-02T15:00:00', 'play',     0,    0.00, '720p',  '2024-01-02T15:00:01'),
(5048, 'USR-009', 'CNT-005', 'DEV-MOB-002', '2024-01-02T15:30:00', 'abandon',  1800, 31.58, '720p',  '2024-01-02T15:30:01'),
-- USR-010: Casual action viewer
(5049, 'USR-010', 'CNT-007', 'DEV-WEB-001', '2024-02-14T21:00:00', 'play',     0,    0.00, '1080p', '2024-02-14T21:00:01'),
(5050, 'USR-010', 'CNT-007', 'DEV-WEB-001', '2024-02-14T23:05:00', 'complete', 7500, 100.00, '1080p','2024-02-14T23:05:01'),
(5051, 'USR-010', 'CNT-001', 'DEV-WEB-001', '2024-02-20T20:00:00', 'play',     0,    0.00, '1080p', '2024-02-20T20:00:01'),
(5052, 'USR-010', 'CNT-001', 'DEV-WEB-001', '2024-02-20T21:15:00', 'seek',     4500, 50.68, '1080p', '2024-02-20T21:15:01'),
(5053, 'USR-010', 'CNT-001', 'DEV-WEB-001', '2024-02-20T22:00:00', 'abandon',  7200, 81.08, '1080p', '2024-02-20T22:00:01'),
-- USR-011: Documentary binger
(5054, 'USR-011', 'CNT-006', 'DEV-TV-003',  '2024-02-11T08:00:00', 'play',     0,    0.00, '4K',    '2024-02-11T08:00:01'),
(5055, 'USR-011', 'CNT-006', 'DEV-TV-003',  '2024-02-11T09:30:00', 'complete', 5400, 100.00, '4K',   '2024-02-11T09:30:01'),
(5056, 'USR-011', 'CNT-012', 'DEV-TV-003',  '2024-02-11T09:35:00', 'play',     0,    0.00, '4K',    '2024-02-11T09:35:01'),
(5057, 'USR-011', 'CNT-012', 'DEV-TV-003',  '2024-02-11T11:00:00', 'complete', 5100, 100.00, '4K',   '2024-02-11T11:00:01'),
(5058, 'USR-011', 'CNT-015', 'DEV-TV-003',  '2024-02-11T11:05:00', 'play',     0,    0.00, '4K',    '2024-02-11T11:05:01'),
(5059, 'USR-011', 'CNT-015', 'DEV-TV-003',  '2024-02-11T12:23:00', 'complete', 4680, 100.00, '4K',   '2024-02-11T12:23:01'),
-- USR-012: Anime watcher
(5060, 'USR-012', 'CNT-013', 'DEV-MOB-002', '2024-02-07T18:00:00', 'play',     0,    0.00, '1080p', '2024-02-07T18:00:01'),
(5061, 'USR-012', 'CNT-013', 'DEV-MOB-002', '2024-02-07T18:24:00', 'complete', 1440, 100.00, '1080p','2024-02-07T18:24:01'),
(5062, 'USR-012', 'CNT-013', 'DEV-MOB-002', '2024-02-14T18:00:00', 'play',     0,    0.00, '1080p', '2024-02-14T18:00:01'),
(5063, 'USR-012', 'CNT-013', 'DEV-MOB-002', '2024-02-14T18:24:00', 'complete', 1440, 100.00, '1080p','2024-02-14T18:24:01'),
-- Additional events for content variety
(5064, 'USR-004', 'CNT-011', 'DEV-MOB-002', '2024-02-18T23:00:00', 'play',     0,    0.00, '1080p', '2024-02-18T23:00:01'),
(5065, 'USR-004', 'CNT-011', 'DEV-MOB-002', '2024-02-18T23:50:00', 'abandon',  3000, 49.02, '1080p', '2024-02-18T23:50:01'),
(5066, 'USR-002', 'CNT-013', 'DEV-MOB-001', '2024-02-20T17:00:00', 'play',     0,    0.00, '720p',  '2024-02-20T17:00:01'),
(5067, 'USR-002', 'CNT-013', 'DEV-MOB-001', '2024-02-20T17:24:00', 'complete', 1440, 100.00, '720p', '2024-02-20T17:24:01'),
(5068, 'USR-007', 'CNT-010', 'DEV-TV-001',  '2024-02-18T21:00:00', 'play',     0,    0.00, '4K',    '2024-02-18T21:00:01'),
(5069, 'USR-007', 'CNT-010', 'DEV-TV-001',  '2024-02-18T21:47:00', 'complete', 2820, 100.00, '4K',   '2024-02-18T21:47:01'),
(5070, 'USR-005', 'CNT-001', 'DEV-TV-003',  '2024-02-22T20:00:00', 'play',     0,    0.00, '4K',    '2024-02-22T20:00:01'),
(5071, 'USR-005', 'CNT-001', 'DEV-TV-003',  '2024-02-22T22:28:00', 'complete', 8880, 100.00, '4K',   '2024-02-22T22:28:01'),
(5072, 'USR-003', 'CNT-014', 'DEV-TAB-001', '2024-02-24T15:00:00', 'play',     0,    0.00, '1080p', '2024-02-24T15:00:01');

ASSERT ROW_COUNT = 72
SELECT COUNT(*) AS row_count FROM {{zone_prefix}}.bronze.raw_viewing_events;

