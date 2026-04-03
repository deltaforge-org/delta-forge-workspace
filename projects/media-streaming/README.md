# Media Streaming Analytics Pipeline

## Scenario

A video streaming platform tracks viewing events across 12 users with varying engagement patterns: bingers who watch entire series back-to-back, casual viewers who watch intermittently, and churning users who haven't engaged in over 14 days. The platform offers 15 content items (movies, series episodes, documentaries) streamed across 4 device types (smart TV, mobile, tablet, web). The pipeline reconstructs viewing sessions using LAG/LEAD window functions (new session if gap > 30 minutes), scores engagement, detects binge behavior (3+ episodes in one session), and identifies churn signals for retention campaigns.

## Star Schema

```
                    +----------------+
                    | dim_session    |
                    |----------------|
                    | session_key    |<------+
                    | session_id     |       |
                    | user_key       |       |
                    | total_watch_sec|       |
                    | content_count  |       |
                    +----------------+       |
                                             |
+----------------+  +---------------------+  |    +----------------+
| dim_content    |  | fact_viewing_events |  |    | dim_device     |
|----------------|  |---------------------|  |    |----------------|
| content_key    |<-| event_key           |  +--->| device_key     |
| title          |  | user_key         FK |       | device_type    |
| genre          |  | content_key      FK |       | os             |
| duration_min   |  | device_key       FK |       | app_version    |
| rating         |  | session_key      FK |       +----------------+
| content_type   |  | event_timestamp    |
| production_cost|  | event_type         |
+----------------+  | watch_duration_sec |  +----------------+
                    | position_pct       |  | dim_user       |
                    | quality_level      |  |----------------|
                    +---------------------+ | user_key       |
                              |             | subscription   |
                    +---------+--------+    | country        |
                    | kpi_engagement    |    | age_band       |
                    |------------------|    +----------------+
                    | content_id       |
                    | genre, period    |
                    | binge_rate       |
                    | churn_signal_pct |
                    | content_roi      |
                    +------------------+
```

## Medallion Flow

```
BRONZE                        SILVER                             GOLD
+--------------------+    +---------------------------+    +----------------------+
| raw_viewing_events |--->| sessions_reconstructed    |--->| fact_viewing_events  |
| (72+ events)       |    | (LAG/LEAD session gaps)   |    | dim_user             |
+--------------------+    +---------------------------+    | dim_content          |
| raw_users          |--->| engagement_scored         |--->| dim_device           |
+--------------------+    | (completion, binge, churn)|    | dim_session          |
| raw_content        |    +---------------------------+    | kpi_engagement       |
+--------------------+                                     +----------------------+
| raw_devices        |
+--------------------+
```

## Features

- **Session reconstruction**: LAG/LEAD window functions detect 30-minute gaps to define session boundaries
- **Binge detection**: Sessions with 3+ distinct content items flagged as binge viewing
- **Churn signals**: Users with no activity in 14+ days identified for retention targeting
- **Content ROI**: (views x avg_duration) / production_cost for content investment decisions
- **Deletion vectors**: Right-to-be-forgotten compliance via user deletion
- **OPTIMIZE compaction**: Regular compaction for read performance
- **Pseudonymisation**: keyed_hash on user_id for privacy compliance
- **Incremental processing**: INCREMENTAL_FILTER macro for watermark-based processing

## Seed Data

- **12 users** across 4 subscription tiers with varied engagement patterns
- **15 content items**: movies, series episodes, documentaries, anime
- **8 device configurations**: smart TVs (4K), mobile (iOS/Android), tablets, web browsers
- **72 viewing events**: play, pause, seek, complete, abandon across Jan-Feb 2024

## Verification Checklist

- [ ] All 12 users in dim_user
- [ ] All 15 content items in dim_content with production costs
- [ ] Session reconstruction correctly groups events within 30-min windows
- [ ] Binge sessions identified (USR-001 Code Breakers, USR-003 Last Detective, USR-011 docs)
- [ ] Churning users detected (USR-006, USR-009 with no Feb activity)
- [ ] Content ROI calculated for non-zero production cost titles
- [ ] Device preference analysis shows 4+ device types
- [ ] Incremental load processes only new March rows
