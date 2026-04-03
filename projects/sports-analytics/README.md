# Sports Analytics Pipeline

## Scenario

A football (soccer) analytics platform tracks 20 players across 4 elite clubs (Arsenal, Manchester City, FC Barcelona, Bayern Munich) over 2 seasons (2022-23 and 2023-24). The pipeline builds detailed match performance stats, calculates position-weighted composite scores, and constructs a player network graph for transfer analysis, teammate chemistry, and opponent matchup history.

Key transfers include Harry Kane (Man City to Bayern) and Declan Rice (Barcelona to Arsenal). The composite scoring system weights metrics differently by position: forwards emphasize goals, midfielders emphasize passing, and defenders emphasize tackles/interceptions. Graph queries use pageRank to identify the most connected players in the network.

## Star Schema

```
+------------------+     +------------------+     +------------------+
| dim_player       |     | dim_team         |     | dim_match        |
|------------------|     |------------------|     |------------------|
| player_key       |<-+  | team_key         |<-+  | match_key        |<-+
| name             |  |  | team_name        |  |  | home/away_team   |  |
| position         |  |  | league           |  |  | match_date       |  |
| nationality      |  |  | city / country   |  |  | competition      |  |
| market_value_m   |  |  | stadium          |  |  | venue            |  |
| contract_end     |  |  | manager          |  |  | scores           |  |
+------------------+  |  +------------------+  |  +------------------+  |
                      |                        |                        |
                      |  +--------------------+|  +------------------+  |
                      |  | fact_match_perf    ||  | dim_season       |  |
                      +--| performance_key    ||  |------------------|  |
                         | player_key      FK |+  | season_key       |<-+
                         | team_key        FK |-->| season_name      |  |
                         | match_key       FK |-->| start/end_date   |  |
                         | season_key      FK |   | total_matchdays  |  |
                         | minutes_played     |   +------------------+  |
                         | goals / assists    |                         |
                         | passes / pass_pct  |                         |
                         | tackles / intcpts  |                         |
                         | rating             |                         |
                         +--------------------+                         |
                                                                        |
+----------------------------------------------------------------------+
|  player_network (GRAPH)                                               |
|  Vertices: Players (id, type, name, position, team)                  |
|  Edges: TEAMMATE (same team+season), OPPONENT (match), TRANSFER      |
+----------------------------------------------------------------------+
```

## Medallion Flow

```
BRONZE                       SILVER                          GOLD
+------------------+    +------------------------+    +---------------------+
| raw_match_stats  |--->| player_rolling_stats   |--->| fact_match_perf     |
| (75+ records)    |    | - per-90 stats         |    | dim_player          |
+------------------+    | - composite score      |    | dim_team            |
| raw_players      |--->| - MERGE upsert         |    | dim_match           |
| raw_teams        |    +------------------------+    | dim_season          |
| raw_matches      |                                  | kpi_player_rankings |
| raw_transfers    |                                  | player_network      |
+------------------+                                  +---------------------+
```

## Features

- **Graph network**: Player network with TEAMMATE, OPPONENT, and TRANSFER edges
- **Cypher queries**: pageRank for most connected players, teammate chemistry analysis
- **MERGE upsert**: Rolling player stats updated incrementally each match
- **Z-ordering**: Optimized for player_id + season fast lookups
- **Composite scoring**: Position-weighted (Forwards: goals, Midfielders: passing, Defenders: tackles)
- **Per-90 stats**: Goals per 90 minutes, assists per 90, defensive actions per 90
- **Transfer tracking**: Fee-weighted transfer edges in graph
- **INCREMENTAL_FILTER macro**: Incremental load on performance_key

## Seed Data

- **20 players** across 4 teams (5 per team)
- **4 teams** across 3 leagues (Premier League, La Liga, Bundesliga)
- **12 matches** across 2 seasons (mix of league and Champions League)
- **75 match stat records** with detailed per-match player performance
- **3 transfers** between teams across seasons
- Performance varies: Haaland leads goals, De Bruyne leads assists, Saliba leads tackles

## Verification Checklist

- [ ] All 20 players in dim_player with market values
- [ ] All 4 teams in dim_team
- [ ] All 12 matches in dim_match with scores
- [ ] Both seasons in dim_season
- [ ] 75+ performance records in fact table
- [ ] Composite scores weight differently by position
- [ ] Player rankings show Haaland as top forward, Rodri as top midfielder
- [ ] Graph has TEAMMATE, OPPONENT, and TRANSFER edges
- [ ] pageRank identifies most connected players
- [ ] No orphaned foreign keys in fact table
