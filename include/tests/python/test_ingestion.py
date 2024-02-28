#!/usr/bin/env python

"""
Data quality tests on the Postgres database after ingestion from the Ball Don't Lie API.
"""
import os
import sys

sys.path.append("include/ingestion/python")

from ingestion_utils import query_postgres


# Number of games in game = number of games in box_score
test_games_in_games_and_box_score ="""
WITH game as (
    SELECT COUNT(*) num_games_in_game
    FROM nba_basketball.game
    WHERE status = 'Final'
    AND home_team_score <> 0
    AND visitor_team_score <> 0
),
box_score as (
    SELECT COUNT(DISTINCT game_id) num_games_in_box_score
    FROM nba_basketball.box_score
    JOIN nba_basketball.game USING(game_id)
    WHERE status = 'Final'
)
SELECT num_games_in_game = num_games_in_box_score
FROM game
JOIN box_score ON TRUE
;
"""
assert query_postgres(test_games_in_games_and_box_score, fetchall=True)[0][0]

test_points_in_games_and_box_score = """
WITH game AS (
    SELECT season, game_id, home_team_score + visitor_team_score num_pts_games
    FROM nba_basketball.game
    WHERE status = 'Final'
)
, box_score AS (
    SELECT season, game_id, sum(pts) num_pts_box_score
    FROM nba_basketball.box_score
    JOIN nba_basketball.game using(game_id)
    WHERE status = 'Final'
    GROUP BY 1,2
)
SELECT COUNT(*) = 0
FROM game
LEFT JOIN box_score using(game_id)
WHERE num_pts_games <> 0
and (num_pts_games <> num_pts_box_score
OR num_pts_games IS NULL
OR num_pts_box_score IS NULL)
;
"""

assert query_postgres(test_points_in_games_and_box_score, fetchall=True)[0][0]
print('All tests passed')
