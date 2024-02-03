-- team
SELECT COUNT(*) FROM nba_basketball.team;
-- player
SELECT COUNT(*) FROM nba_basketball.player;
-- game
SELECT COUNT(*)
FROM nba_basketball.game
WHERE status = 'Final' -- get completed games
AND home_team_score <> 0
AND visitor_team_score <> 0;
-- box_score
SELECT COUNT(*) FROM nba_basketball.box_score;
