-- The game API end point seems to return some duplicate rows
-- We will de-dupe them using this query

-- For testing
CREATE TABLE nba_basketball.game_test AS
    SELECT * FROM nba_basketball.game;


DELETE FROM nba_basketball.game_test a
    USING nba_basketball.game_test b
    WHERE a.ctid < b.ctid -- physical location of the row
    AND a.game_id = b.game_id
    AND a.game_date = b.game_date
    AND a.home_team_id = b.home_team_id
    AND a.home_team_score = b.home_team_score
    AND a.visitor_team_id = b.visitor_team_id
    AND a.visitor_team_score = b.visitor_team_score
    AND a.season = b.season
    AND a.post_season = b.post_season
    AND a.status = b.status;

-- Re-define the PK
ALTER TABLE nba_basketball.game_test ADD PRIMARY KEY (game_id);
