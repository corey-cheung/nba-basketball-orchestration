-- DROP TABLE IF EXISTS nba_basketball.game;

CREATE TABLE IF NOT EXISTS nba_basketball.game (
    game_id INTEGER PRIMARY KEY,
    game_date DATE,
    home_team_id INTEGER, -- FKs to team
    home_team_score INTEGER,
    visitor_team_id INTEGER, -- FKs to team
    visitor_team_score INTEGER,
    season INTEGER,
    post_season BOOLEAN,
    status TEXT,

    -- Foreign keys
    CONSTRAINT fk_home_team
        FOREIGN KEY(home_team_id)
        REFERENCES nba_basketball.team(team_id),
    CONSTRAINT fk_visitor_team
        FOREIGN KEY(visitor_team_id)
        REFERENCES nba_basketball.team(team_id)
);
