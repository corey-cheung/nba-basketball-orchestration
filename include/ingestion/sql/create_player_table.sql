-- DROP TABLE IF EXISTS nba_basketball.player;

CREATE TABLE IF NOT EXISTS nba_basketball.player (
    player_id INTEGER PRIMARY KEY,
    position TEXT,
    team_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    height_feet INTEGER,
    height_inches INTEGER,
    weight_pounds INTEGER,

    -- Foreign keys
    CONSTRAINT fk_player_team
        FOREIGN KEY(team_id)
        REFERENCES nba_basketball.team(team_id)
);
