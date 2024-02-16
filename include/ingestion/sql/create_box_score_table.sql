-- DROP TABLE IF EXISTS nba_basketball.box_score;

CREATE TABLE IF NOT EXISTS nba_basketball.box_score(

    box_score_id INTEGER,
    game_id INTEGER, -- fks to game
    player_id INTEGER, -- fks to player
    team_id INTEGER, -- fks to player
    pts INTEGER,
    reb INTEGER,
    ast INTEGER,
    blk INTEGER,
    stl INTEGER,
    turnover INTEGER,
    oreb INTEGER,
    dreb INTEGER,
    fg3_pct FLOAT,
    fg3a INTEGER,
    fg3m INTEGER,
    fg_pct FLOAT,
    fga INTEGER,
    fgm INTEGER,
    ft_pct FLOAT,
    fta INTEGER,
    ftm INTEGER,
    min TEXT,
    pf INTEGER,

    -- Foreign keys
    CONSTRAINT fk_box_score_game
        FOREIGN KEY(game_id)
        REFERENCES nba_basketball.game(game_id),
    CONSTRAINT fk_box_score_player
        FOREIGN KEY(player_id)
        REFERENCES nba_basketball.player(player_id),
    CONSTRAINT fk_box_score_team
        FOREIGN KEY(team_id)
        REFERENCES nba_basketball.team(team_id)
);
