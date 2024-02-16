CREATE SCHEMA IF NOT EXISTS nba_basketball;

DROP TABLE IF EXISTS nba_basketball.team;

CREATE TABLE nba_basketball.team (
    team_id INTEGER PRIMARY KEY,
    team_name_abbreviation TEXT,
    city TEXT,
    conference TEXT,
    division TEXT,
    team_full_name TEXT,
    team_name TEXT
);
