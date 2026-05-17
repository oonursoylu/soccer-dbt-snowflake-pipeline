-- 1. Ensure context is correct for creating tables
USE ROLE TRANSFORM_ROLE;
USE WAREHOUSE SOCCER_WH;
USE DATABASE SOCCER_DB;
USE SCHEMA RAW;


-- 2. CREATE AND LOAD: Country

CREATE OR REPLACE TABLE COUNTRY (
    id NUMBER,
    name VARCHAR
);

COPY INTO COUNTRY
FROM @SOCCER_S3_STAGE/Country.csv
FILE_FORMAT = (FORMAT_NAME = SOCCER_CSV_FORMAT);


-- 3. CREATE AND LOAD: League

CREATE OR REPLACE TABLE LEAGUE (
    id NUMBER,
    country_id NUMBER,
    name VARCHAR
);

COPY INTO LEAGUE
FROM @SOCCER_S3_STAGE/League.csv
FILE_FORMAT = (FORMAT_NAME = SOCCER_CSV_FORMAT);


-- 4. CREATE AND LOAD: Matches (Match.csv)

CREATE OR REPLACE TABLE MATCHES (
    id NUMBER,
    country_id NUMBER,
    league_id NUMBER,
    season VARCHAR,
    stage NUMBER,
    date TIMESTAMP,
    match_api_id NUMBER,
    home_team_api_id NUMBER,
    away_team_api_id NUMBER,
    home_team_goal NUMBER,
    away_team_goal NUMBER,
    
    -- X Coordinates
    home_player_X1 NUMBER, home_player_X2 NUMBER, home_player_X3 NUMBER, home_player_X4 NUMBER, home_player_X5 NUMBER, home_player_X6 NUMBER, home_player_X7 NUMBER, home_player_X8 NUMBER, home_player_X9 NUMBER, home_player_X10 NUMBER, home_player_X11 NUMBER,
    away_player_X1 NUMBER, away_player_X2 NUMBER, away_player_X3 NUMBER, away_player_X4 NUMBER, away_player_X5 NUMBER, away_player_X6 NUMBER, away_player_X7 NUMBER, away_player_X8 NUMBER, away_player_X9 NUMBER, away_player_X10 NUMBER, away_player_X11 NUMBER,
    
    -- Y Coordinates
    home_player_Y1 NUMBER, home_player_Y2 NUMBER, home_player_Y3 NUMBER, home_player_Y4 NUMBER, home_player_Y5 NUMBER, home_player_Y6 NUMBER, home_player_Y7 NUMBER, home_player_Y8 NUMBER, home_player_Y9 NUMBER, home_player_Y10 NUMBER, home_player_Y11 NUMBER,
    away_player_Y1 NUMBER, away_player_Y2 NUMBER, away_player_Y3 NUMBER, away_player_Y4 NUMBER, away_player_Y5 NUMBER, away_player_Y6 NUMBER, away_player_Y7 NUMBER, away_player_Y8 NUMBER, away_player_Y9 NUMBER, away_player_Y10 NUMBER, away_player_Y11 NUMBER,
    
    -- Player IDs
    home_player_1 NUMBER, home_player_2 NUMBER, home_player_3 NUMBER, home_player_4 NUMBER, home_player_5 NUMBER, home_player_6 NUMBER, home_player_7 NUMBER, home_player_8 NUMBER, home_player_9 NUMBER, home_player_10 NUMBER, home_player_11 NUMBER,
    away_player_1 NUMBER, away_player_2 NUMBER, away_player_3 NUMBER, away_player_4 NUMBER, away_player_5 NUMBER, away_player_6 NUMBER, away_player_7 NUMBER, away_player_8 NUMBER, away_player_9 NUMBER, away_player_10 NUMBER, away_player_11 NUMBER,
    
    -- Match Events
    goal VARCHAR, shoton VARCHAR, shotoff VARCHAR, foulcommit VARCHAR, card VARCHAR, cross VARCHAR, corner VARCHAR, possession VARCHAR,
    
    -- Betting Odds
    B365H FLOAT, B365D FLOAT, B365A FLOAT,
    BWH FLOAT, BWD FLOAT, BWA FLOAT,
    IWH FLOAT, IWD FLOAT, IWA FLOAT,
    LBH FLOAT, LBD FLOAT, LBA FLOAT,
    PSH FLOAT, PSD FLOAT, PSA FLOAT,
    WHH FLOAT, WHD FLOAT, WHA FLOAT,
    SJH FLOAT, SJD FLOAT, SJA FLOAT,
    VCH FLOAT, VCD FLOAT, VCA FLOAT,
    GBH FLOAT, GBD FLOAT, GBA FLOAT,
    BSH FLOAT, BSD FLOAT, BSA FLOAT
);

COPY INTO MATCHES
FROM @SOCCER_S3_STAGE/Match.csv
FILE_FORMAT = (FORMAT_NAME = SOCCER_CSV_FORMAT);


-- 5. CREATE AND LOAD: Player

CREATE OR REPLACE TABLE Player (
    id NUMBER,
    player_api_id NUMBER,
    player_name VARCHAR,
    player_fifa_api_id NUMBER,
    birthday TIMESTAMP,
    height FLOAT,
    weight NUMBER
);

COPY INTO Player
FROM @SOCCER_S3_STAGE/Player.csv
FILE_FORMAT = (FORMAT_NAME = SOCCER_CSV_FORMAT);

-- 6. CREATE AND LOAD: Player_Attributes

CREATE OR REPLACE TABLE Player_Attributes (
    id NUMBER,
    player_fifa_api_id NUMBER,
    player_api_id NUMBER,
    date TIMESTAMP,
    overall_rating NUMBER,
    potential NUMBER,
    preferred_foot VARCHAR,
    attacking_work_rate VARCHAR,
    defensive_work_rate VARCHAR,
    crossing NUMBER,
    finishing NUMBER,
    heading_accuracy NUMBER,
    short_passing NUMBER,
    volleys NUMBER,
    dribbling NUMBER,
    curve NUMBER,
    free_kick_accuracy NUMBER,
    long_passing NUMBER,
    ball_control NUMBER,
    acceleration NUMBER,
    sprint_speed NUMBER,
    agility NUMBER,
    reactions NUMBER,
    balance NUMBER,
    shot_power NUMBER,
    jumping NUMBER,
    stamina NUMBER,
    strength NUMBER,
    long_shots NUMBER,
    aggression NUMBER,
    interceptions NUMBER,
    positioning NUMBER,
    vision NUMBER,
    penalties NUMBER,
    marking NUMBER,
    standing_tackle NUMBER,
    sliding_tackle NUMBER,
    gk_diving NUMBER,
    gk_handling NUMBER,
    gk_kicking NUMBER,
    gk_positioning NUMBER,
    gk_reflexes NUMBER
);

COPY INTO Player_Attributes
FROM @SOCCER_S3_STAGE/Player_Attributes.csv
FILE_FORMAT = (FORMAT_NAME = SOCCER_CSV_FORMAT);


-- 7. CREATE AND LOAD: Team

CREATE OR REPLACE TABLE Team (
    id NUMBER,
    team_api_id NUMBER,
    team_fifa_api_id NUMBER,
    team_long_name VARCHAR,
    team_short_name VARCHAR
);

COPY INTO Team
FROM @SOCCER_S3_STAGE/Team.csv
FILE_FORMAT = (FORMAT_NAME = SOCCER_CSV_FORMAT);


-- 8. CREATE AND LOAD: Team_Attributes

CREATE OR REPLACE TABLE Team_Attributes (
    id NUMBER,
    team_fifa_api_id NUMBER,
    team_api_id NUMBER,
    date TIMESTAMP,
    buildUpPlaySpeed NUMBER,
    buildUpPlaySpeedClass VARCHAR,
    buildUpPlayDribbling NUMBER,
    buildUpPlayDribblingClass VARCHAR,
    buildUpPlayPassing NUMBER,
    buildUpPlayPassingClass VARCHAR,
    buildUpPlayPositioningClass VARCHAR,
    chanceCreationPassing NUMBER,
    chanceCreationPassingClass VARCHAR,
    chanceCreationCrossing NUMBER,
    chanceCreationCrossingClass VARCHAR,
    chanceCreationShooting NUMBER,
    chanceCreationShootingClass VARCHAR,
    chanceCreationPositioningClass VARCHAR,
    defencePressure NUMBER,
    defencePressureClass VARCHAR,
    defenceAggression NUMBER,
    defenceAggressionClass VARCHAR,
    defenceTeamWidth NUMBER,
    defenceTeamWidthClass VARCHAR,
    defenceDefenderLineClass VARCHAR
);

COPY INTO Team_Attributes
FROM @SOCCER_S3_STAGE/Team_Attributes.csv
FILE_FORMAT = (FORMAT_NAME = SOCCER_CSV_FORMAT);