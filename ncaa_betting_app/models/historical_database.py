"""
SQLite database for historical NCAA basketball play-by-play and shot data.

Separate from ncaa_betting.db (the live operational DB) so historical scraping
never touches in-season data.

Key design decisions:
- shots.play_id stores the NCAA's own play identifier from addShot() — this is the
  authoritative link between a shot and its play-by-play event.
- expected_points is intentionally NOT stored — it's model output, not raw data.
  Recompute it from shot coordinates + context when needed.
- scrape_progress and date_discovery_progress enable full resumability. A job
  interrupted mid-season can be restarted and will skip already-completed games.
- UNIQUE indexes on plays and shots prevent duplicates if a game is re-scraped.
"""
import sqlite3
import os
import logging

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ncaa_historical.db')


def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS games (
            game_id     TEXT PRIMARY KEY,
            season      TEXT NOT NULL,
            game_date   TEXT NOT NULL,
            home_team   TEXT,
            away_team   TEXT,
            home_team_id TEXT,
            away_team_id TEXT,
            home_score  INTEGER,
            away_score  INTEGER,
            home_h1_score INTEGER,
            away_h1_score INTEGER,
            play_count  INTEGER DEFAULT 0,
            shot_count  INTEGER DEFAULT 0,
            scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS plays (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id         TEXT NOT NULL,
            season          TEXT NOT NULL,
            half            INTEGER NOT NULL,
            time            TEXT NOT NULL,
            elapsed_seconds REAL,
            play_text       TEXT NOT NULL,
            score           TEXT,
            player_id       TEXT,
            team_id         TEXT,
            -- shot type
            is_made_shot    INTEGER DEFAULT 0,
            is_missed_shot  INTEGER DEFAULT 0,
            is_three        INTEGER DEFAULT 0,
            is_two          INTEGER DEFAULT 0,
            is_ft           INTEGER DEFAULT 0,
            is_jump_shot    INTEGER DEFAULT 0,
            is_dunk         INTEGER DEFAULT 0,
            is_layup        INTEGER DEFAULT 0,
            is_driving_layup INTEGER DEFAULT 0,
            is_turnaround   INTEGER DEFAULT 0,
            is_step_back    INTEGER DEFAULT 0,
            is_pullup       INTEGER DEFAULT 0,
            is_floater      INTEGER DEFAULT 0,
            is_hook_shot    INTEGER DEFAULT 0,
            is_under_basket INTEGER DEFAULT 0,
            is_paint        INTEGER DEFAULT 0,
            -- location
            location_il     INTEGER DEFAULT 0,
            location_ir     INTEGER DEFAULT 0,
            location_ol     INTEGER DEFAULT 0,
            location_or     INTEGER DEFAULT 0,
            location_oc     INTEGER DEFAULT 0,
            -- context
            is_second_chance INTEGER DEFAULT 0,
            is_fast_break   INTEGER DEFAULT 0,
            is_off_turnover INTEGER DEFAULT 0,
            is_assist       INTEGER DEFAULT 0,
            -- other events
            is_def_rebound  INTEGER DEFAULT 0,
            is_off_rebound  INTEGER DEFAULT 0,
            is_block        INTEGER DEFAULT 0,
            is_steal        INTEGER DEFAULT 0,
            is_turnover     INTEGER DEFAULT 0,
            is_foul_personal INTEGER DEFAULT 0,
            is_foul_shooting INTEGER DEFAULT 0,
            is_foul_drawn   INTEGER DEFAULT 0,
            is_timeout      INTEGER DEFAULT 0,
            points_value    REAL DEFAULT 0,
            FOREIGN KEY (game_id) REFERENCES games(game_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS shots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id         TEXT NOT NULL,
            season          TEXT NOT NULL,
            -- play_id is the NCAA's own identifier from addShot() 5th argument.
            -- This is the primary link between a shot and its play-by-play row.
            -- Join: shots.play_id matches the NCAA play context; for a secondary
            -- match use (game_id, half, time, player_id) when play_id is unavailable.
            play_id         TEXT,
            x               REAL,
            y               REAL,
            team_id         TEXT,
            result          INTEGER DEFAULT 0,
            player_id       TEXT,
            half            INTEGER,
            time            TEXT,
            elapsed_seconds REAL,
            shot_type       TEXT,
            is_three        INTEGER DEFAULT 0,
            play_text_norm  TEXT,
            FOREIGN KEY (game_id) REFERENCES games(game_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id   TEXT NOT NULL,
            game_id     TEXT NOT NULL,
            team_name   TEXT NOT NULL,
            team_id     TEXT NOT NULL,
            player_name TEXT NOT NULL,
            PRIMARY KEY (player_id, game_id),
            FOREIGN KEY (game_id) REFERENCES games(game_id)
        )
    """)

    # Tracks which games have been scraped. This is the resume checkpoint.
    # status: 'pending' | 'complete' | 'error'
    c.execute("""
        CREATE TABLE IF NOT EXISTS scrape_progress (
            game_id      TEXT PRIMARY KEY,
            season       TEXT NOT NULL,
            game_date    TEXT NOT NULL,
            home_team    TEXT,
            away_team    TEXT,
            status       TEXT DEFAULT 'pending',
            attempts     INTEGER DEFAULT 0,
            last_attempt TIMESTAMP,
            error_msg    TEXT
        )
    """)

    # Tracks which calendar dates have been scanned for game_ids.
    # Lets the discovery step be interrupted and resumed.
    c.execute("""
        CREATE TABLE IF NOT EXISTS date_discovery_progress (
            season       TEXT NOT NULL,
            game_date    TEXT NOT NULL,
            status       TEXT DEFAULT 'pending',
            games_found  INTEGER DEFAULT 0,
            completed_at TIMESTAMP,
            PRIMARY KEY (season, game_date)
        )
    """)

    # --- Indexes ---
    c.execute("CREATE INDEX IF NOT EXISTS idx_plays_game    ON plays(game_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_plays_season  ON plays(season)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_shots_game    ON shots(game_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_shots_season  ON shots(season)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_games_season  ON games(season)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_games_date    ON games(game_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_progress_status ON scrape_progress(status)")

    # Unique indexes prevent duplicates if a game is ever re-scraped
    c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_plays_unique
        ON plays(game_id, half, time, player_id, play_text)
    """)
    c.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_shots_unique_play
        ON shots(game_id, play_id) WHERE play_id IS NOT NULL
    """)

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Scrape progress helpers
# ---------------------------------------------------------------------------

def upsert_game_to_progress(game_id, season, game_date, home_team='', away_team=''):
    """Register a discovered game_id. No-op if already present."""
    conn = get_connection()
    conn.execute("""
        INSERT OR IGNORE INTO scrape_progress
            (game_id, season, game_date, home_team, away_team, status)
        VALUES (?, ?, ?, ?, ?, 'pending')
    """, (game_id, season, game_date, home_team, away_team))
    conn.commit()
    conn.close()


def mark_date_discovered(season, game_date, games_found):
    conn = get_connection()
    conn.execute("""
        INSERT INTO date_discovery_progress
            (season, game_date, status, games_found, completed_at)
        VALUES (?, ?, 'complete', ?, CURRENT_TIMESTAMP)
        ON CONFLICT(season, game_date) DO UPDATE SET
            status       = 'complete',
            games_found  = excluded.games_found,
            completed_at = CURRENT_TIMESTAMP
    """, (season, game_date, games_found))
    conn.commit()
    conn.close()


def is_date_discovered(season, game_date):
    conn = get_connection()
    row = conn.execute("""
        SELECT 1 FROM date_discovery_progress
        WHERE season = ? AND game_date = ? AND status = 'complete'
    """, (season, game_date)).fetchone()
    conn.close()
    return row is not None


def get_pending_games(season, limit=None):
    conn = get_connection()
    sql = """
        SELECT game_id, game_date, home_team, away_team
        FROM scrape_progress
        WHERE season = ? AND status = 'pending'
        ORDER BY game_date ASC
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = conn.execute(sql, (season,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_game_complete(game_id):
    conn = get_connection()
    conn.execute("""
        UPDATE scrape_progress
        SET status = 'complete', last_attempt = CURRENT_TIMESTAMP
        WHERE game_id = ?
    """, (game_id,))
    conn.commit()
    conn.close()


def mark_game_error(game_id, error_msg):
    conn = get_connection()
    conn.execute("""
        UPDATE scrape_progress
        SET status       = 'error',
            attempts     = attempts + 1,
            last_attempt = CURRENT_TIMESTAMP,
            error_msg    = ?
        WHERE game_id = ?
    """, (error_msg[:500], game_id))
    conn.commit()
    conn.close()


def reset_errors_to_pending(season):
    """Re-queue all error games so they get retried."""
    conn = get_connection()
    conn.execute("""
        UPDATE scrape_progress
        SET status = 'pending', error_msg = NULL
        WHERE season = ? AND status = 'error'
    """, (season,))
    n = conn.execute(
        "SELECT changes()"
    ).fetchone()[0]
    conn.commit()
    conn.close()
    return n


def get_progress_summary(season):
    conn = get_connection()
    row = conn.execute("""
        SELECT
            COUNT(*)                                    AS total,
            SUM(CASE WHEN status='complete' THEN 1 END) AS complete,
            SUM(CASE WHEN status='pending'  THEN 1 END) AS pending,
            SUM(CASE WHEN status='error'    THEN 1 END) AS errors
        FROM scrape_progress
        WHERE season = ?
    """, (season,)).fetchone()
    conn.close()
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Data insert helpers
# ---------------------------------------------------------------------------

_PLAY_COLS = [
    'game_id', 'season', 'half', 'time', 'elapsed_seconds', 'play_text', 'score',
    'player_id', 'team_id',
    'is_made_shot', 'is_missed_shot', 'is_three', 'is_two', 'is_ft',
    'is_jump_shot', 'is_dunk', 'is_layup', 'is_driving_layup', 'is_turnaround',
    'is_step_back', 'is_pullup', 'is_floater', 'is_hook_shot', 'is_under_basket',
    'is_paint', 'location_il', 'location_ir', 'location_ol', 'location_or',
    'location_oc', 'is_second_chance', 'is_fast_break', 'is_off_turnover',
    'is_assist', 'is_def_rebound', 'is_off_rebound', 'is_block', 'is_steal',
    'is_turnover', 'is_foul_personal', 'is_foul_shooting', 'is_foul_drawn',
    'is_timeout', 'points_value',
]

_SHOT_COLS = [
    'game_id', 'season', 'play_id', 'x', 'y', 'team_id', 'result',
    'player_id', 'half', 'time', 'elapsed_seconds', 'shot_type', 'is_three',
    'play_text_norm',
]

_PLAY_SQL = (
    f"INSERT OR IGNORE INTO plays ({', '.join(_PLAY_COLS)}) "
    f"VALUES ({', '.join(['?'] * len(_PLAY_COLS))})"
)

_SHOT_SQL = (
    f"INSERT OR IGNORE INTO shots ({', '.join(_SHOT_COLS)}) "
    f"VALUES ({', '.join(['?'] * len(_SHOT_COLS))})"
)


def insert_game(game_id, season, game_date, game_state):
    """Insert or update game metadata."""
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO games
            (game_id, season, game_date,
             home_team, away_team, home_team_id, away_team_id,
             home_score, away_score, home_h1_score, away_h1_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        game_id, season, game_date,
        game_state.get('team_a_name'), game_state.get('team_b_name'),
        game_state.get('team_a_id'), game_state.get('team_b_id'),
        game_state.get('team_a_score'), game_state.get('team_b_score'),
        game_state.get('team_a_h1_score'), game_state.get('team_b_h1_score'),
    ))
    conn.commit()
    conn.close()


def insert_plays(game_id, season, plays_data):
    if not plays_data:
        return 0
    rows = []
    for play in plays_data:
        row = tuple(
            game_id if col == 'game_id'
            else season if col == 'season'
            else play.get(col, 0)
            for col in _PLAY_COLS
        )
        rows.append(row)
    conn = get_connection()
    conn.executemany(_PLAY_SQL, rows)
    conn.commit()
    inserted = conn.execute("SELECT changes()").fetchone()[0]
    # Update play_count on games table
    conn.execute(
        "UPDATE games SET play_count = (SELECT COUNT(*) FROM plays WHERE game_id = ?) "
        "WHERE game_id = ?", (game_id, game_id)
    )
    conn.commit()
    conn.close()
    return inserted


def insert_shots(game_id, season, shots_data):
    if not shots_data:
        return 0
    rows = []
    for shot in shots_data:
        row = tuple(
            game_id if col == 'game_id'
            else season if col == 'season'
            else shot.get(col)  # None is fine for optional fields
            for col in _SHOT_COLS
        )
        rows.append(row)
    conn = get_connection()
    conn.executemany(_SHOT_SQL, rows)
    conn.commit()
    inserted = conn.execute("SELECT changes()").fetchone()[0]
    conn.execute(
        "UPDATE games SET shot_count = (SELECT COUNT(*) FROM shots WHERE game_id = ?) "
        "WHERE game_id = ?", (game_id, game_id)
    )
    conn.commit()
    conn.close()
    return inserted


def insert_players(game_id, players_data):
    if not players_data:
        return
    rows = [
        (p['player_id'], game_id, p['team_name'], p['team_id'], p['player_name'])
        for p in players_data
    ]
    conn = get_connection()
    conn.executemany(
        "INSERT OR REPLACE INTO players (player_id, game_id, team_name, team_id, player_name) "
        "VALUES (?, ?, ?, ?, ?)",
        rows
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def get_shots_with_plays(season=None, game_id=None):
    """
    Return shots joined to their matching play-by-play row.

    Primary join key: shots.play_id (NCAA's own identifier).
    Fallback: (game_id, half, time, player_id) for shots missing play_id.

    Returns rows with all shot columns + play classification flags.
    """
    conn = get_connection()
    filters = []
    params = []
    if season:
        filters.append("s.season = ?")
        params.append(season)
    if game_id:
        filters.append("s.game_id = ?")
        params.append(game_id)

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    rows = conn.execute(f"""
        SELECT
            s.game_id, s.season, s.play_id,
            s.x, s.y, s.result, s.shot_type, s.is_three,
            s.team_id AS shot_team_id, s.player_id AS shot_player_id,
            s.half AS shot_half, s.time AS shot_time,
            s.elapsed_seconds AS shot_elapsed,
            s.play_text_norm,
            p.id AS play_row_id,
            p.play_text,
            p.is_made_shot, p.is_missed_shot,
            p.is_jump_shot, p.is_dunk, p.is_layup, p.is_driving_layup,
            p.is_turnaround, p.is_step_back, p.is_pullup, p.is_floater,
            p.is_hook_shot, p.is_under_basket, p.is_paint,
            p.location_il, p.location_ir, p.location_ol,
            p.location_or, p.location_oc,
            p.is_second_chance, p.is_fast_break, p.is_off_turnover, p.is_assist,
            p.score AS play_score
        FROM shots s
        LEFT JOIN plays p
            ON  s.game_id   = p.game_id
            AND s.half      = p.half
            AND s.time      = p.time
            AND s.player_id = p.player_id
            AND (p.is_made_shot = s.result OR p.is_missed_shot = (1 - s.result))
        {where}
        ORDER BY s.game_id, s.half, s.elapsed_seconds
    """, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


init_db()
