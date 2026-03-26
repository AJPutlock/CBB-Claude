"""
SQLite database for NCAA basketball game data, caching, and historical stats.

Improvements over v1:
- Thread-local connection reuse (avoids open/close churn and repeated PRAGMAs)
- executemany() for bulk inserts (significantly faster than per-row INSERT)
- Column whitelist on update_game_state to prevent accidental SQL injection
- Input dicts are never mutated — copies are made before modification
- Uses UPSERT (INSERT OR IGNORE) to avoid SELECT-then-INSERT race conditions
- PRAGMA synchronous=NORMAL for faster writes (safe with WAL mode)
- Pre-compiled INSERT SQL strings (built once, reused every call)
"""
import sqlite3
import os
import threading
import logging
from datetime import date

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ncaa_betting.db')

# Thread-local storage for connection reuse
_local = threading.local()


def get_connection():
    """
    Get a thread-local database connection.
    Reuses the same connection per thread instead of opening/closing each call.
    PRAGMAs are set once per connection lifetime, not per query.
    """
    conn = getattr(_local, 'conn', None)
    if conn is None:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")  # Faster writes, still safe with WAL
        _local.conn = conn
    return conn


def close_connection():
    """Explicitly close this thread's connection (call on shutdown)."""
    conn = getattr(_local, 'conn', None)
    if conn:
        conn.close()
        _local.conn = None


# --- Whitelisted columns for game state updates ---
_GAME_COLUMNS = frozenset({
    'team_a_name', 'team_b_name', 'team_a_id', 'team_b_id',
    'team_a_score', 'team_b_score', 'team_a_h1_score', 'team_b_h1_score',
    'half', 'game_clock', 'status',
    'team_a_fouls_h1', 'team_b_fouls_h1', 'team_a_fouls_h2', 'team_b_fouls_h2',
    'is_timeout',
})

# --- Fixed column order for bulk inserts (avoids rebuilding SQL per row) ---
_PLAY_INSERT_COLS = [
    'game_id', 'half', 'time', 'play_text', 'score', 'player_id', 'team_id',
    'is_made_shot', 'is_missed_shot', 'is_three', 'is_two', 'is_ft',
    'is_jump_shot', 'is_dunk', 'is_layup', 'is_driving_layup', 'is_turnaround',
    'is_step_back', 'is_pullup', 'is_floater', 'is_hook_shot', 'is_under_basket',
    'is_paint', 'location_il', 'location_ir', 'location_ol', 'location_or',
    'location_oc', 'is_second_chance', 'is_fast_break', 'is_off_turnover',
    'is_assist', 'is_def_rebound', 'is_off_rebound', 'is_block', 'is_steal',
    'is_turnover', 'is_foul_personal', 'is_foul_shooting', 'is_foul_drawn',
    'is_timeout', 'points_value', 'expected_points', 'elapsed_seconds',
]

_SHOT_INSERT_COLS = [
    'game_id', 'play_id', 'x', 'y', 'team_id', 'result', 'player_id',
    'half', 'time', 'shot_type', 'is_three', 'play_text_norm',
    'is_late_clock', 'is_transition', 'expected_points',
]

# Pre-compile SQL strings once at module load
_PLAY_INSERT_SQL = (
    f"INSERT OR IGNORE INTO plays ({', '.join(_PLAY_INSERT_COLS)}) "
    f"VALUES ({', '.join(['?'] * len(_PLAY_INSERT_COLS))})"
)

_SHOT_INSERT_SQL = (
    f"INSERT OR IGNORE INTO shots ({', '.join(_SHOT_INSERT_COLS)}) "
    f"VALUES ({', '.join(['?'] * len(_SHOT_INSERT_COLS))})"
)


def init_db():
    """Initialize all database tables."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            game_id TEXT PRIMARY KEY,
            date TEXT NOT NULL,
            team_a_name TEXT,
            team_b_name TEXT,
            team_a_id TEXT,
            team_b_id TEXT,
            team_a_score INTEGER DEFAULT 0,
            team_b_score INTEGER DEFAULT 0,
            team_a_h1_score INTEGER DEFAULT 0,
            team_b_h1_score INTEGER DEFAULT 0,
            half INTEGER DEFAULT 1,
            game_clock TEXT DEFAULT '20:00',
            status TEXT DEFAULT 'scheduled',
            team_a_fouls_h1 INTEGER DEFAULT 0,
            team_b_fouls_h1 INTEGER DEFAULT 0,
            team_a_fouls_h2 INTEGER DEFAULT 0,
            team_b_fouls_h2 INTEGER DEFAULT 0,
            is_timeout INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id TEXT NOT NULL,
            game_id TEXT NOT NULL,
            team_name TEXT NOT NULL,
            team_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            PRIMARY KEY (player_id, game_id),
            FOREIGN KEY (game_id) REFERENCES games(game_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plays (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            half INTEGER NOT NULL,
            time TEXT NOT NULL,
            play_text TEXT NOT NULL,
            score TEXT,
            player_id TEXT,
            team_id TEXT,
            is_made_shot INTEGER DEFAULT 0,
            is_missed_shot INTEGER DEFAULT 0,
            is_three INTEGER DEFAULT 0,
            is_two INTEGER DEFAULT 0,
            is_ft INTEGER DEFAULT 0,
            is_jump_shot INTEGER DEFAULT 0,
            is_dunk INTEGER DEFAULT 0,
            is_layup INTEGER DEFAULT 0,
            is_driving_layup INTEGER DEFAULT 0,
            is_turnaround INTEGER DEFAULT 0,
            is_step_back INTEGER DEFAULT 0,
            is_pullup INTEGER DEFAULT 0,
            is_floater INTEGER DEFAULT 0,
            is_hook_shot INTEGER DEFAULT 0,
            is_under_basket INTEGER DEFAULT 0,
            is_paint INTEGER DEFAULT 0,
            location_il INTEGER DEFAULT 0,
            location_ir INTEGER DEFAULT 0,
            location_ol INTEGER DEFAULT 0,
            location_or INTEGER DEFAULT 0,
            location_oc INTEGER DEFAULT 0,
            is_second_chance INTEGER DEFAULT 0,
            is_fast_break INTEGER DEFAULT 0,
            is_off_turnover INTEGER DEFAULT 0,
            is_assist INTEGER DEFAULT 0,
            is_def_rebound INTEGER DEFAULT 0,
            is_off_rebound INTEGER DEFAULT 0,
            is_block INTEGER DEFAULT 0,
            is_steal INTEGER DEFAULT 0,
            is_turnover INTEGER DEFAULT 0,
            is_foul_personal INTEGER DEFAULT 0,
            is_foul_shooting INTEGER DEFAULT 0,
            is_foul_drawn INTEGER DEFAULT 0,
            is_timeout INTEGER DEFAULT 0,
            points_value REAL DEFAULT 0,
            expected_points REAL DEFAULT 0,
            elapsed_seconds REAL DEFAULT 0,
            FOREIGN KEY (game_id) REFERENCES games(game_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            play_id TEXT,
            x REAL,
            y REAL,
            team_id TEXT,
            result INTEGER DEFAULT 0,
            player_id TEXT,
            half INTEGER,
            time TEXT,
            shot_type TEXT,
            is_three INTEGER DEFAULT 0,
            play_text_norm TEXT,
            is_late_clock INTEGER DEFAULT 0,
            is_transition INTEGER DEFAULT 0,
            expected_points REAL DEFAULT 0,
            FOREIGN KEY (game_id) REFERENCES games(game_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scrape_cache (
            game_id TEXT PRIMARY KEY,
            last_play_index INTEGER DEFAULT 0,
            last_shot_count INTEGER DEFAULT 0,
            last_scrape_time TIMESTAMP,
            etag TEXT,
            FOREIGN KEY (game_id) REFERENCES games(game_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS odds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            source TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            spread_team_a REAL,
            spread_odds_a INTEGER,
            spread_team_b REAL,
            spread_odds_b INTEGER,
            total_points REAL,
            over_odds INTEGER,
            under_odds INTEGER,
            moneyline_a INTEGER,
            moneyline_b INTEGER,
            is_live INTEGER DEFAULT 0,
            FOREIGN KEY (game_id) REFERENCES games(game_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS player_season_stats (
            player_id TEXT PRIMARY KEY,
            player_name TEXT,
            team_id TEXT,
            fg_pct REAL DEFAULT 0.0,
            fg3_pct REAL DEFAULT 0.0,
            ft_pct REAL DEFAULT 0.0,
            fg_attempts INTEGER DEFAULT 0,
            fg3_attempts INTEGER DEFAULT 0,
            ft_attempts INTEGER DEFAULT 0,
            last_updated TIMESTAMP
        )
    """)

    # Indexes (added odds composite index for faster live/pregame lookups)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_plays_game ON plays(game_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_plays_game_half ON plays(game_id, half)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_shots_game ON shots(game_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_odds_game ON odds(game_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_odds_game_live ON odds(game_id, is_live)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_game ON players(game_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_games_date ON games(date)")

    # Migration: recreate players table with composite PK if it has single PK
    # This fixes the bug where a player in multiple games only gets stored once
    try:
        cursor.execute("PRAGMA table_info(players)")
        cols = {r[1]: r for r in cursor.fetchall()}
        # If player_id is the sole PK (old schema), rebuild the table
        if cols and all(r[5] <= 1 for r in cols.values()):  # pk column ≤ 1
            cursor.execute("ALTER TABLE players RENAME TO players_old")
            cursor.execute("""
                CREATE TABLE players (
                    player_id TEXT NOT NULL,
                    game_id TEXT NOT NULL,
                    team_name TEXT NOT NULL,
                    team_id TEXT NOT NULL,
                    player_name TEXT NOT NULL,
                    PRIMARY KEY (player_id, game_id),
                    FOREIGN KEY (game_id) REFERENCES games(game_id)
                )
            """)
            cursor.execute("""
                INSERT OR IGNORE INTO players
                SELECT player_id, game_id, team_name, team_id, player_name
                FROM players_old
            """)
            cursor.execute("DROP TABLE players_old")
            logger.info("Migrated players table to composite (player_id, game_id) PK")
    except Exception as e:
        pass  # Already migrated or other error — safe to continue

    # Fix shots unique index — old version lacked WHERE play_id IS NOT NULL,
    # causing NULL play_ids (live shots) to collide on the constraint.
    try:
        cursor.execute("DROP INDEX IF EXISTS idx_shots_unique")
        cursor.execute("DROP INDEX IF EXISTS idx_shots_unique_play")
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_shots_unique_play "
            "ON shots(game_id, play_id) WHERE play_id IS NOT NULL"
        )
    except Exception:
        pass

    # Dedup plays then add unique index — order matters: dedup first
    try:
        cursor.execute("""
            DELETE FROM plays WHERE id NOT IN (
                SELECT MIN(id) FROM plays
                GROUP BY game_id, half, time, player_id, play_text
            )
        """)
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_plays_unique "
            "ON plays(game_id, half, time, player_id, play_text)"
        )
    except Exception:
        pass

    # Migrations: add columns that may not exist in older DB versions
    _migrations = [
        "ALTER TABLE odds ADD COLUMN spread_a_team_name TEXT",
        "ALTER TABLE shots ADD COLUMN play_id TEXT",
        "ALTER TABLE shots ADD COLUMN is_three INTEGER DEFAULT 0",
        "ALTER TABLE shots ADD COLUMN play_text_norm TEXT",
    ]
    for migration in _migrations:
        try:
            cursor.execute(migration)
        except Exception:
            pass  # Column already exists — safe to ignore

    conn.commit()


def get_or_create_game(game_id, game_date=None):
    """Get or create a game entry. Uses INSERT OR IGNORE to avoid race conditions."""
    conn = get_connection()
    if game_date is None:
        game_date = date.today().isoformat()

    # Single atomic operation instead of SELECT-then-INSERT
    conn.execute(
        "INSERT OR IGNORE INTO games (game_id, date) VALUES (?, ?)",
        (game_id, game_date)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM games WHERE game_id = ?", (game_id,)).fetchone()
    return dict(row)


def update_game_state(game_id, **kwargs):
    """
    Update game state fields.
    Only whitelisted columns are accepted to prevent accidental SQL issues.
    """
    valid = {k: v for k, v in kwargs.items() if k in _GAME_COLUMNS}
    if not valid:
        return

    invalid = set(kwargs.keys()) - _GAME_COLUMNS
    if invalid:
        logger.warning(f"Ignored invalid game columns: {invalid}")

    conn = get_connection()
    set_clause = ", ".join(f"{k} = ?" for k in valid.keys())
    values = list(valid.values()) + [game_id]
    conn.execute(
        f"UPDATE games SET {set_clause}, last_updated = CURRENT_TIMESTAMP WHERE game_id = ?",
        values
    )
    conn.commit()


def get_cache_state(game_id):
    """Get the cache state for a game (last play pulled, etc)."""
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM scrape_cache WHERE game_id = ?", (game_id,)
    ).fetchone()
    return dict(row) if row else None


def update_cache_state(game_id, last_play_index, last_shot_count):
    """Update the cache tracking for a game. Uses excluded.* for cleaner UPSERT."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO scrape_cache (game_id, last_play_index, last_shot_count, last_scrape_time)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(game_id) DO UPDATE SET
            last_play_index = excluded.last_play_index,
            last_shot_count = excluded.last_shot_count,
            last_scrape_time = CURRENT_TIMESTAMP
    """, (game_id, last_play_index, last_shot_count))
    conn.commit()


def insert_plays(game_id, plays_data):
    """
    Bulk insert plays using executemany with a fixed column order.
    Does NOT mutate the input dicts — builds tuples from a fixed column list.
    """
    if not plays_data:
        return

    rows = []
    for play in plays_data:
        row = tuple(
            game_id if col == 'game_id' else play.get(col, 0)
            for col in _PLAY_INSERT_COLS
        )
        rows.append(row)

    conn = get_connection()
    conn.executemany(_PLAY_INSERT_SQL, rows)
    conn.commit()


def insert_shots(game_id, shots_data):
    """
    Bulk insert shots using executemany with a fixed column order.
    Does NOT mutate the input dicts.
    """
    if not shots_data:
        return

    rows = []
    for shot in shots_data:
        row = tuple(
            game_id if col == 'game_id' else shot.get(col, 0)
            for col in _SHOT_INSERT_COLS
        )
        rows.append(row)

    conn = get_connection()
    conn.executemany(_SHOT_INSERT_SQL, rows)
    conn.commit()


def insert_players(game_id, players_data):
    """Bulk insert player records using executemany. Does NOT mutate input."""
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


def insert_odds(game_id, odds_data):
    """Insert odds record. Does NOT mutate the input dict."""
    data = dict(odds_data)  # Copy to avoid mutating caller's dict
    data['game_id'] = game_id
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['?'] * len(data))
    conn = get_connection()
    conn.execute(
        f"INSERT INTO odds ({columns}) VALUES ({placeholders})",
        list(data.values())
    )
    conn.commit()


def get_all_plays(game_id):
    """Get all plays for a game ordered by occurrence."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM plays WHERE game_id = ? ORDER BY id ASC", (game_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_all_shots(game_id):
    """Get all shots for a game."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM shots WHERE game_id = ? ORDER BY id ASC", (game_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_game_odds(game_id, live=False):
    """Get latest odds for a game."""
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM odds WHERE game_id = ? AND is_live = ? ORDER BY timestamp DESC LIMIT 1",
        (game_id, int(live))
    ).fetchone()
    return dict(row) if row else None


def get_games_for_date(game_date=None):
    """Get all games for a given date."""
    if game_date is None:
        game_date = date.today().isoformat()
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM games WHERE date = ?", (game_date,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_players_for_game(game_id):
    """Get all players for a game."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM players WHERE game_id = ?", (game_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def clear_game_data(game_id):
    """Clear all data for a game (for fresh re-scrape)."""
    conn = get_connection()
    conn.execute("DELETE FROM plays WHERE game_id = ?", (game_id,))
    conn.execute("DELETE FROM shots WHERE game_id = ?", (game_id,))
    conn.execute("DELETE FROM scrape_cache WHERE game_id = ?", (game_id,))
    conn.commit()


# Initialize DB on import
init_db()
