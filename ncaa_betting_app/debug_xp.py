"""
App xP Debug Script
===================
Shows exactly how the app generates xP per shot — same code path as
calculate_game_expected_score, printed row by row.

Run from your project root:
    python debug_xp.py [game_id]

Columns:
  Half / Time    — game clock
  PBP Type       — shot type from classify_shot_type_from_play (PBP flags)
  Coord Type     — shot type stored in shots table (description-derived)
  Final Type     — what the model actually uses after priority resolution
  Coords         — x/y from shot chart (-- if no coord match)
  Dist           — distance from basket in feet
  Clock          — shot clock phase (early/mid/late)
  xP             — expected points for this shot
  Player         — player name
"""

import sys
import os
import math
import sqlite3

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

try:
    from models.database import get_connection, get_all_plays, get_all_shots
    from models.expected_points import (
        classify_shot_type_from_play,
        classify_shot_clock,
        calculate_expected_points,
        _normalize_time,
    )
    from models.player_stats import PlayerStatsLookup
except ImportError as e:
    print(f"[!] Import error: {e}")
    print("Make sure you're running from the project root.")
    sys.exit(1)

# ── Court geometry ────────────────────────────────────────────────────────────
COURT_WIDTH_PX  = 940
COURT_HEIGHT_PX = 500
LEFT_BASKET_PX  = (50,  250)
RIGHT_BASKET_PX = (890, 250)
PX_PER_FOOT     = 220.9 / 22.0

def _dist_ft(x_pct, y_pct):
    x_px = (x_pct / 100.0) * COURT_WIDTH_PX
    y_px = (y_pct / 100.0) * COURT_HEIGHT_PX
    if x_px <= COURT_WIDTH_PX / 2:
        bx, by = LEFT_BASKET_PX
    else:
        bx, by = RIGHT_BASKET_PX
    return math.sqrt((x_px - bx)**2 + (y_px - by)**2) / PX_PER_FOOT


def get_recent_game(conn):
    return conn.execute("""
        SELECT g.game_id, g.team_a_name, g.team_b_name, g.team_a_score, g.team_b_score, g.status
        FROM games g
        WHERE EXISTS (SELECT 1 FROM shots s WHERE s.game_id = g.game_id AND s.x IS NOT NULL)
        ORDER BY g.rowid DESC LIMIT 1
    """).fetchone()


def get_game(conn, game_id):
    return conn.execute(
        "SELECT * FROM games WHERE game_id = ?", (game_id,)
    ).fetchone()


def get_players(conn, game_id):
    rows = conn.execute(
        "SELECT player_id, player_name, team_id, team_name FROM players WHERE game_id = ?",
        (game_id,)
    ).fetchall()
    return {str(r['player_id']): r for r in rows}

def main():
    game_id = sys.argv[1] if len(sys.argv) > 1 else None

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    if game_id is None:
        row = get_recent_game(conn)
        if not row:
            print("[!] No games with shot data found.")
            sys.exit(1)
        game_id = row['game_id']

    game    = get_game(conn, game_id)
    players = get_players(conn, game_id)
    plays   = get_all_plays(game_id)
    shots   = get_all_shots(game_id)

    team_a = game['team_a_name'] or 'Team A'
    team_b = game['team_b_name'] or 'Team B'

    # Load player shooting stats for shooter-adjusted xP
    # Convert sqlite3.Row objects to plain dicts so .get() works correctly
    try:
        _pstats_lookup = PlayerStatsLookup()
        players_list   = [dict(p) for p in players.values()]
        players_stats  = _pstats_lookup.build_game_stats(players_list)
        matched = len(players_stats)
        total   = len(players_list)
        print(f"Player stats loaded: {matched}/{total} players matched")
    except Exception as e:
        print(f"[!] Player stats load failed: {e}")
        players_stats = {}

    print(f"\nApp xP Debug: {team_a} vs {team_b}")
    print(f"Game ID: {game_id}  |  Status: {game['status']}")
    print(f"Score: {game['team_a_score']} – {game['team_b_score']}")
    print("=" * 130)

    # Build team name map from players
    team_name_map = {}
    for p in players.values():
        tid   = str(p['team_id'] or '')
        tname = (p['team_name'] or '').strip()
        if tid and tname:
            team_name_map[tid] = tname

    # Replicate exactly what calculate_game_expected_score does
    # ── Build shot_coords lookup ──────────────────────────────────────────────
    shot_coords = {}
    for s in shots:
        if s.get('x') is None or s.get('y') is None:
            continue
        player_id = str(s.get('player_id') or '')
        if not player_id:
            continue
        half      = int(s.get('half', 1))
        time_norm = _normalize_time(s.get('time', ''))
        result    = int(s.get('result', 0))
        key = (half, time_norm, player_id, result)
        shot_coords[key] = (s['x'], s['y'], s.get('shot_type'))

    # ── Process plays ─────────────────────────────────────────────────────────
    team_xp   = {}
    team_shots = {}
    possession_start = None

    print(f"\n{'#':>4}  {'Team':<18}  {'Half':>4}  {'Time':>6}  "
          f"{'PBP Type':<12}  {'Coord Type':<12}  {'Final Type':<12}  "
          f"{'x':>5}  {'y':>5}  {'Dist':>6}  {'Clock':<6}  "
          f"{'Lg Avg':>7}  {'Plyr Base':>10}  {'Coord Adj':>10}  {'Coord Δ':>8}  {'Total Δ':>8}  Player")
    print("-" * 185)

    shot_num = 0
    for play in plays:
        team_id = play.get('team_id', '')
        if team_id and team_id not in team_xp:
            team_xp[team_id]    = 0.0
            team_shots[team_id] = {'made': 0, 'missed': 0, 'ft_made': 0, 'ft_missed': 0}

        is_shot = play.get('is_made_shot', 0) or play.get('is_missed_shot', 0)
        if not (is_shot and team_id):
            # Track possession for shot clock
            if play.get('is_turnover') or play.get('is_def_rebound'):
                possession_start = play.get('elapsed_seconds', 0)
            continue

        shot_num  += 1
        pbp_type   = classify_shot_type_from_play(play)
        elapsed    = play.get('elapsed_seconds', 0)
        player_id  = str(play.get('player_id') or '')
        half       = int(play.get('half', 1))
        time_norm  = _normalize_time(play.get('time', ''))
        result     = 1 if play.get('is_made_shot') else 0
        is_ft      = play.get('is_ft', 0)

        # Coordinate lookup
        coords = shot_coords.get((half, time_norm, player_id, result))
        if coords:
            x_pct, y_pct, coord_type = coords
            coord_type_str = coord_type or '—'
        else:
            x_pct = y_pct = coord_type = None
            coord_type_str = '—'

        # Shot type priority resolution (matches expected_points.py exactly)
        final_type = pbp_type
        if pbp_type in ('ft', 'three'):
            pass
        elif coord_type in ('dunk', 'hook', 'layup', 'rim'):
            final_type = coord_type
        elif pbp_type == 'midrange' and coord_type:
            final_type = coord_type

        # Clock phase
        clock_phase = classify_shot_clock(possession_start, elapsed)

        # xP breakdown — 5 values:
        #   league_avg  : flat league average, no coords, no player stats
        #   player_base : player's own pct * point value, no coords
        #   coord_adj   : player pct anchored to coord zone (final value app uses)
        #   coord_delta : coord_adj - player_base (coordinate contribution)
        #   total_delta : coord_adj - league_avg  (full adjustment)
        p_stats     = players_stats.get(player_id)
        # League avg and player base use 'early' clock so they show pure
        # percentage values without shot clock penalty — for comparison only.
        # The coord_adj and app xP include the actual shot clock multiplier.
        xp_league   = calculate_expected_points(final_type, None,  None,  None,    'early')
        xp_player   = calculate_expected_points(final_type, None,  None,  p_stats, 'early')
        xp_coord    = calculate_expected_points(final_type, x_pct, y_pct, p_stats, clock_phase)
        xp          = xp_coord  # app uses coord-adjusted value

        coord_delta = xp_coord - xp_player
        total_delta = xp_coord - xp_league

        team_xp[team_id] += xp
        if is_ft:
            if result:
                team_shots[team_id]['ft_made']   += 1
            else:
                team_shots[team_id]['ft_missed'] += 1
        else:
            if result:
                team_shots[team_id]['made']   += 1
            else:
                team_shots[team_id]['missed'] += 1

        # Display
        dist_str = f"{_dist_ft(x_pct, y_pct):.1f}ft" if (x_pct is not None) else "  —"
        x_str    = f"{x_pct:.1f}" if x_pct is not None else "  —"
        y_str    = f"{y_pct:.1f}" if y_pct is not None else "  —"
        team_str = team_name_map.get(str(team_id), f"Team {team_id}")[:18]
        player_r = players.get(player_id)
        pname    = (player_r['player_name'] if player_r else f"id:{player_id}")[:22]

        # Flag if final_type differs from pbp_type (means coord/description changed it)
        type_flag = '*' if final_type != pbp_type else ' '

        has_coords = x_pct is not None
        has_stats  = p_stats is not None

        # FT: show player's FT% as player base, no coord col (no location for FTs)
        # Dunk: fixed value regardless of shooter or location — nothing to break down
        # All other types: show player base, coord adj, and both deltas
        if final_type == 'ft':
            plyr_str   = f"{xp_player:.3f}" if has_stats else "   —"
            coord_str  = "   —"
            cdelta_str = "   —"
            tdelta_str = f"{total_delta:+.3f}" if has_stats else "   —"
        elif final_type == 'dunk':
            plyr_str   = "   —"
            coord_str  = "   —"
            cdelta_str = "   —"
            tdelta_str = "   —"
        else:
            plyr_str   = f"{xp_player:.3f}" if has_stats else "   —"
            coord_str  = f"{xp_coord:.3f}"  if has_coords else "   —"
            cdelta_str = f"{coord_delta:+.3f}" if (has_stats and has_coords) else "   —"
            tdelta_str = f"{total_delta:+.3f}" if (has_stats or has_coords) else "   —"

        print(f"{shot_num:>4}  {team_str:<18}  {half:>4}  {time_norm:>6}  "
              f"{pbp_type:<12}  {coord_type_str:<12}  {final_type+type_flag:<12}  "
              f"{x_str:>5}  {y_str:>5}  {dist_str:>6}  {clock_phase:<6}  "
              f"{xp_league:>7.3f}  {plyr_str:>10}  {coord_str:>10}  {cdelta_str:>8}  {tdelta_str:>8}  {pname}")

        # Update possession tracking
        if play.get('is_turnover') or play.get('is_def_rebound') or is_shot:
            possession_start = elapsed

    # ── Team totals ───────────────────────────────────────────────────────────
    print("\n" + "=" * 130)
    print("TEAM TOTALS (exactly as shown in app)")
    print("=" * 130)
    # Recompute base totals for comparison
    team_xp_base = {}
    for play in plays:
        tid = play.get('team_id', '')
        if not (tid and (play.get('is_made_shot') or play.get('is_missed_shot'))):
            continue
        pbp_t  = classify_shot_type_from_play(play)
        pid    = str(play.get('player_id') or '')
        half   = int(play.get('half', 1))
        tnorm  = _normalize_time(play.get('time', ''))
        res    = 1 if play.get('is_made_shot') else 0
        coords = shot_coords.get((half, tnorm, pid, res))
        x2, y2 = (coords[0], coords[1]) if coords else (None, None)
        ct     = coords[2] if coords else None
        ft2 = pbp_t
        if pbp_t not in ('ft', 'three'):
            if ct in ('dunk', 'hook', 'layup', 'rim'):
                ft2 = ct
            elif pbp_t == 'midrange' and ct:
                ft2 = ct
        cl = classify_shot_clock(None, play.get('elapsed_seconds', 0))
        team_xp_base[tid] = team_xp_base.get(tid, 0.0) + calculate_expected_points(ft2, x2, y2, None, cl)

    print(f"\n  {'Team':<25}  {'FGM-FGA':>8}  {'FTM-FTA':>8}  {'Base xP':>9}  {'Adj xP':>8}  {'Diff':>6}  {'Actual':>7}")
    print(f"  {'-'*80}")

    for tid, xp in sorted(team_xp.items()):
        name   = team_name_map.get(str(tid), f"Team {tid}")[:25]
        s      = team_shots.get(tid, {})
        fgm    = s.get('made', 0)
        fga    = fgm + s.get('missed', 0)
        ftm    = s.get('ft_made', 0)
        fta    = ftm + s.get('ft_missed', 0)
        actual = '?'
        if (game['team_a_name'] or '').strip() == name.strip():
            actual = game['team_a_score']
        elif (game['team_b_name'] or '').strip() == name.strip():
            actual = game['team_b_score']
        base_xp = team_xp_base.get(tid, 0.0)
        diff    = xp - base_xp
        print(f"  {name:<25}  {fgm:>3}-{fga:<4}  {ftm:>3}-{fta:<4}  "
              f"{base_xp:>9.1f}  {xp:>8.1f}  {diff:>+6.1f}  {str(actual):>7}")

    print(f"\n  * = coord/description type overrode PBP type")
    print(f"  Total shot plays processed: {shot_num}")
    conn.close()
    print()


if __name__ == '__main__':
    main()
