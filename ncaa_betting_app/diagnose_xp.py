"""
Expected Points Diagnostic Script
==================================
Run from your project root:
    python diagnose_xp.py [game_id]

If no game_id is provided, uses the most recent game with play data.

Checks:
  1. Whether play data exists in the DB
  2. Whether shot coordinate data exists
  3. Whether play_id is present on shots (required for coordinate join)
  4. Runs xP calculation with and without coordinates and compares
  5. Breaks down xP by shot type so you can spot anomalies
  6. Compares total xP to actual score as a sanity check
"""

import sys
import os
import sqlite3
from collections import defaultdict

# ── Path setup ────────────────────────────────────────────────────────────────
# Add project root to path so we can import project modules
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

try:
    from models.database import get_all_plays, get_all_shots, get_games_for_date, get_connection
    from models.expected_points import (
        calculate_expected_points,
        classify_shot_type_from_play,
        classify_shot_clock,
        _coords_to_dist_angle,
    )
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you're running this from the project root directory.")
    sys.exit(1)


# ── Helpers ───────────────────────────────────────────────────────────────────

def find_game_id(requested_id=None):
    """Return a game_id to diagnose — either the one provided or most recent with plays."""
    from datetime import date
    games = get_games_for_date(date.today().isoformat())

    if requested_id:
        match = next((g for g in games if g['game_id'] == requested_id), None)
        if not match:
            print(f"Game {requested_id} not found in today's games.")
            sys.exit(1)
        return requested_id, match

    # Pick game with most plays
    best_id, best_game, best_count = None, None, 0
    for g in games:
        plays = get_all_plays(g['game_id'])
        if len(plays) > best_count:
            best_count = len(plays)
            best_id = g['game_id']
            best_game = g
    if not best_id:
        print("No games with play data found for today.")
        sys.exit(1)
    return best_id, best_game


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def check(label, passed, detail=''):
    status = '✓' if passed else '✗'
    detail_str = f"  ({detail})" if detail else ''
    print(f"  [{status}] {label}{detail_str}")


# ── Main diagnostic ───────────────────────────────────────────────────────────

def run(game_id, game):
    plays = get_all_plays(game_id)
    shots = get_all_shots(game_id)

    team_a_name = game.get('team_a_name', 'Team A')
    team_b_name = game.get('team_b_name', 'Team B')
    team_a_id   = game.get('team_a_id', '')
    team_b_id   = game.get('team_b_id', '')
    team_names  = {team_a_id: team_a_name, team_b_id: team_b_name}

    # ── 1. Data presence ──────────────────────────────────────────────────────
    section("1. DATA PRESENCE")
    check("Play data exists",       len(plays) > 0,  f"{len(plays)} plays")
    check("Shot coordinate data",   len(shots) > 0,  f"{len(shots)} shots")

    shot_plays = [p for p in plays if p.get('is_made_shot') or p.get('is_missed_shot')]
    check("Shot plays found",       len(shot_plays) > 0, f"{len(shot_plays)} shot plays")

    # ── 2. coordinate join check via (half, time) ───────────────────────────
    section("2. COORDINATE JOIN (half + time matching)")
    shots_with_coords = [s for s in shots if s.get('x') is not None and s.get('y') is not None]
    check("shots have x,y coords", len(shots_with_coords) > 0,
          f"{len(shots_with_coords)}/{len(shots)}")

    def _norm_time(t):
        t = (t or '').strip().split('.')[0]
        parts = t.split(':')
        if len(parts) == 3:
            t = f"{parts[0]}:{parts[1]}"
        return t

    # Build coordinate lookup: (half, time_norm, player_id, result)
    shot_coord_map = {}
    for s in shots_with_coords:
        player_id = str(s.get('player_id') or '')
        if not player_id:
            continue
        key = (int(s.get('half', 1)), _norm_time(s.get('time','')),
               player_id, int(s.get('result', 0)))
        shot_coord_map[key] = (s['x'], s['y'])

    ft_plays = [p for p in shot_plays if p.get('is_ft')]
    fg_plays = [p for p in shot_plays if not p.get('is_ft')]

    matched_coords = 0
    unmatched_sample = []
    for p in fg_plays:
        player_id = str(p.get('player_id') or '')
        result    = 1 if p.get('is_made_shot') else 0
        key = (int(p.get('half', 1)), _norm_time(p.get('time','')),
               player_id, result)
        if key in shot_coord_map:
            matched_coords += 1
        elif len(unmatched_sample) < 3:
            unmatched_sample.append(key)

    coord_join_ok = matched_coords > 0
    check(f"FTs excluded ({len(ft_plays)} FTs, {len(fg_plays)} FG attempts)",
          True, f"{len(ft_plays)} FTs filtered")
    check("FG shot plays matched to coordinates",
          coord_join_ok, f"{matched_coords}/{len(fg_plays)}")
    if unmatched_sample:
        print("  [!] Sample unmatched keys (half, time, player_id, result):")
        for k in unmatched_sample:
            print(f"      {k}")

    # ── 3. Shot type breakdown ────────────────────────────────────────────────
    section("3. SHOT TYPE BREAKDOWN")
    type_counts = defaultdict(int)
    for p in shot_plays:
        st = classify_shot_type_from_play(p)
        type_counts[st] += 1

    print(f"  {'Shot Type':<16} {'Count':>6}")
    print(f"  {'-'*24}")
    for st, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {st:<16} {count:>6}")

    # ── 4. xP calculation — flat (no coordinates) ────────────────────────────
    section("4. XP CALCULATION — FLAT (no coordinates)")
    team_xp_flat = defaultdict(float)
    shot_detail  = []

    possession_start = None
    for play in plays:
        is_shot = play.get('is_made_shot') or play.get('is_missed_shot')
        if is_shot:
            team_id    = play.get('team_id', '')
            shot_type  = classify_shot_type_from_play(play)
            if shot_type == 'ft':
                team_xp_flat[team_id] += calculate_expected_points('ft', None, None, None, 'early')
                continue
            elapsed    = play.get('elapsed_seconds', 0)
            clock_phase = classify_shot_clock(possession_start, elapsed)
            xp = calculate_expected_points(shot_type, None, None, None, clock_phase)
            team_xp_flat[team_id] += xp
            shot_detail.append({
                'team_id': team_id, 'shot_type': shot_type,
                'elapsed': elapsed, 'clock_phase': clock_phase,
                'time': play.get('time', ''), 'half': play.get('half', 1),
                'player_id': str(play.get('player_id') or ''),
                'is_made_shot': play.get('is_made_shot', 0),
                'xp_flat': xp, 'x': None, 'y': None, 'xp_coord': None,
            })

        if play.get('is_turnover') or play.get('is_def_rebound') or is_shot:
            possession_start = play.get('elapsed_seconds', 0)

    print(f"  {'Team':<25} {'xP (flat)':>10} {'Actual Score':>13}")
    print(f"  {'-'*50}")
    for tid, name in team_names.items():
        if not tid:
            continue
        xp    = team_xp_flat.get(tid, 0.0)
        score = game.get('team_a_score' if tid == team_a_id else 'team_b_score', '?')
        diff_flag = ''
        try:
            diff = abs(xp - float(score))
            diff_flag = f"  ← Δ{diff:.1f}"
            if diff > 20:
                diff_flag += "  ⚠ large gap"
        except (TypeError, ValueError):
            pass
        print(f"  {name:<25} {xp:>10.1f} {str(score):>13}{diff_flag}")

    # ── 5. xP calculation — with coordinates (if available) ──────────────────
    # shot_coord_map already built above

    if shot_coord_map:
        section("5. XP CALCULATION — WITH COORDINATES")
        team_xp_coord = defaultdict(float)
        coord_hits, coord_misses = 0, 0

        for d in shot_detail:
            player_id = str(d.get('player_id') or '')
            result    = 1 if d.get('is_made_shot') else 0
            key       = (int(d.get('half', 1)), _norm_time(d.get('time', '')),
                         player_id, result)
            coords    = shot_coord_map.get(key)
            if coords:
                xp = calculate_expected_points(
                    d['shot_type'], coords[0], coords[1], None, d['clock_phase']
                )
                d['x'], d['y'], d['xp_coord'] = coords[0], coords[1], xp
                team_xp_coord[d['team_id']] += xp
                coord_hits += 1
            else:
                team_xp_coord[d['team_id']] += d['xp_flat']
                coord_misses += 1

        print(f"  Shots with coordinates: {coord_hits}, without: {coord_misses}")
        print(f"\n  {'Team':<25} {'xP (flat)':>10} {'xP (coord)':>11} {'Diff':>6}")
        print(f"  {'-'*55}")
        for tid, name in team_names.items():
            if not tid:
                continue
            flat  = team_xp_flat.get(tid, 0.0)
            coord = team_xp_coord.get(tid, 0.0)
            diff  = coord - flat
            print(f"  {name:<25} {flat:>10.1f} {coord:>11.1f} {diff:>+6.1f}")
    else:
        section("5. XP WITH COORDINATES")
        print("  [!] Skipped — no play_id join available between shots and plays.")
        print("      All xP values above are using flat (no-coordinate) fallback.")

    # ── 6. Sample shot detail ─────────────────────────────────────────────────
    section("6. SAMPLE SHOT DETAIL (first 10 shot plays)")
    print(f"  {'Team':<20} {'Type':<14} {'Clock':>6} {'Phase':<8} {'xP(flat)':>9}")
    print(f"  {'-'*62}")
    for d in shot_detail[:10]:
        name = team_names.get(d['team_id'], d['team_id'])
        print(f"  {name:<20} {d['shot_type']:<14} "
              f"{d['elapsed']:>5.0f}s {d['clock_phase']:<8} {d['xp_flat']:>9.3f}")

    # ── 7. Quick sanity checks ────────────────────────────────────────────────
    section("7. SANITY CHECKS")
    total_xp = sum(team_xp_flat.values())
    check("Total combined xP in reasonable range (40–200)",
          40 <= total_xp <= 200, f"{total_xp:.1f}")

    dunk_xp = [d for d in shot_detail if d['shot_type'] == 'dunk']
    if dunk_xp:
        check("Dunks have xP = 1.960",
              all(abs(d['xp_flat'] - 1.96) < 0.01 for d in dunk_xp),
              f"{len(dunk_xp)} dunks checked")

    ft_xp = [d for d in shot_detail if d['shot_type'] == 'ft']
    if ft_xp:
        check("Free throws have xP ≈ 0.720",
              all(abs(d['xp_flat'] - 0.720) < 0.01 for d in ft_xp),
              f"{len(ft_xp)} FTs checked")

    three_xp = [d for d in shot_detail if d['shot_type'] == 'three']
    if three_xp:
        check("3pt shots have xP between 0.80 and 1.20",
              all(0.80 <= d['xp_flat'] <= 1.20 for d in three_xp),
              f"{len(three_xp)} threes checked")

    print()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    requested = sys.argv[1] if len(sys.argv) > 1 else None
    game_id, game = find_game_id(requested)

    print(f"\nDiagnosing game: {game.get('team_a_name')} vs {game.get('team_b_name')}")
    print(f"Game ID: {game_id}  |  Status: {game.get('status', 'unknown')}")

    run(game_id, game)
