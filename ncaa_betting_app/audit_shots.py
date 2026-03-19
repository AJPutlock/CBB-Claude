"""
Shot Audit Script
=================
Shows how shots are currently saved and whether their coordinates
are matching up correctly with PBP data (half, time, player).

Run from your project root:
    python audit_shots.py [game_id]

If no game_id is provided, picks the most recent game with shot data.

Output:
  - Per-shot table: coordinates, shot_type, PBP match, player name
  - Summary: match rate, unmatched shots, type distribution
  - Coordinate sanity check: flags shots with suspicious distances
"""

import sys
import os
import math
import sqlite3

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

try:
    from models.database import get_connection, get_all_plays, get_all_shots
    from models.expected_points import calculate_expected_points, classify_shot_type_from_play, calculate_game_expected_score
except ImportError:
    print("[!] Could not import models — make sure you're running from project root")
    sys.exit(1)

# ── Court geometry (must match expected_points.py) ────────────────────────────
COURT_WIDTH_PX  = 940
COURT_HEIGHT_PX = 500
LEFT_BASKET_PX  = (50,  250)
RIGHT_BASKET_PX = (890, 250)
PX_PER_FOOT     = 220.9 / 22.0  # 10.04

def _dist_ft(x_pct, y_pct):
    """Distance from nearest basket in feet."""
    x_px = (x_pct / 100.0) * COURT_WIDTH_PX
    y_px = (y_pct / 100.0) * COURT_HEIGHT_PX
    if x_px <= COURT_WIDTH_PX / 2:
        bx, by = LEFT_BASKET_PX
    else:
        bx, by = RIGHT_BASKET_PX
    return math.sqrt((x_px - bx)**2 + (y_px - by)**2) / PX_PER_FOOT

def _norm_time(t):
    """Normalize time string to MM:SS."""
    t = (t or '').strip().split('.')[0]
    parts = t.split(':')
    if len(parts) == 3:
        t = f"{parts[0]}:{parts[1]}"
    return t

def _flag_distance(dist_ft, shot_type):
    """Return a warning flag if distance contradicts shot_type."""
    if shot_type == 'three' and dist_ft < 18:
        return f"⚠ {dist_ft:.1f}ft labeled three"
    if shot_type in ('rim', 'layup', 'dunk', 'under_basket') and dist_ft > 12:
        return f"⚠ {dist_ft:.1f}ft labeled {shot_type}"
    if shot_type == 'midrange' and dist_ft > 22:
        return f"⚠ {dist_ft:.1f}ft labeled mid"
    return ""

# ── DB helpers ────────────────────────────────────────────────────────────────
def get_recent_game_with_shots(conn):
    row = conn.execute("""
        SELECT g.game_id, g.team_a_name, g.team_b_name
        FROM games g
        WHERE EXISTS (SELECT 1 FROM shots s WHERE s.game_id = g.game_id AND s.x IS NOT NULL)
        ORDER BY g.rowid DESC
        LIMIT 1
    """).fetchone()
    return row

def get_game(conn, game_id):
    return conn.execute(
        "SELECT game_id, team_a_name, team_b_name, team_a_score, team_b_score, status "
        "FROM games WHERE game_id = ?", (game_id,)
    ).fetchone()

def get_shots(conn, game_id):
    return conn.execute("""
        SELECT s.id, s.x, s.y, s.shot_type, s.result, s.half, s.time,
               s.player_id, s.team_id, s.is_three, s.is_late_clock
        FROM shots s
        WHERE s.game_id = ?
        ORDER BY s.half, s.time DESC
    """, (game_id,)).fetchall()

def get_plays(conn, game_id):
    return conn.execute("""
        SELECT p.id, p.half, p.time, p.player_id, p.team_id,
               p.is_made_shot, p.is_missed_shot, p.is_ft, p.is_three,
               p.play_text
        FROM plays p
        WHERE p.game_id = ?
          AND (p.is_made_shot = 1 OR p.is_missed_shot = 1)
          AND p.is_ft = 0
        ORDER BY p.half, p.time DESC
    """, (game_id,)).fetchall()

def get_ft_plays(conn, game_id):
    return conn.execute("""
        SELECT p.team_id, p.player_id,
               p.is_made_shot, p.is_missed_shot
        FROM plays p
        WHERE p.game_id = ?
          AND p.is_ft = 1
          AND (p.is_made_shot = 1 OR p.is_missed_shot = 1)
    """, (game_id,)).fetchall()

def get_players(conn, game_id):
    rows = conn.execute(
        "SELECT player_id, player_name, team_id, team_name FROM players WHERE game_id = ?",
        (game_id,)
    ).fetchall()
    return {r['player_id']: r for r in rows}

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    game_id = sys.argv[1] if len(sys.argv) > 1 else None

    conn = get_connection()
    conn.row_factory = sqlite3.Row

    if game_id is None:
        row = get_recent_game_with_shots(conn)
        if not row:
            print("[!] No games with shot data found in DB.")
            sys.exit(1)
        game_id = row['game_id']

    game = get_game(conn, game_id)
    if not game:
        print(f"[!] Game {game_id} not found.")
        sys.exit(1)

    shots    = get_shots(conn, game_id)
    plays    = get_plays(conn, game_id)
    ft_plays = get_ft_plays(conn, game_id)
    players  = get_players(conn, game_id)

    team_a = game['team_a_name'] or 'Team A'
    team_b = game['team_b_name'] or 'Team B'

    print(f"\nShot Audit: {team_a} vs {team_b}")
    print(f"Game ID: {game_id}  |  Status: {game['status']}")
    print(f"Score: {game['team_a_score']} – {game['team_b_score']}")
    print("=" * 90)

    # Build PBP lookup: (half, time_norm, player_id, result) → play row
    play_map = {}
    for p in plays:
        key = (int(p['half']), _norm_time(p['time']),
               str(p['player_id'] or ''), int(p['is_made_shot']))
        play_map[key] = p

    # ── Shot-by-shot table ────────────────────────────────────────────────────
    print(f"\n{'#':>3}  {'Half':>4}  {'Time':>6}  {'x':>5}  {'y':>5}  "
          f"{'Dist':>6}  {'Type':<12}  {'Res':>3}  {'xP flat':>8}  {'xP coord':>9}  "
          f"{'Player':<22}  {'PBP Match':>9}  {'Flag'}")
    print("-" * 130)

    matched = 0
    unmatched_shots = []
    type_mismatch   = []
    coord_flags     = []

    for i, s in enumerate(shots, 1):
        x, y     = s['x'], s['y']
        half     = int(s['half'] or 1)
        time_s   = _norm_time(s['time'])
        pid      = str(s['player_id'] or '0')
        result   = int(s['result'] or 0)
        stype    = s['shot_type'] or '?'
        team_id  = s['team_id'] or ''

        # Distance sanity
        dist     = _dist_ft(x, y) if (x is not None and y is not None) else None
        dist_str = f"{dist:.1f}ft" if dist is not None else "  n/a"
        coord_flag = _flag_distance(dist, stype) if dist is not None else ""

        # Player name
        player_row  = players.get(pid) or players.get(str(pid))
        player_name = (player_row['player_name'] if player_row else f"id:{pid}")[:22]

        # PBP match
        key   = (half, time_s, pid, result)
        match = play_map.get(key)
        match_str = "  ✓ match" if match else "✗ no match"

        # Use PBP-derived shot type when a match exists — PBP is authoritative
        # for dunk, hook, layup, three. Fall back to shot chart type for generic 2pt.
        pbp_stype = classify_shot_type_from_play(dict(match)) if match else None
        effective_stype = pbp_stype if pbp_stype and pbp_stype != 'midrange' else stype

        # xP flat (no coordinates, using effective shot type)
        xp_flat = calculate_expected_points(effective_stype, None, None)
        # xP coord (with coordinates, using effective shot type)
        xp_coord = calculate_expected_points(effective_stype, x, y) if (x is not None and y is not None) else None

        xp_flat_str  = f"{xp_flat:.3f}"
        xp_coord_str = f"{xp_coord:.3f}" if xp_coord is not None else "   n/a"
        xp_diff      = f"({xp_coord - xp_flat:+.3f})" if xp_coord is not None else ""

        # Show effective type; append * if PBP overrode shot chart type
        display_type = effective_stype
        if pbp_stype and pbp_stype != stype:
            display_type = f"{effective_stype}*"

        print(f"{i:>3}  {half:>4}  {time_s:>6}  {x:>5.1f}  {y:>5.1f}  "
              f"{dist_str:>6}  {display_type:<13}  {'✓' if result else '✗':>3}  "
              f"{xp_flat_str:>8}  {xp_coord_str:>9}  "
              f"{player_name:<22}  {match_str:>9}  {coord_flag} {xp_diff}")

        if match:
            matched += 1
            # Check if shot_type agrees with PBP is_three
            pbp_is_three = int(match['is_three'] or 0)
            shot_is_three = 1 if stype == 'three' else 0
            if pbp_is_three != shot_is_three:
                type_mismatch.append({
                    'shot_id': s['id'], 'half': half, 'time': time_s,
                    'shot_type': stype, 'pbp_is_three': pbp_is_three,
                    'play_text': match['play_text'],
                })
        else:
            unmatched_shots.append({'half': half, 'time': time_s,
                                    'player_id': pid, 'result': result,
                                    'shot_type': stype})

        if coord_flag:
            coord_flags.append({'half': half, 'time': time_s,
                                 'x': x, 'y': y, 'dist': dist,
                                 'shot_type': stype, 'player': player_name})

    total = len(shots)
    fg_plays = len(plays)

    # ── Summary ───────────────────────────────────────────────────────────────
    # Compute xP totals for summary using same effective type as per-shot display
    # (PBP type wins for dunk/hook/layup/three; coord fills in rim vs mid)
    from collections import defaultdict
    team_xp_flat  = defaultdict(float)
    team_xp_coord = defaultdict(float)
    for s in shots:
        tid = str(s['team_id'] or '')
        if not tid or tid == '0':
            continue  # skip unmatched shots — app handles these via PBP team_id
        x, y = s['x'], s['y']
        stype = s['shot_type'] or 'midrange'

        # Replicate the same effective type logic as the per-shot display
        key      = (int(s['half'] or 1), _norm_time(s['time']),
                    str(s['player_id'] or ''), int(s['result'] or 0))
        match    = play_map.get(key)
        pbp_stype = classify_shot_type_from_play(dict(match)) if match else None
        eff_stype = pbp_stype if pbp_stype and pbp_stype != 'midrange' else stype

        team_xp_flat[tid]  += calculate_expected_points(eff_stype, None, None)
        team_xp_coord[tid] += (calculate_expected_points(eff_stype, x, y)
                               if (x is not None and y is not None)
                               else calculate_expected_points(eff_stype, None, None))

    # Get team names by id
    team_ids = list({s['team_id'] for s in shots if s['team_id']})
    team_name_map = {}
    for s in shots:
        tid = s['team_id'] or ''
        if tid and tid not in team_name_map:
            if tid == (game['team_a_id'] if 'team_a_id' in game.keys() else ''):
                team_name_map[tid] = team_a
            elif tid == (game['team_b_id'] if 'team_b_id' in game.keys() else ''):
                team_name_map[tid] = team_b
            else:
                team_name_map[tid] = f"Team {tid}"

    # Override with players table team names (more reliable than game row IDs)
    for p in players.values():
        tid   = str(p['team_id'] or '')
        tname = (p['team_name'] or '').strip()
        if tid and tname:
            team_name_map[tid] = tname

    # ── FT stats per team ────────────────────────────────────────────────────
    from collections import defaultdict as _dd
    ft_made    = _dd(int)
    ft_missed  = _dd(int)
    ft_xp      = _dd(float)
    for ft in ft_plays:
        tid = str(ft['team_id'] or '')
        if ft['is_made_shot']:
            ft_made[tid]   += 1
            ft_xp[tid]     += calculate_expected_points('ft', None, None)
        else:
            ft_missed[tid] += 1

    # ── FG made/missed per team from shots ────────────────────────────────────
    fg_made   = _dd(int)
    fg_missed = _dd(int)
    for s in shots:
        tid = str(s['team_id'] or '')
        if s['result']:
            fg_made[tid]   += 1
        else:
            fg_missed[tid] += 1

    print("\n" + "=" * 90)
    print("SUMMARY")
    print("=" * 90)

    # Combined xP totals per team
    all_tids = sorted(set(list(team_xp_flat.keys()) + list(ft_xp.keys())))
    if all_tids:
        print(f"\n  {'Team':<25}  {'FGM-FGA':>8}  {'FTM-FTA':>8}  "
              f"{'xP FG flat':>11}  {'xP FG coord':>12}  {'xP FT':>7}  "
              f"{'xP Total':>9}  {'Actual':>7}")
        print(f"  {'-'*100}")
        for tid in all_tids:
            name     = team_name_map.get(tid, f"Team {tid}")[:25]
            fgm      = fg_made[tid]
            fga      = fgm + fg_missed[tid]
            ftm      = ft_made[tid]
            fta      = ftm + ft_missed[tid]
            xp_fg_fl = team_xp_flat[tid]
            xp_fg_co = team_xp_coord[tid]
            xp_ft    = ft_xp[tid]
            xp_total = xp_fg_co + xp_ft
            # Actual score from game row
            if tid == str(game['team_a_id'] if 'team_a_id' in game.keys() else ''):
                actual = game['team_a_score'] or '?'
            elif tid == str(game['team_b_id'] if 'team_b_id' in game.keys() else ''):
                actual = game['team_b_score'] or '?'
            else:
                # Match via name from team_name_map
                tname = team_name_map.get(tid, '')
                if tname == team_a:
                    actual = game['team_a_score'] or '?'
                elif tname == team_b:
                    actual = game['team_b_score'] or '?'
                else:
                    actual = '?'
            print(f"  {name:<25}  {fgm:>3}-{fga:<4}  {ftm:>3}-{fta:<4}  "
                  f"{xp_fg_fl:>11.1f}  {xp_fg_co:>12.1f}  {xp_ft:>7.1f}  "
                  f"{xp_total:>9.1f}  {str(actual):>7}")

    print(f"\n  Shots in DB (with coords):  {total}")
    print(f"  FG plays in PBP (no FTs):   {fg_plays}")
    print(f"  Coordinate join matches:    {matched}/{total}  "
          f"({'100%' if total == 0 else f'{matched/total*100:.1f}%'})")

    # ── App xP (what the website shows) ─────────────────────────────────────
    # Call calculate_game_expected_score exactly as the app does
    app_plays = get_all_plays(game_id)
    app_shots = get_all_shots(game_id)
    app_expected = calculate_game_expected_score(app_plays, app_shots)

    print(f"\n  {'':=<90}")
    print(f"  APP xP vs AUDIT xP COMPARISON")
    print(f"  {'':=<90}")
    print(f"  {'Team':<25}  {'App xP':>8}  {'Audit xP':>9}  {'Diff':>6}")
    print(f"  {'-'*55}")
    all_tids_compare = sorted(set(list(app_expected.keys()) + list(all_tids)))
    for tid in all_tids_compare:
        name     = team_name_map.get(str(tid), f"Team {tid}")[:25]
        app_xp   = app_expected.get(tid, 0.0)
        audit_xp = team_xp_coord.get(str(tid), 0.0) + ft_xp.get(str(tid), 0.0)
        diff     = audit_xp - app_xp
        flag     = "  ⚠ check" if abs(diff) > 2.0 else ""
        print(f"  {name:<25}  {app_xp:>8.1f}  {audit_xp:>9.1f}  {diff:>+6.1f}{flag}")
    print(f"  Note: App xP includes shot clock adjustments; Audit uses flat clock phase")

    # Type distribution
    from collections import Counter
    type_counts = Counter(s['shot_type'] for s in shots)
    print(f"\n  Shot type distribution:")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"    {t:<14} {c:>4}")

    # Unmatched shots
    if unmatched_shots:
        print(f"\n  Unmatched shots ({len(unmatched_shots)}):")
        print(f"  {'Half':>4}  {'Time':>6}  {'PlayerID':<14}  {'Result':>6}  {'Type'}")
        for u in unmatched_shots[:15]:
            print(f"  {u['half']:>4}  {u['time']:>6}  {u['player_id']:<14}  "
                  f"{'made' if u['result'] else 'miss':>6}  {u['shot_type']}")
        if len(unmatched_shots) > 15:
            print(f"  ... and {len(unmatched_shots)-15} more")
    else:
        print("\n  ✓ All shots matched to PBP plays")

    # Type mismatches
    if type_mismatch:
        print(f"\n  Shot type mismatches vs PBP ({len(type_mismatch)}):")
        print(f"  {'Half':>4}  {'Time':>6}  {'Shot type':<12}  {'PBP is_three':>12}  Play text")
        for m in type_mismatch[:10]:
            pbp_str = "three" if m['pbp_is_three'] else "2pt"
            print(f"  {m['half']:>4}  {m['time']:>6}  {m['shot_type']:<12}  {pbp_str:>12}  "
                  f"{(m['play_text'] or '')[:50]}")
    else:
        print("\n  ✓ No shot type mismatches vs PBP")

    # Coordinate flags (distance contradicts type)
    if coord_flags:
        print(f"\n  Suspicious coordinates ({len(coord_flags)}):")
        print(f"  {'Half':>4}  {'Time':>6}  {'x':>5}  {'y':>5}  {'Dist':>6}  {'Type':<12}  Player")
        for f in coord_flags[:15]:
            print(f"  {f['half']:>4}  {f['time']:>6}  {f['x']:>5.1f}  {f['y']:>5.1f}  "
                  f"{f['dist']:>5.1f}ft  {f['shot_type']:<12}  {f['player']}")
    else:
        print("\n  ✓ All coordinate distances are consistent with shot type")

    # ── Baseline xP reference table ─────────────────────────────────────────
    print("\n" + "=" * 90)
    print("BASELINE xP VALUES (flat, no coordinates)")
    print("=" * 90)
    ref_types = [
        ('dunk',         'Fixed — at-rim, uncontested'),
        ('rim',          'Generic close-range (putback, tip-in)'),
        ('layup',        'Layup / driving layup'),
        ('hook',         'Hook shot'),
        ('midrange',     'Generic 2pt jump shot (no location flags)'),
        ('three',        'Above-break 3pt (flat avg)'),
        ('ft',           'Free throw'),
    ]
    print(f"\n  {'Type':<14}  {'Flat xP':>8}  {'Make%':>7}  {'Pts':>5}  Description")
    print(f"  {'-'*80}")
    for stype, desc in ref_types:
        xp = calculate_expected_points(stype, None, None)
        pts = 3 if stype == 'three' else 1 if stype == 'ft' else 2
        make_pct = xp / pts * 100
        print(f"  {stype:<14}  {xp:>8.3f}  {make_pct:>6.1f}%  {pts:>5}  {desc}")

    print(f"\n  Coordinate adjustments applied on top of these baselines:")
    print(f"  {'three':<14}  Corner (angle>60°) → {calculate_expected_points('three', 2.0, 50.0):.3f}  "
          f"| Wing → {calculate_expected_points('three', 5.0, 20.0):.3f}  "
          f"| Above-break → {calculate_expected_points('three', 50.0, 5.0):.3f}")
    print(f"  {'midrange':<14}  Short (~8ft) → {calculate_expected_points('midrange', 12.0, 50.0):.3f}  "
          f"| Long (~18ft) → {calculate_expected_points('midrange', 22.0, 50.0):.3f}")
    print(f"  {'three (deep)':<14}  +1ft beyond arc → ~{calculate_expected_points('three', 50.0, 3.0):.3f}  "
          f"| +5ft → ~{calculate_expected_points('three', 50.0, 1.0):.3f}")
    print()

    conn.close()
    print()

if __name__ == '__main__':
    main()
