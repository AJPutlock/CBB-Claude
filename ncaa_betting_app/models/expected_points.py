"""
Expected points model for NCAA basketball.

Coordinate-based model using actual shot location (x, y) from stats.ncaa.org.

Coordinate system:
  - x, y are percentages (0-100) of the SVG court (940px wide x 500px tall)
  - Left basket:  pixel (50, 250)  → pct (5.32, 50.0)
  - Right basket: pixel (890, 250) → pct (94.68, 50.0)
  - Scale: ~10.04 px/foot (derived from corner 3pt line = 22 feet)
  - Each team shoots toward the nearest basket (x < 50 = left, x > 50 = right)

Shot type handling:
  - Dunk:               snapped to basket position, fixed xP = 1.96
  - Under the basket:   snapped to basket position, base rim value with no decay
  - Layup/driving layup: actual coordinates, distance decay from high base %
  - Hook shot:          actual coordinates, midrange-style decay with angle adjustment
  - Midrange jump shot: actual coordinates, distance + angle decay
  - 3pt:                actual coordinates, angle zone (corner/wing/above-break)
  - Free throw:         fixed value, no location adjustment

Formula:
  xP = base_pct(shot_type, distance_ft, angle_deg) * shooter_mult * context_mult * point_value

Context multipliers (future use, currently 1.0):
  - fast break, second chance, assisted, off turnover, late clock
"""
import math
import logging

logger = logging.getLogger(__name__)

# ── Court geometry ────────────────────────────────────────────────────────────
# SVG canvas dimensions (pixels)
COURT_WIDTH_PX  = 940
COURT_HEIGHT_PX = 500

# Basket positions in pixel space
LEFT_BASKET_PX  = (50,  250)
RIGHT_BASKET_PX = (890, 250)

# Scale derived from corner 3pt line (22 feet = 220.9px)
PX_PER_FOOT = 220.9 / 22.0   # ~10.04

# Distance thresholds (feet)
RIM_MAX_FT          = 4.0    # ≤4ft → rim zone (unless dunk/under basket)
SHORT_MID_MAX_FT    = 10.0   # 4–10ft → short midrange
LONG_MID_MAX_FT     = 22.0   # 10–22ft → long midrange (approaching arc)
CORNER_THREE_FT     = 22.0   # corner 3 distance
ABOVE_BREAK_FT      = 23.75  # above-the-break 3 distance

# Angle threshold for corner 3 (degrees from horizontal, measured from basket)
# Corner = angle > 60° (shot is close to the baseline/sideline)
# Above break = angle < 30° (straight on)
CORNER_ANGLE_DEG = 30.0

# ── League averages (NCAA D1 Men's, 2024-25) ──────────────────────────────────
LEAGUE_AVG = {
    'three_corner_pct':      0.385,
    'three_wing_pct':        0.355,
    'three_above_break_pct': 0.340,
    'mid_short_pct':         0.480,   # <10ft (non-rim)
    'mid_long_pct':          0.400,   # 10–22ft
    'rim_pct':               0.600,   # generic rim (layup range)
    'layup_pct':             0.570,   # layup / driving layup
    'dunk_pct':              0.980,   # dunk (fixed, no decay)
    'hook_pct':              0.420,   # hook shot (midrange-like)
    'ft_pct':                0.720,   # free throw
}

# Midrange distance decay: make% drops this much per foot beyond 4ft
# Calibrated so short mid (~6ft) ≈ 48% and long mid (~18ft) ≈ 40%
MID_DECAY_PER_FOOT  = 0.006   # ~0.6% per foot
HOOK_DECAY_PER_FOOT = 0.007   # slightly steeper than midrange

# 3pt distance decay beyond the arc (deep threes are harder)
THREE_DECAY_PER_FOOT = 0.015  # ~1.5% per foot beyond arc distance

# Shot clock multipliers (applied to non-FT shots)
SHOT_CLOCK_MULT = {
    'early': 1.00,   # >15s remaining
    'mid':   0.95,   # 6–15s remaining
    'late':  0.85,   # ≤5s remaining
}

# Context multipliers (hooks for future enrichment — all 1.0 for now)
CONTEXT_MULT = {
    'fast_break':    1.08,   # transition offense
    'second_chance': 1.05,   # offensive rebound putback
    'assisted':      1.04,   # catch-and-shoot
    'off_turnover':  1.03,   # off live-ball turnover
    'default':       1.00,
}


# ── Public API ────────────────────────────────────────────────────────────────

def calculate_expected_points(
    shot_type,
    x_pct=None,
    y_pct=None,
    player_stats=None,
    shot_clock_phase='early',
    context_flags=None,
):
    """
    Calculate expected points for a single shot attempt.

    Args:
        shot_type:        'three', 'midrange', 'rim', 'layup', 'dunk',
                          'under_basket', 'hook', or 'ft'
        x_pct, y_pct:     Shot coordinates as 0-100 percentages of court SVG.
                          If None, falls back to flat base values.
        player_stats:     dict with 'fg3_pct', 'fg_pct', 'ft_pct' or None
        shot_clock_phase: 'early', 'mid', or 'late'
        context_flags:    dict with boolean keys: fast_break, second_chance,
                          assisted, off_turnover

    Returns:
        float: expected points value
    """
    if context_flags is None:
        context_flags = {}

    # ── Dunk: fixed value, snap to basket ────────────────────────────────────
    if shot_type == 'dunk':
        return round(2 * LEAGUE_AVG['dunk_pct'], 3)  # 1.96

    # ── Free throw: use player's projected FT% directly ─────────────────────
    if shot_type == 'ft':
        ft_pct = (player_stats.get('ft_pct') if player_stats else None) or LEAGUE_AVG['ft_pct']
        return round(ft_pct, 3)

    # ── Coordinate-based shots ────────────────────────────────────────────────
    if x_pct is not None and y_pct is not None:
        dist_ft, angle_deg = _coords_to_dist_angle(x_pct, y_pct, shot_type)
    else:
        dist_ft, angle_deg = None, None

    base_pct  = _base_pct(shot_type, dist_ft, angle_deg)
    point_val = 3 if shot_type == 'three' else 2

    if shot_type == 'three' and player_stats and player_stats.get('fg3_pct'):
        # Use player's fg3_pct directly as the base make%.
        # When coordinates exist, scale by the zone ratio so corner/wing/deep
        # threes adjust relative to the player's own average (not league avg).
        # When no coordinates, use fg3_pct flat — no zone ratio applied.
        if dist_ft is not None:
            league_avg_3pt = (LEAGUE_AVG['three_corner_pct'] + LEAGUE_AVG['three_above_break_pct']) / 2
            zone_ratio = base_pct / league_avg_3pt if league_avg_3pt > 0 else 1.0
            effective_pct = player_stats['fg3_pct'] * zone_ratio
        else:
            effective_pct = player_stats['fg3_pct']  # flat: just the player's rate
        shooter_mult = 1.0  # baked into effective_pct
        base_pct = effective_pct
    else:
        shooter_mult = _shooter_mult(shot_type, player_stats)

    clock_mult   = SHOT_CLOCK_MULT.get(shot_clock_phase, 1.0)
    ctx_mult     = _context_mult(context_flags)

    xp = point_val * base_pct * shooter_mult * clock_mult * ctx_mult
    return round(xp, 3)


def classify_shot_type_from_play(play):
    """
    Determine shot type from play classification flags.

    Returns: 'ft', 'three', 'dunk', 'under_basket', 'layup', 'hook',
             'midrange'
    """
    if play.get('is_ft'):
        return 'ft'
    if play.get('is_three'):
        return 'three'
    if play.get('is_dunk'):
        return 'dunk'
    if play.get('is_under_basket'):
        return 'rim'
    if play.get('is_layup') or play.get('is_driving_layup'):
        return 'layup'
    if play.get('is_hook_shot'):
        return 'hook'
    return 'midrange'


def classify_shot_clock(possession_start_elapsed, shot_elapsed):
    """
    Classify shot clock phase from elapsed game seconds.
    College basketball shot clock = 30 seconds.
    """
    if possession_start_elapsed is None:
        return 'mid'
    time_used = shot_elapsed - possession_start_elapsed
    if time_used < 0:
        return 'mid'
    remaining = 30 - time_used
    if remaining > 15:
        return 'early'
    elif remaining > 5:
        return 'mid'
    else:
        return 'late'


def _normalize_time(time_str):
    """
    Normalize game clock string to MM:SS for consistent joining.

    Handles:
      - Live format:     '19:25'       → '19:25'
      - Sub-second:      '00:09.30'    → '00:09'
      - Postgame format: '19:48:00'    → '19:48'
    """
    t = (time_str or '').strip()
    # Strip sub-seconds: '00:09.30' → '00:09'
    t = t.split('.')[0]
    # Strip trailing :00 (postgame): '19:48:00' → '19:48'
    parts = t.split(':')
    if len(parts) == 3:
        t = f"{parts[0]}:{parts[1]}"
    return t


def calculate_game_expected_score(plays, shots=None, players_stats=None):
    """
    Calculate expected score for both teams from play-by-play data.

    Coordinate join key: (half, time_norm, player_id, result)
      - half:       integer period (1 or 2)
      - time_norm:  MM:SS normalized (strips sub-seconds and postgame :00 suffix)
      - player_id:  numeric player ID (already stored on both plays and shots)
      - result:     1 = made, 0 = missed

    This key is unique for any single shot event and works for both live
    and postgame page formats without text matching.

    Args:
        plays:          list of play dicts (chronological)
        shots:          list of shot dicts with x, y coordinates (optional)
        players_stats:  dict of player_id → stats dict (optional)

    Returns:
        dict: {team_id: expected_points_float}
    """
    if players_stats is None:
        players_stats = {}
    # Normalize all keys to strings
    players_stats = {str(k): v for k, v in players_stats.items()}

    # Build coordinate lookup keyed by (half, time_norm, player_id, result)
    # Stores (x, y, shot_type) so coordinate-derived shot type takes priority
    # over PBP classification when a match is found.
    shot_coords = {}
    if shots:
        for s in shots:
            if s.get('x') is None or s.get('y') is None:
                continue
            player_id = str(s.get('player_id') or '')
            if not player_id:
                continue
            half      = int(s.get('half', 1))
            time_norm = _normalize_time(s.get('time', ''))
            result    = int(s.get('result', 0))   # 1 = made, 0 = missed
            key = (half, time_norm, player_id, result)
            shot_coords[key] = (s['x'], s['y'], s.get('shot_type'))

    team_expected = {}
    possession_start_elapsed = None

    for play in plays:
        team_id = play.get('team_id', '')
        if team_id and team_id not in team_expected:
            team_expected[team_id] = 0.0

        is_shot = play.get('is_made_shot', 0) or play.get('is_missed_shot', 0)

        if is_shot and team_id:
            shot_type = classify_shot_type_from_play(play)
            elapsed   = play.get('elapsed_seconds', 0)

            # Look up coordinates by (half, time_norm, player_id, result)
            player_id = str(play.get('player_id') or '')
            half      = int(play.get('half', 1))
            time_norm = _normalize_time(play.get('time', ''))
            result    = 1 if play.get('is_made_shot') else 0
            coords    = shot_coords.get((half, time_norm, player_id, result))
            if coords:
                x_pct, y_pct, coord_shot_type = coords
                # Priority: use the most specific type available.
                # PBP wins for 'three' (is_three is reliable) and 'ft'.
                # Shot chart description wins for 'dunk', 'hook', 'layup'
                # because the description captures these explicitly.
                # Coordinates (geometry) fill in 'rim' vs 'mid' for generic 2pt.
                if shot_type in ('ft', 'three'):
                    pass  # PBP always authoritative for these
                elif coord_shot_type in ('dunk', 'hook', 'layup', 'rim'):
                    shot_type = coord_shot_type  # shot chart description is more specific
                elif shot_type == 'midrange' and coord_shot_type:
                    shot_type = coord_shot_type  # geometry fills in rim vs mid
            else:
                x_pct, y_pct = None, None


            clock_phase  = classify_shot_clock(possession_start_elapsed, elapsed)
            p_stats      = players_stats.get(str(play.get('player_id') or ''))
            ctx_flags    = _extract_context_flags(play)

            xp = calculate_expected_points(
                shot_type, x_pct, y_pct, p_stats, clock_phase, ctx_flags
            )
            team_expected[team_id] += xp

        # Track possession changes for shot clock estimation
        if play.get('is_turnover') or play.get('is_def_rebound'):
            possession_start_elapsed = play.get('elapsed_seconds', 0)
        elif is_shot:
            possession_start_elapsed = play.get('elapsed_seconds', 0)

    return team_expected


def calculate_h1_expected_score(plays, shots=None, players_stats=None):
    """Calculate expected score using only first-half plays."""
    h1_plays = [p for p in plays if p.get('half', 1) == 1]
    h1_shots  = [s for s in (shots or []) if s.get('half', 1) == 1]
    return calculate_game_expected_score(h1_plays, h1_shots, players_stats)


# ── Private helpers ───────────────────────────────────────────────────────────

def _coords_to_dist_angle(x_pct, y_pct, shot_type):
    """
    Convert percentage coordinates to (distance_ft, angle_deg).

    Distance is from the nearest basket.
    Angle is measured from the baseline (0° = corner, 90° = top of key).

    Dunk and under_basket coordinates are snapped to the basket before
    any calculation; layup/driving layup use actual coordinates.
    """
    # Convert % → pixels
    x_px = (x_pct / 100.0) * COURT_WIDTH_PX
    y_px = (y_pct / 100.0) * COURT_HEIGHT_PX

    # Snap dunks and under-basket shots to the nearest basket
    if shot_type in ('dunk', 'under_basket'):
        bx, by = _nearest_basket_px(x_px)
        x_px, y_px = bx, by

    bx, by = _nearest_basket_px(x_px)

    dx = x_px - bx
    dy = y_px - by
    dist_px = math.sqrt(dx * dx + dy * dy)
    dist_ft = dist_px / PX_PER_FOOT

    # Angle from baseline: 0° at corner (along sideline), 90° at top of key
    # baseline runs along x-axis from basket's perspective
    angle_deg = math.degrees(math.atan2(abs(dy), abs(dx))) if dist_px > 0 else 0.0

    return dist_ft, angle_deg


def _nearest_basket_px(x_px):
    """Return pixel coords of the basket nearest to x_px."""
    if x_px <= COURT_WIDTH_PX / 2:
        return LEFT_BASKET_PX
    return RIGHT_BASKET_PX


def _base_pct(shot_type, dist_ft, angle_deg):
    """
    Compute base make% for a shot given type, distance, and angle.
    Falls back to flat league averages when coordinates are unavailable.
    """
    # ── Three-pointer ─────────────────────────────────────────────────────────
    if shot_type == 'three':
        base = _three_base_pct(angle_deg)
        if dist_ft is not None:
            # Decay for deep threes beyond the arc distance
            arc_dist = CORNER_THREE_FT if (angle_deg or 0) < CORNER_ANGLE_DEG else ABOVE_BREAK_FT
            extra_ft = max(0, dist_ft - arc_dist)
            base = max(0.20, base - THREE_DECAY_PER_FOOT * extra_ft)
        return base

    # ── Dunk: fixed (handled upstream, but included for completeness) ─────────
    if shot_type == 'dunk':
        return LEAGUE_AVG['dunk_pct']

    # ── Under basket: snapped, fixed base ────────────────────────────────────
    if shot_type == 'under_basket':
        return LEAGUE_AVG['rim_pct']

    # ── Layup / driving layup: distance decay from layup base ────────────────
    if shot_type == 'layup':
        if dist_ft is None:
            return LEAGUE_AVG['layup_pct']
        # Slight decay as distance increases (still rim-range shots)
        base = max(0.40, LEAGUE_AVG['layup_pct'] - MID_DECAY_PER_FOOT * max(0, dist_ft - 2))
        return base

    # ── Hook shot: midrange-style decay with angle adjustment ─────────────────
    if shot_type == 'hook':
        if dist_ft is None:
            return LEAGUE_AVG['hook_pct']
        angle_adj = _angle_adjustment(angle_deg)
        base = max(0.25, LEAGUE_AVG['hook_pct'] - HOOK_DECAY_PER_FOOT * max(0, dist_ft - 4))
        return base * angle_adj

    # ── Rim: close-range non-layup (putback, short floater at basket) ─────────
    if shot_type == 'rim':
        if dist_ft is None:
            return LEAGUE_AVG['rim_pct']   # flat: 0.600 → xP = 1.200
        # Slight decay beyond 2ft but still a high-percentage shot
        return max(0.45, LEAGUE_AVG['rim_pct'] - MID_DECAY_PER_FOOT * max(0, dist_ft - 2))

    # ── Midrange: distance + angle decay ──────────────────────────────────────
    if dist_ft is None:
        return LEAGUE_AVG['mid_long_pct']

    if dist_ft <= RIM_MAX_FT:
        base = LEAGUE_AVG['mid_short_pct']   # very close non-rim shot
    elif dist_ft <= SHORT_MID_MAX_FT:
        base = LEAGUE_AVG['mid_short_pct'] - MID_DECAY_PER_FOOT * (dist_ft - RIM_MAX_FT)
    else:
        base = LEAGUE_AVG['mid_long_pct'] - MID_DECAY_PER_FOOT * (dist_ft - SHORT_MID_MAX_FT)

    angle_adj = _angle_adjustment(angle_deg)
    return max(0.25, base * angle_adj)


def _three_base_pct(angle_deg):
    """
    Return base 3pt% based on shot angle from horizontal (x-axis from basket).
    High angle = close to baseline = corner.
    Low angle  = straight ahead    = above the break.

    Corner 3:      angle > 60° (near sideline/baseline)
    Wing 3:        30-60°
    Above break 3: angle < 30° (top of key / straight on)
    """
    if angle_deg is None:
        return LEAGUE_AVG['three_above_break_pct']
    if angle_deg > 60:
        return LEAGUE_AVG['three_corner_pct']        # corner 3
    elif angle_deg > 30:
        return LEAGUE_AVG['three_wing_pct']          # wing 3
    else:
        return LEAGUE_AVG['three_above_break_pct']   # above the break


def _angle_adjustment(angle_deg):
    """
    Slight efficiency adjustment by shot angle for midrange and hook shots.
    High angle (near baseline/corner) = slightly harder (out-of-bounds pressure).
    Mid angle (wing/elbow) = most efficient.
    Low angle (straight on, top of key) = slight difficulty increase.
    Returns a multiplier near 1.0.
    """
    if angle_deg is None:
        return 1.0
    if angle_deg > 70:
        return 0.95   # deep corner — baseline pressure
    elif angle_deg > 25:
        return 1.00   # wing / elbow — most efficient zone
    else:
        return 0.97   # straight away top-of-key


def _shooter_mult(shot_type, player_stats):
    """Shooter adjustment relative to league average, clamped [0.5, 1.8]."""
    if not player_stats:
        return 1.0
    if shot_type == 'three' and player_stats.get('fg3_pct'):
        avg = (LEAGUE_AVG['three_corner_pct'] + LEAGUE_AVG['three_above_break_pct']) / 2
        mult = player_stats['fg3_pct'] / avg
    elif shot_type in ('midrange', 'hook') and player_stats.get('fg_pct'):
        mult = player_stats['fg_pct'] / LEAGUE_AVG['mid_long_pct']
    elif shot_type in ('layup', 'under_basket', 'rim') and player_stats.get('fg_pct'):
        mult = player_stats['fg_pct'] / LEAGUE_AVG['rim_pct']
    elif shot_type == 'dunk':
        return 1.0   # dunk efficiency is near-fixed regardless of shooter
    else:
        return 1.0
    return max(0.5, min(1.8, mult))


def _shooter_mult_ft(player_stats):
    """Free throw shooter adjustment."""
    if not player_stats or not player_stats.get('ft_pct'):
        return 1.0
    mult = player_stats['ft_pct'] / LEAGUE_AVG['ft_pct']
    return max(0.5, min(1.5, mult))


def _context_mult(flags):
    """
    Combined context multiplier from play flags.
    Multiplicative — fast break layup gets both fast_break and layup bonuses.
    Capped at 1.20 to prevent runaway values.
    """
    mult = 1.0
    if flags.get('fast_break'):
        mult *= CONTEXT_MULT['fast_break']
    if flags.get('second_chance'):
        mult *= CONTEXT_MULT['second_chance']
    if flags.get('assisted'):
        mult *= CONTEXT_MULT['assisted']
    if flags.get('off_turnover'):
        mult *= CONTEXT_MULT['off_turnover']
    return min(mult, 1.20)


def _extract_context_flags(play):
    """Extract context flag dict from a play dict."""
    return {
        'fast_break':    bool(play.get('is_fast_break')),
        'second_chance': bool(play.get('is_second_chance')),
        'assisted':      bool(play.get('is_assist')),
        'off_turnover':  bool(play.get('is_off_turnover')),
    }
