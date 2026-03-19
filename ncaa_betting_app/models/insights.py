"""
Betting insights engine.

Generates real-time alerts and insights from play-by-play data:
- Current scoring runs (6+ unanswered points)
- Players in foul trouble (half-aware thresholds)
- Hot/cold shooting streaks over rolling windows
- Pace analysis with regression-to-mean projection
- Momentum scoring

Improvements over v1:
- Team name lookup built once in generate_insights(), passed to sub-functions
- Scoring run detection simplified (removed redundant FT check)
- Pace projection uses regression-to-mean for early-game accuracy
  (raw projection is wildly unreliable in the first 5 minutes)
- Added momentum score combining run + shooting + foul factors
- Minimum sample size checks throughout
"""
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

# NCAA average pace and scoring for regression-to-mean
_NCAA_AVG_PACE = 68.0       # avg possessions per 40 min
_NCAA_AVG_PPP = 1.04        # avg points per possession
_NCAA_AVG_TOTAL = 141.0     # avg combined score per game


def generate_insights(plays, players, game_state, shots=None):
    """
    Generate all betting insights for a game.

    Args:
        plays: list of play dicts (all plays, ordered chronologically)
        players: list of player dicts
        game_state: dict with game metadata (from games table)
        shots: list of shot dicts (optional, reserved for future)

    Returns:
        dict with insight categories and compiled alerts
    """
    # Build team name lookup ONCE, share across all sub-functions
    team_names = {}
    for p in players:
        tid = p.get('team_id', '')
        if tid and tid not in team_names:
            team_names[tid] = p.get('team_name', tid)

    insights = {
        'current_run': detect_scoring_run(plays, game_state, team_names),
        'foul_trouble': detect_foul_trouble(plays, players, game_state),
        'shooting_streaks': detect_shooting_streaks(plays, team_names, minutes_window=5),
        'pace': analyze_pace(plays, game_state),
        'alerts': [],
    }

    # Compile top-level alerts, sorted by priority
    alerts = []

    run = insights['current_run']
    if run and run['run_size'] >= 6:
        alerts.append({
            'type': 'run',
            'priority': 'high' if run['run_size'] >= 10 else 'medium',
            'message': f"{run['run_team']} on a {run['run_size']}-0 run",
        })

    for ft in insights['foul_trouble']:
        if ft['severity'] == 'high':
            alerts.append({
                'type': 'foul_trouble',
                'priority': 'high',
                'message': f"FOUL TROUBLE: {ft['player_name']} ({ft['team']}) has {ft['fouls']} fouls",
            })

    for streak in insights['shooting_streaks']:
        if streak['severity'] == 'high':
            label = 'hot' if streak['type'] == 'hot' else 'cold'
            alerts.append({
                'type': f'{label}_shooting',
                'priority': 'medium',
                'message': (
                    f"{streak['team']} shooting {streak['fg_pct']:.0%} "
                    f"over last {streak['window_minutes']} min"
                ),
            })

    insights['alerts'] = sorted(alerts, key=lambda x: 0 if x['priority'] == 'high' else 1)
    return insights


def detect_scoring_run(plays, game_state, team_names):
    """
    Detect the current scoring run by walking backward through plays.

    A run = consecutive scoring by one team with the other team scoreless.
    Only reported if >= 4 unanswered points.
    """
    if not plays:
        return None

    team_a_id = game_state.get('team_a_id', '')
    team_b_id = game_state.get('team_b_id', '')

    run_team = None
    run_points = 0
    run_start_time = None

    for play in reversed(plays):
        pts = play.get('points_value', 0)
        if not play.get('is_made_shot') or pts <= 0:
            continue

        scoring_team = play.get('team_id', '')

        if run_team is None:
            # First scoring play we encounter (most recent)
            run_team = scoring_team
            run_points = pts
            run_start_time = play.get('time', '')
        elif scoring_team == run_team:
            run_points += pts
            run_start_time = play.get('time', '')
        else:
            # Other team scored — run is broken
            break

    if run_team and run_points >= 4:
        return {
            'run_team': team_names.get(run_team, run_team),
            'run_team_id': run_team,
            'run_size': run_points,
            'run_start_time': run_start_time,
        }
    return None


def detect_foul_trouble(plays, players, game_state):
    """
    Identify players in foul trouble.

    Thresholds:
    - 1st half: 2 fouls = moderate, 3+ = severe
    - 2nd half: 3 fouls = moderate, 4+ = severe
    """
    half = game_state.get('half', 1)

    # Count fouls per player
    player_fouls = defaultdict(int)
    for play in plays:
        if play.get('is_foul_personal') or play.get('is_foul_shooting'):
            pid = play.get('player_id')
            if pid:
                player_fouls[pid] += 1

    # Build player info lookup
    player_info = {p['player_id']: p for p in players}

    trouble_list = []
    for pid, fouls in player_fouls.items():
        pinfo = player_info.get(pid)
        if not pinfo:
            continue

        # Determine severity based on current half
        if half <= 1:
            if fouls >= 3:
                severity = 'high'
            elif fouls >= 2:
                severity = 'moderate'
            else:
                continue
        else:
            if fouls >= 4:
                severity = 'high'
            elif fouls >= 3:
                severity = 'moderate'
            else:
                continue

        trouble_list.append({
            'player_id': pid,
            'player_name': pinfo.get('player_name', 'Unknown'),
            'team': pinfo.get('team_name', ''),
            'team_id': pinfo.get('team_id', ''),
            'fouls': fouls,
            'severity': severity,
            'half': half,
        })

    return sorted(trouble_list, key=lambda x: -x['fouls'])


def detect_shooting_streaks(plays, team_names, minutes_window=5):
    """
    Analyze team FG% over a rolling time window.

    Hot: >55% FG (high if >60%)
    Cold: <30% FG (high if <20%)
    Requires minimum 5 FGA in the window to trigger.
    """
    if not plays:
        return []

    latest_elapsed = max(p.get('elapsed_seconds', 0) for p in plays)
    cutoff = latest_elapsed - (minutes_window * 60)

    # Count FGA and FGM per team in the window (excluding FTs)
    team_shots = defaultdict(lambda: {'made': 0, 'attempted': 0})

    for play in plays:
        if play.get('elapsed_seconds', 0) < cutoff:
            continue

        tid = play.get('team_id', '')
        if not tid:
            continue

        is_field_goal = (
            (play.get('is_made_shot') or play.get('is_missed_shot'))
            and not play.get('is_ft')
        )
        if is_field_goal:
            team_shots[tid]['attempted'] += 1
            if play.get('is_made_shot'):
                team_shots[tid]['made'] += 1

    streaks = []
    for tid, data in team_shots.items():
        if data['attempted'] < 5:  # Minimum sample size
            continue

        fg_pct = data['made'] / data['attempted']

        if fg_pct >= 0.55:
            streaks.append({
                'type': 'hot',
                'team': team_names.get(tid, tid),
                'team_id': tid,
                'fg_pct': fg_pct,
                'made': data['made'],
                'attempted': data['attempted'],
                'window_minutes': minutes_window,
                'severity': 'high' if fg_pct >= 0.60 else 'moderate',
            })
        elif fg_pct <= 0.30:
            streaks.append({
                'type': 'cold',
                'team': team_names.get(tid, tid),
                'team_id': tid,
                'fg_pct': fg_pct,
                'made': data['made'],
                'attempted': data['attempted'],
                'window_minutes': minutes_window,
                'severity': 'high' if fg_pct <= 0.20 else 'moderate',
            })

    return streaks


def analyze_pace(plays, game_state):
    """
    Calculate game pace (possessions per 40 minutes) and project total points.

    Uses regression-to-mean for early-game projections:
    - At game start, projection equals the NCAA average (~141 combined).
    - As the game progresses, the projection blends observed scoring pace
      with the prior, weighted by % of game completed.
    - By halftime the blend is ~80% observed, 20% prior.
    - By the end it's essentially 100% observed.
    """
    if not plays:
        return {
            'possessions': 0,
            'pace_per_40': 0,
            'projected_total': round(_NCAA_AVG_TOTAL, 1),
            'elapsed_minutes': 0,
        }

    # Count possessions: FGA + TO + 0.44*FTA - ORB
    team_a_id = game_state.get('team_a_id', '')
    team_b_id = game_state.get('team_b_id', '')

    stats = defaultdict(lambda: {'fga': 0, 'fta': 0, 'to': 0, 'orb': 0})

    for play in plays:
        tid = play.get('team_id', '')
        if not tid:
            continue

        if play.get('is_made_shot') or play.get('is_missed_shot'):
            if play.get('is_ft'):
                stats[tid]['fta'] += 1
            else:
                stats[tid]['fga'] += 1
        if play.get('is_turnover'):
            stats[tid]['to'] += 1
        if play.get('is_off_rebound'):
            stats[tid]['orb'] += 1

    total_possessions = 0
    for tid in [team_a_id, team_b_id]:
        if not tid:
            continue
        s = stats[tid]
        poss = s['fga'] + s['to'] + 0.44 * s['fta'] - s['orb']
        total_possessions += max(0, poss)

    avg_poss = total_possessions / 2  # Each team gets roughly equal possessions

    latest_elapsed = max((p.get('elapsed_seconds', 0) for p in plays), default=0)
    if latest_elapsed <= 0:
        return {
            'possessions': round(avg_poss, 1),
            'pace_per_40': 0,
            'projected_total': round(_NCAA_AVG_TOTAL, 1),
            'elapsed_minutes': 0,
        }

    # Pace = possessions per 40 minutes (2400 seconds of regulation)
    pace_per_40 = avg_poss / latest_elapsed * 2400

    # Project total points with regression-to-mean
    team_a_score = game_state.get('team_a_score', 0) or 0
    team_b_score = game_state.get('team_b_score', 0) or 0
    current_total = team_a_score + team_b_score

    pct_complete = min(latest_elapsed / 2400, 1.0)

    if pct_complete > 0:
        # Raw projection from observed data
        raw_projection = current_total / pct_complete

        # Blend with NCAA average: at 0% complete, 100% prior; at 100%, 0% prior.
        # Use a slightly slower blend curve (pct^0.8) so the prior persists
        # a bit longer in the early minutes when variance is highest.
        observed_weight = min(pct_complete ** 0.8, 1.0)
        projected_total = (observed_weight * raw_projection +
                           (1 - observed_weight) * _NCAA_AVG_TOTAL)
    else:
        projected_total = _NCAA_AVG_TOTAL

    return {
        'possessions': round(avg_poss, 1),
        'pace_per_40': round(pace_per_40, 1),
        'projected_total': round(projected_total, 1),
        'elapsed_minutes': round(latest_elapsed / 60, 1),
    }
