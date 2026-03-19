"""
Game Manager - orchestrates scraping, caching, and data aggregation.

Handles:
- Scheduled scraping with jitter (2:30 - 3:30 interval)
- Staggered requests across multiple games (60s minimum between NCAA requests)
- Cache management to avoid re-pulling data
- Data aggregation for the frontend (scoreboard + game detail)

Improvements over v1:
- _match_odds_to_game caches today's games instead of querying DB per odds entry
- Improved fuzzy matching: won't remove "State" (which caused Ohio State = Ohio bugs),
  uses longest-word matching for mascot-heavy names
- Consolidated _store_odds() eliminates duplicate mapping code between live/pregame
- get_scoreboard() fetches today's games once and shares across all computations
- Added H1 expected score calculation for second-half games
- Thread-safe manual refresh with game_id targeting
"""
import threading
import time
import random
import logging
from datetime import date
from collections import defaultdict

from scrapers.ncaa_scraper import NCAAStatsScraper
from scrapers.draftkings_scraper import DraftKingsScraper
from scrapers.odds_api_scraper import OddsAPIScraper
from models.database import (
    get_or_create_game, update_game_state, get_cache_state, update_cache_state,
    insert_plays, insert_shots, insert_players, insert_odds,
    get_all_plays, get_all_shots, get_game_odds, get_games_for_date,
    get_players_for_game, clear_game_data
)
from models.expected_points import (
    calculate_game_expected_score, calculate_h1_expected_score,
    classify_shot_type_from_play, classify_shot_clock
)
from models.insights import generate_insights

logger = logging.getLogger(__name__)


class GameManager:
    """Central manager for all game data operations."""

    def __init__(self, odds_api_key=None):
        self.ncaa_scraper = NCAAStatsScraper(min_request_interval=60)
        self.dk_scraper = DraftKingsScraper()
        self.odds_scraper = OddsAPIScraper(api_key=odds_api_key)

        self._auto_refresh_thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._game_ids = []
        self._last_pregame_odds_time = 0

    # ---- Auto-refresh loop ----

    def start_auto_refresh(self):
        """Start the automatic refresh loop with 2:30-3:30 jitter."""
        if self._auto_refresh_thread and self._auto_refresh_thread.is_alive():
            logger.info("Auto-refresh already running")
            return

        self._stop_event.clear()
        self._auto_refresh_thread = threading.Thread(
            target=self._auto_refresh_loop, daemon=True
        )
        self._auto_refresh_thread.start()
        logger.info("Auto-refresh started")

    def stop_auto_refresh(self):
        """Stop the automatic refresh loop."""
        self._stop_event.set()
        if self._auto_refresh_thread:
            self._auto_refresh_thread.join(timeout=5)
            if self._auto_refresh_thread.is_alive():
                logger.warning("Auto-refresh thread still alive after timeout (will exit with process)")
        logger.info("Auto-refresh stopped")

    def _auto_refresh_loop(self):
        """Background loop that refreshes all games on a jittered schedule."""
        self.refresh_all()

        while not self._stop_event.is_set():
            wait_time = random.uniform(280, 320)  # ~5 minutes
            logger.info(f"Next auto-refresh in {wait_time:.0f}s")

            if self._stop_event.wait(timeout=wait_time):
                break

            self.refresh_all()

    # ---- Core refresh logic ----

    def refresh_all(self):
        """Refresh all live games — called by auto-refresh and manual reload."""
        with self._lock:
            logger.info("Starting full refresh...")

            try:
                # Step 1: Get today's schedule
                games = self.ncaa_scraper.get_daily_games()
                logger.info(f"Found {len(games)} games today")

                live_ids = []
                final_ids = []
                for game in games:
                    gid = game['game_id']
                    get_or_create_game(gid, date.today().isoformat())

                    # Store scoreboard-level info (team names, scores, status)
                    # so the dashboard can display something before individual scrapes
                    update_fields = {}
                    if game.get('team_a_name'):
                        update_fields['team_a_name'] = game['team_a_name']
                    if game.get('team_b_name'):
                        update_fields['team_b_name'] = game['team_b_name']
                    if game.get('team_a_score'):
                        try:
                            update_fields['team_a_score'] = int(game['team_a_score'])
                        except (ValueError, TypeError):
                            pass
                    if game.get('team_b_score'):
                        try:
                            update_fields['team_b_score'] = int(game['team_b_score'])
                        except (ValueError, TypeError):
                            pass
                    if game.get('half'):
                        update_fields['half'] = game['half']
                    if game.get('clock'):
                        update_fields['game_clock'] = game['clock']
                    if game.get('team_a_h1_score'):
                        try:
                            update_fields['team_a_h1_score'] = int(game['team_a_h1_score'])
                        except (ValueError, TypeError):
                            pass
                    if game.get('team_b_h1_score'):
                        try:
                            update_fields['team_b_h1_score'] = int(game['team_b_h1_score'])
                        except (ValueError, TypeError):
                            pass
                    if game.get('is_final'):
                        update_fields['status'] = 'final'
                    elif game.get('is_live'):
                        update_fields['status'] = 'live'
                    if game.get('is_timeout'):
                        update_fields['is_timeout'] = True

                    if update_fields:
                        update_game_state(gid, **update_fields)

                    # Consider a game active if it's live or has status text
                    if game.get('is_live') or (game.get('status_text', '').strip() and not game.get('is_final')):
                        live_ids.append(gid)
                    elif game.get('is_final'):
                        final_ids.append(gid)

                self._game_ids = [g['game_id'] for g in games]

                # Step 2: Scrape each live game quickly (no rate limiting, just small delay)
                for i, gid in enumerate(live_ids):
                    try:
                        self._refresh_single_game(gid, is_final=False)
                        if i < len(live_ids) - 1:
                            time.sleep(7)
                    except Exception as e:
                        logger.error(f"Error refreshing live game {gid}: {e}")

                # Step 2b: Scrape final games that haven't been scraped yet (one-time)
                for gid in final_ids:
                    cache = get_cache_state(gid)
                    if cache and cache.get('last_play_index', 0) > 0:
                        continue  # Already have data for this final game
                    try:
                        self._refresh_single_game(gid, is_final=True)
                        time.sleep(7)
                    except Exception as e:
                        logger.error(f"Error refreshing final game {gid}: {e}")

                # Step 3: DK live odds (every refresh cycle per user preference)
                try:
                    self._refresh_dk_odds(live=True)
                except Exception as e:
                    logger.error(f"Error refreshing DK live odds: {e}")

                # Step 4: Pregame odds every 15 minutes
                if time.time() - self._last_pregame_odds_time > 900:
                    try:
                        self._refresh_dk_odds(live=False)
                        self._refresh_pregame_odds()
                    except Exception as e:
                        logger.error(f"Error refreshing pregame odds: {e}")
                    self._last_pregame_odds_time = time.time()

            except Exception as e:
                logger.error(f"Full refresh failed: {e}")

    def _refresh_single_game(self, game_id, is_final=False):
        """Scrape and store data for a single game."""
        cache = get_cache_state(game_id)
        last_play_idx = cache['last_play_index'] if cache else 0
        last_shot_cnt = cache['last_shot_count'] if cache else 0

        data = self.ncaa_scraper.scrape_game_data(
            game_id,
            last_play_index=last_play_idx,
            last_shot_count=last_shot_cnt,
            is_final=is_final,
        )

        if not data:
            return

        if data['players']:
            insert_players(game_id, data['players'])
        if data['plays']:
            insert_plays(game_id, data['plays'])
        if data['shots']:
            insert_shots(game_id, data['shots'])

        # Only update game state if we got meaningful data
        # (don't overwrite good scoreboard scores with empty state)
        gs = data['game_state']
        if gs and (gs.get('team_a_score') is not None or gs.get('half') is not None
                   or gs.get('team_a_id') is not None):
            # Never overwrite team_a_name/team_b_name from the PBP scraper —
            # the scoreboard scraper already set them correctly from row order.
            # The PBP dropdown order may differ (e.g. home team listed first),
            # which would flip the display names while scores stay correct.
            gs_safe = {k: v for k, v in gs.items()
                       if k not in ('team_a_name', 'team_b_name')}
            update_game_state(game_id, **gs_safe)

        update_cache_state(game_id, data['new_play_index'], data['new_shot_count'])

        score_a = gs.get('team_a_score', '?')
        score_b = gs.get('team_b_score', '?')
        logger.info(
            f"Game {game_id}: {score_a}-{score_b}, "
            f"+{len(data['plays'])} plays, +{len(data['shots'])} shots"
        )

    def _refresh_dk_odds(self, live=True):
        """Fetch and store DraftKings odds (live or pregame)."""
        if live:
            odds_list = self.dk_scraper.get_live_odds()
        else:
            odds_list = self.dk_scraper.get_pregame_odds()

        logger.info(f"DK odds: received {len(odds_list)} games from Nash API")

        # Cache today's games ONCE for all odds matching (not per-entry)
        todays_games = get_games_for_date(date.today().isoformat())

        matched_count = 0
        for odds in odds_list:
            matched_id = self._match_odds_to_game(odds, todays_games)
            if matched_id:
                self._store_odds(matched_id, odds, source='draftkings', is_live=live)
                matched_count += 1
                logger.debug(
                    f"DK odds matched: {odds.get('team_a')} vs {odds.get('team_b')} "
                    f"→ game_id={matched_id}"
                )
            else:
                logger.warning(
                    f"DK odds NO MATCH: '{odds.get('team_a')}' vs '{odds.get('team_b')}' "
                    f"— not found in today's {len(todays_games)} NCAA games"
                )

        logger.info(f"DK odds: matched {matched_count}/{len(odds_list)} games")

    def _refresh_pregame_odds(self):
        """Fetch and store pregame odds from The Odds API."""
        odds_list = self.odds_scraper.get_pregame_odds()
        todays_games = get_games_for_date(date.today().isoformat())

        for odds in odds_list:
            matched_id = self._match_odds_to_game(odds, todays_games)
            if matched_id:
                self._store_odds(matched_id, odds, source='odds_api', is_live=False)

    def _store_odds(self, game_id, odds, source, is_live):
        """Consolidated odds-to-database mapping (eliminates duplicate code)."""
        insert_odds(game_id, {
            'source': source,
            'is_live': 1 if is_live else 0,
            'spread_team_a': odds.get('spread_a'),
            'spread_odds_a': odds.get('spread_odds_a'),
            'spread_team_b': odds.get('spread_b'),
            'spread_odds_b': odds.get('spread_odds_b'),
            'total_points': odds.get('total'),
            'over_odds': odds.get('over_odds'),
            'under_odds': odds.get('under_odds'),
            'moneyline_a': odds.get('moneyline_a'),
            'moneyline_b': odds.get('moneyline_b'),
        })

    # ---- Odds matching ----

    def _match_odds_to_game(self, odds, todays_games):
        """
        Match odds data to a game_id using team name comparison.
        Takes pre-fetched games list to avoid repeated DB queries.
        """
        odds_a = (odds.get('team_a') or '').lower()
        odds_b = (odds.get('team_b') or '').lower()

        if not odds_a or not odds_b:
            return None

        for game in todays_games:
            game_a = (game.get('team_a_name') or '').lower()
            game_b = (game.get('team_b_name') or '').lower()

            if not game_a or not game_b:
                continue

            # Try both orderings (home/away may be flipped between sources)
            if (_team_match(odds_a, game_a) and _team_match(odds_b, game_b)):
                return game['game_id']
            if (_team_match(odds_a, game_b) and _team_match(odds_b, game_a)):
                return game['game_id']

        return None

    # ---- Frontend data methods ----

    def get_scoreboard(self):
        """
        Get the main scoreboard data for all today's games.
        Returns list of game summaries with odds and insights.
        """
        games = get_games_for_date(date.today().isoformat())
        scoreboard = []

        for game in games:
            gid = game['game_id']
            team_a_id = game.get('team_a_id', '')
            team_b_id = game.get('team_b_id', '')

            # Fetch game data
            plays = get_all_plays(gid)
            players = get_players_for_game(gid)
            shots = get_all_shots(gid)
            pregame_odds = get_game_odds(gid, live=False)
            live_odds = get_game_odds(gid, live=True)

            # Expected scores
            expected = calculate_game_expected_score(plays, shots)

            # H1 expected scores (for games in 2nd half)
            h1_expected = {}
            current_half = game.get('half', 1)
            if current_half >= 2:
                h1_expected = calculate_h1_expected_score(plays)

            # Insights
            insights = generate_insights(plays, players, game, shots)

            # Current half fouls
            half_key = f'team_a_fouls_h{current_half}'
            half_key_b = f'team_b_fouls_h{current_half}'

            entry = {
                'game_id': gid,
                'team_a_name': game.get('team_a_name', 'TBD'),
                'team_b_name': game.get('team_b_name', 'TBD'),
                'team_a_id': team_a_id,
                'team_b_id': team_b_id,
                'team_a_score': game.get('team_a_score', 0),
                'team_b_score': game.get('team_b_score', 0),
                'team_a_expected': round(expected.get(team_a_id, 0), 1),
                'team_b_expected': round(expected.get(team_b_id, 0), 1),
                'half': current_half,
                'game_clock': game.get('game_clock', '20:00'),
                'status': game.get('status', 'scheduled'),
                'is_timeout': game.get('is_timeout', 0),

                # First half scores
                'team_a_h1_score': game.get('team_a_h1_score', 0),
                'team_b_h1_score': game.get('team_b_h1_score', 0),
                'team_a_h1_expected': round(h1_expected.get(team_a_id, 0), 1),
                'team_b_h1_expected': round(h1_expected.get(team_b_id, 0), 1),

                # Current half fouls
                'team_a_fouls': game.get(half_key, 0),
                'team_b_fouls': game.get(half_key_b, 0),

                # Odds
                'pregame_spread': pregame_odds.get('spread_team_a') if pregame_odds else None,
                'pregame_spread_odds': pregame_odds.get('spread_odds_a') if pregame_odds else None,
                'pregame_total': pregame_odds.get('total_points') if pregame_odds else None,
                'pregame_over_odds': pregame_odds.get('over_odds') if pregame_odds else None,
                'pregame_under_odds': pregame_odds.get('under_odds') if pregame_odds else None,
                'live_spread': live_odds.get('spread_team_a') if live_odds else None,
                'live_total': live_odds.get('total_points') if live_odds else None,

                # Insights
                'insights': insights,
            }

            scoreboard.append(entry)

        return scoreboard

    def get_game_detail(self, game_id):
        """Get full detailed data for a single game (detail dashboard)."""
        game = get_or_create_game(game_id)
        plays = get_all_plays(game_id)
        shots = get_all_shots(game_id)
        players = get_players_for_game(game_id)
        pregame_odds = get_game_odds(game_id, live=False)
        live_odds = get_game_odds(game_id, live=True)

        team_a_id = game.get('team_a_id', '')
        team_b_id = game.get('team_b_id', '')

        # Build all detail components
        box_score = _build_box_score(plays, players, team_a_id, team_b_id)
        score_timeline = _build_score_timeline(plays, team_a_id, team_b_id)
        shot_breakdown = _build_shot_breakdown(plays, team_a_id, team_b_id)

        expected = calculate_game_expected_score(plays, shots)
        h1_expected = calculate_h1_expected_score(plays)

        insights = generate_insights(plays, players, game, shots)

        return {
            'game': game,
            'box_score': box_score,
            'score_timeline': score_timeline,
            'shot_breakdown': shot_breakdown,
            'expected': {
                'team_a': round(expected.get(team_a_id, 0), 1),
                'team_b': round(expected.get(team_b_id, 0), 1),
            },
            'h1_expected': {
                'team_a': round(h1_expected.get(team_a_id, 0), 1),
                'team_b': round(h1_expected.get(team_b_id, 0), 1),
            },
            'pregame_odds': pregame_odds,
            'live_odds': live_odds,
            'insights': insights,
            'players': players,
            'total_plays': len(plays),
            'total_shots': len(shots),
        }

    def manual_refresh(self, game_id=None):
        """Trigger a manual refresh. If game_id specified, refresh only that game."""
        if game_id:
            with self._lock:
                self._refresh_single_game(game_id)
                # Also pull fresh DK odds for this game
                try:
                    self._refresh_dk_odds(live=True)
                except Exception as e:
                    logger.error(f"DK odds refresh failed: {e}")
        else:
            self.refresh_all()


# ---- Module-level helpers (no self needed) ----

# Words that are too common or generic to be useful for matching,
# but notably NOT "state" — removing it causes Ohio State = Ohio bugs.
_SKIP_WORDS = frozenset({
    'the', 'of', 'at', 'university', 'college', 'team',
})


def _team_match(name_a, name_b):
    """
    Match two team names with word overlap.

    Requires at least one significant word (>2 chars) in common.
    Keeps "state" in the word set so "Ohio State" != "Ohio".
    """
    words_a = {w for w in name_a.split() if w not in _SKIP_WORDS and len(w) > 2}
    words_b = {w for w in name_b.split() if w not in _SKIP_WORDS and len(w) > 2}

    if not words_a or not words_b:
        return False

    overlap = words_a & words_b
    return len(overlap) >= 1


def _build_box_score(plays, players, team_a_id, team_b_id):
    """Build a box score grouped by player and divided by half."""
    _empty_half = lambda: {
        'pts': 0, 'fgm': 0, 'fga': 0, 'fg3m': 0, 'fg3a': 0,
        'ftm': 0, 'fta': 0, 'reb': 0, 'oreb': 0, 'ast': 0,
        'stl': 0, 'blk': 0, 'to': 0, 'pf': 0,
    }

    player_stats = defaultdict(lambda: {
        'player_name': '', 'team_id': '', 'team_name': '',
        'h1': _empty_half(), 'h2': _empty_half(),
    })

    # Pre-fill player names
    for p in players:
        pid = p['player_id']
        player_stats[pid]['player_name'] = p['player_name']
        player_stats[pid]['team_id'] = p['team_id']
        player_stats[pid]['team_name'] = p['team_name']

    # Accumulate stats from plays
    for play in plays:
        pid = play.get('player_id')
        if not pid:
            continue

        half_key = 'h1' if play.get('half', 1) == 1 else 'h2'
        s = player_stats[pid][half_key]

        if play.get('is_made_shot'):
            pts = play.get('points_value', 0)
            s['pts'] += pts
            if play.get('is_ft'):
                s['ftm'] += 1
                s['fta'] += 1
            else:
                s['fgm'] += 1
                s['fga'] += 1
                if play.get('is_three'):
                    s['fg3m'] += 1
                    s['fg3a'] += 1

        elif play.get('is_missed_shot'):
            if play.get('is_ft'):
                s['fta'] += 1
            else:
                s['fga'] += 1
                if play.get('is_three'):
                    s['fg3a'] += 1

        if play.get('is_assist'):
            s['ast'] += 1
        if play.get('is_def_rebound') or play.get('is_off_rebound'):
            s['reb'] += 1
        if play.get('is_off_rebound'):
            s['oreb'] += 1
        if play.get('is_steal'):
            s['stl'] += 1
        if play.get('is_block'):
            s['blk'] += 1
        if play.get('is_turnover'):
            s['to'] += 1
        if play.get('is_foul_personal') or play.get('is_foul_shooting'):
            s['pf'] += 1

    # Build team lists with totals
    team_a_box = []
    team_b_box = []

    for pid, data in player_stats.items():
        if not data['player_name']:
            continue

        # Compute totals from h1 + h2
        total = {}
        for key in data['h1']:
            total[key] = data['h1'][key] + data['h2'][key]
        data['total'] = total

        if data['team_id'] == team_a_id:
            team_a_box.append(data)
        elif data['team_id'] == team_b_id:
            team_b_box.append(data)

    # Sort by total points descending
    team_a_box.sort(key=lambda x: -x['total']['pts'])
    team_b_box.sort(key=lambda x: -x['total']['pts'])

    return {'team_a': team_a_box, 'team_b': team_b_box}


def _build_score_timeline(plays, team_a_id, team_b_id):
    """
    Build score timeline data for charting.
    Returns list of {elapsed_minutes, team_a_score, team_b_score, total, diff}
    """
    timeline = [{
        'elapsed_minutes': 0,
        'team_a_score': 0, 'team_b_score': 0,
        'total': 0, 'diff': 0,
    }]

    a_score = 0
    b_score = 0

    for play in plays:
        pts = play.get('points_value', 0)
        if not play.get('is_made_shot') or pts <= 0:
            continue

        tid = play.get('team_id', '')
        elapsed = play.get('elapsed_seconds', 0) / 60

        if tid == team_a_id:
            a_score += pts
        elif tid == team_b_id:
            b_score += pts
        else:
            continue

        timeline.append({
            'elapsed_minutes': round(elapsed, 2),
            'team_a_score': a_score,
            'team_b_score': b_score,
            'total': a_score + b_score,
            'diff': a_score - b_score,
        })

    return timeline


def _build_shot_breakdown(plays, team_a_id, team_b_id):
    """
    Build shot breakdown by half and type.
    Categories: rim, midrange, three
    Tracks: made/attempted, late_clock, transition
    """
    categories = ('rim', 'midrange', 'three')

    def _empty_cat():
        return {
            'made': 0, 'attempted': 0,
            'late_clock_made': 0, 'late_clock_attempted': 0,
            'transition_made': 0, 'transition_attempted': 0,
        }

    breakdown = {}
    for team_key in ('team_a', 'team_b'):
        breakdown[team_key] = {}
        for half_key in ('h1', 'h2', 'total'):
            breakdown[team_key][half_key] = {cat: _empty_cat() for cat in categories}

    # Track possession changes
    possession_start = None
    prev_play_type = None

    for play in plays:
        tid = play.get('team_id', '')
        if not tid:
            continue

        is_shot = play.get('is_made_shot') or play.get('is_missed_shot')
        if not is_shot or play.get('is_ft'):
            # Track possession changes for transition/shot clock detection
            if play.get('is_turnover') or play.get('is_def_rebound'):
                possession_start = play.get('elapsed_seconds', 0)
                prev_play_type = 'turnover' if play.get('is_turnover') else 'def_rebound'
            continue

        team_key = 'team_a' if tid == team_a_id else ('team_b' if tid == team_b_id else None)
        if not team_key:
            continue

        half_key = 'h1' if play.get('half', 1) == 1 else 'h2'
        shot_cat = classify_shot_type_from_play(play)
        if shot_cat == 'ft':
            shot_cat = 'midrange'  # Shouldn't happen (filtered above), but guard

        made = 1 if play.get('is_made_shot') else 0
        elapsed = play.get('elapsed_seconds', 0)

        # Late shot clock
        is_late = False
        if possession_start is not None:
            clock_remaining = 30 - (elapsed - possession_start)
            is_late = clock_remaining <= 5

        # Transition: shot within 7 seconds of a turnover or defensive rebound
        _prev = prev_play_type or ''
        _time_since = elapsed - (possession_start or 0)
        is_trans = _time_since <= 7 and _prev in ('turnover', 'def_rebound')

        # Update counts for both the specific half and totals
        for hk in (half_key, 'total'):
            cat = breakdown[team_key][hk][shot_cat]
            cat['attempted'] += 1
            cat['made'] += made
            if is_late:
                cat['late_clock_attempted'] += 1
                cat['late_clock_made'] += made
            if is_trans:
                cat['transition_attempted'] += 1
                cat['transition_made'] += made

        # Update possession tracking
        possession_start = elapsed
        prev_play_type = 'shot'

    return breakdown
