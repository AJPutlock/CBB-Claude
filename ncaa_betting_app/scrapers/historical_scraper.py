"""
Bulk historical scraper for stats.ncaa.org play-by-play and shot data.

Designed for off-season use. Fully resumable — progress is checkpointed to
ncaa_historical.db after every game. Interrupt with Ctrl+C at any time and
restart; already-completed games are skipped automatically.

Usage (run from the ncaa_betting_app directory):

  # Step 1: Discover all game IDs for a season (iterates every calendar date)
  python -m scrapers.historical_scraper discover --season 2024-25

  # Step 2: Scrape all discovered games (runs until complete or interrupted)
  python -m scrapers.historical_scraper run --season 2024-25

  # Check progress at any time
  python -m scrapers.historical_scraper status --season 2024-25

  # Retry games that errored
  python -m scrapers.historical_scraper retry --season 2024-25

Rate limiting:
  Default is 90 seconds between game scrapes. Each game makes 2 page requests
  (box_score + play_by_play). A full season (~5,500 D1 games) takes ~6 days.
  You can lower the rate limit at your own risk: --rate-limit 60
"""
import argparse
import logging
import sys
import time
from datetime import date, timedelta, datetime

from scrapers.ncaa_scraper import NCAAStatsScraper
from models import historical_database as hdb

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Season registry
# Maps season label → academic_year (NCAA's URL parameter) and date range.
# academic_year is the year the season ENDS in (2024-25 season → '2025').
# ---------------------------------------------------------------------------
SEASONS = {
    '2025-26': {
        'academic_year': '2026',
        'start': '2025-11-01',
        'end':   '2026-04-10',
    },
    '2024-25': {
        'academic_year': '2025',
        'start': '2024-11-01',
        'end':   '2025-04-10',
    },
    '2023-24': {
        'academic_year': '2024',
        'start': '2023-11-01',
        'end':   '2024-04-10',
    },
    '2022-23': {
        'academic_year': '2023',
        'start': '2022-11-01',
        'end':   '2023-04-10',
    },
}


class HistoricalNCAAStatsScraper(NCAAStatsScraper):
    """
    NCAAStatsScraper subclass that overrides ACADEMIC_YEAR for a specific season.
    Everything else (browser, rate limiting, parsing) is inherited unchanged.
    """
    def __init__(self, academic_year, min_request_interval=5):
        super().__init__(min_request_interval=min_request_interval)
        # Instance attribute shadows the class variable
        self.ACADEMIC_YEAR = academic_year


# ---------------------------------------------------------------------------
# Discovery: find all game_ids for a season
# ---------------------------------------------------------------------------

def discover_season(season_label, force=False):
    """
    Iterate every calendar date in the season and call get_daily_games() for
    each date to collect game_ids. Stores results in scrape_progress.

    Skips dates already marked complete in date_discovery_progress unless
    force=True.
    """
    if season_label not in SEASONS:
        logger.error(f"Unknown season '{season_label}'. Known: {list(SEASONS.keys())}")
        sys.exit(1)

    cfg = SEASONS[season_label]
    scraper = HistoricalNCAAStatsScraper(
        academic_year=cfg['academic_year'],
        min_request_interval=3,   # discovery is just scoreboard pages, lighter load
    )

    start = date.fromisoformat(cfg['start'])
    end   = date.fromisoformat(cfg['end'])

    total_dates = (end - start).days + 1
    total_games_found = 0
    dates_skipped = 0

    logger.info(f"Discovering {season_label} — {start} to {end} ({total_dates} dates)")

    current = start
    while current <= end:
        date_str = current.isoformat()

        if not force and hdb.is_date_discovered(season_label, date_str):
            dates_skipped += 1
            current += timedelta(days=1)
            continue

        try:
            games = scraper.get_daily_games(game_date=current)
        except Exception as e:
            logger.warning(f"  {date_str}: fetch failed — {e}")
            current += timedelta(days=1)
            continue

        # Filter to final games only — we only want completed games
        final_games = [g for g in games if g.get('is_final')]

        for g in final_games:
            hdb.upsert_game_to_progress(
                game_id=g['game_id'],
                season=season_label,
                game_date=date_str,
                home_team=g.get('team_a_name', ''),
                away_team=g.get('team_b_name', ''),
            )

        hdb.mark_date_discovered(season_label, date_str, len(final_games))
        total_games_found += len(final_games)

        if final_games:
            logger.info(f"  {date_str}: {len(final_games)} games found")

        current += timedelta(days=1)

    summary = hdb.get_progress_summary(season_label)
    logger.info(
        f"\nDiscovery complete for {season_label}. "
        f"Total pending: {summary.get('pending', 0)}  "
        f"(+{dates_skipped} dates already done)"
    )


# ---------------------------------------------------------------------------
# Scraping: process all pending games
# ---------------------------------------------------------------------------

def run_season(season_label, rate_limit_sec=90):
    """
    Scrape all pending games for a season. Checkpoints after every game.
    Safe to interrupt with Ctrl+C — restart picks up where it left off.
    """
    if season_label not in SEASONS:
        logger.error(f"Unknown season '{season_label}'. Known: {list(SEASONS.keys())}")
        sys.exit(1)

    cfg = SEASONS[season_label]
    scraper = HistoricalNCAAStatsScraper(
        academic_year=cfg['academic_year'],
        min_request_interval=rate_limit_sec,
    )

    pending = hdb.get_pending_games(season_label)
    if not pending:
        logger.info(f"No pending games for {season_label}. Run 'discover' first.")
        return

    summary = hdb.get_progress_summary(season_label)
    total = summary.get('total', len(pending))
    done_at_start = summary.get('complete', 0)

    logger.info(
        f"Starting scrape for {season_label}: "
        f"{len(pending)} pending / {total} total  "
        f"({done_at_start} already complete)"
    )

    session_complete = 0
    session_errors = 0

    for i, game in enumerate(pending):
        game_id   = game['game_id']
        game_date = game['game_date']
        home      = game.get('home_team', '')
        away      = game.get('away_team', '')

        logger.info(
            f"[{i+1}/{len(pending)}]  {game_date}  {home} vs {away}  (id={game_id})"
        )

        try:
            result = scraper.scrape_game_data(
                game_id=game_id,
                last_play_index=0,
                last_shot_count=0,
                is_final=True,
            )
        except KeyboardInterrupt:
            logger.info(f"\nInterrupted after {session_complete} complete, {session_errors} errors.")
            logger.info("Safe to restart — progress is saved.")
            sys.exit(0)
        except Exception as e:
            logger.warning(f"  Scrape exception: {e}")
            hdb.mark_game_error(game_id, str(e))
            session_errors += 1
            continue

        if result is None:
            hdb.mark_game_error(game_id, "scrape_game_data returned None")
            session_errors += 1
            logger.warning("  Result was None — marked as error")
            continue

        plays  = result.get('plays', [])
        shots  = result.get('shots', [])
        gs     = result.get('game_state', {})

        # Basic sanity check — a real game should have plays
        if not plays:
            hdb.mark_game_error(game_id, "0 plays returned")
            session_errors += 1
            logger.warning("  0 plays returned — marked as error")
            continue

        # Store everything in historical DB
        hdb.insert_game(game_id, season_label, game_date, gs)
        n_plays = hdb.insert_plays(game_id, season_label, plays)
        n_shots = hdb.insert_shots(game_id, season_label, shots)
        hdb.insert_players(game_id, result.get('players', []))
        hdb.mark_game_complete(game_id)

        session_complete += 1
        logger.info(f"  OK — {n_plays} plays, {n_shots} shots")

        # Log cumulative progress every 25 games
        if session_complete % 25 == 0:
            s = hdb.get_progress_summary(season_label)
            pct = 100 * s['complete'] / s['total'] if s['total'] else 0
            logger.info(
                f"  Progress: {s['complete']}/{s['total']} complete ({pct:.1f}%)  "
                f"errors={s['errors']}"
            )

    logger.info(
        f"\nSession complete — {session_complete} scraped, {session_errors} errors."
    )
    _print_status(season_label)


# ---------------------------------------------------------------------------
# Status / retry helpers
# ---------------------------------------------------------------------------

def _print_status(season_label):
    if season_label:
        seasons_to_show = [season_label] if season_label in SEASONS else list(SEASONS.keys())
    else:
        seasons_to_show = list(SEASONS.keys())

    print()
    print(f"{'Season':<12} {'Total':>7} {'Complete':>9} {'Pending':>8} {'Errors':>7}  {'%Done':>6}")
    print("-" * 55)
    for s in seasons_to_show:
        summary = hdb.get_progress_summary(s)
        if not summary or not summary.get('total'):
            print(f"{s:<12} {'(not started)':>32}")
            continue
        total    = summary['total']    or 0
        complete = summary['complete'] or 0
        pending  = summary['pending']  or 0
        errors   = summary['errors']   or 0
        pct = 100 * complete / total if total else 0
        print(f"{s:<12} {total:>7} {complete:>9} {pending:>8} {errors:>7}  {pct:>5.1f}%")
    print()


def retry_errors(season_label):
    n = hdb.reset_errors_to_pending(season_label)
    logger.info(f"Reset {n} error games to pending for {season_label}")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Historical NCAA stats scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest='command', required=True)

    # discover
    p_discover = sub.add_parser('discover', help='Find all game IDs for a season')
    p_discover.add_argument('--season', required=True, choices=list(SEASONS.keys()))
    p_discover.add_argument('--force', action='store_true',
                            help='Re-scan dates already marked complete')

    # run
    p_run = sub.add_parser('run', help='Scrape all pending games for a season')
    p_run.add_argument('--season', required=True, choices=list(SEASONS.keys()))
    p_run.add_argument('--rate-limit', type=int, default=90,
                       help='Seconds between game scrapes (default: 90)')

    # status
    p_status = sub.add_parser('status', help='Show scrape progress')
    p_status.add_argument('--season', default=None)

    # retry
    p_retry = sub.add_parser('retry', help='Re-queue all error games for a season')
    p_retry.add_argument('--season', required=True, choices=list(SEASONS.keys()))

    args = parser.parse_args()

    if args.command == 'discover':
        discover_season(args.season, force=args.force)
    elif args.command == 'run':
        run_season(args.season, rate_limit_sec=args.rate_limit)
    elif args.command == 'status':
        _print_status(args.season)
    elif args.command == 'retry':
        retry_errors(args.season)


if __name__ == '__main__':
    main()
