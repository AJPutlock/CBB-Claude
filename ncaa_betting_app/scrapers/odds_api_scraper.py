"""
The Odds API scraper for pregame college basketball odds.
https://the-odds-api.com/

Requires an API key set as ODDS_API_KEY environment variable
or passed to the constructor.

Improvements over v1:
- Session reuse (connection keep-alive) instead of bare requests.get()
- API quota tracking from response headers (remaining requests / used)
- Time-based caching to avoid burning API calls on rapid refreshes
- Logs remaining quota after each call so you can monitor usage
"""
import requests
import logging
import os
import time

logger = logging.getLogger(__name__)


class OddsAPIScraper:
    """Fetch pregame odds from The Odds API with quota tracking and caching."""

    BASE_URL = "https://api.the-odds-api.com/v4"
    SPORT_KEY = "basketball_ncaab"

    def __init__(self, api_key=None, cache_ttl=300):
        """
        Args:
            api_key: The Odds API key. Falls back to ODDS_API_KEY env var.
            cache_ttl: Seconds to cache results before allowing a new API call.
                       Default 300 (5 min) to conserve quota.
        """
        self.api_key = api_key or os.environ.get('ODDS_API_KEY', '')
        self.cache_ttl = cache_ttl

        if not self.api_key:
            logger.warning("No ODDS_API_KEY set. Pregame odds will not be available.")

        # Session for connection reuse
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
        })

        # Quota tracking (populated from response headers)
        self.requests_remaining = None
        self.requests_used = None

        # Response caches (separate for pregame and live)
        self._pregame_cache = None
        self._pregame_cache_time = 0
        self._live_cache = None
        self._live_cache_time = 0

    def get_pregame_odds(self, regions='us', markets='spreads,totals'):
        """
        Fetch upcoming (pregame) NCAAB odds.
        Uses a 5-minute cache to avoid burning quota on rapid refreshes.
        Returns list of standardized odds dicts for scheduled games only.
        """
        if not self.api_key:
            logger.error("No API key configured for The Odds API")
            return []

        if self._pregame_cache is not None and (time.time() - self._pregame_cache_time) < self.cache_ttl:
            logger.debug(f"Returning cached pregame odds ({len(self._pregame_cache)} events)")
            return self._pregame_cache

        try:
            url = f"{self.BASE_URL}/sports/{self.SPORT_KEY}/odds/"
            params = {
                'apiKey': self.api_key,
                'regions': regions,
                'markets': markets,
                'oddsFormat': 'american',
            }

            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            self._update_quota(response.headers)

            data = response.json()
            result = self._parse_response(data)

            self._pregame_cache = result
            self._pregame_cache_time = time.time()
            return result

        except requests.RequestException as e:
            logger.error(f"Odds API pregame request failed: {e}")
            if self._pregame_cache is not None:
                logger.info("Returning stale pregame cache after API error")
                return self._pregame_cache
            return []
        except (ValueError, KeyError) as e:
            logger.error(f"Odds API pregame parse error: {e}")
            return []

    def get_live_odds(self, regions='us', markets='spreads,totals'):
        """
        Fetch live (in-progress) NCAAB odds from The Odds API.
        Hits the same endpoint with commenceTime filtering — events that have
        already started and still have open markets are returned.
        Cache TTL is shorter (60s) since live odds change frequently.
        Returns list of standardized odds dicts for in-progress games.
        """
        if not self.api_key:
            logger.error("No API key configured for The Odds API")
            return []

        live_cache_ttl = 60  # 1 minute for live odds
        if self._live_cache is not None and (time.time() - self._live_cache_time) < live_cache_ttl:
            logger.debug(f"Returning cached live odds ({len(self._live_cache)} events)")
            return self._live_cache

        try:
            url = f"{self.BASE_URL}/sports/{self.SPORT_KEY}/odds/"
            params = {
                'apiKey': self.api_key,
                'regions': regions,
                'markets': markets,
                'oddsFormat': 'american',
                'commenceTimeTo': self._now_utc_str(),  # only games that have started
            }

            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            self._update_quota(response.headers)

            data = response.json()
            result = self._parse_response(data)

            self._live_cache = result
            self._live_cache_time = time.time()
            logger.info(f"Live odds fetched: {len(result)} in-progress games")
            return result

        except requests.RequestException as e:
            logger.error(f"Odds API live request failed: {e}")
            if self._live_cache is not None:
                logger.info("Returning stale live cache after API error")
                return self._live_cache
            return []
        except (ValueError, KeyError) as e:
            logger.error(f"Odds API live parse error: {e}")
            return []

    def _now_utc_str(self):
        """Return current UTC time as ISO 8601 string for API params."""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    def _update_quota(self, headers):
        """Extract and log API quota from response headers."""
        try:
            self.requests_remaining = int(headers.get('x-requests-remaining', -1))
            self.requests_used = int(headers.get('x-requests-used', -1))
            logger.info(
                f"Odds API quota: {self.requests_used} used, "
                f"{self.requests_remaining} remaining"
            )
            if self.requests_remaining is not None and self.requests_remaining < 10:
                logger.warning(
                    f"LOW QUOTA: Only {self.requests_remaining} Odds API requests remaining!"
                )
        except (ValueError, TypeError):
            pass

    def _parse_response(self, data):
        """Parse The Odds API response into standardized format."""
        odds_list = []

        for event in data:
            home = event.get('home_team', '')
            away = event.get('away_team', '')

            entry = {
                'event_id': event.get('id', ''),
                'team_a': home,
                'team_b': away,
                'commence_time': event.get('commence_time', ''),
                'spread_a': None, 'spread_odds_a': None,
                'spread_b': None, 'spread_odds_b': None,
                'spread_a_team': None, 'spread_b_team': None,
                'total': None, 'over_odds': None, 'under_odds': None,
                'moneyline_a': None, 'moneyline_b': None,
            }

            # Pick bookmaker: prefer DraftKings, fall back to first available
            bookmakers = event.get('bookmakers', [])
            preferred = None
            for bm in bookmakers:
                if 'draftkings' in bm.get('key', '').lower():
                    preferred = bm
                    break
            if preferred is None and bookmakers:
                preferred = bookmakers[0]

            if preferred:
                self._parse_bookmaker(preferred, entry, home, away)

            odds_list.append(entry)

        return odds_list

    def _parse_bookmaker(self, bookmaker, entry, home, away):
        """
        Parse a single bookmaker's markets into the odds entry.

        Spread outcomes are matched by team name from the API response —
        NOT by home/away position. This ensures the favourite's line is
        always correctly attributed regardless of home/away status.

        spread_a -> whichever team has the negative (favourite) line
        spread_b -> whichever team has the positive (underdog) line
        """
        for market in bookmaker.get('markets', []):
            key = market.get('key', '')
            outcomes = market.get('outcomes', [])

            if key == 'spreads':
                # First pass: collect both outcomes by name
                spread_by_name = {}
                for o in outcomes:
                    name = o.get('name', '')
                    if name:
                        spread_by_name[name] = o

                # Assign favourite (negative line) to spread_a, underdog to spread_b
                # This decouples favourite/underdog from home/away entirely
                home_outcome = spread_by_name.get(home)
                away_outcome = spread_by_name.get(away)

                if home_outcome and away_outcome:
                    home_point = home_outcome.get('point', 0) or 0
                    away_point = away_outcome.get('point', 0) or 0
                    # Favourite has the lower (more negative) point value
                    if home_point <= away_point:
                        fav, dog = home_outcome, away_outcome
                        entry['spread_a_team'] = home
                        entry['spread_b_team'] = away
                    else:
                        fav, dog = away_outcome, home_outcome
                        entry['spread_a_team'] = away
                        entry['spread_b_team'] = home
                    entry['spread_a'] = fav.get('point')
                    entry['spread_odds_a'] = fav.get('price')
                    entry['spread_b'] = dog.get('point')
                    entry['spread_odds_b'] = dog.get('price')
                elif home_outcome:
                    entry['spread_a'] = home_outcome.get('point')
                    entry['spread_odds_a'] = home_outcome.get('price')
                    entry['spread_a_team'] = home
                elif away_outcome:
                    entry['spread_a'] = away_outcome.get('point')
                    entry['spread_odds_a'] = away_outcome.get('price')
                    entry['spread_a_team'] = away

            elif key == 'totals':
                for o in outcomes:
                    if o.get('name') == 'Over':
                        entry['total'] = o.get('point')
                        entry['over_odds'] = o.get('price')
                    elif o.get('name') == 'Under':
                        entry['under_odds'] = o.get('price')



    def get_historical_odds(self, snapshot_dt=None, regions='us'):
        """
        Fetch a historical odds snapshot for NCAAB from The Odds API.

        Instead of querying once per game (expensive), we fetch a single
        snapshot of the entire day's slate at a given moment — one API call
        returns all games available at that time.  The recommended usage is
        to call this once at startup with a time 5-10 minutes before the
        earliest game of the day, which captures closing lines for all games.

        Args:
            snapshot_dt: datetime object (UTC) for the snapshot.  Defaults to
                         5 minutes before "now" if not provided.
            regions:     Odds regions (default 'us').

        Returns:
            List of standardized odds dicts (same shape as get_pregame_odds),
            with moneyline fields always None (we only request spreads/totals).
            Returns [] on failure.
        """
        if not self.api_key:
            logger.error("No API key configured for The Odds API historical endpoint")
            return []

        from datetime import datetime, timezone, timedelta

        if snapshot_dt is None:
            snapshot_dt = datetime.now(timezone.utc) - timedelta(minutes=5)

        date_str = snapshot_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        try:
            url = f"{self.BASE_URL}/historical/sports/{self.SPORT_KEY}/odds/"
            params = {
                'apiKey': self.api_key,
                'regions': regions,
                'markets': 'spreads,totals',
                'oddsFormat': 'american',
                'date': date_str,
            }

            logger.info(f"Fetching historical odds snapshot at {date_str}")
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            self._update_quota(response.headers)

            data = response.json()
            # Historical endpoint wraps results under a "data" key
            events = data.get('data', data) if isinstance(data, dict) else data
            return self._parse_response(events)

        except requests.RequestException as e:
            logger.error(f"Historical Odds API request failed: {e}")
            return []
        except (ValueError, KeyError) as e:
            logger.error(f"Historical Odds API parse error: {e}")
            return []


    def get_quota_status(self):
        """Return current known API quota status."""
        return {
            'requests_remaining': self.requests_remaining,
            'requests_used': self.requests_used,
            'pregame_cache_age_seconds': round(time.time() - self._pregame_cache_time, 1) if self._pregame_cache_time else None,
            'pregame_cache_entries': len(self._pregame_cache) if self._pregame_cache else 0,
            'live_cache_age_seconds': round(time.time() - self._live_cache_time, 1) if self._live_cache_time else None,
            'live_cache_entries': len(self._live_cache) if self._live_cache else 0,
        }
