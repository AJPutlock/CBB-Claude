"""
DraftKings Nash API scraper for NCAAB spreads, moneylines, and totals.
Uses the existing Selenium browser session (from scrapers/browser.py) to
execute the API request via JavaScript fetch — this reuses the browser's
cookies and session so DK sees it as a normal page request.
"""

import json
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

# ── Nash API config ────────────────────────────────────────────────────────────

NASH_URL = (
    "https://sportsbook-nash.draftkings.com/sites/US-OH-SB/api/sportscontent"
    "/controldata/league/leagueSubcategory/v1/markets"
)

NASH_PARAMS = {
    "isBatchable": "false",
    "templateVars": "92483,4511",
    "eventsQuery": (
        "$filter=leagueId eq '92483' AND "
        "clientMetadata/Subcategories/any(s: s/Id eq '4511')"
    ),
    "marketsQuery": (
        "$filter=clientMetadata/subCategoryId eq '4511' AND "
        "tags/all(t: t ne 'SportcastBetBuilder')"
    ),
    "include": "Events",
    "entity": "events",
}

NASH_HEADERS = {
    "x-client-name": "web",
    "x-client-page": "league",
    "x-client-version": "2609.2.1.5",
    "x-client-widget-name": "cms",
    "x-client-widget-version": "2.5.0",
    "x-client-feature": "leagueSubcategory",
}


# ── Public class (used by GameManager) ────────────────────────────────────────

class DraftKingsScraper:
    """
    Wrapper class used by GameManager to fetch DraftKings NCAAB odds.

    GameManager calls:
        self.dk_scraper.get_live_odds()     → for in-game odds
        self.dk_scraper.get_pregame_odds()  → for pregame odds

    Both methods return a list of odds dicts in the standardized format
    that GameManager._store_odds() expects:
        {
            team_a, team_b,
            spread_a, spread_odds_a,
            spread_b, spread_odds_b,
            total, over_odds, under_odds,
            moneyline_a, moneyline_b,
        }

    The Nash API doesn't separate live vs. pregame at the URL level — DK
    handles that internally and only returns currently-offered markets.
    So both methods hit the same endpoint; the live= distinction is
    preserved for the database (is_live flag) but the fetch is identical.
    """

    def __init__(self):
        # Browser driver is fetched lazily on each call so we don't hold
        # a stale reference if the browser restarts between refreshes.
        pass

    def _get_driver(self):
        """
        Get the shared Selenium driver, ensuring the browser is on a DraftKings
        page before returning.  The Nash API XHR is subject to CORS — it only
        succeeds when executed from a draftkings.com origin.  If the browser has
        navigated away (e.g. the NCAA scraper visited stats.ncaa.org) we must
        return to a DK page first.
        """
        from scrapers.browser import get_driver, warm_dk_session
        import time as _time

        warm_dk_session()
        driver = get_driver()

        # Check if we're still on a DK domain; if not, navigate back
        try:
            current = driver.current_url or ''
        except Exception:
            current = ''

        if 'draftkings.com' not in current:
            logger.info("Browser navigated away from DK — returning to sportsbook page for CORS")
            try:
                driver.get("https://sportsbook.draftkings.com/leagues/basketball/ncaab")
                _time.sleep(5)
            except Exception as e:
                logger.warning("Could not navigate back to DK page: %s", e)

        return driver

    def _fetch_and_convert(self) -> list[dict]:
        """
        Fetch raw Nash API data, parse it, and convert each game into the
        standardized odds format GameManager expects.
        """
        try:
            driver = self._get_driver()
        except Exception as e:
            logger.error("Could not get browser driver: %s", e)
            return []

        games = scrape_draftkings(driver)
        return [_to_standard_odds(g) for g in games]

    def get_live_odds(self) -> list[dict]:
        """
        Fetch current DraftKings odds (live in-game markets).
        Returns standardized odds dicts for GameManager._store_odds().
        """
        logger.info("Fetching DK live odds via Nash API...")
        return self._fetch_and_convert()

    def get_pregame_odds(self) -> list[dict]:
        """
        Fetch current DraftKings odds (pregame markets).
        Returns standardized odds dicts for GameManager._store_odds().
        """
        logger.info("Fetching DK pregame odds via Nash API...")
        return self._fetch_and_convert()


def _to_standard_odds(game: dict) -> dict:
    """
    Convert a parsed game dict (from parse_games) into the standardized
    odds format that GameManager._store_odds() / _match_odds_to_game() reads.

    Selections now carry outcome_type ("Home", "Away", "Over", "Under")
    so we use that directly instead of label matching.
    """
    home = game.get("home_team", "")
    away = game.get("away_team", "")

    result = {
        "team_a": home,
        "team_b": away,
        "spread_a": None, "spread_odds_a": None,
        "spread_b": None, "spread_odds_b": None,
        "total": None, "over_odds": None, "under_odds": None,
        "moneyline_a": None, "moneyline_b": None,
    }

    # ── Spread ────────────────────────────────────────────────────────────────
    for o in (game.get("spread") or []):
        ot = o.get("outcome_type", "")
        if ot == "Home":
            result["spread_a"] = o.get("line")
            result["spread_odds_a"] = o.get("odds_raw")
        elif ot == "Away":
            result["spread_b"] = o.get("line")
            result["spread_odds_b"] = o.get("odds_raw")

    # ── Moneyline ─────────────────────────────────────────────────────────────
    for o in (game.get("moneyline") or []):
        ot = o.get("outcome_type", "")
        if ot == "Home":
            result["moneyline_a"] = o.get("odds_raw")
        elif ot == "Away":
            result["moneyline_b"] = o.get("odds_raw")

    # ── Total ─────────────────────────────────────────────────────────────────
    for o in (game.get("total") or []):
        ot = o.get("outcome_type", "")
        if ot == "Over":
            result["total"] = o.get("line")
            result["over_odds"] = o.get("odds_raw")
        elif ot == "Under":
            result["under_odds"] = o.get("odds_raw")

    return result




# ── Fetching ───────────────────────────────────────────────────────────────────

def _build_full_url() -> str:
    """Assemble URL with query string (urllib handles encoding)."""
    from urllib.parse import urlencode, urlparse, urlunparse, ParseResult
    query = urlencode(NASH_PARAMS)
    parts = urlparse(NASH_URL)
    full = ParseResult(
        parts.scheme, parts.netloc, parts.path,
        parts.params, query, parts.fragment
    )
    return urlunparse(full)


def fetch_raw_data(driver) -> Optional[dict]:
    """
    Fire the Nash API request from inside the Selenium browser session using
    a synchronous XMLHttpRequest executed via JavaScript.  This reuses the
    browser's existing DK cookies so no extra auth is needed.

    Args:
        driver: The Selenium WebDriver instance from scrapers/browser.py

    Returns:
        Parsed JSON dict from DK, or None on failure.
    """
    url = _build_full_url()

    # Build the headers object as a JS literal
    headers_js = json.dumps(NASH_HEADERS)

    # We use a synchronous XHR (open(..., false)) so execute_script can
    # return the response text directly without needing async handling.
    js = f"""
        var xhr = new XMLHttpRequest();
        xhr.open('GET', {json.dumps(url)}, false);   // false = synchronous

        var headers = {headers_js};
        for (var key in headers) {{
            xhr.setRequestHeader(key, headers[key]);
        }}

        try {{
            xhr.send(null);
        }} catch(e) {{
            return JSON.stringify({{error: e.toString()}});
        }}

        return xhr.responseText;
    """

    try:
        raw = driver.execute_script(js)
    except Exception as e:
        logger.error("JavaScript execution failed: %s", e)
        return None

    if not raw:
        logger.error("Empty response from Nash API")
        return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse Nash API JSON: %s | raw=%.200s", e, raw)
        return None

    if isinstance(data, dict) and "error" in data and len(data) == 1:
        logger.error("Nash API JS error: %s", data["error"])
        return None

    return data


# ── Parsing ────────────────────────────────────────────────────────────────────

def _parse_american_odds(value) -> Optional[float]:
    """
    Parse an American odds value to a float.

    Nash API returns odds as strings in displayOdds.american, using the
    Unicode minus sign (U+2212 '−') instead of a regular hyphen-minus ('-').
    We normalize both before converting.
    """
    if value is None:
        return None
    try:
        # Replace Unicode minus (U+2212) with regular hyphen-minus
        cleaned = str(value).replace('\u2212', '-').replace('+', '').strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _format_american_odds(value) -> Optional[str]:
    """Convert a raw odds value to a readable American odds string."""
    parsed = _parse_american_odds(value)
    if parsed is None:
        return None
    odds = int(round(parsed))
    return f"+{odds}" if odds > 0 else str(odds)


def _parse_selection(selection: dict) -> dict:
    """
    Parse one selection (outcome) from the top-level selections list.

    Nash API selection structure:
      {
        id, marketId, label,
        displayOdds: { american: "−110", decimal: "1.91", fractional: "10/11" },
        trueOdds: 1.909...,
        points: -5.5,          ← spread/total line (absent for moneylines)
        outcomeType: "Home"|"Away"|"Over"|"Under",
        participants: [{name, venueRole, ...}]
      }
    """
    label = selection.get("label", "")
    line = selection.get("points")          # spread line or total points
    outcome_type = selection.get("outcomeType", "")

    odds_str = (selection.get("displayOdds") or {}).get("american")
    odds_raw = _parse_american_odds(odds_str)

    return {
        "label": label,
        "line": line,
        "odds": _format_american_odds(odds_str),
        "odds_raw": odds_raw,
        "outcome_type": outcome_type,   # "Home", "Away", "Over", "Under"
    }


def _classify_market_type(name: str) -> str:
    """Classify a market by name into spread / moneyline / total / other."""
    n = name.lower()
    if "spread" in n or "point spread" in n or "handicap" in n:
        return "spread"
    elif "moneyline" in n or "money line" in n:
        return "moneyline"
    elif "total" in n or "over/under" in n or "points" in n:
        return "total"
    return "other"


def _parse_event(event: dict) -> dict:
    """
    Extract game info + all markets for one event.

    Nash API event structure (actual):
      {
        id, name, startEventDate,
        participants: [
          {name, venueRole: "Home"|"Away", ...},
          ...
        ],
        markets: [     ← attached by parse_games from top-level markets+selections
          {name, marketType, selections: [...]}
        ]
      }
    """
    home_team = "Unknown"
    away_team = "Unknown"

    for p in event.get("participants", []):
        role = p.get("venueRole", "")
        name = p.get("name", "")
        if role == "Home":
            home_team = name
        elif role == "Away":
            away_team = name

    # Fallback: parse "Away @ Home" from flat event name
    if home_team == "Unknown" and event.get("name"):
        parts = event["name"].split(" @ ")
        if len(parts) == 2:
            away_team, home_team = parts[0].strip(), parts[1].strip()

    game = {
        "event_id": event.get("id"),
        "home_team": home_team,
        "away_team": away_team,
        "start_time": event.get("startEventDate"),
        "spread": None,
        "moneyline": None,
        "total": None,
        "other_markets": [],
    }

    for market in event.get("markets", []):
        market_name = market.get("name", "")
        market_type = _classify_market_type(market_name)
        selections = [_parse_selection(s) for s in market.get("selections", [])]

        if not selections:
            continue

        if market_type == "spread":
            game["spread"] = selections
        elif market_type == "moneyline":
            game["moneyline"] = selections
        elif market_type == "total":
            game["total"] = selections
        else:
            game["other_markets"].append({
                "market_type": market_type,
                "market_name": market_name,
                "outcomes": selections,
            })

    return game


def parse_games(raw_data: dict) -> list[dict]:
    """
    Top-level parser. Actual Nash API response shape:
      {
        "events":     [ {id, name, participants, startEventDate, ...} ],
        "markets":    [ {id, eventId, name, marketType, ...} ],
        "selections": [ {id, marketId, label, displayOdds, points, outcomeType, ...} ],
      }

    Three-way join:
      selections → markets (via marketId)
      markets    → events  (via eventId)

    We build lookups and attach selections→markets→events before parsing.
    """
    from collections import defaultdict

    # 1. selections keyed by marketId
    selections_by_market = defaultdict(list)
    for sel in raw_data.get("selections", []):
        mid = str(sel.get("marketId", ""))
        if mid:
            selections_by_market[mid].append(sel)

    # 2. markets (with selections attached) keyed by eventId
    markets_by_event = defaultdict(list)
    for market in raw_data.get("markets", []):
        mid = str(market.get("id", ""))
        eid = str(market.get("eventId", ""))
        if eid:
            market_with_selections = dict(market)
            market_with_selections["selections"] = selections_by_market.get(mid, [])
            markets_by_event[eid].append(market_with_selections)

    # 3. parse each event with its markets+selections attached
    games = []
    events = raw_data.get("events", [])
    if not events:
        for group in raw_data.get("eventGroups", []):
            events.extend(group.get("events", []))

    for event in events:
        try:
            eid = str(event.get("id", ""))
            event_with_markets = dict(event)
            event_with_markets["markets"] = markets_by_event.get(eid, [])
            games.append(_parse_event(event_with_markets))
        except Exception as e:
            logger.warning("Skipping event due to parse error: %s", e)

    logger.info("Parsed %d games from Nash API", len(games))
    return games


# ── Low-level entry point (used by DraftKingsScraper internally) ───────────────

def scrape_draftkings(driver, retries: int = 3, retry_delay: float = 5.0) -> list[dict]:
    """
    Fetch and parse all NCAAB games from the Nash API.

    Args:
        driver:      Selenium WebDriver from scrapers/browser.py
        retries:     How many times to retry on failure
        retry_delay: Seconds to wait between retries

    Returns:
        List of parsed game dicts (home_team, away_team, spread, moneyline,
        total, other_markets). Use DraftKingsScraper.get_live_odds() or
        get_pregame_odds() for the format GameManager expects.
    """
    for attempt in range(1, retries + 1):
        logger.info("Nash API fetch attempt %d/%d", attempt, retries)
        raw = fetch_raw_data(driver)

        if raw is not None:
            games = parse_games(raw)
            if games:
                return games
            logger.warning("API returned data but no games were parsed")

        if attempt < retries:
            logger.info("Retrying in %.1fs…", retry_delay)
            time.sleep(retry_delay)

    logger.error("All %d Nash API fetch attempts failed", retries)
    return []
