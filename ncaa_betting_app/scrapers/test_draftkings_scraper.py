"""
Tests for draftkings_scraper.py — no browser/Selenium needed.

Run from your project root:
    python -m pytest scrapers/test_draftkings_scraper.py -v
    # or just:
    python scrapers/test_draftkings_scraper.py

Covers:
  - Odds formatting edge cases
  - Market type detection
  - Full event parsing
  - Top-level parse_games with the market_map join pattern
  - Empty / malformed response handling
"""

import sys
import os

# ── Path setup ─────────────────────────────────────────────────────────────────
# Adjust this if your project layout differs
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scrapers.draftkings_scraper import (
    _format_american_odds,
    _parse_outcome,
    _parse_market,
    _parse_event,
    parse_games,
)

# ── Helpers ────────────────────────────────────────────────────────────────────

def run(name, fn):
    """Minimal test runner — prints PASS/FAIL."""
    try:
        fn()
        print(f"  PASS  {name}")
    except AssertionError as e:
        print(f"  FAIL  {name}")
        print(f"        {e}")
    except Exception as e:
        print(f"  ERROR {name}: {type(e).__name__}: {e}")


# ── Fixtures ───────────────────────────────────────────────────────────────────

def make_outcome(label="Duke", line=-5.5, odds=-110):
    return {"label": label, "line": line, "oddsAmerican": odds}

def make_market(name="Point Spread", outcomes=None):
    if outcomes is None:
        outcomes = [make_outcome("Duke", -5.5, -110), make_outcome("UNC", 5.5, -110)]
    return {"name": name, "outcomes": outcomes}

def make_event(event_id="EVT1", home="Duke", away="UNC", markets=None):
    """
    NOTE: In the real Nash API, markets are NOT nested inside events.
    They're joined via marketIds + a top-level market_map.
    This fixture is used to test _parse_event directly, so we inject
    markets directly as if they were already joined.
    """
    return {
        "id": event_id,
        "teamHome": {"name": home},
        "teamAway": {"name": away},
        "startDate": "2025-03-15T19:00:00Z",
        "markets": markets if markets is not None else [make_market()],
    }

def make_raw_response(events=None, market_map=None):
    """
    Mirrors the real Nash API shape:
      { "events": [...], "markets": [...] }
    Events reference markets via "marketIds".
    """
    if events is None and market_map is None:
        # Default: one game with all three market types
        spread_market = {
            "marketId": "M1",
            "name": "Point Spread",
            "outcomes": [make_outcome("Duke", -5.5, -110), make_outcome("UNC", 5.5, -110)],
        }
        ml_market = {
            "marketId": "M2",
            "name": "Moneyline",
            "outcomes": [make_outcome("Duke", None, -220), make_outcome("UNC", None, 180)],
        }
        total_market = {
            "marketId": "M3",
            "name": "Total Points",
            "outcomes": [make_outcome("Over", 145.5, -110), make_outcome("Under", 145.5, -110)],
        }
        event = {
            "id": "EVT1",
            "teamHome": {"name": "Duke"},
            "teamAway": {"name": "UNC"},
            "startDate": "2025-03-15T19:00:00Z",
            "marketIds": ["M1", "M2", "M3"],
        }
        return {
            "events": [event],
            "markets": [spread_market, ml_market, total_market],
        }
    return {"events": events or [], "markets": list((market_map or {}).values())}


# ── Tests: _format_american_odds ───────────────────────────────────────────────

def test_odds_positive():
    assert _format_american_odds(180) == "+180"

def test_odds_negative():
    assert _format_american_odds(-110) == "-110"

def test_odds_zero():
    # Unusual but shouldn't crash
    result = _format_american_odds(0)
    assert result == "0", f"Got {result}"

def test_odds_none():
    assert _format_american_odds(None) is None

def test_odds_float_input():
    # API sometimes sends floats like -109.99
    assert _format_american_odds(-109.99) == "-110"

def test_odds_string_input():
    # Verifies the float() cast fix: int(round(float(value)))
    # Without it, round("-110") raises TypeError.
    result = _format_american_odds("-110")
    assert result == "-110", f"Got {result}"


# ── Tests: _parse_outcome ──────────────────────────────────────────────────────

def test_parse_outcome_basic():
    o = _parse_outcome(make_outcome("Duke", -5.5, -110))
    assert o["label"] == "Duke"
    assert o["line"] == -5.5
    assert o["odds"] == "-110"
    assert o["odds_raw"] == -110

def test_parse_outcome_display_odds_fallback():
    """Odds in displayOdds.american instead of oddsAmerican."""
    raw = {"label": "UNC", "line": 5.5, "displayOdds": {"american": -110}}
    o = _parse_outcome(raw)
    assert o["odds"] == "-110"

def test_parse_outcome_no_odds():
    raw = {"label": "UNC", "line": 5.5}
    o = _parse_outcome(raw)
    assert o["odds"] is None
    assert o["odds_raw"] is None


# ── Tests: _parse_market ───────────────────────────────────────────────────────

def test_market_spread():
    m = _parse_market(make_market("Point Spread"))
    assert m["market_type"] == "spread"
    assert len(m["outcomes"]) == 2

def test_market_moneyline():
    m = _parse_market(make_market("Moneyline"))
    assert m["market_type"] == "moneyline"

def test_market_total_points():
    m = _parse_market(make_market("Total Points"))
    assert m["market_type"] == "total", (
        '"Total Points" classified as other — add `or "points" in name` to _parse_market'
    )

def test_market_over_under():
    m = _parse_market(make_market("Over/Under"))
    assert m["market_type"] == "total"

def test_market_empty_outcomes():
    m = _parse_market({"name": "Spread", "outcomes": []})
    assert m is None

def test_market_unknown():
    # "Alt Handicap" contains no spread/moneyline/total keywords
    m = _parse_market(make_market("Alt Handicap"))
    assert m["market_type"] == "other"


# ── Tests: _parse_event ────────────────────────────────────────────────────────

def test_parse_event_team_names():
    e = _parse_event(make_event(home="Duke", away="UNC"))
    assert e["home_team"] == "Duke"
    assert e["away_team"] == "UNC"

def test_parse_event_flat_name_fallback():
    """Events without teamHome/teamAway use 'Away at Home' name format."""
    event = {
        "id": "EVT2",
        "name": "UNC at Duke",
        "startDate": "2025-03-15T19:00:00Z",
        "markets": [],
    }
    e = _parse_event(event)
    assert e["home_team"] == "Duke"
    assert e["away_team"] == "UNC"

def test_parse_event_spread_parsed():
    e = _parse_event(make_event(markets=[make_market("Point Spread")]))
    assert e["spread"] is not None
    assert e["moneyline"] is None
    assert e["total"] is None

def test_parse_event_all_markets():
    markets = [
        make_market("Point Spread"),
        make_market("Moneyline", [make_outcome("Duke", None, -220)]),
        make_market("Total Points", [make_outcome("Over", 145.5, -110)]),
    ]
    e = _parse_event(make_event(markets=markets))
    assert e["spread"] is not None
    assert e["moneyline"] is not None
    # This will fail until "Total Points" detection is fixed
    assert e["total"] is not None, (
        "total is None — fix the 'Total Points' detection bug in _parse_market"
    )

def test_parse_event_id_and_start_time():
    e = _parse_event(make_event(event_id="EVT99"))
    assert e["event_id"] == "EVT99"
    assert e["start_time"] == "2025-03-15T19:00:00Z"


# ── Tests: parse_games (top-level, with market_map join) ──────────────────────

def test_parse_games_basic():
    """
    IMPORTANT: This tests the FIXED version of parse_games where markets
    are joined from a top-level market_map rather than nested on events.
    If this fails with 0 games, the market_map join hasn't been implemented yet.
    """
    raw = make_raw_response()
    games = parse_games(raw)
    assert len(games) == 1, f"Expected 1 game, got {len(games)}"

def test_parse_games_fields_present():
    raw = make_raw_response()
    games = parse_games(raw)
    g = games[0]
    for key in ("event_id", "home_team", "away_team", "start_time",
                "spread", "moneyline", "total", "other_markets"):
        assert key in g, f"Missing key: {key}"

def test_parse_games_empty_response():
    assert parse_games({}) == []

def test_parse_games_event_group_fallback():
    """Responses that nest events under eventGroups."""
    raw = {
        "eventGroups": [
            {"events": [make_event().__class__ and {
                "id": "EVT3",
                "teamHome": {"name": "Kansas"},
                "teamAway": {"name": "Houston"},
                "startDate": "2025-03-15T21:00:00Z",
                "markets": [make_market("Moneyline")],
            }]}
        ]
    }
    games = parse_games(raw)
    assert len(games) == 1
    assert games[0]["home_team"] == "Kansas"

def test_parse_games_skips_bad_events():
    """A malformed event shouldn't crash the whole parse."""
    raw = {
        "events": [
            None,  # will raise AttributeError
            {
                "id": "EVT4",
                "teamHome": {"name": "Gonzaga"},
                "teamAway": {"name": "Arizona"},
                "startDate": "2025-03-15T20:00:00Z",
                "markets": [],
            },
        ]
    }
    games = parse_games(raw)
    # Should recover and still return the valid event
    assert any(g["home_team"] == "Gonzaga" for g in games)


# ── Runner ─────────────────────────────────────────────────────────────────────

ALL_TESTS = [
    # _format_american_odds
    ("odds: positive",              test_odds_positive),
    ("odds: negative",              test_odds_negative),
    ("odds: zero",                  test_odds_zero),
    ("odds: None input",            test_odds_none),
    ("odds: float input",           test_odds_float_input),
    ("odds: string input",          test_odds_string_input),
    # _parse_outcome
    ("outcome: basic",              test_parse_outcome_basic),
    ("outcome: displayOdds fallback", test_parse_outcome_display_odds_fallback),
    ("outcome: no odds",            test_parse_outcome_no_odds),
    # _parse_market
    ("market: Point Spread",        test_market_spread),
    ("market: Moneyline",           test_market_moneyline),
    ("market: Total Points",        test_market_total_points),
    ("market: Over/Under",          test_market_over_under),
    ("market: empty outcomes",      test_market_empty_outcomes),
    ("market: unknown type",        test_market_unknown),
    # _parse_event
    ("event: team names",           test_parse_event_team_names),
    ("event: flat name fallback",   test_parse_event_flat_name_fallback),
    ("event: spread parsed",        test_parse_event_spread_parsed),
    ("event: all markets",          test_parse_event_all_markets),
    ("event: id + start_time",      test_parse_event_id_and_start_time),
    # parse_games
    ("games: basic",                test_parse_games_basic),
    ("games: all fields present",   test_parse_games_fields_present),
    ("games: empty response",       test_parse_games_empty_response),
    ("games: eventGroups fallback", test_parse_games_event_group_fallback),
    ("games: skips bad events",     test_parse_games_skips_bad_events),
]

if __name__ == "__main__":
    print(f"\nRunning {len(ALL_TESTS)} tests...\n")
    passed = failed = 0
    for name, fn in ALL_TESTS:
        try:
            fn()
            print(f"  PASS  {name}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {name}")
            print(f"        {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {name}: {type(e).__name__}: {e}")
            failed += 1
    print(f"\n{'='*50}")
    print(f"  {passed} passed, {failed} failed out of {len(ALL_TESTS)} tests")
    print()
