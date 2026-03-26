"""
Player Stats Loader
===================
Loads Evan Miya player projections CSV and builds a lookup keyed by
(normalized_name, normalized_team) -> stats dict.

At game time, the lookup is resolved against the players table using
player_name + team_name since the Evan Miya player_id differs from
the NCAA player_id used in the plays/shots tables.

Usage in game_manager.py:
    from models.player_stats import PlayerStatsLookup
    _pstats = PlayerStatsLookup()                     # load once at startup
    ...
    players_stats = _pstats.build_game_stats(players) # per game
    expected = calculate_game_expected_score(plays, shots, players_stats)

players_stats is a dict: {ncaa_player_id: {'fg3_pct': float, 'ft_pct': float}}
"""
import os
import re
import csv
import logging

logger = logging.getLogger(__name__)

_DEFAULT_CSV = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    'Evan_Miya_Player_Projections.csv'
)

_SUFFIXES = re.compile(r',?\s+(jr\.?|sr\.?|ii|iii|iv|v)\.?$', re.IGNORECASE)


def _norm_name(name):
    name = (name or '').strip().lower()
    name = _SUFFIXES.sub('', name)
    return re.sub(r'\s+', ' ', name).strip()


def _norm_team(team):
    return (team or '').strip().lower()


class PlayerStatsLookup:
    def __init__(self, csv_path=None):
        self._path = csv_path or _DEFAULT_CSV
        self._by_name_team = {}
        self._by_name = {}
        self._loaded = False
        self._load()

    def _load(self):
        if not os.path.exists(self._path):
            logger.warning(f"Player stats CSV not found at {self._path} — shooter adjustments disabled")
            return

        count = 0
        try:
            with open(self._path, newline='', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    name = (row.get('player') or '').strip()
                    team = (row.get('team') or '').strip()
                    if not name:
                        continue
                    try:
                        three_pct = float(row['value_three_pct']) / 100.0
                        ft_pct    = float(row['value_ft_pct'])    / 100.0
                    except (ValueError, KeyError):
                        continue

                    three_pct = max(0.10, min(0.60, three_pct))
                    ft_pct    = max(0.40, min(1.00, ft_pct))

                    stats = {'fg3_pct': round(three_pct, 4), 'ft_pct': round(ft_pct, 4)}
                    key = (_norm_name(name), _norm_team(team))
                    self._by_name_team[key] = stats
                    self._by_name[_norm_name(name)] = stats
                    count += 1

            self._loaded = True
            logger.info(f"Loaded player stats for {count} players from {os.path.basename(self._path)}")
        except Exception as e:
            logger.error(f"Failed to load player stats CSV: {e}")

    def lookup(self, player_name, team_name):
        nn = _norm_name(player_name)
        nt = _norm_team(team_name)
        return self._by_name_team.get((nn, nt)) or self._by_name.get(nn)

    def build_game_stats(self, players):
        if not self._loaded:
            return {}
        result = {}
        for p in players:
            pid   = str(p.get('player_id') or '')
            pname = p.get('player_name') or ''
            tname = p.get('team_name') or ''
            if not pid or not pname:
                continue
            stats = self.lookup(pname, tname)
            if stats:
                result[pid] = stats
        matched = len(result)
        total = len([p for p in players if p.get('player_id')])
        if total:
            logger.debug(f"Player stats matched {matched}/{total} players ({matched/total*100:.0f}%)")
        return result
