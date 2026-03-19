"""
NCAA Stats scraper for live play-by-play, box score, and shot location data.
Built from stats.ncaa.org live scoreboards.

Rate limiting: minimum 60 seconds between requests to avoid IP blocks.

Improvements over v1:
- Player name matching sorted longest-first to avoid substring false matches
  (e.g. "Brown" won't match when "Brownlee" is in the game)
- valid_ids set built once per game, not rebuilt per shot
- Half numbers pre-parsed once, not re-parsed in _count_fouls and _extract_h1_scores
- Pre-compiled regex for half number extraction
- Retry logic with exponential backoff on transient failures
"""
import time
import re
import logging
from bs4 import BeautifulSoup
from datetime import datetime, date

logger = logging.getLogger(__name__)


class _BrowserResponse:
    """Minimal response-like wrapper so BeautifulSoup code still works with .content"""
    def __init__(self, html):
        self.content = html.encode('utf-8') if isinstance(html, str) else html
        self.text = html if isinstance(html, str) else html.decode('utf-8')

# Pre-compile regex used on every play
_HALF_RE = re.compile(r'\d+')

# Play classification mappings (from notebook)
# Stored as tuple of (column_name, search_phrase) for direct iteration
# Play classification phrases — each entry is (column, phrase).
# Multiple entries for the same column use OR logic (see _classify_plays).
# Live format uses natural language; postgame uses compact/camelCase.
PLAY_CLASSIFICATIONS = (
    ('is_made_shot',     'made'),
    ('is_missed_shot',   'missed'),
    ('is_three',         '3pt'),
    ('is_two',           '2pt'),
    # Free throw — live: 'Free throw', postgame: 'freethrow'
    ('is_ft',            'Free throw'),
    ('is_ft',            'freethrow'),
    # Jump shot — live: 'jump shot', postgame: 'jumpshot'
    ('is_jump_shot',     'jump shot'),
    ('is_jump_shot',     'jumpshot'),
    ('is_dunk',          'dunk'),
    ('is_layup',         'layup'),
    # Driving layup — live: 'driving layup', postgame: 'drivinglayup'
    ('is_driving_layup', 'driving layup'),
    ('is_driving_layup', 'drivinglayup'),
    ('is_turnaround',    'turnaround'),
    ('is_step_back',     'step back'),
    ('is_pullup',        'pull up'),
    ('is_floater',       'floating'),
    # Hook shot — live: 'hook shot', postgame: 'hook' only
    ('is_hook_shot',     'hook shot'),
    ('is_hook_shot',     'hook'),
    # Under basket — live: 'under the basket', postgame: 'underthebask' or 'pointsinthepaint'
    ('is_under_basket',  'under the basket'),
    ('is_under_basket',  'underthebask'),
    ('is_paint',         'paint'),
    ('is_paint',         'pointsinthepaint'),
    ('location_il',      'inside left'),
    ('location_ir',      'inside right'),
    ('location_ol',      'outside left'),
    ('location_or',      'outside right'),
    ('location_oc',      'outside center'),
    # Context — live uses spaces, postgame uses camelCase/compact
    ('is_second_chance', '2nd chance'),
    ('is_second_chance', '2ndchance'),
    ('is_fast_break',    'fast break'),
    ('is_fast_break',    'fastbreak'),
    ('is_off_turnover',  'off turnover'),
    ('is_off_turnover',  'fromturnover'),
    ('is_assist',        'assist'),
    ('is_def_rebound',   'rebound defensive'),
    ('is_off_rebound',   'rebound offensive'),
    ('is_block',         'block '),
    ('is_steal',         'steal'),
    ('is_turnover',      'Turnover'),
    ('is_foul_personal', 'Foul personal'),
    ('is_foul_shooting', 'shooting'),
    ('is_foul_drawn',    'foul on'),
)


class NCAAStatsScraper:
    """Scraper for stats.ncaa.org with rate limiting and caching."""

    BASE_URL = 'https://stats.ncaa.org'
    SPORT_CODE = 'MBB'       # Men's Basketball
    DIVISION = '1'            # Division I
    # Academic year = the year the season ends in (2025-26 season = 2026)
    ACADEMIC_YEAR = '2026'

    def __init__(self, min_request_interval=60):
        self.min_request_interval = min_request_interval
        self._last_request_time = 0

    def _rate_limit(self):
        """Enforce minimum time between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.min_request_interval:
            wait = self.min_request_interval - elapsed
            logger.info(f"Rate limiting: waiting {wait:.1f}s")
            time.sleep(wait)
        self._last_request_time = time.time()

    def _get(self, url, retries=2, rate_limit=True):
        """Fetch a page using the shared undetected Chrome browser."""
        from scrapers.browser import fetch_page
        if rate_limit:
            self._rate_limit()
        for attempt in range(retries + 1):
            html = fetch_page(url, wait_seconds=3)
            if html and 'Forbidden' not in html[:500] and len(html) > 500:
                # Wrap in a response-like object so BeautifulSoup can use .content
                return _BrowserResponse(html)
            if attempt < retries:
                logger.warning(f"NCAA page fetch failed/empty, retry {attempt+1}")
                time.sleep(5 * (attempt + 1))
            else:
                logger.error(f"NCAA request failed after {retries+1} attempts: {url}")
        return None

    def get_daily_games(self, game_date=None):
        """
        Scrape the daily schedule from stats.ncaa.org.
        Returns list of dicts: [{game_id, status_text, is_live, is_final}, ...]
        """
        if game_date is None:
            game_date = date.today()

        month = f"{game_date.month:02d}"
        day = f"{game_date.day:02d}"
        year = str(game_date.year)

        url = (
            f"{self.BASE_URL}/contests/livestream_scoreboards"
            f"?utf8=%E2%9C%93"
            f"&sport_code={self.SPORT_CODE}"
            f"&academic_year={self.ACADEMIC_YEAR}"
            f"&division={self.DIVISION}"
            f"&game_date={month}%2F{day}%2F{year}"
            f"&commit=Submit"
        )

        response = self._get(url)
        if not response:
            return []

        soup = BeautifulSoup(response.content, "lxml")
        raw_html = response.text  # Need raw HTML to find commented-out status divs

        # The page uses <tr id="contest_XXXXXX"> for each team row.
        # Each game has TWO <tr> rows with the same contest ID (one per team).
        #
        # LIVE games: have <span id="period_XXXXXX"> and <span id="clock_XXXXXX">
        # FINAL games: status is inside an HTML comment containing
        #   <div class="livestream_status_XXXXXX ... livestream_game_over">Final</div>
        #
        # Team names are in <a class="skipMask"> tags inside contest rows.
        # Scores are in <div id="score_XXXXXXX"> (competitor IDs, not game IDs).

        # Pre-scan: find all game IDs that are final (status in HTML comments)
        final_game_ids = set()
        for match in re.finditer(r'livestream_status_(\d+)\s+livestream_status\s+livestream_game_over', raw_html):
            final_game_ids.add(match.group(1))

        # Find all contest rows
        contest_rows = soup.find_all('tr', id=lambda x: x and x.startswith('contest_'))

        seen = {}
        for row in contest_rows:
            try:
                game_id = row['id'].replace('contest_', '')
                if game_id in seen:
                    # Second row for same game — extract team B info
                    team_link = row.select_one('a.skipMask')
                    score_div = row.select_one('div[id^="score_"]')
                    if team_link:
                        seen[game_id]['team_b_name'] = team_link.get_text(strip=True)
                    if score_div:
                        seen[game_id]['team_b_score'] = score_div.get_text(strip=True)
                        seen[game_id]['team_b_competitor_id'] = score_div['id'].replace('score_', '')
                    continue

                # First row for this game — extract team A info and game status
                team_link = row.select_one('a.skipMask')
                score_div = row.select_one('div[id^="score_"]')

                # Game status: check live spans first, then HTML comments for final
                period_span = soup.find('span', id=f'period_{game_id}')
                clock_span = soup.find('span', id=f'clock_{game_id}')

                period_text = period_span.get_text(strip=True) if period_span else ''
                clock_text = clock_span.get_text(strip=True) if clock_span else ''

                # Determine game state
                is_final = game_id in final_game_ids
                if not is_final and period_text:
                    is_final = 'final' in period_text.lower()
                is_live = bool(period_text) and not is_final

                # Extract last play text (only present for live games)
                play_div = soup.find('div', id=f'play_{game_id}')
                last_play = play_div.get_text(strip=True) if play_div else ''

                # Check for timeout
                is_timeout = 'timeout' in last_play.lower() if last_play else False

                # Extract half number from period text
                half = 0
                if '1st' in period_text:
                    half = 1
                elif '2nd' in period_text:
                    half = 2
                elif 'ot' in period_text.lower():
                    half = 3

                # Extract linescore data (half-by-half scores)
                # Linescore rows may or may not have IDs — just use row order
                linescore_table = soup.find('table', id=f'linescore_{game_id}_table')
                h1_scores_list = []  # [team_a_h1, team_b_h1]
                if linescore_table:
                    ls_rows = linescore_table.find_all('tr')
                    for ls_row in ls_rows:
                        cells = ls_row.find_all('td')
                        if cells and cells[0].get_text(strip=True):
                            h1_scores_list.append(cells[0].get_text(strip=True))

                entry = {
                    'game_id': game_id,
                    'status_text': f"{period_text} {clock_text}".strip() or ('Final' if is_final else ''),
                    'period': period_text,
                    'clock': clock_text,
                    'is_live': is_live,
                    'is_final': is_final,
                    'is_timeout': is_timeout,
                    'half': half,
                    'last_play': last_play,
                    'team_a_name': team_link.get_text(strip=True) if team_link else '',
                    'team_a_score': score_div.get_text(strip=True) if score_div else '0',
                    'team_a_competitor_id': score_div['id'].replace('score_', '') if score_div else '',
                    'team_b_name': '',
                    'team_b_score': '0',
                    'team_b_competitor_id': '',
                    'team_a_h1_score': h1_scores_list[0] if len(h1_scores_list) > 0 else None,
                    'team_b_h1_score': h1_scores_list[1] if len(h1_scores_list) > 1 else None,
                }
                seen[game_id] = entry

            except (AttributeError, IndexError, KeyError) as e:
                logger.debug(f"Error parsing contest row: {e}")
                continue

        games = list(seen.values())
        logger.info(f"Parsed {len(games)} games from scoreboard")
        return games

    def scrape_game_data(self, game_id, last_play_index=0, last_shot_count=0, is_final=False):
        """
        Scrape a single game's box score, play-by-play, and shot data.
        Uses cache markers to only return NEW data since last scrape.

        Live games: single page at /contests/livestream_scoreboards/{id}/box_score
          (contains box score, play-by-play, and shot data all on one page)
        Final games: two separate pages:
          - /contests/{id}/box_score (box score + shot data)
          - /contests/{id}/play_by_play (play-by-play)

        Returns:
            dict with keys: players, plays, shots, game_state, new_play_index, new_shot_count
            or None on failure
        """
        result = {
            'players': [],
            'plays': [],
            'shots': [],
            'game_state': {},
            'new_play_index': last_play_index,
            'new_shot_count': last_shot_count,
        }

        if is_final:
            # Final games: separate box score and play-by-play pages
            box_url = f'{self.BASE_URL}/contests/{game_id}/box_score'
            pbp_url = f'{self.BASE_URL}/contests/{game_id}/play_by_play'

            box_response = self._get(box_url, rate_limit=False)
            pbp_response = self._get(pbp_url, rate_limit=False)

            if not box_response and not pbp_response:
                return None

            box_soup = BeautifulSoup(box_response.content, "lxml") if box_response else None
            pbp_soup = BeautifulSoup(pbp_response.content, "lxml") if pbp_response else None

            # Players come from the box score page
            if box_soup:
                players, team_names = self._extract_players(box_soup)
                result['players'] = players
            else:
                players, team_names = [], []

            # Play-by-play comes from the PBP page
            if pbp_soup:
                all_plays_raw = self._extract_raw_plays(pbp_soup)
            else:
                all_plays_raw = []

            # Shots come from the box score page
            soup_for_shots = box_soup
        else:
            # Live games: everything on one page
            url = f'{self.BASE_URL}/contests/livestream_scoreboards/{game_id}/box_score'
            response = self._get(url, rate_limit=False)
            if not response:
                return None

            soup = BeautifulSoup(response.content, "lxml")
            players, team_names = self._extract_players(soup)
            result['players'] = players
            all_plays_raw = self._extract_raw_plays(soup)
            soup_for_shots = soup

        # Set team names/IDs in game state
        if len(team_names) >= 2:
            result['game_state']['team_a_name'] = team_names[0]
            result['game_state']['team_b_name'] = team_names[1]
            for tn_idx, key in enumerate(['team_a_id', 'team_b_id']):
                team_players = [p for p in players if p['team_name'] == team_names[tn_idx]]
                if team_players:
                    result['game_state'][key] = team_players[0]['team_id']

        # Build player lookup sorted by name length descending.
        # Strip suffixes (Jr., Sr., II, III) so 'Jamie Kaiser, Jr.'
        # matches 'Jamie Kaiser' in play text.
        def _strip_suffix(name):
            return re.sub(r',?\s+(Jr\.|Sr\.|II|III|IV|V)\.?$', '', name).strip()

        sorted_players = sorted(players, key=lambda p: len(p['player_name']), reverse=True)
        player_lookup = [
            (_strip_suffix(p['player_name']), p['player_id'], p['team_id'])
            for p in sorted_players
        ]

        # Build valid player IDs set ONCE (used for shot validation)
        valid_player_ids = frozenset(p['player_id'] for p in players)

        # Pre-parse half numbers once for all plays
        play_halves = []
        for raw in all_plays_raw:
            m = _HALF_RE.search(raw['half_text'])
            play_halves.append(int(m.group()) if m else 1)

        # The most recent play has the current score and clock.
        # Live pages list plays newest-first, so index [0] is most recent.
        # Final/PBP pages list plays oldest-first, so index [-1] is most recent.
        # We detect order by comparing clocks: if first play has a lower clock
        # than last play, the list is newest-first (reverse chronological).
        if len(all_plays_raw) >= 2:
            first_clock = all_plays_raw[0]['time']
            last_clock = all_plays_raw[-1]['time']
            if first_clock < last_clock:
                # Newest-first (live page) — reverse to chronological order
                all_plays_raw.reverse()
                play_halves.reverse()

        # Parse game clock and half from most recent play (now always last)
        if all_plays_raw:
            latest = all_plays_raw[-1]
            result['game_state']['game_clock'] = latest['time']
            result['game_state']['half'] = play_halves[-1]

            # Parse current score
            self._parse_score_text(latest['score'], result['game_state'],
                                   'team_a_score', 'team_b_score')

        # Only classify NEW plays (after last_play_index)
        classified_plays = self._classify_plays(
            all_plays_raw[last_play_index:],
            play_halves[last_play_index:],
            player_lookup
        )
        result['plays'] = classified_plays
        result['new_play_index'] = len(all_plays_raw)

        # --- Detect timeout state ---
        if all_plays_raw:
            result['game_state']['is_timeout'] = (
                1 if 'timeout' in all_plays_raw[-1]['play_text'].lower() else 0
            )

        # --- Count fouls by half (uses pre-parsed halves) ---
        self._count_fouls(all_plays_raw, play_halves, result, player_lookup)

        # --- Extract H1 scores if in 2nd half (uses pre-parsed halves) ---
        self._extract_h1_scores(all_plays_raw, play_halves, result)

        # --- Extract Shot Location Data ---
        if soup_for_shots:
            all_shots = self._extract_shots(soup_for_shots, valid_player_ids)
            # Pass ALL shots every scrape — INSERT OR IGNORE on (game_id, play_id)
            # handles deduplication in the DB. Slicing by last_shot_count was
            # unreliable because addShot() order in the HTML is not chronological.
            result['shots'] = all_shots
            result['new_shot_count'] = len(all_shots)

        return result

    # ---- Private helpers ----

    def _extract_players(self, soup):
        """
        Extract players and team names from the player select dropdown.
        
        Live games: player IDs like "10_559" (jersey_teamid)
        Final games: player IDs like "789768126" (full numeric, no team suffix)
        
        For final games, team_id is extracted from the team name -> team_id
        mapping built from the addShot data or optgroup structure.
        """
        players_select = soup.find_all("select", id="player_select")
        team_names = []
        players = []

        # First pass: build team_name -> team_id map from addShot calls if available
        team_name_to_id = {}
        for script in soup.find_all('script'):
            script_text = script.string or ''
            for line in script_text.split('\n'):
                if 'addShot(' in line and 'function' not in line:
                    m = re.search(r'addShot\([^,]+,[^,]+,\s*(\d+),.+?\(([^)]+)\)', line)
                    if m:
                        team_name_to_id[m.group(2)] = m.group(1)
                if len(team_name_to_id) >= 2:
                    break
            if len(team_name_to_id) >= 2:
                break

        for select in players_select:
            for group in select.find_all("optgroup"):
                team_name = group['label']
                if team_name not in team_names:
                    team_names.append(team_name)

                # Determine team_id for this group
                group_team_id = team_name_to_id.get(team_name, '')

                for opt in group.find_all('option'):
                    player_id = opt['value']
                    if not player_id:
                        continue

                    if '_' in player_id:
                        # Live format: "10_559" -> team_id = "559"
                        team_id = player_id.split('_')[1]
                    else:
                        # Final format: use team_id from addShot mapping
                        team_id = group_team_id

                    players.append({
                        'player_id': player_id,
                        'team_name': team_name,
                        'team_id': team_id,
                        'player_name': opt.text.strip(),
                    })

        return players, team_names

    def _extract_raw_plays(self, soup):
        """
        Extract raw play-by-play rows.
        
        Live games: single table id="contest_plays_data_table"
          Columns: Half, Clock, Play, Score
          Order: newest-first (reverse chronological)
          
        Final games: separate table per half, no special ID
          Columns: Time, Team A play, Score, Team B play
          Order: oldest-first (chronological)
          Half is determined by which table the row belongs to.
        """
        # Try live format first
        plays_table = soup.find(id="contest_plays_data_table")
        if plays_table:
            tds = plays_table.find_all("td")
            plays = []
            for i in range(0, len(tds) - 3, 4):
                plays.append({
                    'half_text': tds[i].text.strip(),
                    'time': tds[i + 1].text.strip(),
                    'play_text': tds[i + 2].text.strip(),
                    'score': tds[i + 3].text.strip(),
                })
            return plays

        # Final format: look for "1st Half", "2nd Half" headers with tables
        plays = []
        half_sections = soup.find_all('div', class_='card-header')

        current_half = 0
        for header in half_sections:
            header_text = header.get_text(strip=True).lower()
            if '1st' in header_text and 'half' in header_text:
                current_half = 1
            elif '2nd' in header_text and 'half' in header_text:
                current_half = 2
            elif 'ot' in header_text or 'overtime' in header_text:
                current_half = 3
            else:
                continue

            # Find the table that follows this header
            card = header.find_parent('div', class_='card')
            if not card:
                continue
            table = card.find('table')
            if not table:
                continue

            tbody = table.find('tbody')
            if not tbody:
                continue

            for row in tbody.find_all('tr'):
                cells = row.find_all('td')
                if not cells:
                    continue

                time_text = cells[0].get_text(strip=True) if cells else ''

                if len(cells) == 4:
                    # Standard row: Time, Team A play, Score, Team B play
                    team_a_play = cells[1].get_text(strip=True)
                    score = cells[2].get_text(strip=True)
                    team_b_play = cells[3].get_text(strip=True)

                    # Determine which team has the play
                    play_text = team_a_play if team_a_play else team_b_play
                elif len(cells) == 2:
                    # Event row: Time, colspan event (game start, period start, etc.)
                    play_text = cells[1].get_text(strip=True)
                    score = ''
                else:
                    continue

                if not play_text:
                    continue

                # Clean up bold tags that wrap player names
                play_text = play_text.replace('\xa0', ' ').strip()

                plays.append({
                    'half_text': str(current_half),
                    'time': time_text,
                    'play_text': play_text,
                    'score': score,
                })

        return plays

    def _classify_plays(self, raw_plays, halves, player_lookup):
        """
        Expand compound plays and classify each sub-play.

        player_lookup is pre-sorted longest-name-first so that
        "Brownlee" matches before "Brown".
        """
        classified = []

        for raw_play, half in zip(raw_plays, halves):
            # Split compound plays and reverse for chronological order
            sub_plays = raw_play['play_text'].split(', ')
            sub_plays.reverse()

            for sub_play in sub_plays:
                play_data = {
                    'half': half,
                    'time': raw_play['time'],
                    'play_text': sub_play,
                    'score': raw_play['score'],
                }

                # Classify play type — multiple phrases per column use OR logic
                # Initialize all columns to 0 first, then set 1 on any match
                seen_cols = set()
                for col, phrase in PLAY_CLASSIFICATIONS:
                    if col not in seen_cols:
                        play_data[col] = 0
                        seen_cols.add(col)
                for col, phrase in PLAY_CLASSIFICATIONS:
                    if phrase in sub_play:
                        play_data[col] = 1

                # Detect timeout
                play_data['is_timeout'] = 1 if 'timeout' in sub_play.lower() else 0

                # Match player — longest name first prevents substring false matches
                for pname, pid, tid in player_lookup:
                    if pname in sub_play:
                        play_data['player_id'] = pid
                        play_data['team_id'] = tid
                        break

                # Calculate elapsed seconds from game start
                play_data['elapsed_seconds'] = self._time_to_elapsed(raw_play['time'], half)

                # Calculate points value for made shots
                if play_data['is_made_shot']:
                    if play_data['is_three']:
                        play_data['points_value'] = 3
                    elif play_data['is_ft']:
                        play_data['points_value'] = 1
                    else:
                        play_data['points_value'] = 2

                classified.append(play_data)

        return classified

    def _extract_shots(self, soup, valid_player_ids):
        """
        Extract shot location data from addShot() JavaScript calls.
        
        Both live and final pages embed shots as addShot() calls.
        Live: addShot(x, y, team_id, made, play_id, 'description', 'classes', highlight)
        Final: same format but simplified descriptions (no shot type).
        """
        # Find the script block(s) containing addShot calls
        all_text = soup.get_text()
        shot_pattern = re.compile(
            r'addShot\(\s*'
            r'([\d.]+)\s*,\s*'           # x
            r'([\d.]+)\s*,\s*'           # y
            r'(\d+)\s*,\s*'              # team_id
            r'(true|false)\s*,\s*'       # made
            r'(\d+)\s*,\s*'              # play_id
            r"'([^']*)'\s*,\s*"          # description
            r"'([^']*)'\s*,\s*"          # css classes
            r'(true|false)\s*\)'         # highlight
        )

        shots = []
        for script in soup.find_all('script'):
            script_text = script.string or ''
            for match in shot_pattern.finditer(script_text):
                shot = self._parse_shot_match(match, valid_player_ids)
                if shot:
                    shots.append(shot)

        return shots

    def _parse_shot_match(self, match, valid_player_ids):
        """Parse a regex match from addShot() into a shot dict."""
        try:
            x = float(match.group(1))
            y = float(match.group(2))
            team_id = match.group(3)
            made = match.group(4) == 'true'
            play_id = match.group(5)        # addShot 5th arg — links shot to play
            description = match.group(6)
            classes = match.group(7)

            # Extract player_id from CSS classes: "period_1 player_10_559 team_559"
            # or "period_1 player_789768126 team_810"
            player_match = re.search(r'player_(\S+)', classes)
            if not player_match:
                return None
            player_id = player_match.group(1)

            # Validate player is in game (skip if we can't validate)
            if valid_player_ids and player_id not in valid_player_ids:
                return None

            # Extract half and time from description
            # Live format:     "2nd 19:25 : ..."
            # Postgame format: "1st 19:48:00 : ..."
            time_match = re.search(r'(\d+)\w*\s+(\d+:\d+[:\d.]*)', description)
            half = int(time_match.group(1)) if time_match else 1
            raw_time = time_match.group(2) if time_match else ''
            # Normalize to MM:SS — strip sub-seconds and postgame :00 suffix
            shot_time = raw_time.split('.')[0]
            parts = shot_time.split(':')
            if len(parts) == 3:
                shot_time = f"{parts[0]}:{parts[1]}"  # '19:48:00' → '19:48'

            # Determine shot type from description or coordinates
            shot_type = self._classify_shot_from_description(description)
            if not shot_type:
                shot_type = self._classify_shot_location(x, y)

            # is_three comes directly from the description ('3pt' present)
            # Used as join key to match shots to plays — more reliable than
            # shot_type which may differ from PBP classification.
            is_three = 1 if ('3pt' in description.lower() or '3-point' in description.lower()) else 0

            # Strip "Nth HH:MM : " prefix from description to get bare play text
            desc_body_match = re.search(r'\d+\w*\s+[\d:.]+\s*:\s*(.+)', description)
            desc_body = desc_body_match.group(1) if desc_body_match else description
            play_text_norm = self._normalize_play_text(desc_body)

            return {
                'play_id': play_id,
                'x': x,
                'y': y,
                'team_id': team_id,
                'result': 1 if made else 0,
                'player_id': player_id,
                'half': half,
                'time': shot_time,
                'shot_type': shot_type,
                'is_three': is_three,
                'play_text_norm': play_text_norm,
            }
        except (ValueError, IndexError) as e:
            logger.debug(f"Shot parse error: {e}")
            return None

    @staticmethod
    def _normalize_play_text(text):
        """Normalize play text for shot-to-play matching.
        Strips trailing score (e.g. '28-26'), decodes HTML entities,
        and collapses whitespace so shot chart and PBP texts match.
        """
        import html as _html
        text = _html.unescape(text)
        text = re.sub(r'\s+\d+-\d+\s*$', '', text)  # strip trailing score
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _classify_shot_from_description(self, description):
        """
        Extract shot type from the shot chart description text.

        Uses the most specific classification possible — in priority order:
          dunk > hook > layup > driving_layup > rim > mid > three

        Live format:     '2pt dunk (under the basket) made by Player(Team)'
        Postgame format: 'Player,2pt dunk pointsinthepaint;  made'
        Returns one of: 'dunk', 'hook', 'layup', 'rim', 'mid', 'three', or None.
        None means fall through to coordinate-based classification.
        """
        desc_lower = description.lower()

        if '3pt' in desc_lower or '3-point' in desc_lower:
            return 'three'

        if '2pt' in desc_lower or '2-point' in desc_lower:
            # Check most specific types first
            if 'dunk' in desc_lower:
                return 'dunk'
            if any(kw in desc_lower for kw in ['hook shot', 'hook']):
                return 'hook'
            if any(kw in desc_lower for kw in ['driving layup', 'drivinglayup']):
                return 'layup'
            if any(kw in desc_lower for kw in ['tipinlayup', 'tip-in', 'tipin']):
                return 'rim'   # tip-in: rim contact, not a true layup
            if 'layup' in desc_lower:
                return 'layup'
            if any(kw in desc_lower for kw in ['under the basket', 'underthebask',
                                                'pointsinthepaint']):
                return 'rim'
            if any(kw in desc_lower for kw in ['floating', 'floater', 'runner']):
                return 'mid'
            return 'mid'   # generic 2pt jump shot

        # Final/postgame descriptions have no 2pt/3pt prefix —
        # fall through to coordinate-based classification
        return None

    def _classify_shot_location(self, x, y):
        """
        Classify shot type from coordinates using proper court geometry.

        Coordinates are percentages (0-100) of the SVG court (940x500px).
        Left basket:  (5.32%, 50%)
        Right basket: (94.68%, 50%)
        Scale: ~10.04 px/foot → 1% width ≈ 0.94ft

        Thresholds (feet from nearest basket):
          rim      ≤ 4ft
          midrange  4–22ft
          three    >22ft (beyond the arc)
        """
        import math
        # Court constants (must match expected_points.py)
        COURT_W_PCT = 100.0
        COURT_H_PCT = 100.0
        # Basket positions as percentage of court dimensions
        LEFT_BX  = 50  / 940 * 100   # 5.32%
        LEFT_BY  = 250 / 500 * 100   # 50.0%
        RIGHT_BX = 890 / 940 * 100   # 94.68%
        RIGHT_BY = 250 / 500 * 100   # 50.0%
        PX_PER_FOOT = 220.9 / 22.0   # 10.04

        # Choose nearest basket
        if x <= 50:
            bx, by = LEFT_BX, LEFT_BY
        else:
            bx, by = RIGHT_BX, RIGHT_BY

        # Convert % difference to pixels then to feet
        dx_px = (x - bx) / 100.0 * 940
        dy_px = (y - by) / 100.0 * 500
        dist_ft = math.sqrt(dx_px * dx_px + dy_px * dy_px) / PX_PER_FOOT

        if dist_ft <= 4.0:
            return 'rim'
        elif dist_ft <= 22.0:
            return 'midrange'
        else:
            return 'three'

    def _time_to_elapsed(self, time_str, half):
        """Convert game clock to elapsed seconds from game start."""
        try:
            parts = time_str.split(':')
            if len(parts) != 2:
                return 0

            remaining = int(parts[0]) * 60 + int(parts[1])

            if half == 1:
                return 1200 - remaining
            elif half == 2:
                return 2400 - remaining
            else:
                # Overtime periods are 5 minutes
                return 2400 + (half - 3) * 300 + (300 - remaining)
        except (ValueError, IndexError):
            return 0

    def _parse_score_text(self, score_text, target_dict, key_a, key_b):
        """Parse a 'XX-YY' score string into the target dict."""
        if not score_text:
            return
        parts = score_text.split('-')
        if len(parts) == 2:
            try:
                target_dict[key_a] = int(parts[0].strip())
                target_dict[key_b] = int(parts[1].strip())
            except ValueError:
                pass

    def _count_fouls(self, all_plays_raw, play_halves, result, player_lookup):
        """
        Count fouls for each team by half.
        Uses pre-parsed play_halves list instead of re-parsing half text.
        """
        team_a_id = result['game_state'].get('team_a_id', '')
        team_b_id = result['game_state'].get('team_b_id', '')
        fouls = {'a_h1': 0, 'b_h1': 0, 'a_h2': 0, 'b_h2': 0}

        for raw_play, half in zip(all_plays_raw, play_halves):
            play_text = raw_play['play_text']

            if 'Foul personal' not in play_text and 'shooting' not in play_text:
                continue

            # Find which team committed the foul (longest name first)
            for pname, pid, tid in player_lookup:
                if pname in play_text:
                    # Make sure this player committed the foul, not had it drawn on them
                    idx = play_text.find(pname)
                    prefix = play_text[max(0, idx - 10):idx]
                    if 'foul on' in prefix:
                        continue  # This player had the foul drawn, not committed

                    half_key = 'h1' if half == 1 else 'h2'
                    if tid == team_a_id:
                        fouls[f'a_{half_key}'] += 1
                    elif tid == team_b_id:
                        fouls[f'b_{half_key}'] += 1
                    break

        result['game_state']['team_a_fouls_h1'] = fouls['a_h1']
        result['game_state']['team_b_fouls_h1'] = fouls['b_h1']
        result['game_state']['team_a_fouls_h2'] = fouls['a_h2']
        result['game_state']['team_b_fouls_h2'] = fouls['b_h2']

    def _extract_h1_scores(self, all_plays_raw, play_halves, result):
        """
        Extract end-of-first-half scores.
        Uses pre-parsed play_halves instead of re-parsing half text.
        """
        for i, half in enumerate(play_halves):
            if half == 2 and i > 0:
                self._parse_score_text(
                    all_plays_raw[i - 1]['score'],
                    result['game_state'],
                    'team_a_h1_score', 'team_b_h1_score'
                )
                break
