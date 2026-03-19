/**
 * Scoreboard page JavaScript.
 *
 * Fetches game data from /api/scoreboard and renders game cards.
 * Auto-refreshes every 30 seconds (light poll — the heavy scraping
 * happens on the backend's 3-minute cycle).
 */

const POLL_INTERVAL = 30_000; // 30 second UI refresh
let pollTimer = null;

// ---- Fetch & Render ----

async function fetchScoreboard() {
    try {
        const resp = await fetch('/api/scoreboard');
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        renderScoreboard(data.games);
        updateStatus(true, data.timestamp);
    } catch (err) {
        console.error('Scoreboard fetch failed:', err);
        updateStatus(false);
    }
}

function renderScoreboard(games) {
    const container = document.getElementById('scoreboard');
    const loading = document.getElementById('loading');
    if (loading) loading.classList.add('hidden');

    if (!games || games.length === 0) {
        container.innerHTML = '<div class="no-games">No games scheduled today</div>';
        return;
    }

    container.innerHTML = games.map(g => renderGameCard(g)).join('');
}

function renderGameCard(g) {
    const isLive = g.status === 'live' || g.status === 'in_progress';
    const isFinal = g.status === 'final';
    const isTimeout = g.is_timeout;
    const half = g.half || 1;
    const clock = g.game_clock || '';

    // Card classes
    let cardClass = 'game-card';
    if (isTimeout) cardClass += ' game-card--timeout';
    if (isFinal) cardClass += ' game-card--final';

    // Half display
    let halfText = '';
    if (isFinal) halfText = 'FINAL';
    else if (half === 1) halfText = '1ST HALF';
    else if (half === 2) halfText = '2ND HALF';
    else halfText = `OT${half - 2 > 1 ? half - 2 : ''}`;

    // H1 scores (only show in 2nd half or later)
    let h1Html = '';
    if (half >= 2 && (g.team_a_h1_score || g.team_b_h1_score)) {
        const expA = g.team_a_h1_expected ? ` (exp ${g.team_a_h1_expected})` : '';
        const expB = g.team_b_h1_expected ? ` (exp ${g.team_b_h1_expected})` : '';
        h1Html = `<div class="h1-line">H1: ${g.team_a_h1_score || 0}${expA} – ${g.team_b_h1_score || 0}${expB}</div>`;
    }

    // Odds chips
    let oddsHtml = '';
    const chips = [];
    if (g.pregame_spread != null) {
        const sign = g.pregame_spread > 0 ? '+' : '';
        const spreadTeam = g.pregame_spread_team || '';
        chips.push(`<span class="odds-chip">${spreadTeam} ${sign}${g.pregame_spread}</span>`);
    }
    if (g.pregame_total != null) {
        chips.push(`<span class="odds-chip">O/U ${g.pregame_total}</span>`);
    }
    if (g.live_spread != null) {
        const sign = g.live_spread > 0 ? '+' : '';
        const liveTeam = g.live_spread_team || g.pregame_spread_team || '';
        chips.push(`<span class="odds-chip odds-chip--live">Live ${liveTeam} ${sign}${g.live_spread}</span>`);
    }
    if (g.live_total != null) {
        chips.push(`<span class="odds-chip odds-chip--live">Live O/U ${g.live_total}</span>`);
    }
    if (chips.length) {
        oddsHtml = `<div class="odds-strip">${chips.join('')}</div>`;
    }

    // Fouls
    let foulsHtml = '';
    if (isLive && (g.team_a_fouls || g.team_b_fouls)) {
        foulsHtml = `<div class="fouls-line">
            <span>Fouls: ${g.team_a_fouls || 0}</span>
            <span>Fouls: ${g.team_b_fouls || 0}</span>
        </div>`;
    }

    // Alerts
    let alertsHtml = '';
    if (g.insights && g.insights.alerts && g.insights.alerts.length) {
        alertsHtml = '<div class="game-card__alerts">' +
            g.insights.alerts.slice(0, 3).map(a =>
                `<div class="alert-chip alert-chip--${a.priority}">${a.message}</div>`
            ).join('') + '</div>';
    }

    // Expected scores
    const expA = g.team_a_expected ? `Exp: ${g.team_a_expected}` : '';
    const expB = g.team_b_expected ? `Exp: ${g.team_b_expected}` : '';

    return `
    <div class="${cardClass}" onclick="window.location='/game/${g.game_id}'">
        <div class="game-card__header">
            <span class="half-indicator">${halfText}</span>
            ${isTimeout ? '<span class="timeout-indicator">TIMEOUT</span>' : ''}
            ${isLive ? `<span class="game-clock">${clock}</span>` : ''}
        </div>

        <div class="game-card__scores">
            <div class="team-line">
                <span class="score-num">${g.team_a_score || 0}</span>
                <div>
                    <div class="team-name-sm">${g.team_a_name || 'TBD'}</div>
                    <div class="score-expected-sm">${expA}</div>
                </div>
            </div>

            <div class="vs-divider">VS</div>

            <div class="team-line team-line--right">
                <span class="score-num">${g.team_b_score || 0}</span>
                <div>
                    <div class="team-name-sm">${g.team_b_name || 'TBD'}</div>
                    <div class="score-expected-sm">${expB}</div>
                </div>
            </div>
        </div>

        ${h1Html}
        ${oddsHtml}
        ${foulsHtml}
        ${alertsHtml}
    </div>`;
}

// ---- Status indicator ----

function updateStatus(connected, timestamp) {
    const dot = document.getElementById('status-indicator');
    const text = document.getElementById('status-text');
    const refresh = document.getElementById('last-refresh');

    if (connected) {
        dot.className = 'status-dot status-dot--active';
        text.textContent = 'Connected';
        if (timestamp) {
            const d = new Date(timestamp);
            refresh.textContent = d.toLocaleTimeString();
        }
    } else {
        dot.className = 'status-dot status-dot--inactive';
        text.textContent = 'Disconnected';
    }
}

// ---- Manual Refresh ----

async function manualRefresh() {
    const btn = document.getElementById('refresh-btn');
    btn.disabled = true;
    btn.textContent = '↻ Refreshing...';

    try {
        await fetch('/api/refresh', { method: 'POST' });
        await fetchScoreboard();
    } catch (err) {
        console.error('Manual refresh failed:', err);
    } finally {
        btn.disabled = false;
        btn.textContent = '↻ Refresh';
    }
}

// ---- Auto-poll ----

function startPolling() {
    fetchScoreboard();
    pollTimer = setInterval(fetchScoreboard, POLL_INTERVAL);
}

// ---- Init ----
startPolling();
