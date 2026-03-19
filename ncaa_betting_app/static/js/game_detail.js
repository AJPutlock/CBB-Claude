/**
 * Game Detail page JavaScript.
 *
 * Fetches full game data from /api/game/<game_id> and renders:
 * - Score header with expected points
 * - Alerts banner
 * - Total combined points chart (with pregame O/U line)
 * - Score difference chart (with pregame spread line)
 * - Pace stats bar
 * - Box score tables (tabbed by half)
 * - Shot breakdown tables (tabbed by half)
 * - Betting insights panel
 *
 * Auto-refreshes every 30 seconds.
 * GAME_ID is injected by the template.
 */

const POLL_INTERVAL = 30_000;
let pollTimer = null;
let totalChart = null;
let diffChart = null;
let currentBoxTab = 'total';
let currentShotTab = 'total';
let gameData = null;

// ---- Chart.js defaults ----
Chart.defaults.color = '#6b7a8d';
Chart.defaults.borderColor = 'rgba(255,255,255,0.04)';
Chart.defaults.font.family = "'JetBrains Mono', monospace";
Chart.defaults.font.size = 11;

// ---- Fetch & Render ----

async function fetchGameDetail() {
    try {
        const resp = await fetch(`/api/game/${GAME_ID}`);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        gameData = await resp.json();
        renderAll(gameData);
    } catch (err) {
        console.error('Game detail fetch failed:', err);
    }
}

function renderAll(d) {
    renderScoreHeader(d);
    renderAlerts(d.insights);
    renderCharts(d);
    renderPaceBar(d.insights);
    renderBoxScore(d.box_score, d.game);
    renderShotBreakdown(d.shot_breakdown, d.game);
    renderInsightsPanel(d);
}

// ---- Score Header ----

function renderScoreHeader(d) {
    const g = d.game;

    document.getElementById('matchup-title').textContent =
        `${g.team_a_name || 'TBD'} vs ${g.team_b_name || 'TBD'}`;

    document.getElementById('team-a-name').textContent = g.team_a_name || '--';
    document.getElementById('team-b-name').textContent = g.team_b_name || '--';
    document.getElementById('team-a-score').textContent = g.team_a_score || 0;
    document.getElementById('team-b-score').textContent = g.team_b_score || 0;

    // Expected scores with color coding
    const expA = d.expected.team_a || 0;
    const expB = d.expected.team_b || 0;
    const scoreA = g.team_a_score || 0;
    const scoreB = g.team_b_score || 0;

    const elA = document.getElementById('team-a-expected');
    elA.textContent = `Exp: ${expA}`;
    elA.className = 'score-expected' + (scoreA > expA ? ' score-expected--ahead' : scoreA < expA ? ' score-expected--behind' : '');

    const elB = document.getElementById('team-b-expected');
    elB.textContent = `Exp: ${expB}`;
    elB.className = 'score-expected' + (scoreB > expB ? ' score-expected--ahead' : scoreB < expB ? ' score-expected--behind' : '');

    // Half & clock
    const half = g.half || 1;
    let halfText = half === 1 ? '1st Half' : half === 2 ? '2nd Half' : `OT${half - 2 > 1 ? half - 2 : ''}`;
    if (g.status === 'final') halfText = 'FINAL';
    document.getElementById('half-info').textContent = halfText;
    document.getElementById('game-clock').textContent = g.game_clock || '';

    // Timeout badge
    const badge = document.getElementById('timeout-badge');
    if (g.is_timeout) badge.classList.remove('hidden');
    else badge.classList.add('hidden');

    // Odds info
    let oddsLines = [];
    if (d.pregame_odds) {
        const po = d.pregame_odds;
        if (po.spread_team_a != null) {
            const sign = po.spread_team_a > 0 ? '+' : '';
            oddsLines.push(`Pregame: ${g.team_a_name} ${sign}${po.spread_team_a} (${fmtOdds(po.spread_odds_a)})`);
        }
        if (po.total_points != null) {
            oddsLines.push(`O/U ${po.total_points} (O ${fmtOdds(po.over_odds)} / U ${fmtOdds(po.under_odds)})`);
        }
    }
    if (d.live_odds) {
        const lo = d.live_odds;
        if (lo.spread_team_a != null) {
            const sign = lo.spread_team_a > 0 ? '+' : '';
            oddsLines.push(`Live: ${g.team_a_name} ${sign}${lo.spread_team_a}`);
        }
        if (lo.total_points != null) {
            oddsLines.push(`Live O/U ${lo.total_points}`);
        }
    }
    document.getElementById('odds-info').innerHTML = oddsLines.join('<br>');

    // H1 scores
    const h1El = document.getElementById('h1-scores');
    if (half >= 2 && (g.team_a_h1_score || g.team_b_h1_score)) {
        const h1A = g.team_a_h1_score || 0;
        const h1B = g.team_b_h1_score || 0;
        const h1ExpA = d.h1_expected ? d.h1_expected.team_a : 0;
        const h1ExpB = d.h1_expected ? d.h1_expected.team_b : 0;
        h1El.className = 'h1-scores-detail';
        h1El.textContent = `H1: ${h1A} (exp ${h1ExpA}) – ${h1B} (exp ${h1ExpB})`;
    } else {
        h1El.textContent = '';
    }

    // Update diff label
    const diffLabel = document.getElementById('diff-label');
    if (diffLabel) {
        diffLabel.textContent = `${shortName(g.team_a_name)} – ${shortName(g.team_b_name)}`;
    }
}

// ---- Alerts ----

function renderAlerts(insights) {
    const el = document.getElementById('alerts-banner');
    if (!insights || !insights.alerts || !insights.alerts.length) {
        el.innerHTML = '';
        return;
    }
    el.innerHTML = insights.alerts.map(a =>
        `<div class="alert-banner-chip alert-banner-chip--${a.priority}">${a.message}</div>`
    ).join('');
}

// ---- Charts ----

function renderCharts(d) {
    const timeline = d.score_timeline || [];
    if (timeline.length < 2) return;

    const labels = timeline.map(t => t.elapsed_minutes.toFixed(1));
    const totals = timeline.map(t => t.total);
    const diffs = timeline.map(t => t.diff);

    // Pregame lines for reference
    const pregameTotal = d.pregame_odds ? d.pregame_odds.total_points : null;
    const pregameSpread = d.pregame_odds ? d.pregame_odds.spread_team_a : null;

    renderTotalChart(labels, totals, pregameTotal);
    renderDiffChart(labels, diffs, pregameSpread, d.game);
}

function renderTotalChart(labels, totals, pregameTotal) {
    const ctx = document.getElementById('total-points-chart');
    if (!ctx) return;

    const datasets = [{
        label: 'Combined Score',
        data: totals,
        borderColor: '#4da6ff',
        backgroundColor: 'rgba(77, 166, 255, 0.08)',
        borderWidth: 2,
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        pointHitRadius: 8,
    }];

    if (pregameTotal != null) {
        datasets.push({
            label: `Pregame O/U (${pregameTotal})`,
            data: Array(labels.length).fill(pregameTotal),
            borderColor: 'rgba(251, 191, 36, 0.5)',
            borderWidth: 1,
            borderDash: [6, 4],
            pointRadius: 0,
            fill: false,
        });
    }

    if (totalChart) totalChart.destroy();
    totalChart = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: chartOptions('Total Points'),
    });
}

function renderDiffChart(labels, diffs, pregameSpread, game) {
    const ctx = document.getElementById('diff-chart');
    if (!ctx) return;

    const teamA = shortName(game.team_a_name);

    const datasets = [{
        label: `${teamA} Lead`,
        data: diffs,
        borderColor: '#34d399',
        backgroundColor: ctx => {
            const v = ctx.raw;
            return v >= 0 ? 'rgba(52, 211, 153, 0.08)' : 'rgba(248, 113, 113, 0.08)';
        },
        borderWidth: 2,
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        pointHitRadius: 8,
        segment: {
            borderColor: ctx => ctx.p1.parsed.y < 0 ? '#f87171' : '#34d399',
        },
    }];

    // Zero line is built into the grid
    if (pregameSpread != null) {
        // Spread is from team_a perspective: negative = favored
        datasets.push({
            label: `Pregame Spread (${pregameSpread > 0 ? '+' : ''}${pregameSpread})`,
            data: Array(labels.length).fill(-pregameSpread), // Negate: spread -3.5 means A expected +3.5 lead
            borderColor: 'rgba(251, 191, 36, 0.5)',
            borderWidth: 1,
            borderDash: [6, 4],
            pointRadius: 0,
            fill: false,
        });
    }

    if (diffChart) diffChart.destroy();
    diffChart = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: chartOptions('Score Diff'),
    });
}

function chartOptions(title) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
            legend: { display: true, labels: { boxWidth: 10, padding: 12, font: { size: 10 } } },
            tooltip: {
                backgroundColor: '#192030',
                borderColor: '#2a3a50',
                borderWidth: 1,
                titleFont: { size: 11 },
                bodyFont: { size: 11 },
                padding: 10,
                displayColors: true,
                callbacks: {
                    title: items => `${items[0].label} min`,
                },
            },
        },
        scales: {
            x: {
                title: { display: true, text: 'Minutes', font: { size: 10 } },
                grid: { display: false },
                ticks: { maxTicksLimit: 8, font: { size: 10 } },
            },
            y: {
                grid: { color: 'rgba(255,255,255,0.04)' },
                ticks: { font: { size: 10 } },
            },
        },
    };
}

// ---- Pace Bar ----

function renderPaceBar(insights) {
    // Insert pace bar after charts row if it doesn't exist
    let paceEl = document.getElementById('pace-bar');
    if (!paceEl) {
        const chartsRow = document.querySelector('.charts-row');
        if (!chartsRow) return;
        paceEl = document.createElement('section');
        paceEl.id = 'pace-bar';
        paceEl.className = 'pace-bar';
        chartsRow.after(paceEl);
    }

    const pace = insights ? insights.pace : null;
    if (!pace || !pace.pace_per_40) {
        paceEl.innerHTML = '<div class="pace-stat"><span class="pace-stat__label">Pace</span><span class="pace-stat__value">--</span></div>';
        return;
    }

    paceEl.innerHTML = `
        <div class="pace-stat">
            <span class="pace-stat__label">Pace (per 40)</span>
            <span class="pace-stat__value">${pace.pace_per_40}</span>
        </div>
        <div class="pace-stat">
            <span class="pace-stat__label">Possessions</span>
            <span class="pace-stat__value">${pace.possessions}</span>
        </div>
        <div class="pace-stat">
            <span class="pace-stat__label">Projected Total</span>
            <span class="pace-stat__value pace-stat__value--accent">${pace.projected_total}</span>
        </div>
        <div class="pace-stat">
            <span class="pace-stat__label">Elapsed</span>
            <span class="pace-stat__value">${pace.elapsed_minutes} min</span>
        </div>
    `;
}

// ---- Box Score ----

function renderBoxScore(box, game) {
    if (!box) return;
    renderBoxTeam('box-team-a', box.team_a, game.team_a_name, currentBoxTab);
    renderBoxTeam('box-team-b', box.team_b, game.team_b_name, currentBoxTab);
}

function renderBoxTeam(containerId, players, teamName, tab) {
    const el = document.getElementById(containerId);
    if (!el || !players) return;

    const cols = ['PTS', 'FGM-A', '3PM-A', 'FTM-A', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF'];

    let rows = players.map(p => {
        const s = tab === 'total' ? p.total : (tab === 'h1' ? p.h1 : p.h2);
        if (!s) return null;

        return `<tr>
            <td>${p.player_name}</td>
            <td><strong>${s.pts}</strong></td>
            <td>${s.fgm}-${s.fga}</td>
            <td>${s.fg3m}-${s.fg3a}</td>
            <td>${s.ftm}-${s.fta}</td>
            <td>${s.reb}</td>
            <td>${s.ast}</td>
            <td>${s.stl}</td>
            <td>${s.blk}</td>
            <td>${s.to}</td>
            <td>${s.pf}</td>
        </tr>`;
    }).filter(Boolean).join('');

    // Team totals row
    const totals = { pts:0, fgm:0, fga:0, fg3m:0, fg3a:0, ftm:0, fta:0, reb:0, ast:0, stl:0, blk:0, to:0, pf:0 };
    players.forEach(p => {
        const s = tab === 'total' ? p.total : (tab === 'h1' ? p.h1 : p.h2);
        if (!s) return;
        Object.keys(totals).forEach(k => totals[k] += s[k] || 0);
    });

    rows += `<tr style="border-top: 2px solid var(--border-bright); font-weight: 700;">
        <td>TOTAL</td>
        <td><strong>${totals.pts}</strong></td>
        <td>${totals.fgm}-${totals.fga}</td>
        <td>${totals.fg3m}-${totals.fg3a}</td>
        <td>${totals.ftm}-${totals.fta}</td>
        <td>${totals.reb}</td>
        <td>${totals.ast}</td>
        <td>${totals.stl}</td>
        <td>${totals.blk}</td>
        <td>${totals.to}</td>
        <td>${totals.pf}</td>
    </tr>`;

    el.innerHTML = `
    <div class="box-table-wrap">
        <h4>${teamName || 'Team'}</h4>
        <div style="overflow-x: auto;">
        <table class="box-table">
            <thead><tr>
                <th>Player</th>
                ${cols.map(c => `<th>${c}</th>`).join('')}
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>
        </div>
    </div>`;
}

function switchBoxTab(btn) {
    document.querySelectorAll('.box-score-tabs .tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    currentBoxTab = btn.dataset.tab;
    if (gameData) renderBoxScore(gameData.box_score, gameData.game);
}

// ---- Shot Breakdown ----

function renderShotBreakdown(breakdown, game) {
    if (!breakdown) return;
    const el = document.getElementById('shot-tables');
    if (!el) return;

    const tab = currentShotTab;
    const teamAData = breakdown.team_a ? breakdown.team_a[tab] : null;
    const teamBData = breakdown.team_b ? breakdown.team_b[tab] : null;

    el.innerHTML = `
        ${renderShotTable(teamAData, game.team_a_name)}
        ${renderShotTable(teamBData, game.team_b_name)}
    `;
}

function renderShotTable(data, teamName) {
    if (!data) return '<div class="shot-table-wrap"><h4>' + (teamName || 'Team') + '</h4><p style="padding:16px;color:var(--text-dim);">No data</p></div>';

    const categories = [
        { key: 'rim', label: 'At Rim' },
        { key: 'midrange', label: 'Midrange' },
        { key: 'three', label: 'Three-Point' },
    ];

    let rows = categories.map(cat => {
        const d = data[cat.key] || { made: 0, attempted: 0 };
        const pct = d.attempted > 0 ? (d.made / d.attempted * 100) : 0;
        const pctClass = pct >= 50 ? 'pct-good' : pct < 33 ? 'pct-bad' : 'pct-avg';

        // Late clock sub-row data
        const lc = d.late_clock_attempted || 0;
        const lcm = d.late_clock_made || 0;
        const lcPct = lc > 0 ? (lcm / lc * 100) : 0;

        // Transition sub-row data
        const tr = d.transition_attempted || 0;
        const trm = d.transition_made || 0;
        const trPct = tr > 0 ? (trm / tr * 100) : 0;

        let html = `<tr>
            <td>${cat.label}</td>
            <td>${d.made}-${d.attempted}</td>
            <td class="${pctClass}">${pct.toFixed(1)}%</td>
            <td>${lc > 0 ? `${lcm}-${lc}` : '–'}</td>
            <td>${lc > 0 ? `<span class="${lcPct >= 50 ? 'pct-good' : lcPct < 33 ? 'pct-bad' : 'pct-avg'}">${lcPct.toFixed(0)}%</span>` : '–'}</td>
            <td>${tr > 0 ? `${trm}-${tr}` : '–'}</td>
            <td>${tr > 0 ? `<span class="${trPct >= 50 ? 'pct-good' : trPct < 33 ? 'pct-bad' : 'pct-avg'}">${trPct.toFixed(0)}%</span>` : '–'}</td>
        </tr>`;

        return html;
    }).join('');

    // Totals
    let totalM = 0, totalA = 0;
    categories.forEach(c => {
        const d = data[c.key] || {};
        totalM += d.made || 0;
        totalA += d.attempted || 0;
    });
    const totalPct = totalA > 0 ? (totalM / totalA * 100) : 0;
    const totalClass = totalPct >= 50 ? 'pct-good' : totalPct < 33 ? 'pct-bad' : 'pct-avg';

    rows += `<tr style="border-top: 2px solid var(--border-bright); font-weight: 700;">
        <td>Total FG</td>
        <td>${totalM}-${totalA}</td>
        <td class="${totalClass}">${totalPct.toFixed(1)}%</td>
        <td></td><td></td><td></td><td></td>
    </tr>`;

    return `
    <div class="shot-table-wrap">
        <h4>${teamName || 'Team'}</h4>
        <div style="overflow-x: auto;">
        <table class="shot-table">
            <thead><tr>
                <th>Zone</th>
                <th>FGM-A</th>
                <th>FG%</th>
                <th>Late Clk</th>
                <th>LC%</th>
                <th>Trans</th>
                <th>T%</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>
        </div>
    </div>`;
}

function switchShotTab(btn) {
    document.querySelectorAll('.shot-breakdown-tabs .tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    currentShotTab = btn.dataset.tab;
    if (gameData) renderShotBreakdown(gameData.shot_breakdown, gameData.game);
}

// ---- Insights Panel ----

function renderInsightsPanel(d) {
    const el = document.getElementById('insights-content');
    if (!el) return;

    const insights = d.insights || {};
    const cards = [];

    // Scoring run
    if (insights.current_run) {
        const run = insights.current_run;
        cards.push(cardHtml('Scoring Run',
            `<strong>${run.run_team}</strong> on a <strong>${run.run_size}-0</strong> run since ${run.run_start_time}`
        ));
    }

    // Foul trouble
    if (insights.foul_trouble && insights.foul_trouble.length) {
        const lines = insights.foul_trouble.slice(0, 5).map(ft =>
            `<strong>${ft.player_name}</strong> (${ft.team}) — ${ft.fouls} fouls [${ft.severity}]`
        ).join('<br>');
        cards.push(cardHtml('Foul Trouble', lines));
    }

    // Shooting streaks
    if (insights.shooting_streaks && insights.shooting_streaks.length) {
        const lines = insights.shooting_streaks.map(s => {
            const emoji = s.type === 'hot' ? '🔥' : '❄️';
            return `${emoji} <strong>${s.team}</strong>: ${(s.fg_pct * 100).toFixed(0)}% FG (${s.made}/${s.attempted}) last ${s.window_minutes} min`;
        }).join('<br>');
        cards.push(cardHtml('Shooting Trends', lines));
    }

    // Game summary
    cards.push(cardHtml('Game Summary',
        `<strong>${d.total_plays}</strong> plays · <strong>${d.total_shots}</strong> shots tracked`
    ));

    el.innerHTML = `<div class="insights-grid">${cards.join('')}</div>`;
}

function cardHtml(title, body) {
    return `<div class="insight-card">
        <div class="insight-card__title">${title}</div>
        <div class="insight-card__body">${body}</div>
    </div>`;
}

// ---- Utilities ----

function fmtOdds(v) {
    if (v == null) return '--';
    return v > 0 ? `+${v}` : `${v}`;
}

function shortName(name) {
    if (!name) return '??';
    // Take last word (usually the mascot or short school name)
    const parts = name.split(' ');
    return parts.length > 1 ? parts[parts.length - 1] : name;
}

// ---- Manual Refresh ----

async function manualRefresh() {
    try {
        await fetch('/api/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ game_id: GAME_ID }),
        });
        await fetchGameDetail();
    } catch (err) {
        console.error('Manual refresh failed:', err);
    }
}

// ---- Init ----

fetchGameDetail();
pollTimer = setInterval(fetchGameDetail, POLL_INTERVAL);
