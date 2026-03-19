"""
Flask backend for NCAA Basketball Live Betting Dashboard.

Serves the web UI and provides JSON API endpoints consumed by
the scoreboard and game detail JavaScript.

Uses GameManager to orchestrate all scraping and data aggregation.
"""
import os
import logging
from datetime import datetime
from flask import Flask, render_template, jsonify, request

# Load environment variables from .env file if present
try:
    from dotenv import load_dotenv
    import pathlib
    # Always load .env relative to this file's directory, regardless of cwd
    _env_path = pathlib.Path(__file__).parent / '.env'
    load_dotenv(dotenv_path=_env_path)
    print("ODDS API KEY:", os.environ.get('ODDS_API_KEY', 'NOT FOUND'))
except ImportError:
    pass  # dotenv not installed — fall back to system environment variables

from game_manager import GameManager
from models.database import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize database and game manager
init_db()
gm = GameManager(odds_api_key=os.environ.get('ODDS_API_KEY', ''))


# ---- HTML Routes ----

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/game/<game_id>")
def game_detail(game_id):
    return render_template("game_detail.html", game_id=game_id)


# ---- API Routes ----

@app.route("/api/scoreboard")
def api_scoreboard():
    """Return all today's games with scores, odds, and insights."""
    scoreboard = gm.get_scoreboard()
    return jsonify({
        "games": scoreboard,
        "timestamp": datetime.now().isoformat(),
    })


@app.route("/api/game/<game_id>")
def api_game_detail(game_id):
    """Return full detail for a single game."""
    detail = gm.get_game_detail(game_id)
    if not detail:
        return jsonify({"error": "Game not found"}), 404
    return jsonify(detail)


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Trigger a manual refresh. Optional game_id param for single-game refresh."""
    game_id = request.json.get("game_id") if request.is_json else None
    try:
        gm.manual_refresh(game_id=game_id)
        return jsonify({"status": "ok", "message": "Refresh complete"})
    except Exception as e:
        logger.error(f"Manual refresh failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/status")
def api_status():
    """Return scraper status and API quota info."""
    quota = gm.odds_scraper.get_quota_status()
    return jsonify({
        "auto_refresh_running": gm._auto_refresh_thread is not None and gm._auto_refresh_thread.is_alive(),
        "odds_api_quota": quota,
    })


# ---- Startup ----

if __name__ == "__main__":
    import signal
    from datetime import date

    print("\n" + "=" * 60)
    print("  NCAA Basketball Live Betting Dashboard")
    print("=" * 60)
    print(f"\n  Server:  http://localhost:5000")
    print(f"  Date:    {date.today().strftime('%B %d, %Y')}")
    print(f"  Odds API Key: {'Set' if os.environ.get('ODDS_API_KEY') else 'Not set'}")
    print("=" * 60 + "\n")

    def shutdown(sig=None, frame=None):
        """Clean shutdown: stop scraper, close browser, exit."""
        print("\nShutting down...")
        gm.stop_auto_refresh()
        try:
            from scrapers.browser import close_driver
            close_driver()
        except Exception:
            pass
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    gm.start_auto_refresh()
    try:
        app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
    finally:
        shutdown()
