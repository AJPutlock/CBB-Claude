"""
Shared browser module using undetected-chromedriver.

Manages a single Chrome instance that both the NCAA and DraftKings
scrapers use. Includes automatic recovery if the browser window
closes unexpectedly.

Install: pip install undetected-chromedriver
"""
import undetected_chromedriver as uc
import threading
import logging
import time
import json

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_driver = None
_dk_warmed = False


def _detect_chrome_version():
    """Detect installed Chrome major version from the Windows registry."""
    import subprocess
    import re
    import platform

    try:
        if platform.system() == 'Windows':
            result = subprocess.run(
                ['reg', 'query',
                 r'HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon',
                 '/v', 'version'],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                match = re.search(r'(\d+)\.\d+\.\d+\.\d+', result.stdout)
                if match:
                    return int(match.group(1))
        elif platform.system() == 'Darwin':
            result = subprocess.run(
                ['/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
                 '--version'],
                capture_output=True, text=True, timeout=5,
            )
            match = re.search(r'(\d+)\.\d+', result.stdout)
            if match:
                return int(match.group(1))
        else:
            result = subprocess.run(
                ['google-chrome', '--version'],
                capture_output=True, text=True, timeout=5,
            )
            match = re.search(r'(\d+)\.\d+', result.stdout)
            if match:
                return int(match.group(1))
    except Exception as e:
        logger.debug(f"Chrome version detection failed: {e}")
    return None


def _create_driver():
    """Create a new undetected Chrome driver instance."""
    logger.info("Launching undetected Chrome browser...")
    options = uc.ChromeOptions()
    options.add_argument('--start-minimized')
    options.add_argument('--no-first-run')
    options.add_argument('--no-default-browser-check')
    options.add_argument('--disable-popup-blocking')

    chrome_ver = _detect_chrome_version()
    if chrome_ver:
        logger.info(f"Detected Chrome version: {chrome_ver}")
        driver = uc.Chrome(
            options=options,
            use_subprocess=True,
            version_main=chrome_ver,
        )
    else:
        logger.info("Could not detect Chrome version, letting uc auto-detect")
        driver = uc.Chrome(options=options, use_subprocess=True)

    driver.set_page_load_timeout(45)
    driver.implicitly_wait(5)
    logger.info("Chrome browser launched successfully")
    return driver


def _is_driver_alive(driver):
    """Check if the browser window is still responsive."""
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def get_driver():
    """
    Get the shared Chrome driver, creating or recreating if needed.
    Thread-safe.
    """
    global _driver
    if _driver is not None and _is_driver_alive(_driver):
        return _driver

    with _lock:
        # Double-check inside lock
        if _driver is not None and _is_driver_alive(_driver):
            return _driver

        # Clean up dead driver if any — also clear DK warmup state since
        # cookies don't survive a browser restart
        if _driver is not None:
            logger.warning("Browser window was closed, restarting...")
            try:
                _driver.quit()
            except Exception:
                pass
            _driver = None
            _dk_warmed = False

        try:
            _driver = _create_driver()
        except Exception as e:
            logger.error(f"Failed to launch Chrome: {e}")
            logger.error(
                "TIP: If you see a version mismatch, try: "
                "1) Update Chrome, or "
                "2) In PowerShell run: Remove-Item -Recurse -Force \"$env:APPDATA\\undetected_chromedriver\""
            )
            _driver = None
            raise

    return _driver


def fetch_page(url, wait_seconds=3):
    """
    Navigate to a URL and return the page source HTML.
    Automatically recovers if the browser window was closed.
    """
    for attempt in range(2):
        try:
            driver = get_driver()
            driver.get(url)
            time.sleep(wait_seconds)
            return driver.page_source
        except Exception as e:
            if attempt == 0:
                logger.warning(f"Browser navigation failed, restarting browser: {e}")
                _force_reset()
            else:
                logger.error(f"Browser fetch failed for {url}: {e}")
                _force_reset()
                return None
    return None


def warm_dk_session():
    """
    Visit the DraftKings sportsbook frontend so the browser passes
    Akamai's bot challenge and picks up required cookies.

    Re-warms automatically if the browser was restarted since last warmup
    (e.g. after a crash), since cookies don't survive a browser restart.
    """
    global _dk_warmed, _driver
    # If marked as warmed but the driver is dead or was recreated, re-warm
    if _dk_warmed:
        if _driver is not None and _is_driver_alive(_driver):
            return True
        else:
            # Browser was restarted — must re-warm
            _dk_warmed = False

    try:
        driver = get_driver()
        logger.info("Warming DK session (visiting sportsbook page)...")
        driver.get("https://sportsbook.draftkings.com/leagues/basketball/ncaab")
        time.sleep(8)

        title = driver.title or ''
        logger.info(f"DK page title: '{title}'")

        if 'draftkings' not in title.lower() and 'sportsbook' not in title.lower():
            logger.info("Waiting for Akamai challenge to clear...")
            time.sleep(10)

        _dk_warmed = True
        return True
    except Exception as e:
        logger.error(f"DK session warmup failed: {e}")
        _force_reset()
        return False


def fetch_dk_json(url, wait_seconds=4):
    """
    Fetch JSON from a DraftKings API endpoint.
    First warms DK session, then navigates to the JSON endpoint.
    """
    if not warm_dk_session():
        return None

    for attempt in range(2):
        try:
            driver = get_driver()
            driver.get(url)
            time.sleep(wait_seconds)

            # Try multiple extraction methods
            # Method 1: <pre> tag (Chrome wraps raw JSON in <pre>)
            try:
                pre_el = driver.find_element('tag name', 'pre')
                text = pre_el.text
                if text and text.strip().startswith('{'):
                    return json.loads(text)
            except Exception:
                pass

            # Method 2: body text
            try:
                body_text = driver.find_element('tag name', 'body').text
                if body_text and body_text.strip().startswith('{'):
                    return json.loads(body_text)
            except Exception:
                pass

            # Method 3: JavaScript extraction
            try:
                raw = driver.execute_script("return document.body.innerText;")
                if raw and raw.strip().startswith('{'):
                    return json.loads(raw)
            except Exception:
                pass

            # Method 4: Regex on page source
            import re
            page_source = driver.page_source or ''
            json_match = re.search(r'(\{"eventGroup".*\})', page_source)
            if json_match:
                try:
                    return json.loads(json_match.group(1))
                except json.JSONDecodeError:
                    pass

            snippet = page_source[:300] if page_source else '(empty)'
            logger.warning(f"DK page not JSON. Snippet: {snippet}")
            return None

        except Exception as e:
            if attempt == 0:
                logger.warning(f"DK fetch failed, restarting browser: {e}")
                _force_reset()
                # Re-warm DK after browser restart
                global _dk_warmed
                _dk_warmed = False
                warm_dk_session()
            else:
                logger.error(f"DK JSON fetch failed for {url}: {e}")
                _force_reset()
                return None
    return None


def close_driver():
    """Shut down the shared browser. Called on app exit."""
    global _driver, _dk_warmed
    with _lock:
        if _driver is not None:
            try:
                _driver.quit()
                logger.info("Chrome browser closed")
            except Exception:
                pass
            _driver = None
            _dk_warmed = False


def _force_reset():
    """Force-reset the driver. Used after crashes or closed windows."""
    global _driver, _dk_warmed
    with _lock:
        if _driver is not None:
            try:
                _driver.quit()
            except Exception:
                pass
            _driver = None
            _dk_warmed = False
