import asyncio
import random
import re
import json
import os
import logging
import argparse
import traceback
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
import polars as pl
import threading
import queue
import signal
import sys
import re
from bs4 import BeautifulSoup
import traceback
from file_watcher import ProfileFileWatcher
from helper import POSTS_SCRIPT, COMMENTS_SCRIPT, REACTIONS_SCRIPT, stealth_mode_script
from database.db import *

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("linkedin_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

STATE_FILE = "linkedin_state.json"
logging.getLogger().setLevel(logging.DEBUG)

def load_state() -> dict:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

def save_state(state: dict):
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

def parse_linkedin_timestamp(ts):
    if not ts:
        return None
    try:
        # ISO datetime
        return datetime.fromisoformat(ts)
    except Exception:
        pass
    # Relative dates like '2d', '3mo', '1yr'
    match = re.match(r'(\d+)\s*(d|mo|yr)', ts)
    if match:
        num, unit = int(match.group(1)), match.group(2)
        if unit == "d":
            return datetime.now() - timedelta(days=num)
        elif unit == "mo":
            return datetime.now() - timedelta(days=num * 30)
        elif unit == "yr":
            return datetime.now() - timedelta(days=num * 365)
    # If all fails
    return None

class PlaywrightProfileScraper:
    """LinkedIn profile scraper using Playwright"""
    def __init__(self, worker_id: int, credentials: Dict[str, str], proxy: Optional[str] = None, headless: bool = False):
        """Initialize the scraper with credentials"""
        self.worker_id = worker_id
        self.email = credentials['email']
        self.password = credentials['password']
        self.proxy = proxy
        self.headless = headless
        self.browser = None
        self.context = None
        self.page = None
        self.session_start_time = None
        self.profiles_scraped = 0
        self.max_profiles_per_session = random.randint(5, 10)  # Randomize session limits
        self.session_duration_limit = timedelta(hours=random.uniform(2, 4))  # Random session duration
        
        # State tracking
        self.is_logged_in = False
        self.in_cooldown = False
        self.cooldown_until = None
        
        # Configuration
        self.config = {
            'scrape_activity': True  # Whether to scrape activity (posts, comments)
        }
    
    async def _is_authwall_present(self) -> bool:
        """Detect if we’re stuck on an auth-wall or login page rather than seeing feed content."""
        # 1) If we see the main feed container, we're good.
        if await self.page.locator("div.feed-identity-module").count() > 0:
            return False

        # 2) If the login form or authwall overlay is visible, we're blocked
        if await self.page.locator("form.login__form, div.authwall, div.sign-in-form").count() > 0:
            return True

        # 3) URL heuristics for checkpoints or authwalls
        url = self.page.url.lower()
        if any(token in url for token in ("checkpoint", "challenge", "authwall", "/login", "/signup")):
            return True

        # 4) Fallback: if we see the username field but aren't in feed
        if await self.page.locator("#username").count() > 0:
            return True

        return False

    async def save_cookies(self, context, path):
        try:
            cookies = await context.cookies()
            with open(path, "w") as f:
                json.dump(cookies, f)
            logger.info(f"Worker {self.worker_id}: Cookies saved to {path}")
        except Exception as e:
            logger.warning(f"Worker {self.worker_id}: Could not save cookies: {e}")

    async def load_cookies(self, context, path):
        try:
            if os.path.exists(path) and os.path.getsize(path) > 0:
                with open(path, "r") as f:
                    cookies = json.load(f)
                if cookies:  # Only load if cookies exist
                    await context.add_cookies(cookies)
                    logger.info(f"Worker {self.worker_id}: Cookies loaded from {path}")
                    return True
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.warning(f"Worker {self.worker_id}: Could not load cookies: {e}")
        return False

    def test_proxy(self, proxy_url):
        """Test if a proxy is working"""
        import requests
        try:
            proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            response = requests.get('https://httpbin.org/ip', proxies=proxies, timeout=10)
            if response.status_code == 200:
                logger.info(f"Proxy {proxy_url} is working")
                return True
        except Exception as e:
            logger.warning(f"Proxy {proxy_url} failed test: {e}")
        return False

    async def initialize(self):
        """Initialize Playwright browser, context, and optionally restore session via cookies."""
        try:
            # Launch Playwright
            self.playwright = await async_playwright().start()

            # Browser launch args
            browser_args = [
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]

            proxy_config = None
            if self.proxy:
                logger.info(f"Worker {self.worker_id}: Testing proxy {self.proxy}")
                if self.test_proxy(self.proxy):
                    # Configure proxy for Playwright
                    if self.proxy.startswith('http://') or self.proxy.startswith('https://'):
                        proxy_parts = self.proxy.replace('http://', '').replace('https://', '').split(':')
                        if len(proxy_parts) >= 2:
                            proxy_config = {
                                'server': f"http://{proxy_parts[0]}:{proxy_parts[1]}"
                            }
                            # Add authentication if provided
                            if len(proxy_parts) >= 4:
                                proxy_config['username'] = proxy_parts[2]
                                proxy_config['password'] = proxy_parts[3]
                else:
                    logger.warning(f"Worker {self.worker_id}: Proxy failed test, proceeding without proxy")
                    self.proxy = None

            self.browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=browser_args
            )
            if not self.browser:
                logger.error(f"Worker {self.worker_id}: Failed to launch browser.")
                await self.cleanup()
                return False

            # Randomize viewport & UA
            viewport = random.choice([
                {'width': 1366, 'height': 768},
                {'width': 1440, 'height': 900},
                {'width': 1536, 'height': 864},
                {'width': 1680, 'height': 1050},
                {'width': 1920, 'height': 1080}
            ])

            with open("userAgents.json", "r") as ua:
                user_agents = json.load(ua)

            user_agent = random.choice(user_agents)

            # Create context
            self.context = await self.browser.new_context(
                viewport=viewport,
                user_agent=user_agent,
                locale="en-US",
                timezone_id=random.choice([
                    "America/New_York", "Europe/London", "Asia/Tokyo"
                ]),
                geolocation={"longitude": -122.084, "latitude": 37.422},
                permissions=["geolocation"],
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            if not self.context:
                logger.error(f"Worker {self.worker_id}: Failed to create browser context.")
                await self.cleanup()
                return False

            # Stealth
            await self._apply_stealth_mode()

            # New page
            self.page = await self.context.new_page()
            if not self.page:
                logger.error(f"Worker {self.worker_id}: Failed to create browser page.")
                await self.cleanup()
                return False

            self.page.set_default_timeout(30000)
            self.page.on("console", lambda msg: logger.debug(f"Browser console: {msg.text}"))
            self._check_cooldown_state()

            if self.in_cooldown:
                logger.info(f"Worker {self.worker_id}: In cooldown until {self.cooldown_until}")
                return True

            # --- Attempt to load cookies BEFORE any navigation ---
            cookie_path = f"cookies_worker_{self.worker_id}.json"
            cookies_loaded = await self.load_cookies(self.context, cookie_path)

            self.session_start_time = datetime.now()
            self.profiles_scraped = 0

            # Always go to /feed and check login status
            try:
                await self.page.goto("https://www.linkedin.com/feed/", wait_until="networkidle")
            except Exception as nav_ex:
                logger.error(f"Worker {self.worker_id}: Couldn't reach feed page: {nav_ex}")
                await self.cleanup()
                return False

            # Robust login/authwall check
            if cookies_loaded and "feed" in self.page.url and not await self._is_authwall_present():
                self.is_logged_in = True
                logger.info(f"Worker {self.worker_id}: Session restored via cookies.")
            else:
                self.is_logged_in = False
                logger.info(f"Worker {self.worker_id}: Not logged in, will perform fresh login.")

            self.last_activity_time = datetime.now()
            self._check_cooldown_state()

            # Only start activity simulation if logged in and not in cooldown
            if not self.in_cooldown and self.is_logged_in:
                await self.start_activity_simulation()

            return True  # So login() will run if not authenticated

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error initializing browser: {e}")
            logger.debug(traceback.format_exc())
            await self.cleanup()
            return False

    async def _apply_stealth_mode(self):
        """Apply stealth mode to avoid detection"""
        # JavaScript to modify navigator properties
        await self.context.add_init_script(stealth_mode_script)

    async def start_activity_simulation(self):
        """Start a background task to keep the session alive with random activity"""
        task = asyncio.create_task(self._activity_simulation_loop())
        task.set_name(f"activity_simulation_{self.worker_id}")

    async def _activity_simulation_loop(self):
        """Loop that performs random human-like actions to keep the session alive"""
        logger.info(f"Worker {self.worker_id}: Started activity simulation loop")
        
        while self.is_logged_in and not self.in_cooldown:
            # Wait for a random interval (5-15 minutes between activities)
            await asyncio.sleep(random.uniform(300, 900))
            
            if not self.is_logged_in or self.in_cooldown:
                break
                
            try:
                logger.info(f"Worker {self.worker_id}: Performing random activity to keep session alive")
                
                # Select a random activity
                activities = [
                    self._check_feed_activity,
                    self._check_notifications_activity,
                    self._check_my_network_activity,
                    self._check_messaging_activity,
                    self._visit_own_profile_activity
                ]
                
                # Perform 1-2 random activities
                for _ in range(random.randint(1, 2)):
                    activity = random.choice(activities)
                    await activity()
                    await self._human_sleep(2, 5)
                    
            except Exception as e:
                logger.error(f"Worker {self.worker_id}: Error in activity simulation: {e}")

    async def _check_feed_activity(self):
        """Check feed and scroll through it"""
        try:
            await self.page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded")
            await self._human_sleep(2, 4)
            
            # Scroll feed 2-5 times
            scroll_count = random.randint(2, 5)
            for _ in range(scroll_count):
                scroll_amount = random.randint(300, 800)
                await self.page.evaluate(f"window.scrollBy(0, {scroll_amount});")
                await self._human_sleep(1, 3)
                
            logger.debug(f"Worker {self.worker_id}: Performed feed activity")
        except Exception as e:
            logger.warning(f"Worker {self.worker_id}: Feed activity failed: {e}")

    async def _check_notifications_activity(self):
        """Check notifications"""
        try:
            # Click notifications icon
            await self.page.click("a[data-test-global-nav-link='notifications']")
            await self._human_sleep(2, 4)
            
            # Scroll through notifications
            scroll_count = random.randint(1, 3)
            for _ in range(scroll_count):
                await self.page.evaluate("window.scrollBy(0, 300);")
                await self._human_sleep(1, 2)
                
            # Click back to close
            await self.page.click("body")
            await self._human_sleep(1, 2)
            
            logger.debug(f"Worker {self.worker_id}: Performed notifications activity")
        except Exception as e:
            logger.warning(f"Worker {self.worker_id}: Notifications activity failed: {e}")

    async def _check_my_network_activity(self):
        """Check my network page"""
        try:
            await self.page.click("a[data-test-global-nav-link='mynetwork']")
            await self._human_sleep(2, 4)
            
            # Scroll through network page
            scroll_count = random.randint(1, 3)
            for _ in range(scroll_count):
                await self.page.evaluate("window.scrollBy(0, 300);")
                await self._human_sleep(1, 2)
                
            logger.debug(f"Worker {self.worker_id}: Performed my network activity")
        except Exception as e:
            logger.warning(f"Worker {self.worker_id}: My network activity failed: {e}")

    async def _check_messaging_activity(self):
        """Check messaging page"""
        try:
            await self.page.click("a[data-test-global-nav-link='messaging']")
            await self._human_sleep(2, 4)
            
            # Scroll through messages
            await self.page.evaluate("window.scrollBy(0, 200);")
            await self._human_sleep(1, 2)
            
            logger.debug(f"Worker {self.worker_id}: Performed messaging activity")
        except Exception as e:
            logger.warning(f"Worker {self.worker_id}: Messaging activity failed: {e}")

    async def _visit_own_profile_activity(self):
        """Visit own profile"""
        try:
            # Click on profile picture/menu
            await self.page.click("button.global-nav__me-photo")
            await self._human_sleep(1, 2)
            
            # Click "View profile"
            profile_link = await self.page.query_selector("a[href*='/in/'][data-link-to='profile']")
            if profile_link:
                await profile_link.click()
                await self._human_sleep(3, 5)
                
                # Scroll profile
                await self._scroll_page()
                
            logger.debug(f"Worker {self.worker_id}: Performed own profile visit activity")
        except Exception as e:
            logger.warning(f"Worker {self.worker_id}: Own profile activity failed: {e}")

    async def cleanup(self):
        for task in asyncio.all_tasks():
            if task.get_name().startswith(f"activity_simulation_{self.worker_id}"):
                task.cancel()
        try:
            if self.page:
                await self.page.close()
                self.page = None
            if self.context:
                await self.context.close()
                self.context = None
            if self.browser:
                await self.browser.close()
                self.browser = None
            if hasattr(self, 'playwright') and self.playwright:
                await self.playwright.stop()
            logger.info(f"Worker {self.worker_id}: Resources cleaned up")
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error during cleanup: {e}")

    async def login(self):
        """Log into LinkedIn with robust session and authentication checks."""
        # 1) Try to restore an existing session
        try:
            logger.info(f"Worker {self.worker_id}: Checking LinkedIn authentication status")
            await self.page.goto("https://www.linkedin.com/feed/", wait_until="networkidle")
        except Exception as e:
            logger.warning(f"Worker {self.worker_id}: Could not reach feed page for session check: {e}")

        # If feed loads and no auth-wall, we’re good
        if "feed" in self.page.url and not await self._is_authwall_present():
            logger.info(f"Worker {self.worker_id}: Session restored via cookies.")
            self.is_logged_in = True
            return True

        # 2) Fresh login
        logger.info(f"Worker {self.worker_id}: Performing fresh login")
        await self.page.goto("https://www.linkedin.com/login", wait_until="networkidle")

        # Enter email
        logger.debug(f"Worker {self.worker_id}: Entering email")
        await self.page.wait_for_selector("#username", state="visible", timeout=10000)
        await self._human_type("#username", self.email)

        # Enter password
        logger.debug(f"Worker {self.worker_id}: Entering password")
        await self._human_type("input[name='session_password']", self.password)

        # Submit form
        logger.debug(f"Worker {self.worker_id}: Submitting login form")
        await self.page.click("button[type='submit']")
        await self._human_sleep(3, 6)

        # Handle 2FA or other challenges
        await self._handle_verification()

        # 3) Verify real authentication by revisiting the feed
        await self.page.goto("https://www.linkedin.com/feed/", wait_until="networkidle")
        if "feed" in self.page.url and not await self._is_authwall_present():
            logger.info(f"Worker {self.worker_id}: Login succeeded")
            self.is_logged_in = True

            # Persist cookies for next run
            cookie_path = f"cookies_worker_{self.worker_id}.json"
            await self.save_cookies(self.context, cookie_path)
            return True

        # Login failed or hit auth-wall
        logger.warning(f"Worker {self.worker_id}: Login failed or auth-wall detected. URL={self.page.url}")
        self.is_logged_in = False
        return False
    
    async def _handle_verification(self):
        """Handle verification challenges if they appear"""
        try:
            # Check for security verification
            if "checkpoint" in self.page.url or "challenge" in self.page.url:
                logger.warning(f"Worker {self.worker_id}: Hit verification challenge: {self.page.url}")
                
                # Check for email verification option
                try:
                    email_button = await self.page.wait_for_selector("button[data-auth-method='EMAIL']", timeout=5000)
                    if email_button:
                        await email_button.click()
                        logger.info(f"Worker {self.worker_id}: Selected email verification")
                        await self._human_sleep(2, 3)
                        
                        # Click send code button
                        send_button = await self.page.wait_for_selector("button[type='submit']", timeout=5000)
                        if send_button:
                            await send_button.click()
                            logger.info(f"Worker {self.worker_id}: Requested verification code")
                            
                            # Get verification code from user
                            code = input(f"Worker {self.worker_id}: Enter the verification code sent to your email: ")
                            
                            # Enter the code
                            await self._human_type("#input__email_verification_pin", code)
                            
                            # Submit the form
                            await self.page.click("button[type='submit']")
                            await self._human_sleep(3, 5)
                except Exception as e:
                    logger.warning(f"Worker {self.worker_id}: Couldn't process email verification: {e}")
                
                # If we're still on a challenge page, ask for manual intervention
                if "checkpoint" in self.page.url or "challenge" in self.page.url:
                    logger.warning(f"Worker {self.worker_id}: Manual verification required")
                    input(f"Worker {self.worker_id}: Please complete verification manually in the browser window and press Enter when done...")
                
                return True
            
            # Check for "Remember this device" prompt
            try:
                remember_button = await self.page.wait_for_selector(
                    "button[data-litms-control-urn='remember_me_save']", 
                    timeout=3000
                )
                if remember_button:
                    await remember_button.click()
                    logger.info(f"Worker {self.worker_id}: Clicked 'Remember this device'")
                    await self._human_sleep(1, 2)
            except:
                pass
            
            # Check for premium offer or other popups
            try:
                dismiss_button = await self.page.wait_for_selector(
                    "button.artdeco-modal__dismiss", 
                    timeout=3000
                )
                if dismiss_button:
                    await dismiss_button.click()
                    logger.info(f"Worker {self.worker_id}: Dismissed modal popup")
                    await self._human_sleep(1, 2)
            except:
                pass
            
            return False
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error handling verification: {e}")
            return False
    
    async def scrape_profile(self, profile_url: str):
        """Scrape a LinkedIn profile with robust error handling and processed URL tracking"""

        if not self.session_start_time:
            self.session_start_time = datetime.now()

        # Check cooldown status first - if in cooldown, don't process anything
        if self.in_cooldown:
            if datetime.now() < self.cooldown_until:
                remaining_time = (self.cooldown_until - datetime.now()).total_seconds()
                logger.warning(f"Worker {self.worker_id}: In cooldown until {self.cooldown_until} ({remaining_time/3600:.1f} hours remaining)")
                return None
            else:
                logger.info(f"Worker {self.worker_id}: Cooldown expired, resuming operations")
                self.in_cooldown = False
                # Clear cooldown state
                state = load_state()
                state[f"worker_{self.worker_id}_cooldown"] = {"in_cooldown": False, "cooldown_until": None}
                save_state(state)

        # Check if session needs to be refreshed
        if (
            datetime.now() - self.session_start_time > self.session_duration_limit or 
            self.profiles_scraped >= self.max_profiles_per_session
        ):
            logger.info(f"Worker {self.worker_id}: Session limit reached. Reinitializing browser.")
            await self.cleanup()
            await self.initialize()

        # Ensure logged in
        if not self.is_logged_in:
            login_success = await self.login()
            if not login_success:
                logger.error(f"Worker {self.worker_id}: Failed to login")
                return None

        # --- Skip if already processed ---
        state = load_state()
        if profile_url in state.get("processed_urls", []):
            logger.info(f"Worker {self.worker_id}: Profile {profile_url} already processed, skipping.")
            return None

        # Get previous scraping state for this profile
        profile_state = state.get(profile_url, {})
        last_post_time = profile_state.get("last_post_time")
        last_comment_time = profile_state.get("last_comment_time")
        last_reaction_time = profile_state.get("last_reaction_time")

        # Initialize profile data structure
        profile_data = {
            'basic_info': {},
            'experience': [],
            'scraped_at': datetime.now().isoformat(),
            'profile_url': profile_url,
            'scraper_worker_id': self.worker_id
        }

        try:
            # Navigate to profile
            logger.info(f"Worker {self.worker_id}: Navigating to {profile_url}")
            await self.page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
            await self._human_sleep(2, 4)

            # Check if we need to handle sign-in wall
            await self._handle_sign_in_wall()

            # Scroll the page to trigger lazy loading
            await self._scroll_page()

            # Save page screenshot and source for debugging
            if logger.level == logging.DEBUG:
                content = await self.page.content()
                with open(f"profile_source_{self.worker_id}.html", "w", encoding="utf-8") as f:
                    f.write(content)

            # Check for blocks or limits
            if await self._check_for_blocks():
                logger.warning(f"Worker {self.worker_id}: Detected block or limit")
                self._enter_cooldown()
                return None

            # Extract profile sections
            logger.info(f"Worker {self.worker_id}: Extracting profile data")
            html = await self.page.content()
            profile_data['basic_info'] = await self._extract_basic_info()
            profile_data['experience'] = self._extract_experience(html)

            # Extract activity data if configured
            if self.config.get('scrape_activity', False):
                logger.info(f"Worker {self.worker_id}: Extracting activity data")
                activity_data, new_times = await self.scrape_user_activity(
                    profile_url,
                    last_post_time,
                    last_comment_time,
                    last_reaction_time
                )
                profile_data['activity'] = activity_data

                # Save the latest timestamps for incremental scraping
                state[profile_url] = new_times
                save_state(state)

            # Increment the profile count
            self.profiles_scraped += 1

            # Save the profile data
            self._save_profile_data(profile_data)

            # --- Mark as processed ---
            if "processed_urls" not in state:
                state["processed_urls"] = []
            if profile_url not in state["processed_urls"]:
                state["processed_urls"].append(profile_url)
                save_state(state)

            logger.info(f"Worker {self.worker_id}: Successfully scraped profile {profile_url}")

            return profile_data

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error scraping profile {profile_url}: {e}")
            logger.debug(traceback.format_exc())
            return None

    async def _handle_sign_in_wall(self):
        """Handle the LinkedIn sign-in wall if it appears"""
        try:
            # Check if we hit a sign-in wall
            sign_in_wall = await self.page.query_selector(".signin-content, .organic-signup-modal")
            
            if sign_in_wall:
                logger.info(f"Worker {self.worker_id}: Hit sign-in wall, logging in again")
                await self.login()
                
                # Navigate back to the profile
                current_url = self.page.url
                await self.page.goto(current_url, wait_until="domcontentloaded")
                await self._human_sleep(2, 4)
        except:
            pass
    
    async def _scroll_page(self):
        """Scroll the page to simulate human behavior and trigger lazy loading"""
        # Get page dimensions
        page_height = await self.page.evaluate("document.body.scrollHeight")
        viewport_height = await self.page.evaluate("window.innerHeight")
        
        # Initial pause to let the page load
        await self._human_sleep(2, 4)
        
        # Scroll down gradually with random pauses
        current_position = 0
        scroll_step = random.randint(int(viewport_height * 0.2), int(viewport_height * 0.8))
        
        while current_position < page_height:
            # Calculate next position with variable step size
            next_position = current_position + scroll_step
            
            # Scroll with smooth behavior
            await self.page.evaluate(f"""
                window.scrollTo({{
                    top: {next_position},
                    behavior: 'smooth'
                }});
            """)
            
            current_position = next_position
            
            # Random pause between scrolls
            await self._human_sleep(0.7, 2.0)
            
            # Occasionally pause longer to simulate reading
            if random.random() < 0.3:  # 30% chance
                await self._human_sleep(1.5, 4.0)
            
            # Occasionally scroll back up slightly
            if random.random() < 0.15:  # 15% chance
                scroll_back = random.randint(100, 300)
                current_position = max(0, current_position - scroll_back)
                await self.page.evaluate(f"window.scrollTo(0, {current_position});")
                await self._human_sleep(0.7, 1.5)
            
            # Update page height as it might have changed due to lazy loading
            page_height = await self.page.evaluate("document.body.scrollHeight")
            
            # Update scroll step for variability
            scroll_step = random.randint(int(viewport_height * 0.2), int(viewport_height * 0.8))
        
        # After reaching bottom, scroll back up partially
        final_position = random.randint(int(page_height * 0.2), int(page_height * 0.5))
        await self.page.evaluate(f"window.scrollTo(0, {final_position});")
        await self._human_sleep(1.0, 2.5)
    
    async def _check_for_blocks(self):
        """
        Improved block/captcha detection:
        - Checks for known block URLs.
        - Checks for visible or structural CAPTCHA in HTML (not just the word).
        - Only blocks on visible reCAPTCHA iframe.
        - Logs findings for debugging.
        """
        try:
            current_url = self.page.url.lower()
            html = await self.page.content()
            blocked = False

            # 1. URL-based block detection (very reliable)
            block_url_keywords = [
                "/checkpoint", "/authwall", "/login", "/signup", "/challenge", "/verify"
            ]
            for kw in block_url_keywords:
                if kw in current_url:
                    logger.warning(f"Worker {self.worker_id}: Block detected by URL: '{kw}' in '{current_url}'")
                    blocked = True

            # 2. Visible reCAPTCHA iframe (not just present in DOM)
            recaptcha_iframes = await self.page.query_selector_all('iframe[src*="recaptcha"]')
            recaptcha_visible = False

            for iframe in recaptcha_iframes:
                box = await iframe.bounding_box()
                if box and box['height'] > 20 and box['width'] > 20:
                    recaptcha_visible = True
                    break

            if recaptcha_visible:
                logger.warning(f"Worker {self.worker_id}: **Visible** reCAPTCHA iframe detected on the page")
                blocked = True
            elif recaptcha_iframes:
                logger.info(f"Worker {self.worker_id}: reCAPTCHA iframe(s) present but not visible—continuing")

            # 3. Common structural CAPTCHA triggers in HTML (not just the word)
            if (
                '<strong>reCAPTCHA</strong>' in html
                or 'id="captcha"' in html
                or 'class="g-recaptcha"' in html
            ):
                logger.warning(f"Worker {self.worker_id}: CAPTCHA widget detected in HTML")
                blocked = True

            # 4. Heuristic: page overlays asking to verify identity/human
            if "please verify you are a human" in html.lower():
                logger.warning(f"Worker {self.worker_id}: Human verification message found")
                blocked = True

            # 5. Save HTML for debugging if a block was detected
            if blocked:
                with open(f"debug_block_page_{self.worker_id}.html", "w", encoding="utf-8") as f:
                    f.write(html)

            return blocked

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error checking for blocks: {e}")
            return False
        
    def _enter_cooldown(self, hours=None):
        """
        Enter a cooldown period to avoid detection
        Optionally specify cooldown duration in hours, otherwise uses random default
        """
        if hours is None:
            hours = random.uniform(2, 4)
        
        self.cooldown_until = datetime.now() + timedelta(hours=hours)
        self.in_cooldown = True
        
        # Save the cooldown state to persist across restarts
        try:
            state = load_state()
            state[f"worker_{self.worker_id}_cooldown"] = {
                "in_cooldown": True,
                "cooldown_until": self.cooldown_until.isoformat()
            }
            save_state(state)
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Failed to save cooldown state: {e}")
        
        logger.warning(f"Worker {self.worker_id}: Entering cooldown until {self.cooldown_until}")

    def _check_cooldown_state(self):
        """Check if worker should be in cooldown based on saved state"""
        try:
            state = load_state()
            worker_state = state.get(f"worker_{self.worker_id}_cooldown", {})
            
            if worker_state.get("in_cooldown", False):
                cooldown_until = datetime.fromisoformat(worker_state.get("cooldown_until", ""))
                
                if datetime.now() < cooldown_until:
                    self.in_cooldown = True
                    self.cooldown_until = cooldown_until
                    logger.info(f"Worker {self.worker_id}: Restored cooldown state until {cooldown_until}")
                else:
                    # Cooldown expired
                    self.in_cooldown = False
                    self.cooldown_until = None
                
            # Clear the cooldown state
            state[f"worker_{self.worker_id}_cooldown"] = {
                "in_cooldown": False,
                "cooldown_until": None
            }
            save_state(state)
            
            logger.info(f"Worker {self.worker_id}: Cooldown expired")
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error checking cooldown state: {e}")

    async def refresh_session(self):
        """Refresh the session to avoid timeouts/logouts"""
        try:
            logger.info(f"Worker {self.worker_id}: Refreshing session")

            # Navigate to LinkedIn homepage
            await self.page.goto("https://www.linkedin.com/", wait_until="domcontentloaded")
            await self._human_sleep(2, 4)

            # Check if we need to re-login
            if await self._is_authwall_present():
                logger.info(f"Worker {self.worker_id}: Session expired, logging in again")
                await self.login()
            else:
                logger.info(f"Worker {self.worker_id}: Session still valid")
                
            # Save cookies to maintain session for next time
            cookie_path = f"cookies_worker_{self.worker_id}.json"
            await self.save_cookies(self.context, cookie_path)

            # Reset session start time
            self.session_start_time = datetime.now()
            return True

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error refreshing session: {e}")
            return False

    async def _perform_random_actions(self):
        """Perform random actions to simulate natural browsing"""
        try:
            actions = [
                self._random_scroll_action,
                self._check_notifications_action,
                self._hover_random_elements_action
            ]
                    
            # Choose 1-2 random actions
            num_actions = random.randint(1, 2)
            selected_actions = random.sample(actions, num_actions)
            
            for action in selected_actions:
                await action()
                await self._human_sleep(1, 3)
                    
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error performing random actions: {e}")
    
    async def _random_scroll_action(self):
        """Random scrolling action"""
        try:
            scroll_amount = random.randint(300, 800)
            direction = random.choice([-1, 1])
            await self.page.evaluate(f"window.scrollBy(0, {direction * scroll_amount});")
            logger.debug(f"Worker {self.worker_id}: Performed random scroll")
        except:
            pass
    
    async def _check_notifications_action(self):
        """Check notifications action"""
        try:
            # Try to find and click notifications icon
            notif_icon = await self.page.query_selector("a[data-test-global-nav-link='notifications']")
            if notif_icon:
                await notif_icon.click()
                await self._human_sleep(2, 4)
                
                # Click back to close
                await self.page.click("body")
                logger.debug(f"Worker {self.worker_id}: Checked notifications")
        except:
            pass
    
    async def _hover_random_elements_action(self):
        """Hover over random elements action"""
        try:
            # Find all interactive elements
            elements = await self.page.query_selector_all("a, button, [role='button']")
            
            if elements:
                # Hover over 2-3 random elements
                for _ in range(random.randint(2, 3)):
                    element = random.choice(elements)
                    await element.hover()
                    await self._human_sleep(0.5, 1.5)
                
                logger.debug(f"Worker {self.worker_id}: Hovered over random elements")
        except:
            pass
    
    async def _scrape_contact_info_modal(self):
        """
        Parses the contact information from the modal dialog.
        This function is designed to be called after the modal is visible.
        """
        contact_info = {}
        try:
            # Select all sections within the modal (e.g., for Profile, Email, etc.)
            sections = await self.page.query_selector_all("section.pv-contact-info__contact-type")
            
            for section in sections:
                # Get the header to identify the type of information
                header_elem = await section.query_selector("h3.pv-contact-info__header")
                if not header_elem:
                    continue
                
                header_text = (await header_elem.inner_text()).strip()

                # Extract data based on the header text
                if "Profile" in header_text:
                    link_elem = await section.query_selector("a")
                    if link_elem:
                        contact_info['linkedin_profile_url'] = await link_elem.get_attribute('href')
                elif "Email" in header_text:
                    email_elem = await section.query_selector("a")
                    if email_elem:
                        contact_info['email'] = (await email_elem.inner_text()).strip()
                elif "Connected" in header_text:
                    date_elem = await section.query_selector("span.t-14.t-black.t-normal")
                    if date_elem:
                        contact_info['connected_date'] = (await date_elem.inner_text()).strip()
                # This can be extended for other fields like 'Phone', 'Website', etc.

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error parsing contact info modal: {e}")
            
        return contact_info

    async def _extract_basic_info(self):
        """Extracts basic profile information and clicks to reveal/scrape contact info."""
        basic_info = {}
        
        try:
            # --- Scrape Basic Info (Name, Headline, etc.) ---
            name_elem = await self.page.query_selector("h1.t-24.v-align-middle")
            if name_elem:
                basic_info['name'] = (await name_elem.inner_text()).strip()

            headline_elem = await self.page.query_selector(".text-body-medium.break-words")
            if headline_elem:
                basic_info['headline'] = (await headline_elem.inner_text()).strip()

            location_elem = await self.page.query_selector(".text-body-small.inline.t-black--light.break-words")
            if location_elem:
                basic_info['location'] = (await location_elem.inner_text()).strip()
                
            # --- Click to Open and Scrape Contact Info Modal ---
            try:
                contact_info_link = await self.page.query_selector("a#top-card-text-details-contact-info")
                if contact_info_link:
                    await contact_info_link.click()
                    
                    # Wait for the modal to be visible
                    await self.page.wait_for_selector("div.artdeco-modal__content section.pv-contact-info__contact-type", timeout=8000)
                    
                    # Use the new helper function to scrape the modal content
                    contact_details = await self._scrape_contact_info_modal()
                    basic_info.update(contact_details)
                    
                    # Close the modal
                    close_button = await self.page.query_selector("button[aria-label='Dismiss']")
                    if close_button:
                        await close_button.click()
                        await self.page.wait_for_timeout(1000) # Give it a moment to close
                    
                    logger.info(f"Worker {self.worker_id}: Successfully scraped contact details: {contact_details}")

            except Exception as e:
                logger.warning(f"Worker {self.worker_id}: Could not open or scrape contact info modal: {e}")

            logger.info(f"Worker {self.worker_id}: Successfully extracted basic info.")
            return basic_info

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: A critical error occurred in _extract_basic_info: {e}")
            return basic_info

    def safe_get_text(self, soup_elem):
        return soup_elem.get_text(strip=True) if soup_elem else None
       
    def _is_grouped_experience(self, top_li):
        """Check if this is a grouped experience (multiple roles at same company)"""
        # Look for nested ul elements that contain multiple li items (roles)
        nested_uls = top_li.find_all("ul", recursive=True)
        for ul in nested_uls:
            # Make sure this ul is a direct child structure of the top li
            parent_li = ul.find_parent('li')
            if parent_li == top_li:
                role_lis = ul.find_all('li', recursive=False)
                # Filter out non-role items (like skills sections)
                actual_roles = []
                for li in role_lis:
                    # Check if this li contains a job title
                    title_spans = li.select('span[aria-hidden="true"]')
                    for span in title_spans:
                        text = span.get_text(strip=True)
                        # Skip if it looks like skills or other metadata
                        if text and not any(skip_word in text.lower() for skip_word in ['skills', 'see more', '…see more']):
                            # Check if parent structure suggests it's a job title
                            parent_div = span.find_parent('div')
                            if parent_div and ('hoverable-link-text' in parent_div.get('class', []) or 't-bold' in parent_div.get('class', [])):
                                actual_roles.append(li)
                                break
                
                if len(actual_roles) > 1:
                    return True
        return False

    def _extract_role(self,li, is_ungrouped):
        """Extract a single role from an li element"""
        role = {
            "title": None,
            "dates": None,
            "location": None
        }
        
        # Extract job title - look for spans with aria-hidden="true" in hoverable-link-text divs
        title_spans = li.select('div.hoverable-link-text.t-bold span[aria-hidden="true"]')
        if not title_spans:
            # Alternative selector for titles
            title_spans = li.select('span[aria-hidden="true"]')
            for span in title_spans:
                text = span.get_text(strip=True)
                if text and not any(skip_word in text.lower() for skip_word in ['skills', 'see more', '…see more', 'full-time', 'part-time', 'internship']):
                    parent_div = span.find_parent('div')
                    if parent_div and ('hoverable-link-text' in parent_div.get('class', []) or 't-bold' in parent_div.get('class', [])):
                        role["title"] = text
                        break
        else:
            role["title"] = title_spans[0].get_text(strip=True)
        
        if not role["title"]:
            return None

        # Extract dates and location using LinkedIn's standard structure
        # LinkedIn typically structures role info as: Title -> Company/Duration -> Date -> Location
        
        # Get all text spans in order
        all_spans = li.select('span[aria-hidden="true"]')
        span_texts = []
        
        for span in all_spans:
            text = span.get_text(strip=True)
            if text:
                span_texts.append(text)
        
        # Extract dates first - look for patterns with years, months, duration
        date_patterns = ['present', '20', 'yr', 'mo', 'month', 'year', ' - ', '·']
        for text in span_texts:
            if text == role["title"]:
                continue
                
            # Check if this looks like a date/duration
            text_lower = text.lower()
            has_date_indicators = any(pattern in text_lower for pattern in date_patterns)
            
            # Additional check for date format patterns
            has_date_format = (
                ('20' in text and len(text) > 4) or  # Contains year
                (' - ' in text) or  # Date range
                ('·' in text and ('yr' in text_lower or 'mo' in text_lower)) or  # Duration
                'present' in text_lower
            )
            
            if has_date_indicators and has_date_format and not role["dates"]:
                role["dates"] = text
                break
        
        # Extract location - it's typically the remaining text that's not title, dates, or company info
        for text in span_texts:
            if text == role["title"] or text == role.get("dates"):
                continue
                
            # Skip company info patterns (contains employment type indicators)
            if '·' in text and any(work_type in text.lower() for work_type in ['full-time', 'part-time', 'internship', 'contract']):
                continue
                
            # Skip if it looks like a company name (no location-specific patterns)
            text_lower = text.lower()
            
            # Generic location patterns (not hardcoded places)
            location_patterns = [
                '·', # Often separates location from work type
                ',',  # City, State format
                'area',  # Geographic area indicator
                'region',  # Geographic region
                'metroplex',  # Metro area
                'remote',  # Work arrangement
                'hybrid',  # Work arrangement
                'on-site',  # Work arrangement
                'onsite'  # Work arrangement
            ]
            
            # Check if text has location-like patterns
            has_location_pattern = any(pattern in text_lower for pattern in location_patterns)
            
            # Additional structural checks
            is_likely_location = (
                has_location_pattern or
                (', ' in text and len(text.split(', ')) >= 2) or  # City, State pattern
                text.endswith(' Area') or
                text.endswith(' Region') or
                text.endswith(' Metroplex') or
                ('·' in text and not any(work_type in text_lower for work_type in ['full-time', 'part-time', 'internship']))
            )
            
            if is_likely_location and not role["location"]:
                role["location"] = text
                break
        
        # Alternative approach: use caption wrappers which often contain structured data
        if not role["dates"] or not role["location"]:
            caption_spans = li.select('span.pvs-entity__caption-wrapper[aria-hidden="true"]')
            
            for i, span in enumerate(caption_spans):
                text = span.get_text(strip=True)
                if not text:
                    continue
                    
                # First caption wrapper is usually dates
                if i == 0 and not role["dates"]:
                    # Check if it looks like a date
                    if any(pattern in text.lower() for pattern in ['20', 'present', 'yr', 'mo', ' - ']):
                        role["dates"] = text
                        
                # Second caption wrapper or non-date text is usually location
                elif not role["location"]:
                    # If it doesn't look like a date, treat as location
                    if not any(pattern in text.lower() for pattern in ['20', 'present', 'yr', 'mo']) or '·' in text:
                        role["location"] = text

        return role
    
    def _find_experience_section(self,soup: BeautifulSoup):
        """Find the experience section in the HTML"""
        # Look for the section with id="experience" or containing "Experience" header
        exp_section = soup.find('div', {'id': 'experience'})
        if exp_section:
            return exp_section.find_parent('section')
        
        # Fallback: look for headers containing "Experience"
        headers = soup.find_all(['h2', 'h3'])
        for h in headers:
            if 'experience' in h.get_text(strip=True).lower():
                return h.find_parent('section') or h.find_parent('div')
        return None

    def _extract_experience(self, html: str) -> Dict[str, Any]:
        """Main function to extract all experience data"""
        soup = BeautifulSoup(html, "html.parser")
        experience = {}

        exp_section = self._find_experience_section(soup)
        if not exp_section:
            return experience

        exp_list = exp_section.find("ul")
        if not exp_list:
            return experience

        # Get all top-level experience items
        top_level_items = exp_list.find_all('li', recursive=False)
        
        for i, top_li in enumerate(top_level_items):
            # Skip items that don't look like experience entries
            if not top_li.select('div.hoverable-link-text'):
                continue

            company_name = None
            company_url = None
            total_period = None
            company_location = None

            # Extract company URL
            company_link_elem = top_li.select_one("a.optional-action-target-wrapper")
            if company_link_elem:
                company_url = company_link_elem.get('href')

            is_grouped = self._is_grouped_experience(top_li)

            if is_grouped:
                # For grouped experiences, extract company info from the top level
                main_div = top_li.select_one("div.display-flex.flex-column.align-self-center.flex-grow-1")
                if main_div:
                    # Look for company name in hoverable-link-text spans
                    company_elem = main_div.select_one("div.hoverable-link-text.t-bold span[aria-hidden='true']")
                    if company_elem:
                        company_name = company_elem.get_text(strip=True)

                    # Look for total period and company location
                    all_spans = main_div.select("span[aria-hidden='true']")
                    for span in all_spans:
                        text = span.get_text(strip=True)
                        if not text:
                            continue
                            
                        # Check for total period (duration)
                        if ('yr' in text.lower() or 'mo' in text.lower() or 'year' in text.lower() or 'month' in text.lower()) and not total_period:
                            total_period = text
                        # Check for company location (not company name, not duration)
                        elif not company_location and text != company_name and text != total_period:
                            # Generic location pattern detection
                            text_lower = text.lower()
                            location_patterns = [
                                'area', 'region', 'metroplex', 'county', 'district',
                                'remote', 'hybrid', 'on-site', 'onsite',
                                ',',  # Geographic separator
                                '·'   # LinkedIn separator
                            ]
                            
                            # Check if this looks like a location
                            is_location = (
                                any(pattern in text_lower for pattern in location_patterns) or
                                (', ' in text and len(text.split(', ')) >= 2) or  # City, State format
                                text.endswith(' Area') or
                                text.endswith(' Region') or
                                text.endswith(' Metroplex')
                            )
                            
                            if is_location:
                                company_location = text

                company_key = company_name if company_name else f"company_{i}"
                if company_key not in experience:
                    experience[company_key] = {
                        "company_url": company_url,
                        "total_period": total_period,
                        "positions": []
                    }
                # Extract individual roles from nested lists
                nested_uls = top_li.find_all("ul", recursive=True)
                for ul in nested_uls:
                    if ul.find_parent('li') == top_li:
                        for role_li in ul.find_all('li', recursive=False):
                            role = self._extract_role(role_li, is_ungrouped=False)
                            if role:
                                # If role doesn't have location but company does, use company location
                                if not role["location"] and company_location:
                                    role["location"] = company_location
                                experience[company_key]["positions"].append(role)
            else:
                # For ungrouped experiences, extract company info differently
                # Look for company name in the span.t-14.t-normal text (like "Zinc Technologies · Internship")
                company_span = top_li.select_one("span.t-14.t-normal span[aria-hidden='true']")
                if company_span:
                    company_text = company_span.get_text(strip=True)
                    # Extract company name (before the · symbol)
                    if '·' in company_text:
                        company_name = company_text.split('·')[0].strip()
                    else:
                        company_name = company_text
                # If no company name found, use the job title as fallback
                if not company_name:
                    title_elem = top_li.select_one("div.hoverable-link-text.t-bold span[aria-hidden='true']")
                    if title_elem:
                        company_name = title_elem.get_text(strip=True)
                
                company_key = company_name if company_name else f"company_{i}"
                # For ungrouped, each top-level li is a separate company, so always create new entry
                experience[company_key] = {
                    "company_url": company_url,
                    "total_period": total_period,
                    "positions": []
                }
                role = self._extract_role(top_li, is_ungrouped=True)
                if role:
                    experience[company_key]["positions"].append(role)

        return experience
        
    async def _extract_education(self):
        """Extract education information"""
        education = []

        try:
            # Find the education section anchor by id
            edu_section = await self.page.query_selector("div#education")
            if edu_section:
                # Try to click "show all education" if it exists
                try:
                    show_all = await self.page.query_selector(
                        ".pvs-list__footer .artdeco-button"
                    )
                    if show_all:
                        await show_all.click()
                        await self._human_sleep(2, 3)
                except Exception:
                    pass

                # Get the education list container: ul with a long class name
                edu_list = await edu_section.evaluate_handle(
                    '''node => node.parentElement.querySelector('ul.WgIFHisduBdzsrWAQusrmrSnsmWzyvZPoKDpc')'''
                )
                if edu_list:
                    edu_items = await edu_list.query_selector_all(
                        "li.artdeco-list__item"
                    )
                    for item in edu_items:
                        try:
                            edu = {}

                            # School name
                            school_elem = await item.query_selector(
                                ".mr1.hoverable-link-text.t-bold span[aria-hidden='true']"
                            )
                            if school_elem:
                                edu['school'] = (await school_elem.inner_text()).strip()

                            # Degree
                            degree_elem = await item.query_selector(
                                ".t-14.t-normal span[aria-hidden='true']"
                            )
                            if degree_elem:
                                edu['degree'] = (await degree_elem.inner_text()).strip()

                            # Date range
                            date_elem = await item.query_selector(
                                ".t-14.t-normal.t-black--light .pvs-entity__caption-wrapper[aria-hidden='true']"
                            )
                            if date_elem:
                                edu['date_range'] = (await date_elem.inner_text()).strip()

                            # Description (optional, e.g. coursework, activities)
                            desc_elem = await item.query_selector(
                                ".PmOOsbJzcyufrBWTZcPmdIKMvpIECBvYKLZYQ span[aria-hidden='true']"
                            )
                            if desc_elem:
                                edu['description'] = (await desc_elem.inner_text()).strip()

                            education.append(edu)

                        except Exception as ex:
                            logger.error(f"Worker {self.worker_id}: Error extracting individual education: {ex}")

                # Close the modal if it was opened
                try:
                    close_button = await self.page.query_selector("button.artdeco-modal__dismiss")
                    if close_button:
                        await close_button.click()
                        await self._human_sleep(1, 2)
                except Exception:
                    pass

            return education

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error extracting education section: {e}")
            return education
    
    async def _extract_certifications(self):
        """Extract certifications"""
        certifications = []
        
        try:
            # Find the certifications section
            cert_section = await self.page.query_selector("section#certifications")
            
            if cert_section:
                cert_items = await cert_section.query_selector_all(".pvs-list__item-container")
                
                for item in cert_items:
                    try:
                        cert = {}
                        
                        # Name
                        name_elem = await item.query_selector(".t-bold span[aria-hidden='true']")
                        if name_elem:
                            cert['name'] = await name_elem.inner_text()
                        
                        # Issuer
                        issuer_elem = await item.query_selector(".t-normal.t-black--light span[aria-hidden='true']")
                        if issuer_elem:
                            cert['issuer'] = await issuer_elem.inner_text()
                        
                        # Date
                        date_elements = await item.query_selector_all(".t-normal.t-black--light span[aria-hidden='true']")
                        if len(date_elements) > 1:
                            cert['date'] = await date_elements[1].inner_text()
                        
                        certifications.append(cert)
                        
                    except Exception as ex:
                        logger.error(f"Worker {self.worker_id}: Error extracting certification: {ex}")
            
            return certifications
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error extracting certifications: {e}")
            return certifications

    async def scrape_user_activity(self, profile_url, last_post_time=None, last_comment_time=None, last_reaction_time=None):
        """
        Scrapes a user's activity incrementally. It gets posts from the '/all' endpoint
        and then navigates directly to the specific URLs for comments and reactions.
        Returns both the activity_data dict and a new_times dict for state saving.
        """
        logger.info(f"[STATE] {profile_url}: last_post_time={last_post_time}, last_comment_time={last_comment_time}, last_reaction_time={last_reaction_time}")

        activity_data = {
            'posts': [],
            'comments': [],
            'reactions': [],
            'scraped_at': datetime.now().isoformat()
        }
        new_times = {
            "last_post_time": last_post_time,
            "last_comment_time": last_comment_time,
            "last_reaction_time": last_reaction_time
        }

        base_url = profile_url.rstrip('/')

        try:
            # --- 1. Scrape Posts from the '/all' Activity View ---
            all_activity_url = f"{base_url}/recent-activity/all/"
            print(f"Navigating to the 'All' activity feed for posts: {all_activity_url}")
            await self.page.goto(all_activity_url, wait_until="domcontentloaded")
            await self.page.wait_for_timeout(3000)

            no_activity = await self.page.query_selector(".pv-recent-activity-empty-container")
            if not no_activity:
                posts, most_recent_post_time = await self._extract_posts(since_timestamp=last_post_time)
                activity_data['posts'] = posts
                if most_recent_post_time:
                    new_times["last_post_time"] = most_recent_post_time
            else:
                logger.info("No activity found on the profile.")

            # --- 2. Scrape Comments from its Direct URL ---
            comments_url = f"{base_url}/recent-activity/comments/"
            print(f"Navigating directly to Comments: {comments_url}")
            await self.page.goto(comments_url, wait_until="domcontentloaded")
            await self.page.wait_for_timeout(3000)

            no_activity = await self.page.query_selector(".pv-recent-activity-empty-container")
            if not no_activity:
                comments, most_recent_comment_time = await self._extract_comments(since_timestamp=last_comment_time)
                activity_data['comments'] = comments
                if most_recent_comment_time:
                    new_times["last_comment_time"] = most_recent_comment_time
            else:
                logger.info("No comment activity found.")

            # --- 3. Scrape Reactions from its Direct URL ---
            reactions_url = f"{base_url}/recent-activity/reactions/"
            print(f"Navigating directly to Reactions: {reactions_url}")
            await self.page.goto(reactions_url, wait_until="domcontentloaded")
            await self.page.wait_for_timeout(3000)

            no_activity = await self.page.query_selector(".pv-recent-activity-empty-container")
            if not no_activity:
                reactions, most_recent_reaction_time = await self._extract_reactions(since_timestamp=last_reaction_time)
                activity_data['reactions'] = reactions
                if most_recent_reaction_time:
                    new_times["last_reaction_time"] = most_recent_reaction_time
            else:
                logger.info("No reaction activity found.")

            print("Successfully completed all activity scraping.")
            return activity_data, new_times

        except Exception as e:
            logger.error(f"A critical error occurred in scrape_user_activity: {e}")
            return activity_data, new_times

    async def efficient_scroll_page(self, page, max_scrolls=20, scroll_pause=1, patience=3):
        """
        Scrolls the main page efficiently and patiently to ensure all activity cards are loaded.
        Stops early if 0 cards are found for 'patience' consecutive scrolls.
        This is the correct method for the 'Comments' activity feed.
        """
        last_count = 0
        stagnant_scrolls = 0
        zero_scrolls = 0  # Tracks consecutive zero-card scrolls

        print("🔁 Starting smart scroll of the main page to load all activity cards...")

        for i in range(max_scrolls):
            # 1. Count the number of loaded activity cards
            current_count = await page.evaluate(
                "document.querySelectorAll('ul.display-flex.flex-wrap.list-style-none.justify-center > li').length"
            )
            
            print(f"Scroll {i+1}/{max_scrolls}: Found {current_count} cards (previously {last_count}).")

            # Stop if 0 cards found for 'patience' consecutive scrolls
            if current_count == 0:
                zero_scrolls += 1
                if zero_scrolls >= patience:
                    print(f"No cards found for {patience} consecutive scrolls. Stopping scroll.")
                    break
            else:
                zero_scrolls = 0

            # Check if scrolling has stalled (no new cards loaded for 'patience' scrolls)
            if current_count == last_count and last_count > 0:
                stagnant_scrolls += 1
                if stagnant_scrolls >= patience:
                    print(f"No new cards loaded for {patience} consecutive scrolls. Assuming all are loaded.")
                    break
            else:
                stagnant_scrolls = 0  # Reset if new content is found

            last_count = current_count

            # 3. Scroll to bottom
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

            # 4. Wait for new content to load
            await asyncio.sleep(scroll_pause)

        print(f"🏁 Finished scrolling. A total of {last_count} cards are loaded and ready for extraction.")

    def incremental_filter(self, data, since_timestamp, max_count):
        filtered = []
        most_recent = None

        cutoff = parse_linkedin_timestamp(since_timestamp) if since_timestamp else None
        for activity in data:
            activity_time = parse_linkedin_timestamp(activity.get('timestamp'))
            if cutoff and activity_time and activity_time <= cutoff:
                continue
            filtered.append(activity)
            if not most_recent or (activity_time and activity_time > most_recent):
                most_recent = activity_time
            if max_count and len(filtered) >= max_count:
                break
        return filtered, most_recent.isoformat() if most_recent else None

    async def _extract_posts(self, since_timestamp=None, max_posts=None,days_back=None):
        """
        High-performance extractor for the 'Posts' tab.
        Only returns posts newer than since_timestamp (if set).
        Returns: (filtered_posts, most_recent_time_iso)
        """
        if days_back and not since_timestamp:
            cutoff_date = datetime.now() - timedelta(days=days_back)
            since_timestamp = cutoff_date.isoformat()

        await self.efficient_scroll_page(self.page)
        try:
            print(" Starting post extraction with all selectors...")

            extraction_script = POSTS_SCRIPT

            posts_data = await self.page.evaluate(extraction_script)
            if max_posts is not None:
                posts_data = posts_data[:max_posts]
            print(f"Successfully extracted {len(posts_data)} posts with all selectors.")

            # --- Incremental filtering ---
            response = self.incremental_filter(posts_data,since_timestamp, max_posts)

            return response
        except Exception as e:
            logger.error(f"Error during universal post extraction: {e}")
            return [], None
        
    async def _extract_comments(self, since_timestamp=None, max_comments=None, days_back=None):
        """
        High-performance extractor for the 'Comments' activity tab.
        Returns only comments newer than since_timestamp, plus most recent ISO time.
        """

        if days_back and not since_timestamp:
            cutoff_date = datetime.now() - timedelta(days=days_back)
            since_timestamp = cutoff_date.isoformat()

        await self.efficient_scroll_page(self.page)
        try:
            print("Starting high-performance comment extraction with all selectors...")

            extraction_script = COMMENTS_SCRIPT

            comments = await self.page.evaluate(extraction_script)
            if max_comments is not None:
                comments = comments[:max_comments]
            print(f"Successfully extracted {len(comments)} comments using all selectors.")

            # --- Incremental filtering ---
            response = self.incremental_filter(comments,since_timestamp, max_comments)

            return response
        except Exception as e:
            logger.error(f"Error during high-performance extraction: {e}")
            return [], None

    async def _extract_reactions(self, since_timestamp=None, max_reactions=None, days_back=None):
        """
        High-performance extractor for the 'Reactions' activity tab.
        Returns only reactions newer than since_timestamp, plus most recent ISO time.
        """
        
        if days_back and not since_timestamp:
            cutoff_date = datetime.now() - timedelta(days=days_back)
            since_timestamp = cutoff_date.isoformat()

        await self.efficient_scroll_page(self.page)
        try:
            print("Starting high-performance reaction extraction with all selectors...")

            extraction_script = REACTIONS_SCRIPT 

            reactions_data = await self.page.evaluate(extraction_script)
            if max_reactions is not None:
                reactions_data = reactions_data[:max_reactions]
            print(f"✅ Successfully extracted {len(reactions_data)} reactions with all selectors.")

            # --- Incremental filtering ---
            response = self.incremental_filter(reactions_data,since_timestamp, max_reactions)

        except Exception as e:
            logger.error(f"Error during universal reaction extraction: {e}")
            return [], None

    def _save_profile_data(self, profile_data):
        """
        Save LinkedIn profile data to PostgreSQL database
        """
        try:
            conn = get_db_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Insert or update profile
            basic = profile_data.get('basic_info', {})
            cur.execute("""
                INSERT INTO profiles (
                    profile_url, name, headline, location, 
                    linkedin_profile_url, scraped_at, scraper_worker_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (profile_url) DO UPDATE SET
                    name = EXCLUDED.name,
                    headline = EXCLUDED.headline,
                    location = EXCLUDED.location,
                    linkedin_profile_url = EXCLUDED.linkedin_profile_url,
                    scraped_at = EXCLUDED.scraped_at,
                    scraper_worker_id = EXCLUDED.scraper_worker_id
                RETURNING id;
            """, (
                profile_data['profile_url'],
                basic.get('name'),
                basic.get('headline'),
                basic.get('location'),
                basic.get('linkedin_profile_url'),
                profile_data.get('scraped_at'),
                profile_data.get('scraper_worker_id')
            ))
            profile_id = cur.fetchone()['id']

            # Delete existing experiences for this profile (to handle updates)
            cur.execute("DELETE FROM experiences WHERE profile_id = %s", (profile_id,))

            # Insert experiences and positions
            for company, exp in profile_data.get('experience', {}).items():
                cur.execute("""
                    INSERT INTO experiences (profile_id, company_name, company_url, total_period)
                    VALUES (%s, %s, %s, %s) RETURNING id;
                """, (profile_id, company, exp.get('company_url'), exp.get('total_period')))
                exp_id = cur.fetchone()['id']

                # Insert positions for this experience
                for pos in exp.get('positions', []):
                    cur.execute("""
                        INSERT INTO positions (experience_id, title, dates, location)
                        VALUES (%s, %s, %s, %s);
                    """, (exp_id, pos.get('title'), pos.get('dates'), pos.get('location')))

            # Delete existing activity data for this profile (to handle updates)
            cur.execute("DELETE FROM activity_posts WHERE profile_id = %s", (profile_id,))
            cur.execute("DELETE FROM activity_comments WHERE profile_id = %s", (profile_id,))
            cur.execute("DELETE FROM activity_reactions WHERE profile_id = %s", (profile_id,))

            # Insert activity posts
            activity = profile_data.get('activity', {})
            for post in activity.get('posts', []):
                # Extract engagement numbers
                engagement = post.get('engagement', {})
                likes = int(engagement.get('likes', '0').replace(',', ''))
                comments = int(engagement.get('comments', '0').replace(',', ''))
                shares = int(engagement.get('shares', '0').replace(',', ''))
                
                cur.execute("""
                    INSERT INTO activity_posts (
                        profile_id, reposted, author_name, author_url, 
                        url, text, timestamp, likes, comments, shares
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id;
                """, (
                    profile_id,
                    bool(post.get('reposted', 0)),
                    post.get('author_name'),
                    post.get('author_url'),
                    post.get('url'),
                    post.get('text'),
                    post.get('timestamp'),
                    likes,
                    comments,
                    shares
                ))
                post_id = cur.fetchone()['id']
                
                # Insert media for this post
                for media in post.get('media', []):
                    cur.execute("""
                        INSERT INTO activity_post_media (post_id, media_type, media_url)
                        VALUES (%s, %s, %s);
                    """, (post_id, media.get('type'), media.get('url')))

            # Insert activity comments
            for comment in activity.get('comments', []):
                cur.execute("""
                    INSERT INTO activity_comments (
                        profile_id, post_owner_name, post_owner_url, 
                        post_url, parent_post_text, text, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (
                    profile_id,
                    comment.get('post_owner_name'),
                    comment.get('post_owner_url'),
                    comment.get('post_url'),
                    comment.get('parent_post_text'),
                    comment.get('text'),
                    comment.get('timestamp')
                ))

            # Insert activity reactions
            for reaction in activity.get('reactions', []):
                cur.execute("""
                    INSERT INTO activity_reactions (
                        profile_id, post_owner_name, post_owner_url, 
                        post_url, post_text, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (
                    profile_id,
                    reaction.get('post_owner_name'),
                    reaction.get('post_owner_url'),
                    reaction.get('post_url'),
                    reaction.get('post_text'),
                    reaction.get('timestamp')
                ))

            conn.commit()
            cur.close()
            conn.close()
            print(f"✅ Profile data for {basic.get('name', 'Unknown')} saved successfully!")
            return profile_id

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"❌ Error saving profile data: {e}")
            raise e
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    async def _human_sleep(self, min_seconds, max_seconds):
        """Sleep for a random duration to mimic human behavior"""
        sleep_time = random.uniform(min_seconds, max_seconds)
        await asyncio.sleep(sleep_time)
    
    async def _human_type(self, selector, text):
        """Type text like a human with variable speed and occasional mistakes"""
        try:
            # Find the element
            element = await self.page.query_selector(selector)
            if not element:
                logger.error(f"Worker {self.worker_id}: Element not found for typing: {selector}")
                return
            
            # Clear the field first
            await element.click()
            await element.focus()
            
            # Type with human-like variations
            for i, char in enumerate(text):
                # Occasionally add a typo and then correct it
                if random.random() < 0.03 and i < len(text) - 1:  # 3% chance of typo
                    typo_char = random.choice('qwertyuiop[]asdfghjkl;\'zxcvbnm,./1234567890-=')
                    await self.page.keyboard.type(typo_char)
                    await asyncio.sleep(random.uniform(0.1, 0.3))
                    await self.page.keyboard.press("Backspace")
                    await asyncio.sleep(random.uniform(0.1, 0.3))
                
                # Type the character
                await self.page.keyboard.type(char)
                
                # Variable delay between keystrokes
                if char in ' .,;:?!':  # Longer pauses after punctuation
                    await asyncio.sleep(random.uniform(0.1, 0.4))
                else:
                    await asyncio.sleep(random.uniform(0.05, 0.15))
                
                # Occasionally pause longer to simulate thinking
                if random.random() < 0.02:  # 2% chance to pause
                    await asyncio.sleep(random.uniform(0.5, 1.2))
                    
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error during human typing: {e}")

class LinkedInMassProfileScraper:
    """Distributed scraper for handling thousands of LinkedIn profiles"""
    
    def __init__(self, config):
        """Initialize the scraper with configuration"""
        self.config = config
        self.worker_pool = []
        self.profile_queue = queue.Queue()
        self.results_queue = queue.Queue()
        self.active_workers = 0
        self.processed_profiles = 0
        self.successful_profiles = 0
        self.running = False
        self.lock = threading.Lock()
        
        self.file_watcher = None
        if config.get('profile_file'):
            self.file_watcher = ProfileFileWatcher(config.get('profile_file'), self.profile_queue)
        
        # Load proxy list if provided
        self.proxies = self._load_proxies(config.get('proxy_file'))
        
        # Initialize event loop for each worker
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Initialize workers
        self._init_workers()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _load_proxies(self, proxy_file):
        """Load proxies from a file if provided"""
        proxies = []
        if not proxy_file:
            return proxies
        try:
            with open(proxy_file, 'r') as f:
                for line in f:
                    proxy = line.strip()
                    if proxy:
                        proxies.append(proxy)
            
            logger.info(f"Loaded {len(proxies)} proxies from {proxy_file}")
            return proxies
        except Exception as e:
            logger.error(f"Error loading proxies: {e}")
            return []
    
    def _init_workers(self):
        """Initialize worker threads based on configuration"""
        worker_count = self.config.get('worker_count', 1)
        credentials_list = self.config.get('credentials', [])
        
        if not credentials_list:
            logger.error("No credentials provided. Cannot initialize workers.")
            return
        logger.info(f"Initializing {worker_count} workers")
        for i in range(worker_count):
            # Assign credentials with round-robin distribution
            credentials = credentials_list[i % len(credentials_list)]
            # Assign proxy if available
            proxy = None
            if self.proxies:
                proxy = self.proxies[i % len(self.proxies)]
            # Create worker
            worker = PlaywrightProfileScraper(
                worker_id=i,
                credentials=credentials,
                proxy=proxy,
                headless=self.config.get('headless', False)
            )
            self.worker_pool.append(worker)
    
    def load_profile_urls(self, source):
        """Load profile URLs from file or database"""
        try:
            if isinstance(source, str) and os.path.isfile(source):
                # Load from file
                with open(source, 'r') as f:
                    count = 0
                    for line in f:
                        url = line.strip()
                        if url and "/in/" in url:
                            self.profile_queue.put(url)
                            count += 1
                
                logger.info(f"Loaded {count} profile URLs from {source}")
                
            elif isinstance(source, list):
                # Load from list
                count = 0
                for url in source:
                    if url and "/in/" in url:
                        self.profile_queue.put(url)
                        count += 1
                
                logger.info(f"Loaded {count} profile URLs from provided list")
                
            else:
                logger.error(f"Unsupported profile source: {source}")
                
        except Exception as e:
            logger.error(f"Error loading profile URLs: {e}")
    
    def start_scraping(self):
        """Start the scraping process with multiple workers"""
        if self.running:
            logger.warning("Scraping already in progress")
            return
            
        if self.profile_queue.empty():
            logger.error("No profiles to scrape. Load profiles first.")
            return
            
        self.running = True
        logger.info(f"Starting scraping with {len(self.worker_pool)} workers")
        
        if self.file_watcher:
            self.file_watcher.start()
    
        # Create and start worker threads
        threads = []
        for i, worker in enumerate(self.worker_pool):
            thread = threading.Thread(
                target=self._worker_thread,
                args=(worker,),
                name=f"Worker-{i}"
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
            logger.info(f"Started worker thread {i}")
        
        # Start result processor thread
        result_thread = threading.Thread(
            target=self._result_processor,
            name="ResultProcessor"
        )
        result_thread.daemon = True
        result_thread.start()
        
        # Start progress monitor thread
        monitor_thread = threading.Thread(
            target=self._progress_monitor,
            name="ProgressMonitor"
        )
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # Wait for all threads to complete
        try:
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Shutting down...")
            self._shutdown()
        
        self.running = False
        logger.info("Scraping completed")
    
    def _worker_thread(self, worker):
        """Worker thread with improved async handling"""
        with self.lock:
            self.active_workers += 1
        
        try:
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Initialize the worker
            init_success = loop.run_until_complete(worker.initialize())
            if not init_success:
                logger.error(f"Worker {worker.worker_id}: Initialization failed")
                return
                
            last_profile_time = time.time()

            while self.running:
                try:
                    # Check if worker is in cooldown
                    if worker.in_cooldown and worker.cooldown_until:
                        remaining_cooldown = (worker.cooldown_until - datetime.now()).total_seconds()
                        if remaining_cooldown > 0:
                            logger.info(f"Worker {worker.worker_id}: In cooldown for {remaining_cooldown/3600:.1f} more hours")
                            time.sleep(min(300, remaining_cooldown))  # Sleep for 5 minutes or remaining time
                            continue
                        else:
                            worker.in_cooldown = False
                            logger.info(f"Worker {worker.worker_id}: Cooldown expired")
                    
                    # Check for session timeout
                    if time.time() - last_profile_time > 1800:  # 30 minutes
                        logger.info(f"Worker {worker.worker_id}: Refreshing session due to inactivity")
                        loop.run_until_complete(worker.refresh_session())
                        last_profile_time = time.time()
                    
                    # Get next profile URL
                    try:
                        profile_url = self.profile_queue.get(timeout=5)
                        last_profile_time = time.time()
                    except queue.Empty:
                        logger.debug(f"Worker {worker.worker_id}: Queue empty, waiting...")
                        time.sleep(10)
                        continue
                    
                    # Process the profile
                    logger.info(f"Worker {worker.worker_id}: Processing {profile_url}")
                    profile_data = loop.run_until_complete(worker.scrape_profile(profile_url))
                    
                    # Put result in results queue
                    self.results_queue.put({
                        'url': profile_url,
                        'success': profile_data is not None,
                        'data': profile_data,
                        'worker_id': worker.worker_id,
                        'timestamp': datetime.now().isoformat()
                    })
                    
                    self.profile_queue.task_done()
                    
                    # If scraping failed (likely due to blocks), increase delay
                    if profile_data is None:
                        delay = random.uniform(300, 600)  # 5-10 minutes if failed
                        logger.warning(f"Worker {worker.worker_id}: Profile failed, extended wait of {delay:.1f}s")
                    else:
                        # Normal delay for successful scrapes
                        delay = random.uniform(
                            self.config.get('min_delay', 120),
                            self.config.get('max_delay', 300)
                        )
                    
                    logger.info(f"Worker {worker.worker_id}: Waiting {delay:.1f}s before next profile")
                    time.sleep(delay)
                    
                except Exception as e:
                    logger.error(f"Worker {worker.worker_id}: Error processing profile: {e}")
                    time.sleep(5)
        
        finally:
            # Clean up
            try:
                loop.run_until_complete(worker.cleanup())
                loop.close()
            except Exception as e:
                logger.error(f"Worker {worker.worker_id}: Cleanup error: {e}")
                
            with self.lock:
                self.active_workers -= 1
    
    def _result_processor(self):
        """Process and store results from workers"""
        while self.running or not self.results_queue.empty():
            try:
                # Get result with timeout
                try:
                    result = self.results_queue.get(timeout=5)
                except queue.Empty:
                    continue
                
                # Update counters
                with self.lock:
                    self.processed_profiles += 1
                    if result['success']:
                        self.successful_profiles += 1
                
                # Save combined data periodically
                if self.processed_profiles % 10 == 0:
                    self._save_progress_stats()
                
                # Mark as done
                self.results_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error processing result: {e}")
    
    def _progress_monitor(self):
        """Monitor progress"""
        while self.running or self.active_workers > 0:
            try:
                with self.lock:
                    remaining = self.profile_queue.qsize()
                    active = self.active_workers
                    processed = self.processed_profiles
                    successful = self.successful_profiles
                
                logger.info(f"Progress: {processed} processed ({successful} successful), {remaining} remaining, {active} active workers")
                
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in progress monitor: {e}")
                time.sleep(60)
    
    def _save_progress_stats(self):
        """Save progress statistics"""
        try:
            stats = {
                'timestamp': datetime.now().isoformat(),
                'total_processed': self.processed_profiles,
                'successful': self.successful_profiles,
                'remaining': self.profile_queue.qsize(),
                'active_workers': self.active_workers,
                'worker_stats': [
                    {
                        'worker_id': worker.worker_id,
                        'profiles_scraped': worker.profiles_scraped,
                        'in_cooldown': worker.in_cooldown,
                        'cooldown_until': worker.cooldown_until.isoformat() if worker.cooldown_until else None
                    }
                    for worker in self.worker_pool
                ]
            }
            
            with open(f"linkedin_data/scraping_stats_{datetime.now().strftime('%Y%m%d')}.json", 'w') as f:
                json.dump(stats, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error saving progress stats: {e}")
    
    def _signal_handler(self, sig, frame):
        """Handle termination signals"""
        logger.info(f"Received signal {sig}. Shutting down gracefully...")
        self._shutdown()
        sys.exit(0)
    
    def _shutdown(self):
        """Shutdown the scraper gracefully"""
        logger.info("Shutting down scrapers...")
        self.running = False
        
        # Final progress report
        with self.lock:
            logger.info(f"Final stats: {self.processed_profiles} processed, {self.successful_profiles} successful")
        
        if self.file_watcher:
            self.file_watcher.stop()

        # Save final stats
        self._save_progress_stats()
        
        # Give time for threads to finish current tasks
        time.sleep(5)

def main():
    """Main function to run the mass profile scraper"""
    parser = argparse.ArgumentParser(description='LinkedIn Mass Profile Scraper (Playwright version)')
    
    parser.add_argument('--profile-file', type=str, help='File containing LinkedIn profile URLs')
    parser.add_argument('--workers', type=int, default=3, help='Number of worker threads')
    parser.add_argument('--credentials-file', type=str, required=True, help='JSON file with LinkedIn credentials')
    parser.add_argument('--proxy-file', type=str, help='File containing proxy list')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    parser.add_argument('--min-delay', type=int, default=30, help='Minimum delay between profiles in seconds')
    parser.add_argument('--max-delay', type=int, default=90, help='Maximum delay between profiles in seconds')
    
    args = parser.parse_args()
    # Load credentials from file
    try:
        with open(args.credentials_file, 'r') as f:
            credentials = json.load(f)
    except Exception as e:
        logger.error(f"Error loading credentials: {e}")
        return
    
    # Configure the scraper
    config = {
        'worker_count': 1,
        'credentials': credentials,
        'proxy_file': args.proxy_file,
        # 'headless': args.headless,
        'headless': False,
        'min_delay': 120,
        'max_delay': 300,
        'profile_file': args.profile_file
    }
    
    # Initialize the scraper
    scraper = LinkedInMassProfileScraper(config)
    
    # Load profile URLs
    if args.profile_file:
        scraper.load_profile_urls(args.profile_file)
    else:
        logger.error("No profile source provided. Use --profile-file")
        return
    
    # Start scraping
    scraper.start_scraping()

if __name__ == "__main__":
    main()  