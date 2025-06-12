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
            if os.path.exists(path):
                with open(path, "r") as f:
                    cookies = json.load(f)
                await context.add_cookies(cookies)
                logger.info(f"Worker {self.worker_id}: Cookies loaded from {path}")
        except Exception as e:
            logger.warning(f"Worker {self.worker_id}: Could not load cookies: {e}")

    async def initialize(self):
        """Initialize Playwright browser, context, and optionally restore session via cookies."""
        try:
            # Launch Playwright
            self.playwright = await async_playwright().start()

            # Browser launch args
            browser_args = []
            if self.proxy:
                browser_args.append(f'--proxy-server={self.proxy}')

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
            user_agent = random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
                "(KHTML, like Gecko) Version/16.3 Safari/605.1.15",
            ])

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

            # --- Attempt to load cookies ---
            cookie_path = f"cookies_worker_{self.worker_id}.json"
            cookies_loaded = False
            if os.path.exists(cookie_path):
                try:
                    await self.load_cookies(self.context, cookie_path)
                    cookies_loaded = True
                    logger.info(f"Worker {self.worker_id}: Loaded cookies from {cookie_path}")
                except Exception as e:
                    logger.warning(f"Worker {self.worker_id}: Failed to load cookies: {e}")

            self.session_start_time = datetime.now()
            self.profiles_scraped = 0

            # --- If cookies were loaded, verify by hitting /feed/ ---
            if cookies_loaded:
                try:
                    await self.page.goto("https://www.linkedin.com/feed/", wait_until="networkidle")
                except Exception as nav_ex:
                    logger.error(f"Worker {self.worker_id}: Couldn't reach feed page: {nav_ex}")
                    await self.cleanup()
                    return False

                # If we're on feed and not blocked by auth-wall, we're logged in
                if "feed" in self.page.url and not await self._is_authwall_present():
                    self.is_logged_in = True
                    logger.info(f"Worker {self.worker_id}: Session restored via cookies.")
                    return True
                else:
                    logger.info(f"Worker {self.worker_id}: Cookie session invalid or authwall present.")
                    self.is_logged_in = False

            else:
                logger.info(f"Worker {self.worker_id}: No cookie file; will perform fresh login.")

            # Either no cookies, or they didn’t work → return True so login() will run later
            return True

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error initializing browser: {e}")
            logger.debug(traceback.format_exc())
            await self.cleanup()
            return False

    async def _apply_stealth_mode(self):
        """Apply stealth mode to avoid detection"""
        # JavaScript to modify navigator properties
        await self.context.add_init_script("""
        () => {
            // Function to override property
            const overrideProperty = (obj, propName, value) => {
                Object.defineProperty(obj, propName, {
                    value,
                    writable: false,
                    configurable: false,
                    enumerable: true
                });
            };
            
            // WebDriver
            overrideProperty(navigator, 'webdriver', false);
            
            // Plugins
            overrideProperty(navigator, 'plugins', {
                length: Math.floor(Math.random() * 5) + 3,
                refresh: () => {},
                item: () => {},
                namedItem: () => {},
                // Add some fake plugins
                0: { name: 'Chrome PDF Plugin', description: 'Portable Document Format', filename: 'internal-pdf-viewer' },
                1: { name: 'Chrome PDF Viewer', description: '', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                2: { name: 'Native Client', description: '', filename: 'internal-nacl-plugin' }
            });
            
            // User agent components
            const brands = [
                { brand: 'Chromium', version: '115' },
                { brand: 'Not:A-Brand', version: '8' },
                { brand: 'Google Chrome', version: '115' }
            ];
            
            // userAgentData
            if (!navigator.userAgentData) {
                overrideProperty(navigator, 'userAgentData', {
                    brands,
                    mobile: false,
                    platform: 'Windows',
                    toJSON: () => ({}),
                    getHighEntropyValues: () => Promise.resolve({
                        architecture: 'x86',
                        bitness: '64',
                        brands,
                        mobile: false,
                        model: '',
                        platform: 'Windows',
                        platformVersion: '10.0',
                        uaFullVersion: '115.0.5790.110'
                    })
                });
            }
            
            // Hardware concurrency
            overrideProperty(navigator, 'hardwareConcurrency', Math.floor(Math.random() * 8) + 4);
            
            // Device memory
            overrideProperty(navigator, 'deviceMemory', Math.pow(2, Math.floor(Math.random() * 4) + 4));
            
            // Languages
            overrideProperty(navigator, 'languages', ['en-US', 'en']);
            
            // Add Chrome object
            if (!window.chrome) {
                window.chrome = {
                    runtime: {},
                    loadTimes: () => {},
                    csi: () => {},
                    app: {}
                };
            }
        }
        """)
    
    async def cleanup(self):
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
                
                # Save screenshot for debugging
                
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
        """Scrape a LinkedIn profile with robust error handling"""
        if not self.session_start_time:
             self.session_start_time = datetime.now()

        if self.in_cooldown:
            if datetime.now() < self.cooldown_until:
                logger.info(f"Worker {self.worker_id}: In cooldown until {self.cooldown_until}")
                return None
            else:
                self.in_cooldown = False
        
        # Check if session needs to be refreshed
        if (
            datetime.now() - self.session_start_time > self.session_duration_limit or 
            self.profiles_scraped >= self.max_profiles_per_session
        ):
            logger.info(f"Worker {self.worker_id}: Session limit reached. Reinitializing browser.")
            await self.cleanup()
            await self.initialize()
            self.is_logged_in = False
        
        # Ensure logged in
        if not self.is_logged_in:
            login_success = await self.login()
            if not login_success:
                logger.error(f"Worker {self.worker_id}: Failed to login")
                return None
        
        state = load_state()
        profile_state = state.get(profile_url, {})
        last_post_time = profile_state.get("last_post_time")
        last_comment_time = profile_state.get("last_comment_time")
        last_reaction_time = profile_state.get("last_reaction_time")

        # Initialize profile data structure
        profile_data = {
            'basic_info': {},
            # 'about': '',
            'experience': [],
            # 'education': [],
            # 'skills': [],
            # 'certifications': [],
            'scraped_at': datetime.now().isoformat(),
            'profile_url': profile_url,
            'scraper_worker_id': self.worker_id
        }
        
        try:
            # Navigate to profile
            logger.info(f"Worker {self.worker_id}: Navigating to {profile_url}")
            
            await self.page.goto(profile_url, wait_until="domcontentloaded")
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
            # profile_data['about'] = await self._extract_about()
            profile_data['experience'] = self._extract_experience(html)
            # profile_data['education'] = await self._extract_education()
            # profile_data['skills'] = await self._extract_skills()
            # profile_data['certifications'] = await self._extract_certifications()

         
            
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
            
            # Increment the profile count
            self.profiles_scraped += 1
            
            # Save the profile data
            self._save_profile_data(profile_data)
             # --- NEW: Save the latest timestamps for incremental scraping ---
            if self.config.get('scrape_activity', False):
                state[profile_url] = new_times
                save_state(state)
            
            logger.info(f"Worker {self.worker_id}: Successfully scraped profile {profile_url}")
            
            # Add randomization between profiles to avoid detection
            if random.random() < 0.1:  # 10% chance to simulate more random behavior
                await self._perform_random_actions()
            
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
        """Check if we've been blocked or limited"""
        try:
            # Get page source and URL
            content = await self.page.content()
            current_url = self.page.url
            
            # Check for block indicators in content
            block_indicators = [
                "you've reached the commercial use limit",
                "please verify you're a person",
                "unusual activity from your account",
                "we've restricted your account temporarily",
                "security verification",
                "we need to verify it's you",
                "we've detected an issue with your account",
                "this profile is not available",
                "this page is no longer available",
                "this profile doesn't exist"
            ]
            
            for indicator in block_indicators:
                if indicator.lower() in content.lower():
                    logger.warning(f"Worker {self.worker_id}: Detected block indicator: '{indicator}'")
                    return True
            
            # Check for redirects
            redirect_indicators = [
                "linkedin.com/checkpoint",
                "linkedin.com/authwall",
                "linkedin.com/psettings",
                "linkedin.com/uas",
                "linkedin.com/login"
            ]
            
            for indicator in redirect_indicators:
                if indicator in current_url and "/in/" not in current_url:
                    logger.warning(f"Worker {self.worker_id}: Detected redirect to {current_url}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error checking for blocks: {e}")
            return False
    
    def _enter_cooldown(self):
        """Enter a cooldown period to avoid detection"""
        cooldown_hours = random.uniform(2, 4)
        self.cooldown_until = datetime.now() + timedelta(hours=cooldown_hours)
        self.in_cooldown = True
        
        logger.warning(f"Worker {self.worker_id}: Entering cooldown until {self.cooldown_until}")
    
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

    def _extract_role(self, li, is_ungrouped=False):
        # Title
        title = None
        title_elem = li.select_one(".mr1.hoverable-link-text.t-bold span[aria-hidden='true']")
        if not title_elem:
            title_elem = li.select_one(".mr1.hoverable-link-text.t-bold")
        if title_elem:
            title = title_elem.get_text(strip=True)

        # Date range - try multiple selectors
        date_range = None
        # First try the standard selector
        date_elem = li.select_one(".pvs-entity__caption-wrapper[aria-hidden='true']")
        if date_elem:
            date_range = date_elem.get_text(strip=True)
        else:
            # Fallback: look for spans with date patterns in t-black--light spans
            date_spans = li.select("span.t-14.t-normal.t-black--light span[aria-hidden='true']")
            for span in date_spans:
                text = span.get_text(strip=True)
                # Check if it looks like a date range (contains digits and time indicators)
                if (any(char.isdigit() for char in text) and 
                    ('·' in text or '-' in text or 'to' in text.lower()) and
                    ('mos' in text or 'yr' in text or 'month' in text or 'year' in text)):
                    date_range = text
                    break

        # Location - find spans that look like locations
        location = None
        location_spans = li.select("span.t-14.t-normal.t-black--light span[aria-hidden='true']")
        for span in location_spans:
            text = span.get_text(strip=True)
            # Location typically contains location keywords and is not the date
            if (text != date_range and 
                (',' in text or 'Remote' in text or 'India' in text or 'Mumbai' in text or 'Maharashtra' in text)):
                location = text
                break

        # Description - try multiple selectors for description content
        description = None
        desc_selectors = [
            ".sijsBosfJTeOOcQcHewSIFlxgWMjZaico span[aria-hidden='true']",
            "div.full-width.t-14.t-normal.t-black.display-flex.align-items-center span[aria-hidden='true']",
            ".inline-show-more-text span[aria-hidden='true']"
        ]
        
        for selector in desc_selectors:
            desc_elem = li.select_one(selector)
            if desc_elem:
                desc_text = desc_elem.get_text(separator="\n", strip=True)
                # Make sure it's not just skills or other metadata
                if len(desc_text) > 50:  # Only consider substantial descriptions
                    description = desc_text
                    break

        if title:
            return {
                "title": title,
                "date_range": date_range,
                "location": location,
                "description": description
            }
        return None

    def _is_grouped_experience(self, top_li):
        """
        Determine if this is grouped by analyzing HTML structure patterns, not content
        """
        # Method 1: Check for multiple nested role items (most reliable)
        nested_uls = top_li.select("ul.WgIFHisduBdzsrWAQusrmrSnsmWzyvZPoKDpc")
        
        for ul in nested_uls:
            if ul.find_parent('li') == top_li:  # Direct child ul
                role_lis = ul.find_all('li', recursive=False)
                if len(role_lis) > 1:  # Multiple roles = definitely grouped
                    return True
        
        # Method 2: Check for "Company · Job Type" pattern at MAIN LEVEL only
        # This pattern only exists in ungrouped experiences at the main level
        main_content_div = top_li.select_one("div.display-flex.flex-column.align-self-center.flex-grow-1")
        if main_content_div:
            main_row_div = main_content_div.select_one("div.display-flex.flex-row.justify-space-between")
            if main_row_div:
                # Only check spans that are direct children of the main row, not nested ones
                company_spans = main_row_div.select("span.t-14.t-normal:not(.t-black--light) span[aria-hidden='true']")
                for span in company_spans:
                    text = span.get_text(strip=True)
                    if "·" in text and not any(char.isdigit() for char in text):
                        # This looks like "Company Name · Job Type" = ungrouped
                        return False
        
        # Method 3: Check for complex sub-components structure
        # Grouped experiences have multiple nested items with role details
        sub_components = top_li.select("div.CraoorGRCeibcmAFEClEpvZwdgMjeMRZNAY.pvs-entity__sub-components")
        if sub_components:
            # Count nested items that have role-like structure (contain t-bold elements)
            nested_items_with_roles = top_li.select("li.ZbRaWpTCciIYYqsUKYLBXeWDyhYxXLKmw .mr1.hoverable-link-text.t-bold")
            if len(nested_items_with_roles) > 1:  # Multiple role titles = grouped
                return True
        
        # Method 4: Check date location patterns
        # Find the main row area first
        main_row_div = top_li.select_one("div.display-flex.flex-row.justify-space-between")
        if main_row_div:
            main_level_dates = main_row_div.select(".pvs-entity__caption-wrapper")
            nested_level_dates = top_li.select("ul.WgIFHisduBdzsrWAQusrmrSnsmWzyvZPoKDpc .pvs-entity__caption-wrapper")
            
            if main_level_dates and not nested_level_dates:
                return False  # Dates only at main level = ungrouped
            elif nested_level_dates and not main_level_dates:
                return True   # Dates only in nested items = grouped
        
        # Method 5: Fallback - if we have any nested ul structure, likely grouped
        if nested_uls:
            return True
        
        return False

    def _extract_experience(self, html):
        soup = BeautifulSoup(html, "html.parser")
        experience = {}

        exp_list = soup.select_one('ul.WgIFHisduBdzsrWAQusrmrSnsmWzyvZPoKDpc')
        if not exp_list:
            return experience

        for top_li in exp_list.find_all('li', recursive=False):
            company_name = None
            company_url = None
            total_period = None
            
            # Get company URL from any optional-action-target-wrapper link
            company_link_elem = top_li.select_one("a.optional-action-target-wrapper")
            if company_link_elem:
                company_url = company_link_elem.get('href')

            # Determine if this is grouped or ungrouped
            is_grouped = self._is_grouped_experience(top_li)
            
            if is_grouped:
                # GROUPED EXPERIENCE
                # Company name is in the FIRST t-bold element (direct child of top_li)
                # We need to be very specific to avoid nested role titles
                main_content_div = top_li.select_one("div.display-flex.flex-column.align-self-center.flex-grow-1")
                if main_content_div:
                    company_name_elem = main_content_div.select_one("div.display-flex.flex-row.justify-space-between .mr1.hoverable-link-text.t-bold span[aria-hidden='true']")
                    if not company_name_elem:
                        company_name_elem = main_content_div.select_one("div.display-flex.flex-row.justify-space-between .mr1.hoverable-link-text.t-bold")
                    if company_name_elem:
                        company_name = company_name_elem.get_text(strip=True)
                
                # Total period - get from the FIRST span.t-14.t-normal at the main level (not from nested roles)
                if main_content_div:
                    main_row_div = main_content_div.select_one("div.display-flex.flex-row.justify-space-between")
                    if main_row_div:
                        period_span = main_row_div.select_one("span.t-14.t-normal span[aria-hidden='true']")
                        if period_span:
                            text = period_span.get_text(strip=True)
                            # This should be the total period like "4 yrs 1 mo"
                            if 'yrs' in text or 'mos' in text or 'yr' in text or 'mo' in text:
                                total_period = text
                            
            else:
                # UNGROUPED EXPERIENCE
                # Company name and job type are in "Company Name · Job Type" format
                # Look for the first span.t-14.t-normal that's not in t-black--light
                company_spans = top_li.select("span.t-14.t-normal:not(.t-black--light) span[aria-hidden='true']")
                for span in company_spans:
                    text = span.get_text(strip=True)
                    if "·" in text and not any(char.isdigit() for char in text):
                        # This should be "Company Name · Job Type"
                        parts = text.split("·", 1)  # Split only on first ·
                        company_name = parts[0].strip()
                        if len(parts) > 1:
                            total_period = parts[1].strip()
                        break
                    elif text and not any(char.isdigit() for char in text) and len(text) > 3:
                        # Fallback: just company name without job type
                        company_name = text.strip()

            # Use company name as key, fallback to generated key
            company_key = company_name if company_name else f"company_{id(top_li)}"
            
            if company_key not in experience:
                experience[company_key] = {
                    "company_url": company_url,
                    "total_period": total_period,
                    "positions": []
                }

            if is_grouped:
                # GROUPED: Extract roles from nested ul elements
                nested_uls = top_li.select("ul.WgIFHisduBdzsrWAQusrmrSnsmWzyvZPoKDpc")
                for ul in nested_uls:
                    # Only process direct child ul elements
                    if ul.find_parent('li') == top_li:
                        for role_li in ul.find_all('li', recursive=False):
                            # Skip li elements that don't contain role info (like skills)
                            if role_li.select_one(".mr1.hoverable-link-text.t-bold"):
                                role = self._extract_role(role_li, is_ungrouped=False)
                                if role:
                                    experience[company_key]["positions"].append(role)
            else:
                # UNGROUPED: Extract single role from the main li
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

    async def _extract_posts(self, since_timestamp=None, max_posts=None):
        """
        High-performance extractor for the 'Posts' tab.
        Only returns posts newer than since_timestamp (if set).
        Returns: (filtered_posts, most_recent_time_iso)
        """
        await self.efficient_scroll_page(self.page)
        try:
            print(" Starting high-performance post extraction with all selectors...")

            extraction_script = """
                () => {
                    // Gather all possible post containers (li, div, and data-urn)
                    const post_cards = [
                        ...document.querySelectorAll('ul.display-flex.flex-wrap.list-style-none.justify-center > li'),
                        ...document.querySelectorAll('li.artdeco-list__item'),
                        ...document.querySelectorAll('div.fie-impression-container'),
                        ...document.querySelectorAll('div[data-urn^="urn:li:activity:"]')
                    ];
                    const seen = new Set();
                    const posts = [];

                    // Helper functions
                    const getText = (element, selectors) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return (el.innerText || el.textContent).trim();
                        }
                        return '';
                    };
                    const getAttr = (element, selectors, attr) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return el.getAttribute(attr);
                        }
                        return '';
                    };

                    for (const card of post_cards) {
                        // Deduplicate by data-urn if present
                        const postUrn = getAttr(card, ['.feed-shared-update-v2', '[data-urn^="urn:li:activity:"]'], 'data-urn');
                        if (postUrn && seen.has(postUrn)) continue;
                        if (postUrn) seen.add(postUrn);

                        // Repost detection
                        const repostHeaderText = getText(card, [
                            '.update-components-header__text-view',
                            '.feed-shared-actor__meta--repost',
                            '.update-components-actor__sub-description'
                        ]);
                        const isRepost = repostHeaderText.toLowerCase().includes('reposted this') ||
                                        repostHeaderText.toLowerCase().includes('reshared this') ? 1 : 0;

                        // Author
                        const authorNameSelectors = [
                            '.update-components-actor__title span[dir="ltr"] span[aria-hidden="true"]',
                            '.update-components-actor__name',
                            '.feed-shared-actor__name'
                        ];
                        const authorUrlSelectors = [
                            '.update-components-actor__meta-link',
                            '.update-components-actor__image',
                            '.feed-shared-actor__container-link'
                        ];
                        const postTextSelectors = [
                            '.update-components-update-v2__commentary .update-components-text',
                            '.feed-shared-update-v2__description',
                            '.feed-shared-text',
                            '.update-components-text',
                            '.feed-shared-article__description',
                            '.feed-shared-external-video__description',
                            '.feed-shared-linkedin-video__description'
                        ];
                        const timestampSelectors = [
                            '.update-components-actor__sub-description span[aria-hidden="true"]',
                            '.feed-shared-actor__sub-description span[aria-hidden="true"]'
                        ];

                        const authorName = getText(card, authorNameSelectors);
                        let authorUrl = getAttr(card, authorUrlSelectors, 'href');
                        if (authorUrl && authorUrl.startsWith('/')) {
                            authorUrl = 'https://www.linkedin.com' + authorUrl;
                        }
                        const postText = getText(card, postTextSelectors);
                        const timestampText = getText(card, timestampSelectors);

                        // Post URL from data-urn
                        const postUrl = postUrn ? `https://www.linkedin.com/feed/update/${postUrn}` : '';

                        const timestamp = timestampText.split('•')[0].trim();

                        // --- Engagement Metrics from both sides ---
                        // LEFT: reactions (likes)
                        let leftReactions = '';
                        const leftReactionsElem = card.querySelector('.social-details-social-counts__reactions--left-aligned .social-details-social-counts__reactions-count');
                        if (leftReactionsElem) {
                            leftReactions = leftReactionsElem.innerText.trim();
                        }

                        // RIGHT: reposts/shares/comments
                        let rightReposts = '';
                        let rightComments = '';
                        const rightItems = card.querySelectorAll('.social-details-social-counts__item--right-aligned');
                        rightItems.forEach(item => {
                            const text = item.innerText.trim();
                            if (/repost|share/i.test(text)) {
                                rightReposts = text.replace(/[^0-9]/g, '');
                            }
                            if (/comment/i.test(text)) {
                                rightComments = text.replace(/[^0-9]/g, '');
                            }
                        });

                        // Fallback: also parse the general engagement text as before
                        const engagementText = getText(card, [
                            '.social-details-social-counts',
                            '.feed-shared-social-counts'
                        ]);
                        const likesMatch = engagementText.match(/([\\d,.]+\\w*)\\s*(like|reaction)/i);
                        const commentsMatch = engagementText.match(/([\\d,.]+\\w*)\\s*comment/i);
                        const sharesMatch = engagementText.match(/([\\d,.]+\\w*)\\s*(repost|share)/i);

                        // Combine all sources, prefer left/right if available, else fallback to regex
                        const engagement = {
                            likes: leftReactions || (likesMatch ? likesMatch[1] : '0'),
                            comments: rightComments || (commentsMatch ? commentsMatch[1] : '0'),
                            shares: rightReposts || (sharesMatch ? sharesMatch[1] : '0')
                        };

                        // Media selectors
                        const media = [];
                        const imageElements = card.querySelectorAll(
                            '.update-components-image__image, .feed-shared-image__image, .feed-shared-image img'
                        );
                        imageElements.forEach(img => {
                            if (img.src) media.push({ type: 'image', url: img.src });
                        });
                        if (card.querySelector('.update-components-linkedin-video, .feed-shared-video, .feed-shared-external-video')) {
                            media.push({ type: 'video', present: true });
                        }

                        // Only add if we have a post URL (guarantees uniqueness and validity)
                        if (postUrl) {
                            posts.push({
                                reposted: isRepost,
                                author_name: authorName,
                                author_url: authorUrl,
                                url: postUrl,
                                text: postText,
                                timestamp: timestamp,
                                engagement: engagement,
                                media: media
                            });
                        }
                    }
                    return posts;
                }
            """

            posts_data = await self.page.evaluate(extraction_script)
            if max_posts is not None:
                posts_data = posts_data[:max_posts]
            print(f"✅ Successfully extracted {len(posts_data)} posts with all selectors.")

            # --- Incremental filtering ---
            filtered = []
            most_recent = None

            cutoff = parse_linkedin_timestamp(since_timestamp) if since_timestamp else None

            for post in posts_data:
                post_time = parse_linkedin_timestamp(post.get('timestamp'))
                if cutoff and post_time and post_time <= cutoff:
                    continue
                filtered.append(post)
                if not most_recent or (post_time and post_time > most_recent):
                    most_recent = post_time
                if max_posts and len(filtered) >= max_posts:
                    break

            return filtered, most_recent.isoformat() if most_recent else None

        except Exception as e:
            logger.error(f"Error during universal post extraction: {e}")
            return [], None

    async def efficient_scroll_page(self, page, max_scrolls=100, scroll_pause=1, patience=3):
        """
        Scrolls the main page efficiently and patiently to ensure all activity cards are loaded.
        This is the correct method for the 'Comments' activity feed.
        """
        last_count = 0
        stagnant_scrolls = 0
        
        print("🔁 Starting smart scroll of the main page to load all activity cards...")
        
        for i in range(max_scrolls):
            # 1. Count the number of loaded activity cards
            # This selector must match the main list item for each activity
            current_count = await page.evaluate(
                "document.querySelectorAll('ul.display-flex.flex-wrap.list-style-none.justify-center > li').length"
            )
            
            print(f"Scroll {i+1}/{max_scrolls}: Found {current_count} cards (previously {last_count}).")

            # 2. Check if scrolling has stalled
            if current_count == last_count and last_count > 0:
                stagnant_scrolls += 1
                if stagnant_scrolls >= patience:
                    print(f"No new cards loaded for {patience} consecutive scrolls. Assuming all are loaded.")
                    break
            else:
                stagnant_scrolls = 0  # Reset counter if new content is found
            
            last_count = current_count
            
            # 3. Scroll the entire page window to the bottom
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            
            # 4. Wait for new content to load
            await asyncio.sleep(scroll_pause)
                
        print(f"🏁 Finished scrolling. A total of {last_count} cards are loaded and ready for extraction.")

    async def _extract_comments(self, since_timestamp=None, max_comments=None):
        """
        High-performance extractor for the 'Comments' activity tab.
        Returns only comments newer than since_timestamp, plus most recent ISO time.
        """
        await self.efficient_scroll_page(self.page)
        try:
            print("Starting high-performance comment extraction with all selectors...")

            extraction_script = """
                () => {
                    // Gather all possible comment containers (li, div, and data-urn)
                    const cards = [
                        ...document.querySelectorAll('ul.display-flex.flex-wrap.list-style-none.justify-center > li'),
                        ...document.querySelectorAll('li.artdeco-list__item'),
                        ...document.querySelectorAll('div.fie-impression-container'),
                        ...document.querySelectorAll('div[data-urn^="urn:li:activity:"]')
                    ];
                    const seen = new Set();
                    const comments = [];

                    // Helper functions with fallback logic
                    const getText = (element, selectors) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return (el.innerText || el.textContent).trim();
                        }
                        return '';
                    };
                    const getAttr = (element, selectors, attr) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return el.getAttribute(attr);
                        }
                        return '';
                    };

                    for (const card of cards) {
                        // Deduplicate by data-urn if present
                        const postUrn = getAttr(card, ['.feed-shared-update-v2', '[data-urn^="urn:li:activity:"]'], 'data-urn');
                        if (postUrn && seen.has(postUrn)) continue;
                        if (postUrn) seen.add(postUrn);

                        // --- Post Owner Details (with fallbacks) ---
                        const postOwnerName = getText(card, [
                            '.update-components-actor__title span[aria-hidden="true"]',
                            '.feed-shared-actor__name',
                            '.update-components-actor__name'
                        ]);
                        const postOwnerUrl = getAttr(card, [
                            '.update-components-actor__meta-link',
                            '.feed-shared-actor__container-link'
                        ], 'href');

                        // --- Post URL (most reliable method) ---
                        const postUrl = postUrn ? `https://www.linkedin.com/feed/update/${postUrn}` : '';

                        // --- Parent Post Text (fallbacks for various layouts) ---
                        const parentPostText = getText(card, [
                            '.feed-shared-update-v2__description .update-components-text',
                            '.feed-shared-update-v2__description',
                            '.feed-shared-text',
                            '.update-components-text'
                        ]);

                        // --- Your Comment Details (with fallbacks) ---
                        const yourCommentText = getText(card, [
                            'article.comments-comment-entity .comments-comment-item__main-content span[dir="ltr"]',
                            'article.comments-comment-entity .update-components-text',
                            '.comments-comment-entity .update-components-text',
                            'article.comments-comment-entity .comments-comment-item__main-content',
                            '.comments-comment-item__main-content span[aria-hidden="true"]'
                        ]);
                        const yourCommentTimestamp = getText(card, [
                            'article.comments-comment-entity time.comments-comment-meta__data',
                            'time.comments-comment-meta__data',
                            '.comments-comment-entity time',
                            'time'
                        ]);

                        // Filter out cards that are not valid comment activities
                        // Both your comment text and the post owner's name should exist
                        if (yourCommentText && postOwnerName) {
                            comments.push({
                                "post_owner_name": postOwnerName,
                                "post_owner_url": postOwnerUrl,
                                "post_url": postUrl,
                                "parent_post_text": parentPostText,
                                "text": yourCommentText,
                                "timestamp": yourCommentTimestamp,
                            })
                        }
                    }
                    return comments;
                }
            """

            comments = await self.page.evaluate(extraction_script)
            if max_comments is not None:
                comments = comments[:max_comments]
            print(f"Successfully extracted {len(comments)} comments using all selectors.")

            # --- Incremental filtering ---
            filtered = []
            most_recent = None

            cutoff = parse_linkedin_timestamp(since_timestamp) if since_timestamp else None
            for comment in comments:
                comment_time = parse_linkedin_timestamp(comment.get('timestamp'))
                if cutoff and comment_time and comment_time <= cutoff:
                    continue
                filtered.append(comment)
                if not most_recent or (comment_time and comment_time > most_recent):
                    most_recent = comment_time
                if max_comments and len(filtered) >= max_comments:
                    break

            return filtered, most_recent.isoformat() if most_recent else None

        except Exception as e:
            logger.error(f"Error during high-performance extraction: {e}")
            return [], None

    async def _extract_reactions(self, since_timestamp=None, max_reactions=None):
        """
        High-performance extractor for the 'Reactions' activity tab.
        Returns only reactions newer than since_timestamp, plus most recent ISO time.
        """
        await self.efficient_scroll_page(self.page)
        try:
            print("Starting high-performance reaction extraction with all selectors...")

            extraction_script = """
                () => {
                    // Gather all possible reaction containers (li, div, and data-urn)
                    const cards = [
                        ...document.querySelectorAll('ul.display-flex.flex-wrap.list-style-none.justify-center > li'),
                        ...document.querySelectorAll('li.artdeco-list__item'),
                        ...document.querySelectorAll('div.fie-impression-container'),
                        ...document.querySelectorAll('div[data-urn^="urn:li:activity:"]')
                    ];
                    const seen = new Set();
                    const reactions = [];

                    // Helper functions with fallback logic
                    const getText = (element, selectors) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return (el.innerText || el.textContent).trim();
                        }
                        return '';
                    };
                    const getAttr = (element, selectors, attr) => {
                        for (const selector of selectors) {
                            const el = element.querySelector(selector);
                            if (el) return el.getAttribute(attr);
                        }
                        return '';
                    };

                    for (const card of cards) {
                        // Deduplicate by data-urn if present
                        const postUrn = getAttr(card, ['.feed-shared-update-v2', '[data-urn^="urn:li:activity:"]'], 'data-urn');
                        if (postUrn && seen.has(postUrn)) continue;
                        if (postUrn) seen.add(postUrn);

                        // --- Post Owner Details (with fallbacks) ---
                        const postOwnerName = getText(card, [
                            '.update-components-actor__title span[aria-hidden="true"]',
                            '.feed-shared-actor__name',
                            '.update-components-actor__name'
                        ]);
                        const postOwnerUrl = getAttr(card, [
                            '.update-components-actor__meta-link',
                            '.feed-shared-actor__container-link'
                        ], 'href');

                        // --- Post URL (most reliable method) ---
                        const postUrl = postUrn ? `https://www.linkedin.com/feed/update/${postUrn}` : '';

                        // --- Post Text (fallbacks for various layouts) ---
                        const postText = getText(card, [
                            '.feed-shared-update-v2__description .update-components-text',
                            '.feed-shared-update-v2__description',
                            '.feed-shared-text',
                            '.update-components-text'
                        ]);

                        // --- Timestamp (with fallbacks) ---
                        const timestampText = getText(card, [
                            '.update-components-actor__sub-description span[aria-hidden="true"]',
                            '.feed-shared-actor__sub-description span[aria-hidden="true"]'
                        ]);
                        const timestamp = timestampText.split('•')[0].trim();

                        // Only add if we have a post owner and a post URL
                        if (postOwnerName && postUrl) {
                            reactions.push({
                                post_owner_name: postOwnerName,
                                post_owner_url: postOwnerUrl,
                                post_url: postUrl,
                                post_text: postText,
                                timestamp: timestamp
                            });
                        }
                    }
                    return reactions;
                }
            """

            reactions_data = await self.page.evaluate(extraction_script)
            if max_reactions is not None:
                reactions_data = reactions_data[:max_reactions]
            print(f"✅ Successfully extracted {len(reactions_data)} reactions with all selectors.")

            # --- Incremental filtering ---
            filtered = []
            most_recent = None

            cutoff = parse_linkedin_timestamp(since_timestamp) if since_timestamp else None

            for reaction in reactions_data:
                reaction_time = parse_linkedin_timestamp(reaction.get('timestamp'))
                if cutoff and reaction_time and reaction_time <= cutoff:
                    continue
                filtered.append(reaction)
                if not most_recent or (reaction_time and reaction_time > most_recent):
                    most_recent = reaction_time
                if max_reactions and len(filtered) >= max_reactions:
                    break

            return filtered, most_recent.isoformat() if most_recent else None

        except Exception as e:
            logger.error(f"Error during universal reaction extraction: {e}")
            return [], None

    def _save_profile_data(self, profile_data):
        """Save profile data to a JSON file"""
        try:
            if not profile_data:
                return
                
            # Create directory if it doesn't exist
            os.makedirs("linkedin_data", exist_ok=True)
            
            # Create filename from name or URL
            if 'basic_info' in profile_data and 'name' in profile_data['basic_info'] and profile_data['basic_info']['name']:
                safe_name = re.sub(r'[^\w\s-]', '', profile_data['basic_info']['name'])
                safe_name = re.sub(r'[\s-]+', '_', safe_name).strip('_').lower()
                filename = f"linkedin_data/profile_{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            else:
                # Extract username from URL
                username = re.search(r'/in/([^/]+)/?', profile_data['profile_url'])
                if username:
                    filename = f"linkedin_data/profile_{username.group(1)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                else:
                    # Use timestamp if no username or name available
                    filename = f"linkedin_data/profile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(profile_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Worker {self.worker_id}: Saved profile data to {filename}")
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error saving profile data: {e}")
    
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
        worker_count = self.config.get('worker_count', 3)
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
        """Worker thread function that processes profiles"""
        with self.lock:
            self.active_workers += 1
        
        try:
            # Create event loop for this thread
            asyncio.set_event_loop(asyncio.new_event_loop())
            
            # Initialize the worker
            init_success = asyncio.get_event_loop().run_until_complete(worker.initialize())
            if not init_success or not worker.page:
                logger.error(f"Worker {worker.worker_id}: Initialization failed. Skipping this worker.")
                return  # Exit this thread, don't continue
                        
            while self.running:
                try:
                    # Get next profile URL with timeout
                    try:
                        profile_url = self.profile_queue.get(timeout=5)
                    except queue.Empty:
                        logger.info(f"Worker {worker.worker_id}: No more profiles in queue")
                        break
                    
                    # Process the profile
                    logger.info(f"Worker {worker.worker_id}: Processing {profile_url}")
                    profile_data = asyncio.get_event_loop().run_until_complete(worker.scrape_profile(profile_url))
                    
                    # Put result in results queue
                    self.results_queue.put({
                        'url': profile_url,
                        'success': profile_data is not None,
                        'data': profile_data,
                        'worker_id': worker.worker_id,
                        'timestamp': datetime.now().isoformat()
                    })
                    
                    # Mark task as done
                    self.profile_queue.task_done()
                    
                    # Random delay between profiles
                    delay = random.uniform(
                        self.config.get('min_delay', 30),
                        self.config.get('max_delay', 90)
                    )
                    
                    logger.info(f"Worker {worker.worker_id}: Waiting {delay:.1f}s before next profile")
                    time.sleep(delay)
                    
                except Exception as e:
                    logger.error(f"Worker {worker.worker_id}: Error processing profile: {e}")
                    logger.debug(traceback.format_exc())
                    time.sleep(5)  # Brief pause after error
        
        finally:
            # Clean up
            asyncio.get_event_loop().run_until_complete(worker.cleanup())
                
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
        """Monitor and report progress"""
        while self.running or self.active_workers > 0:
            try:
                with self.lock:
                    remaining = self.profile_queue.qsize()
                    active = self.active_workers
                    processed = self.processed_profiles
                    successful = self.successful_profiles
                
                logger.info(f"Progress: {processed} processed ({successful} successful), {remaining} remaining, {active} active workers")
                
                # Estimate completion time
                if processed > 0 and remaining > 0:
                    avg_worker_rate = processed / (sum(w.profiles_scraped for w in self.worker_pool) or 1)
                    estimated_hours = (remaining / (active or 1)) / avg_worker_rate / 3600
                    logger.info(f"Estimated completion time: {estimated_hours:.1f} hours")
                
                time.sleep(60)  # Update every minute
                
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
        'worker_count': args.workers,
        'credentials': credentials,
        'proxy_file': args.proxy_file,
        # 'headless': args.headless,
        'headless': False,
        'min_delay': args.min_delay,
        'max_delay': args.max_delay
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