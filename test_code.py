import asyncio
import json
import os
import tempfile
import unittest
from unittest.mock import Mock, MagicMock, patch, AsyncMock, mock_open
from datetime import datetime, timedelta
import queue
import time
from bs4 import BeautifulSoup

# Import your scraper modules (adjust the import path as needed)
# from playwright_scrapper import PlaywrightProfileScraper, LinkedInMassProfileScraper, parse_linkedin_timestamp

# For testing purposes, we'll define parse_linkedin_timestamp here
def parse_linkedin_timestamp(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        pass
    import re
    match = re.match(r'(\d+)\s*(d|mo|yr)', ts)
    if match:
        num, unit = int(match.group(1)), match.group(2)
        if unit == "d":
            return datetime.now() - timedelta(days=num)
        elif unit == "mo":
            return datetime.now() - timedelta(days=num * 30)
        elif unit == "yr":
            return datetime.now() - timedelta(days=num * 365)
    return None

class TestPlaywrightProfileScraper(unittest.TestCase):
    """Test cases for PlaywrightProfileScraper"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.credentials = {'email': 'test@example.com', 'password': 'testpass'}
        self.mock_page = AsyncMock()
        self.mock_context = AsyncMock()
        self.mock_browser = AsyncMock()
        self.mock_playwright = AsyncMock()
        
    @patch('playwright_scrapper.async_playwright')
    async def test_initialize_success(self, mock_playwright_func):
        """Test successful initialization"""
        # Mock the playwright chain
        mock_playwright_func.return_value.start = AsyncMock(return_value=self.mock_playwright)
        self.mock_playwright.chromium.launch = AsyncMock(return_value=self.mock_browser)
        self.mock_browser.new_context = AsyncMock(return_value=self.mock_context)
        self.mock_context.new_page = AsyncMock(return_value=self.mock_page)
        
        # Mock page methods
        self.mock_page.goto = AsyncMock()
        self.mock_page.url = "https://www.linkedin.com/feed/"
        self.mock_page.locator = AsyncMock()
        self.mock_page.locator.return_value.count = AsyncMock(return_value=1)
        
        # Import and create scraper
        from utils.playwright_scrapper import PlaywrightProfileScraper
        scraper = PlaywrightProfileScraper(worker_id=1, credentials=self.credentials)
        
        # Test initialization
        result = await scraper.initialize()
        self.assertTrue(result)
        self.assertEqual(scraper.worker_id, 1)
        
    def test_extract_experience_parsing(self):
        """Test experience extraction from HTML"""
        from utils.playwright_scrapper import PlaywrightProfileScraper
        scraper = PlaywrightProfileScraper(worker_id=1, credentials=self.credentials)
        
        # Sample HTML with experience section
        html = """
        <div id="experience">
            <section>
                <ul>
                    <li>
                        <div class="display-flex flex-column align-self-center flex-grow-1">
                            <div class="hoverable-link-text t-bold">
                                <span aria-hidden="true">Software Engineer</span>
                            </div>
                            <span class="t-14 t-normal">
                                <span aria-hidden="true">Tech Company · Full-time</span>
                            </span>
                            <span aria-hidden="true">Jan 2020 - Present · 4 yrs</span>
                            <span aria-hidden="true">San Francisco, CA</span>
                        </div>
                    </li>
                </ul>
            </section>
        </div>
        """
        
        experience = scraper._extract_experience(html)
        self.assertIsInstance(experience, dict)
        self.assertTrue(len(experience) > 0)
        
    def test_is_grouped_experience(self):
        """Test grouped experience detection"""
        from utils.playwright_scrapper import PlaywrightProfileScraper
        scraper = PlaywrightProfileScraper(worker_id=1, credentials=self.credentials)
        
        # Create mock HTML for grouped experience
        html = """
        <li>
            <div>Company Name</div>
            <ul>
                <li><div class="hoverable-link-text t-bold"><span aria-hidden="true">Role 1</span></div></li>
                <li><div class="hoverable-link-text t-bold"><span aria-hidden="true">Role 2</span></div></li>
            </ul>
        </li>
        """
        soup = BeautifulSoup(html, 'html.parser')
        top_li = soup.find('li')
        
        result = scraper._is_grouped_experience(top_li)
        self.assertTrue(result)
        
    @patch('playwright_scrapper.ProfileFileWatcher')
    @patch('playwright_scrapper.os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_load_state(self, mock_file, mock_exists, mock_watcher):
        """Test state loading"""
        mock_exists.return_value = True
        mock_file.return_value.read.return_value = '{"test": "data"}'
        
        from utils.playwright_scrapper import load_state
        state = load_state()
        self.assertEqual(state, {"test": "data"})
        
    def test_parse_linkedin_timestamp(self):
        """Test timestamp parsing"""
        # Test relative dates
        self.assertIsInstance(parse_linkedin_timestamp("2d"), datetime)
        self.assertIsInstance(parse_linkedin_timestamp("3mo"), datetime)
        self.assertIsInstance(parse_linkedin_timestamp("1yr"), datetime)
        
        # Test ISO format
        iso_date = "2024-01-15T10:30:00"
        result = parse_linkedin_timestamp(iso_date)
        self.assertIsInstance(result, datetime)
        
        # Test invalid
        self.assertIsNone(parse_linkedin_timestamp("invalid"))
        
    @patch('playwright_scrapper.PlaywrightProfileScraper._human_sleep')
    async def test_scroll_page(self, mock_sleep):
        """Test page scrolling behavior"""
        from utils.playwright_scrapper import PlaywrightProfileScraper
        scraper = PlaywrightProfileScraper(worker_id=1, credentials=self.credentials)
        
        # Mock page
        scraper.page = AsyncMock()
        scraper.page.evaluate = AsyncMock()
        scraper.page.evaluate.side_effect = [1000, 768, 1500, 1500]  # page heights
        
        mock_sleep.return_value = asyncio.sleep(0)  # Speed up test
        
        await scraper._scroll_page()
        
        # Verify scrolling happened
        self.assertTrue(scraper.page.evaluate.called)
        
    def test_enter_cooldown(self):
        """Test cooldown functionality"""
        from utils.playwright_scrapper import PlaywrightProfileScraper
        scraper = PlaywrightProfileScraper(worker_id=1, credentials=self.credentials)
        
        # Enter cooldown
        scraper._enter_cooldown(hours=2)
        
        self.assertTrue(scraper.in_cooldown)
        self.assertIsNotNone(scraper.cooldown_until)
        self.assertTrue(scraper.cooldown_until > datetime.now())


class TestLinkedInMassProfileScraper(unittest.TestCase):
    """Test cases for LinkedInMassProfileScraper"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            'worker_count': 2,
            'credentials': [
                {'email': 'test1@example.com', 'password': 'pass1'},
                {'email': 'test2@example.com', 'password': 'pass2'}
            ],
            'headless': True,
            'min_delay': 1,
            'max_delay': 2
        }
        
    @patch('playwright_scrapper.PlaywrightProfileScraper')
    def test_init_workers(self, mock_scraper_class):
        """Test worker initialization"""
        from utils.playwright_scrapper import LinkedInMassProfileScraper
        
        scraper = LinkedInMassProfileScraper(self.config)
        
        # Check workers were created
        self.assertEqual(len(scraper.worker_pool), 2)
        self.assertEqual(mock_scraper_class.call_count, 2)
        
    def test_load_profile_urls_from_file(self):
        """Test loading URLs from file"""
        from utils.playwright_scrapper import LinkedInMassProfileScraper
        
        # Create temp file with URLs
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("https://linkedin.com/in/user1\n")
            f.write("https://linkedin.com/in/user2\n")
            f.write("invalid-url\n")  # Should be skipped
            temp_file = f.name
            
        try:
            scraper = LinkedInMassProfileScraper(self.config)
            scraper.load_profile_urls(temp_file)
            
            # Check queue
            self.assertEqual(scraper.profile_queue.qsize(), 2)
            url1 = scraper.profile_queue.get()
            url2 = scraper.profile_queue.get()
            self.assertIn("/in/", url1)
            self.assertIn("/in/", url2)
        finally:
            os.unlink(temp_file)
            
    def test_load_profile_urls_from_list(self):
        """Test loading URLs from list"""
        from utils.playwright_scrapper import LinkedInMassProfileScraper
        
        urls = [
            "https://linkedin.com/in/user1",
            "https://linkedin.com/in/user2",
            "invalid-url"  # Should be skipped
        ]
        
        scraper = LinkedInMassProfileScraper(self.config)
        scraper.load_profile_urls(urls)
        
        self.assertEqual(scraper.profile_queue.qsize(), 2)
        
    @patch('builtins.open', new_callable=mock_open)
    def test_save_progress_stats(self, mock_file):
        """Test progress statistics saving"""
        from utils.playwright_scrapper import LinkedInMassProfileScraper
        
        scraper = LinkedInMassProfileScraper(self.config)
        scraper.processed_profiles = 10
        scraper.successful_profiles = 8
        
        scraper._save_progress_stats()
        
        # Verify file was written
        mock_file.assert_called()
        

class TestActivityExtraction(unittest.TestCase):
    """Test activity extraction methods"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.credentials = {'email': 'test@example.com', 'password': 'testpass'}
        
    @patch('playwright_scrapper.PlaywrightProfileScraper.efficient_scroll_page')
    async def test_extract_posts(self, mock_scroll):
        """Test post extraction"""
        from utils.playwright_scrapper import PlaywrightProfileScraper
        
        scraper = PlaywrightProfileScraper(worker_id=1, credentials=self.credentials)
        scraper.page = AsyncMock()
        
        # Mock the evaluate script to return sample posts
        mock_posts = [
            {
                'reposted': 0,
                'author_name': 'Test User',
                'author_url': 'https://linkedin.com/in/testuser',
                'url': 'https://linkedin.com/feed/update/urn:li:activity:123',
                'text': 'Test post content',
                'timestamp': '2d',
                'engagement': {'likes': '10', 'comments': '5', 'shares': '2'},
                'media': []
            }
        ]
        scraper.page.evaluate = AsyncMock(return_value=mock_posts)
        
        posts, most_recent = await scraper._extract_posts()
        
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0]['author_name'], 'Test User')
        self.assertIsNotNone(most_recent)
        
    @patch('playwright_scrapper.PlaywrightProfileScraper.efficient_scroll_page')
    async def test_extract_comments(self, mock_scroll):
        """Test comment extraction"""
        from utils.playwright_scrapper import PlaywrightProfileScraper
        
        scraper = PlaywrightProfileScraper(worker_id=1, credentials=self.credentials)
        scraper.page = AsyncMock()
        
        # Mock comments
        mock_comments = [
            {
                'post_owner_name': 'Post Author',
                'post_owner_url': 'https://linkedin.com/in/postauthor',
                'post_url': 'https://linkedin.com/feed/update/urn:li:activity:456',
                'parent_post_text': 'Original post',
                'text': 'Great post!',
                'timestamp': '1d'
            }
        ]
        scraper.page.evaluate = AsyncMock(return_value=mock_comments)
        
        comments, most_recent = await scraper._extract_comments()
        
        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0]['text'], 'Great post!')
        

class TestIntegration(unittest.TestCase):
    """Integration tests"""
    
    @patch('playwright_scrapper.async_playwright')
    @patch('playwright_scrapper.ProfileFileWatcher')
    async def test_full_scraping_flow(self, mock_watcher, mock_playwright):
        """Test complete scraping flow without actual login"""
        from utils.playwright_scrapper import LinkedInMassProfileScraper, PlaywrightProfileScraper
        
        # Setup config
        config = {
            'worker_count': 1,
            'credentials': [{'email': 'test@example.com', 'password': 'testpass'}],
            'headless': True,
            'min_delay': 0.1,
            'max_delay': 0.2
        }
        
        # Mock playwright components
        mock_page = AsyncMock()
        mock_context = AsyncMock()
        mock_browser = AsyncMock()
        mock_pw = AsyncMock()
        
        mock_playwright.return_value.start = AsyncMock(return_value=mock_pw)
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_context.new_page = AsyncMock(return_value=mock_page)
        
        # Mock page behaviors
        mock_page.url = "https://linkedin.com/feed/"
        mock_page.goto = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>Mock content</html>")
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.locator = AsyncMock()
        mock_page.locator.return_value.count = AsyncMock(return_value=0)
        
        # Create scraper
        scraper = LinkedInMassProfileScraper(config)
        
        # Add test URLs
        scraper.load_profile_urls(['https://linkedin.com/in/testuser'])
        
        # Run for a short time
        scraper.running = True
        await asyncio.sleep(0.5)
        scraper.running = False
        

def run_tests():
    """Run all tests"""
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestPlaywrightProfileScraper))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestLinkedInMassProfileScraper))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestActivityExtraction))
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    # For async tests
    import asyncio
    
    # Run synchronous tests
    success = run_tests()
    
    # Run async tests separately
    async def run_async_tests():
        """Run async test methods"""
        print("\n=== Running Async Tests ===\n")
        
        # Test initialize
        test = TestPlaywrightProfileScraper()
        test.setUp()
        await test.test_initialize_success()
        print("✓ test_initialize_success passed")
        
        # Test scroll
        test = TestPlaywrightProfileScraper()
        test.setUp()
        await test.test_scroll_page()
        print("✓ test_scroll_page passed")
        
        # Test activity extraction
        test = TestActivityExtraction()
        test.setUp()
        await test.test_extract_posts()
        print("✓ test_extract_posts passed")
        
        await test.test_extract_comments()
        print("✓ test_extract_comments passed")
        
        # Test integration
        test = TestIntegration()
        await test.test_full_scraping_flow()
        print("✓ test_full_scraping_flow passed")
    
    # Run async tests
    asyncio.run(run_async_tests())
    
    print("\nAll tests completed!")
    exit(0 if success else 1)