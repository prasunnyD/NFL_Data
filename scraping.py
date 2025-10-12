import pandas as pd
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page
import time
import random
from typing import List, Dict, Optional, Tuple
import logging
from dataclasses import dataclass


@dataclass
class ScrapingResult:
    """Data class to hold scraping results"""
    url: str
    success: bool
    dataframe: Optional[pd.DataFrame] = None
    error: Optional[str] = None
    tables_found: int = 0


class PlaywrightScraper:
    """
    A robust web scraper using Playwright with session reuse, rate limiting,
    and IP protection mechanisms to avoid getting blocked.
    """
    
    def __init__(self, 
                 headless: bool = True,
                 min_delay: float = 1.0,
                 max_delay: float = 3.0,
                 max_retries: int = 3,
                 timeout: int = 30000):
        """
        Initialize the scraper with configuration options.
        
        Args:
            headless: Whether to run browser in headless mode
            min_delay: Minimum delay between requests (seconds)
            max_delay: Maximum delay between requests (seconds)
            max_retries: Maximum number of retries for failed requests
            timeout: Page load timeout in milliseconds
        """
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.timeout = timeout
        
        # User agents for rotation
        self.user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0'
        ]
        
        # Browser and context instances
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self):
        """Context manager entry - initialize browser session"""
        self.start_session()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup browser session"""
        self.close_session()
    
    def start_session(self):
        """Start a new browser session"""
        try:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-dev-shm-usage',
                    '--no-first-run',
                    '--disable-default-apps'
                ]
            )
            self.logger.info("Browser session started successfully")
        except Exception as e:
            self.logger.error(f"Failed to start browser session: {e}")
            raise
    
    def close_session(self):
        """Close the browser session and cleanup resources"""
        try:
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            self.logger.info("Browser session closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing browser session: {e}")
    
    def _get_random_user_agent(self) -> str:
        """Get a random user agent for request rotation"""
        return random.choice(self.user_agents)
    
    def _random_delay(self):
        """Add random delay between requests to avoid detection"""
        delay = random.uniform(self.min_delay, self.max_delay)
        self.logger.debug(f"Waiting {delay:.2f} seconds...")
        time.sleep(delay)
    
    def _create_new_context(self) -> BrowserContext:
        """Create a new browser context with random user agent"""
        return self.browser.new_context(
            user_agent=self._get_random_user_agent(),
            viewport={'width': 1920, 'height': 1080},
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
    
    def scrape_url(self, url: str, wait_time: int = 3, table_index: int = 0) -> ScrapingResult:
        """
        Scrape a single URL and return the result.
        
        Args:
            url: URL to scrape
            wait_time: Time to wait for content to load
            table_index: Index of table to return (0 for first table)
            
        Returns:
            ScrapingResult object with success status and data
        """
        result = ScrapingResult(url=url, success=False)
        
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Scraping {url} (attempt {attempt + 1}/{self.max_retries})")
                
                # Create new context for each request to avoid detection
                context = self._create_new_context()
                page = context.new_page()
                
                # Navigate to the page
                page.goto(url, wait_until='networkidle', timeout=self.timeout)
                
                # Wait for content to load
                self.logger.debug(f"Waiting {wait_time} seconds for content to load...")
                time.sleep(wait_time)
                
                # Get the page content
                html_content = page.content()
                
                # Use pandas to parse the HTML tables
                tables = pd.read_html(html_content)
                
                if tables:
                    result.tables_found = len(tables)
                    if table_index < len(tables):
                        result.dataframe = tables[table_index]
                        result.success = True
                        self.logger.info(f"Successfully scraped {url} - found {len(tables)} table(s)")
                    else:
                        result.error = f"Table index {table_index} not found. Only {len(tables)} tables available."
                        self.logger.warning(result.error)
                else:
                    result.error = "No tables found on the page"
                    self.logger.warning(f"No tables found on {url}")
                
                # Close context
                context.close()
                
                # If successful, break out of retry loop
                if result.success:
                    break
                    
            except Exception as e:
                result.error = str(e)
                self.logger.error(f"Error scraping {url} (attempt {attempt + 1}): {e}")
                
                # Add extra delay before retry
                if attempt < self.max_retries - 1:
                    retry_delay = random.uniform(2, 5)
                    self.logger.info(f"Retrying in {retry_delay:.2f} seconds...")
                    time.sleep(retry_delay)
        
        return result
    
    def scrape_multiple_urls(self, urls: List[str], wait_time: int = 3, table_index: int = 0) -> List[ScrapingResult]:
        """
        Scrape multiple URLs with rate limiting and session reuse.
        
        Args:
            urls: List of URLs to scrape
            wait_time: Time to wait for content to load on each page
            table_index: Index of table to return for each URL
            
        Returns:
            List of ScrapingResult objects
        """
        if not self.browser:
            raise RuntimeError("Browser session not started. Use start_session() or context manager.")
        
        results = []
        total_urls = len(urls)
        
        self.logger.info(f"Starting to scrape {total_urls} URLs...")
        
        for i, url in enumerate(urls, 1):
            self.logger.info(f"Processing URL {i}/{total_urls}: {url}")
            
            # Scrape the URL
            result = self.scrape_url(url, wait_time, table_index)
            results.append(result)
            
            # Add delay between requests (except for the last one)
            if i < total_urls:
                self._random_delay()
        
        # Log summary
        successful = sum(1 for r in results if r.success)
        self.logger.info(f"Scraping completed: {successful}/{total_urls} successful")
        
        return results
    
    def get_successful_dataframes(self, results: List[ScrapingResult]) -> List[pd.DataFrame]:
        """Extract all successful dataframes from results"""
        return [r.dataframe for r in results if r.success and r.dataframe is not None]
    
    def get_failed_urls(self, results: List[ScrapingResult]) -> List[str]:
        """Get list of URLs that failed to scrape"""
        return [r.url for r in results if not r.success]
