"""
England Hockey Analytics - Base Scraper
Base scraper class with Selenium functionality. All scrapers extend this.
"""

import random
import time
from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Callable, Any, List

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from webdriver_manager.chrome import ChromeDriverManager

from extract.config import (
    CHROME_OPTIONS,
    USER_AGENT,
    SELENIUM_CONFIG,
    RATE_LIMITS,
    SELECTORS,
)
from extract.utils import get_logger


T = TypeVar("T")


class BaseScraper(ABC):
    """
    Base scraper class providing shared Selenium functionality.

    Features:
    - Chrome WebDriver setup with configurable headless mode
    - User-agent spoofing
    - Explicit waits with WebDriverWait
    - Loader dismissal wait
    - Rate limiting with random jitter
    - Retry mechanism for transient failures
    - Context manager for browser lifecycle

    Subclasses must implement the `scrape()` method.

    Example:
        class StandingsScraper(BaseScraper):
            def scrape(self):
                self.navigate_to("https://example.com")
                # ... scraping logic

        with StandingsScraper(headless=True) as scraper:
            scraper.scrape()
    """

    def __init__(
        self,
        headless: Optional[bool] = None,
        log_name: str = "extract",
    ) -> None:
        """
        Initialize the base scraper.

        Args:
            headless: Run Chrome in headless mode (default: from SELENIUM_CONFIG)
            log_name: Logger name for this scraper instance
        """
        self.headless = headless if headless is not None else SELENIUM_CONFIG.headless
        self.logger = get_logger(log_name)
        self.driver: Optional[WebDriver] = None
        self._wait: Optional[WebDriverWait] = None

    # =========================================================================
    # CONTEXT MANAGER
    # =========================================================================

    def __enter__(self) -> "BaseScraper":
        """Context manager entry - initialize browser."""
        self._setup_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - cleanup browser."""
        self._teardown_driver()

    # =========================================================================
    # DRIVER SETUP/TEARDOWN
    # =========================================================================

    def _setup_driver(self) -> None:
        """Initialize Chrome WebDriver with configured options."""
        self.logger.info("Setting up Chrome WebDriver...")

        options = Options()

        # Apply configured options
        for opt in CHROME_OPTIONS:
            options.add_argument(opt)

        # Headless mode
        if self.headless:
            options.add_argument("--headless=new")
            self.logger.info("Running in headless mode")

        # User agent spoofing
        options.add_argument(f"--user-agent={USER_AGENT}")

        # Disable automation flags (reduces detection)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        # Initialize driver with webdriver-manager
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

        # Configure timeouts
        self.driver.set_page_load_timeout(SELENIUM_CONFIG.page_load_timeout)
        self.driver.implicitly_wait(SELENIUM_CONFIG.implicit_wait)

        # Create explicit wait object
        self._wait = WebDriverWait(
            self.driver,
            SELENIUM_CONFIG.explicit_wait,
        )

        self.logger.info("Chrome WebDriver ready")

    def _teardown_driver(self) -> None:
        """Cleanup WebDriver resources."""
        if self.driver is not None:
            self.logger.info("Closing Chrome WebDriver...")
            self.driver.quit()
            self.driver = None
            self._wait = None

    # =========================================================================
    # NAVIGATION
    # =========================================================================

    def navigate_to(self, url: str) -> None:
        """
        Navigate to a URL and wait for loader to dismiss.

        Args:
            url: URL to navigate to
        """
        self.logger.info(f"Navigating to: {url}")
        self.driver.get(url)
        self._dismiss_cookie_banner()
        self._wait_for_loader_dismiss()
        self._rate_limit_page_load()

    def _dismiss_cookie_banner(self) -> None:
        """Dismiss cookie consent banner if present."""
        # Common cookie banner selectors to try
        cookie_selectors = [
            "#onetrust-reject-all-handler",  # OneTrust "Reject All"
            "#onetrust-accept-btn-handler",  # OneTrust "Accept"
            ".onetrust-close-btn-handler",   # OneTrust close button
            "[data-testid='cookie-policy-dialog-accept-button']",
            "button[aria-label='Accept cookies']",
            ".cookie-banner__accept",
            "#cookie-accept",
            ".js-cookie-consent-agree",
        ]

        for selector in cookie_selectors:
            try:
                # Quick check - don't wait long
                button = WebDriverWait(self.driver, 2).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                button.click()
                self.logger.info(f"Dismissed cookie banner using: {selector}")
                time.sleep(0.5)  # Brief pause after dismissal
                return
            except (TimeoutException, NoSuchElementException):
                continue

        self.logger.debug("No cookie banner found or already dismissed")

    # =========================================================================
    # WAIT HELPERS
    # =========================================================================

    def _wait_for_loader_dismiss(self) -> None:
        """Wait for the page loader element to disappear."""
        try:
            self._wait.until(
                EC.invisibility_of_element_located(
                    (By.CSS_SELECTOR, SELECTORS.loader)
                )
            )
            self.logger.debug("Loader dismissed")
        except TimeoutException:
            self.logger.debug("Loader wait timed out (may not be present)")

    def wait_for_element(
        self,
        selector: str,
        by: By = By.CSS_SELECTOR,
        timeout: Optional[float] = None,
    ) -> WebElement:
        """
        Wait for an element to be present and return it.

        Args:
            selector: CSS selector or other locator string
            by: Locator strategy (default: CSS_SELECTOR)
            timeout: Custom timeout in seconds (default: from config)

        Returns:
            WebElement when found

        Raises:
            TimeoutException: If element not found within timeout
        """
        wait = WebDriverWait(
            self.driver,
            timeout or SELENIUM_CONFIG.explicit_wait,
        )
        return wait.until(
            EC.presence_of_element_located((by, selector))
        )

    def wait_for_elements(
        self,
        selector: str,
        by: By = By.CSS_SELECTOR,
        timeout: Optional[float] = None,
    ) -> List[WebElement]:
        """
        Wait for multiple elements to be present and return them.

        Args:
            selector: CSS selector or other locator string
            by: Locator strategy (default: CSS_SELECTOR)
            timeout: Custom timeout in seconds

        Returns:
            List of WebElements when found
        """
        wait = WebDriverWait(
            self.driver,
            timeout or SELENIUM_CONFIG.explicit_wait,
        )
        wait.until(EC.presence_of_element_located((by, selector)))
        return self.driver.find_elements(by, selector)

    def wait_for_clickable(
        self,
        selector: str,
        by: By = By.CSS_SELECTOR,
        timeout: Optional[float] = None,
    ) -> WebElement:
        """
        Wait for an element to be clickable and return it.

        Args:
            selector: CSS selector or other locator string
            by: Locator strategy (default: CSS_SELECTOR)
            timeout: Custom timeout in seconds

        Returns:
            WebElement when clickable
        """
        wait = WebDriverWait(
            self.driver,
            timeout or SELENIUM_CONFIG.explicit_wait,
        )
        return wait.until(
            EC.element_to_be_clickable((by, selector))
        )

    # =========================================================================
    # RATE LIMITING
    # =========================================================================

    def _rate_limit_page_load(self) -> None:
        """Apply rate limiting delay after page load."""
        delay = self._get_random_delay(
            RATE_LIMITS.page_load_min,
            RATE_LIMITS.page_load_max,
        )
        self.logger.debug(f"Rate limit delay: {delay:.2f}s")
        time.sleep(delay)

    def _rate_limit_filter_change(self) -> None:
        """Apply rate limiting delay after filter change."""
        delay = self._get_random_delay(
            RATE_LIMITS.filter_change_min,
            RATE_LIMITS.filter_change_max,
        )
        self.logger.debug(f"Filter change delay: {delay:.2f}s")
        time.sleep(delay)

    def _rate_limit_match_detail(self) -> None:
        """Apply rate limiting delay before loading match detail page."""
        delay = self._get_random_delay(
            RATE_LIMITS.match_detail_min,
            RATE_LIMITS.match_detail_max,
        )
        self.logger.debug(f"Match detail delay: {delay:.2f}s")
        time.sleep(delay)

    def _rate_limit_error_backoff(self, attempt: int = 1) -> None:
        """
        Apply error backoff delay with exponential multiplier.

        Args:
            attempt: Current retry attempt number (1-indexed)
        """
        delay = RATE_LIMITS.error_backoff * (
            RATE_LIMITS.retry_backoff_multiplier ** (attempt - 1)
        )
        self.logger.warning(f"Error backoff delay: {delay:.2f}s (attempt {attempt})")
        time.sleep(delay)

    def _get_random_delay(self, min_delay: float, max_delay: float) -> float:
        """
        Get a random delay with jitter.

        Args:
            min_delay: Minimum delay in seconds
            max_delay: Maximum delay in seconds

        Returns:
            Random delay value with jitter applied
        """
        base_delay = random.uniform(min_delay, max_delay)
        jitter = random.uniform(RATE_LIMITS.jitter_min, RATE_LIMITS.jitter_max)
        return max(0.1, base_delay + jitter)

    # =========================================================================
    # RETRY MECHANISM
    # =========================================================================

    def with_retry(
        self,
        func: Callable[[], T],
        max_retries: Optional[int] = None,
        exceptions: tuple = (
            TimeoutException,
            NoSuchElementException,
            StaleElementReferenceException,
        ),
    ) -> T:
        """
        Execute a function with retry logic for transient failures.

        Args:
            func: Function to execute
            max_retries: Maximum retry attempts (default: from config)
            exceptions: Tuple of exception types to catch and retry

        Returns:
            Result of successful function execution

        Raises:
            Exception: If all retries exhausted
        """
        max_retries = max_retries or RATE_LIMITS.max_retries
        last_exception = None

        for attempt in range(1, max_retries + 1):
            try:
                return func()
            except exceptions as e:
                last_exception = e
                self.logger.warning(
                    f"Attempt {attempt}/{max_retries} failed: {type(e).__name__}: {e}"
                )
                if attempt < max_retries:
                    self._rate_limit_error_backoff(attempt)

        self.logger.error(f"All {max_retries} retries exhausted")
        raise last_exception

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def get_element_text(
        self,
        element: WebElement,
        selector: str,
        default: str = "",
    ) -> str:
        """
        Safely get text from a child element.

        Args:
            element: Parent WebElement
            selector: CSS selector for child
            default: Default value if not found

        Returns:
            Element text or default
        """
        try:
            child = element.find_element(By.CSS_SELECTOR, selector)
            return child.text.strip()
        except NoSuchElementException:
            return default

    def get_element_attribute(
        self,
        element: WebElement,
        selector: str,
        attribute: str,
        default: str = "",
    ) -> str:
        """
        Safely get attribute from a child element.

        Args:
            element: Parent WebElement
            selector: CSS selector for child
            attribute: Attribute name to retrieve
            default: Default value if not found

        Returns:
            Attribute value or default
        """
        try:
            child = element.find_element(By.CSS_SELECTOR, selector)
            return child.get_attribute(attribute) or default
        except NoSuchElementException:
            return default

    def get_current_url(self) -> str:
        """Get the current page URL."""
        return self.driver.current_url

    def get_page_source(self) -> str:
        """Get the current page HTML source."""
        return self.driver.page_source

    # =========================================================================
    # ABSTRACT METHOD
    # =========================================================================

    @abstractmethod
    def scrape(self) -> Any:
        """
        Execute the scraping logic.

        Subclasses must implement this method with their specific
        scraping logic.

        Returns:
            Scraping results (type depends on subclass implementation)
        """
        pass
