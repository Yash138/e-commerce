from datetime import datetime as dt
import math

class DelayHandler:
    def __init__(self, initial_delay, max_none_counter, time_window, custom_logger, crawler):
        self.initial_delay = initial_delay
        self.delay = initial_delay
        self.max_none_counter = max_none_counter
        self.time_window = time_window
        self.none_response_timestamps = []
        self.none_counter = 0
        self.custom_logger = custom_logger  # Use custom_logger instead of logger
        self.crawler = crawler

    def _update_none_response_timestamps(self):
        """Remove timestamps older than the time window and return current count"""
        current_time = dt.now()
        self.none_response_timestamps = [
            ts for ts in self.none_response_timestamps 
            if (current_time - ts).total_seconds() <= self.time_window
        ]
        return current_time, len(self.none_response_timestamps)

    def _adjust_delay(self, new_delay):
        """Update delay for the Amazon domain"""
        self.delay = new_delay
        self.crawler.engine.downloader.slots['www.amazon.in'].delay = self.delay
        self.custom_logger.warning(f"Download delay adjusted to: {self.delay}")

    def handle_none_response(self, failed_urls, item, response):
        """Handle None response by adjusting delay and logging"""
        current_time, none_count = self._update_none_response_timestamps()
        
        # Log failed URL
        if item:
            failed_urls.append({
                'asin': item['asin'], 
                'product_url': item['product_url'], 
                'status_code': response.status, 
                'error': 'Page not loading properly'
            })
        
        # Add current timestamp
        self.none_response_timestamps.append(current_time)
        self.none_counter = none_count + 1  # Add 1 for current response
        
        self.custom_logger.warning(f"None responses in last minute: {self.none_counter}")
        
        # Calculate new delay
        if self.none_counter >= self.max_none_counter:
            new_delay = round(math.sqrt(self.delay)+1, 4)**2  # exponential increase
        else:
            new_delay = self.delay + 1  # linear increase
            
        self._adjust_delay(new_delay)
        return True  # Indicates response was handled

    def handle_successful_response(self):
        """Handle successful response by potentially decreasing delay"""
        current_time, none_count = self._update_none_response_timestamps()
        
        if not self.none_response_timestamps:
            new_delay = max(self.initial_delay, round(math.sqrt(self.delay)-1, 4)**2)
            self._adjust_delay(new_delay)
            self.custom_logger.warning("No none responses in last minute, decreasing delay")
        else:
            self.none_counter = none_count
            self.custom_logger.warning(f"Still have {self.none_counter} none responses in last minute")
        
        return False  # Indicates response was handled
