import os
import threading
import time
import logging
import queue
import csv

logger = logging.getLogger("ProfileWatcher")
logging.basicConfig(level=logging.INFO)

class ProfileFileWatcher:
    """Watches a profile URL file for changes and loads new profiles"""
    
    def __init__(self, file_path, profile_queue):
        """Initialize the watcher"""
        self.file_path = file_path
        self.profile_queue = profile_queue
        self.loaded_urls = set()  # Changed from processed_urls to loaded_urls
        self.last_modified = 0
        self.running = False
        self.thread = None
    
    def start(self):
        """Start the watcher thread"""
        self.running = True
        self.thread = threading.Thread(target=self._watch_loop, name="ProfileWatcher")
        self.thread.daemon = True
        self.thread.start()
        logger.info(f"Started profile file watcher for {self.file_path}")
    
    def stop(self):
        """Stop the watcher thread"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)
        logger.info("Stopped profile file watcher")
    
    def _watch_loop(self):
        """Main watcher loop"""
        # Initial load
        self._load_new_profiles()
        
        while self.running:
            try:
                # Check if file was modified
                if os.path.exists(self.file_path):
                    modified_time = os.path.getmtime(self.file_path)
                    if modified_time > self.last_modified:
                        logger.info(f"Profile file {self.file_path} was modified. Loading new profiles.")
                        self._load_new_profiles()
                        self.last_modified = modified_time
            except Exception as e:
                logger.error(f"Error watching profile file: {e}")
                
            # Check every 60 seconds
            time.sleep(60)
    
    def _load_new_profiles(self):
        """Load new profiles from the file"""
        try:
            if not os.path.exists(self.file_path):
                logger.error(f"Profile file {self.file_path} does not exist")
                return
            
            new_count = 0
            
            # Check if it's a CSV file
            if self.file_path.endswith('.csv'):
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    csv_reader = csv.DictReader(f)
                    
                    for row in csv_reader:
                        try:
                            profile_url = row.get('profile_url', '').strip()
                            
                            # Validate URL
                            if profile_url and "linkedin.com/in/" in profile_url and profile_url not in self.loaded_urls:
                                # Create profile data dict
                                profile_data = {
                                    'first_name': row.get('first_name', '').strip(),
                                    'last_name': row.get('last_name', '').strip(),
                                    'company_name': row.get('company_name', '').strip(),
                                    'profile_url': profile_url
                                }
                                
                                self.profile_queue.put(profile_data)
                                self.loaded_urls.add(profile_url)
                                new_count += 1
                                
                        except Exception as e:
                            logger.warning(f"Error processing CSV row: {e}")
                            continue
            
            if new_count > 0:
                logger.info(f"Added {new_count} new profiles to the queue")
                
        except Exception as e:
            logger.error(f"Error loading new profiles: {e}")