import os
import threading
import time
import logging
import queue


logger = logging.getLogger("ProfileWatcher")
logging.basicConfig(level=logging.INFO)

class ProfileFileWatcher:
    """Watches a profile URL file for changes and loads new profiles"""
    
    def __init__(self, file_path, profile_queue):
        """Initialize the watcher"""
        self.file_path = file_path
        self.profile_queue = profile_queue
        self.processed_urls = set()
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
                
            # Load state to get previously processed URLs
            try:
                from playwright_scrapper import load_state, save_state
                state = load_state()
                processed_urls = state.get("processed_urls", [])
                
                # Update the in-memory set with URLs from state
                self.processed_urls.update(processed_urls)
            except Exception as e:
                logger.warning(f"Could not load state: {e}")
                
            with open(self.file_path, 'r') as f:
                new_count = 0
                for line in f:
                    url = line.strip()
                    if url and "/in/" in url and url not in self.processed_urls:
                        self.profile_queue.put(url)
                        self.processed_urls.add(url)
                        new_count += 1
            
            # Save processed URLs back to state
            try:
                state = load_state()  # Reload to avoid overwriting other changes
                state["processed_urls"] = list(self.processed_urls)
                save_state(state)
            except Exception as e:
                logger.warning(f"Could not save state: {e}")
            
            if new_count > 0:
                logger.info(f"Added {new_count} new profiles to the queue")
        except Exception as e:
            logger.error(f"Error loading new profiles: {e}")