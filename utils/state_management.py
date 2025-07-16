# database/state_manager.py - Complete version with all methods

import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import logging
import random
from database.db import get_db_conn

logger = logging.getLogger(__name__)

class DatabaseStateManager:
    """
    Complete database state manager with all required methods.
    """
    
    def __init__(self):
        self._ensure_tables_exist()
    
    def _ensure_tables_exist(self):
        """Create state management tables if they don't exist"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            # Table for tracking scraper sessions
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scraper_sessions (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(255) UNIQUE NOT NULL,
                    worker_id INTEGER NOT NULL,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ended_at TIMESTAMP,
                    profiles_scraped INTEGER DEFAULT 0,
                    status VARCHAR(50) DEFAULT 'active'
                );
            """)
            
            # Table for daily statistics
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scraper_daily_stats (
                    id SERIAL PRIMARY KEY,
                    worker_id INTEGER NOT NULL,
                    date DATE NOT NULL,
                    profiles_scraped INTEGER DEFAULT 0,
                    successful INTEGER DEFAULT 0,
                    failed INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(worker_id, date)
                );
            """)
            
            # Table for worker state and cooldowns
            cur.execute("""
                CREATE TABLE IF NOT EXISTS worker_states (
                    worker_id INTEGER PRIMARY KEY,
                    in_cooldown BOOLEAN DEFAULT FALSE,
                    cooldown_until TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_profiles_scraped INTEGER DEFAULT 0,
                    status VARCHAR(50) DEFAULT 'idle',
                    max_profiles_per_session INTEGER DEFAULT 20,
                    max_profiles_per_day INTEGER DEFAULT 70
                );
            """)
            
            # Table for tracking processed profiles
            cur.execute("""
                CREATE TABLE IF NOT EXISTS processed_profiles (
                    id SERIAL PRIMARY KEY,
                    profile_url VARCHAR(500) UNIQUE NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    worker_id INTEGER,
                    session_id VARCHAR(255),
                    status VARCHAR(50) DEFAULT 'completed',
                    error_message TEXT
                );
            """)
            
            # Table for scraper progress (resume functionality)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scraper_progress (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    last_processed_index INTEGER DEFAULT 0,
                    total_profiles INTEGER DEFAULT 0,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Initialize scraper_progress with default row if empty
            cur.execute("""
                INSERT INTO scraper_progress (id, last_processed_index, total_profiles)
                VALUES (1, 0, 0)
                ON CONFLICT (id) DO NOTHING;
            """)
            
            # Create indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_profiles_url ON processed_profiles(profile_url);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_worker ON scraper_sessions(worker_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_stats_worker_date ON scraper_daily_stats(worker_id, date);")
            
            conn.commit()
            logger.info("State management tables initialized")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating state tables: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    # Session management methods
    def create_session(self, worker_id):
        """Create a new session and return session_id"""
        session_id = f"worker_{worker_id}_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO scraper_sessions (session_id, worker_id, started_at, status)
                VALUES (%s, %s, CURRENT_TIMESTAMP, 'active')
                RETURNING session_id;
            """, (session_id, worker_id))
            
            conn.commit()
            logger.info(f"Created session {session_id} for worker {worker_id}")
            return session_id
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating session: {e}")
            return None
        finally:
            cur.close()
            conn.close()
    
    def end_session(self, session_id):
        """Mark a session as ended"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                UPDATE scraper_sessions 
                SET ended_at = CURRENT_TIMESTAMP, status = 'completed'
                WHERE session_id = %s;
            """, (session_id,))
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error ending session: {e}")
        finally:
            cur.close()
            conn.close()
    
    # Profile tracking methods
    def is_processed(self, profile_url):
        """Check if URL was already successfully processed"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT 1 FROM processed_profiles 
                WHERE profile_url = %s AND status = 'completed';
            """, (profile_url,))
            
            return cur.fetchone() is not None
            
        except Exception as e:
            logger.error(f"Error checking if profile is processed: {e}")
            return False
        finally:
            cur.close()
            conn.close()
    
    def mark_processed(self, profile_url, worker_id=None, session_id=None, status='completed', error_message=None):
        """Mark URL as processed"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO processed_profiles (profile_url, worker_id, session_id, status, error_message)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (profile_url) 
                DO UPDATE SET 
                    processed_at = CURRENT_TIMESTAMP,
                    worker_id = EXCLUDED.worker_id,
                    session_id = EXCLUDED.session_id,
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message;
            """, (profile_url, worker_id, session_id, status, error_message))
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error marking profile as processed: {e}")
        finally:
            cur.close()
            conn.close()
    
    # Worker limits methods
    def get_session_count(self, session_id):
        """Get profile count for current session"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT profiles_scraped FROM scraper_sessions 
                WHERE session_id = %s;
            """, (session_id,))
            
            result = cur.fetchone()
            return result[0] if result else 0
            
        except Exception as e:
            logger.error(f"Error getting session count: {e}")
            return 0
        finally:
            cur.close()
            conn.close()
    
    def get_daily_count(self, worker_id):
        """Get today's profile count for a worker"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT profiles_scraped FROM scraper_daily_stats 
                WHERE worker_id = %s AND date = CURRENT_DATE;
            """, (worker_id,))
            
            result = cur.fetchone()
            return result[0] if result else 0
            
        except Exception as e:
            logger.error(f"Error getting daily count: {e}")
            return 0
        finally:
            cur.close()
            conn.close()
    
    def increment_counts(self, worker_id, session_id, success=True):
        """Increment both session and daily counts"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            # Update session count
            cur.execute("""
                UPDATE scraper_sessions 
                SET profiles_scraped = profiles_scraped + 1
                WHERE session_id = %s;
            """, (session_id,))
            
            # Update daily stats
            if success:
                cur.execute("""
                    INSERT INTO scraper_daily_stats (worker_id, date, profiles_scraped, successful)
                    VALUES (%s, CURRENT_DATE, 1, 1)
                    ON CONFLICT (worker_id, date) 
                    DO UPDATE SET 
                        profiles_scraped = scraper_daily_stats.profiles_scraped + 1,
                        successful = scraper_daily_stats.successful + 1,
                        updated_at = CURRENT_TIMESTAMP;
                """, (worker_id,))
            else:
                cur.execute("""
                    INSERT INTO scraper_daily_stats (worker_id, date, profiles_scraped, failed)
                    VALUES (%s, CURRENT_DATE, 1, 1)
                    ON CONFLICT (worker_id, date) 
                    DO UPDATE SET 
                        profiles_scraped = scraper_daily_stats.profiles_scraped + 1,
                        failed = scraper_daily_stats.failed + 1,
                        updated_at = CURRENT_TIMESTAMP;
                """, (worker_id,))
            
            # Update worker total
            cur.execute("""
                INSERT INTO worker_states (worker_id, total_profiles_scraped, last_activity)
                VALUES (%s, 1, CURRENT_TIMESTAMP)
                ON CONFLICT (worker_id) 
                DO UPDATE SET 
                    total_profiles_scraped = worker_states.total_profiles_scraped + 1,
                    last_activity = CURRENT_TIMESTAMP;
            """, (worker_id,))
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error incrementing counts: {e}")
        finally:
            cur.close()
            conn.close()
    
    def get_worker_limits(self, worker_id):
        """Get configured limits for a worker"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT max_profiles_per_session, max_profiles_per_day 
                FROM worker_states WHERE worker_id = %s;
            """, (worker_id,))
            
            result = cur.fetchone()
            if result:
                return result[0], result[1]
            else:
                # Default limits with randomization
                session_limit = random.randint(15, 25)
                daily_limit = random.randint(50, 80)
                
                # Store for consistency
                cur.execute("""
                    INSERT INTO worker_states (worker_id, max_profiles_per_session, max_profiles_per_day)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (worker_id) DO UPDATE SET
                        max_profiles_per_session = EXCLUDED.max_profiles_per_session,
                        max_profiles_per_day = EXCLUDED.max_profiles_per_day;
                """, (worker_id, session_limit, daily_limit))
                conn.commit()
                
                return session_limit, daily_limit
                
        except Exception as e:
            logger.error(f"Error getting worker limits: {e}")
            return 20, 70  # Default fallback
        finally:
            cur.close()
            conn.close()
    
    def should_worker_continue(self, worker_id, session_id):
        """Check if worker should continue based on limits"""
        session_limit, daily_limit = self.get_worker_limits(worker_id)
        
        # Check session limit
        session_count = self.get_session_count(session_id)
        if session_count >= session_limit:
            return False, "session_limit_reached", session_limit
        
        # Check daily limit
        daily_count = self.get_daily_count(worker_id)
        if daily_count >= daily_limit:
            return False, "daily_limit_reached", daily_limit
        
        return True, "ok", None
    
    # Cooldown management
    def set_worker_cooldown(self, worker_id, hours):
        """Set cooldown for a worker"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cooldown_until = datetime.now() + timedelta(hours=hours)
            
            cur.execute("""
                INSERT INTO worker_states (worker_id, in_cooldown, cooldown_until, status)
                VALUES (%s, TRUE, %s, 'cooldown')
                ON CONFLICT (worker_id) 
                DO UPDATE SET 
                    in_cooldown = TRUE,
                    cooldown_until = EXCLUDED.cooldown_until,
                    status = 'cooldown';
            """, (worker_id, cooldown_until))
            
            conn.commit()
            logger.info(f"Worker {worker_id} in cooldown until {cooldown_until}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error setting worker cooldown: {e}")
        finally:
            cur.close()
            conn.close()
    
    def get_worker_cooldown(self, worker_id):
        """Get cooldown info for a worker"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT in_cooldown, cooldown_until 
                FROM worker_states WHERE worker_id = %s;
            """, (worker_id,))
            
            result = cur.fetchone()
            if result and result[0]:  # in_cooldown is True
                return True, result[1]
            return False, None
            
        except Exception as e:
            logger.error(f"Error getting worker cooldown: {e}")
            return False, None
        finally:
            cur.close()
            conn.close()
    
    def clear_worker_cooldown(self, worker_id):
        """Clear cooldown for a worker"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                UPDATE worker_states 
                SET in_cooldown = FALSE, cooldown_until = NULL, status = 'idle'
                WHERE worker_id = %s;
            """, (worker_id,))
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error clearing worker cooldown: {e}")
        finally:
            cur.close()
            conn.close()
    
    # Progress tracking for resume functionality
    def update_progress(self, last_index):
        """Update last processed index"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO scraper_progress (id, last_processed_index, updated_at)
                VALUES (1, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (id) 
                DO UPDATE SET 
                    last_processed_index = EXCLUDED.last_processed_index,
                    updated_at = CURRENT_TIMESTAMP;
            """, (last_index,))
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating progress: {e}")
        finally:
            cur.close()
            conn.close()
    
    def get_last_processed_index(self):
        """Get last processed index for resume functionality"""
        conn = get_db_conn()
        cur = conn.cursor()
        
        try:
            cur.execute("SELECT last_processed_index FROM scraper_progress WHERE id = 1;")
            result = cur.fetchone()
            return result[0] if result else 0
            
        except Exception as e:
            logger.error(f"Error getting last processed index: {e}")
            return 0
        finally:
            cur.close()
            conn.close()
    
    def get_progress_report(self):
        """Get comprehensive progress report"""
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            report = {}
            
            # Overall progress
            cur.execute("""
                SELECT 
                    COUNT(*) as total_processed,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
                FROM processed_profiles;
            """)
            report['overall'] = cur.fetchone()
            
            # Today's stats by worker
            cur.execute("""
                SELECT 
                    worker_id,
                    profiles_scraped,
                    successful,
                    failed
                FROM scraper_daily_stats
                WHERE date = CURRENT_DATE
                ORDER BY worker_id;
            """)
            report['today_by_worker'] = cur.fetchall()
            
            # Active sessions
            cur.execute("""
                SELECT 
                    session_id,
                    worker_id,
                    started_at,
                    profiles_scraped
                FROM scraper_sessions
                WHERE status = 'active'
                ORDER BY started_at DESC;
            """)
            report['active_sessions'] = cur.fetchall()
            
            # Worker states
            cur.execute("""
                SELECT 
                    worker_id,
                    in_cooldown,
                    cooldown_until,
                    total_profiles_scraped,
                    status
                FROM worker_states
                ORDER BY worker_id;
            """)
            report['worker_states'] = cur.fetchall()
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating progress report: {e}")
            return {}
        finally:
            cur.close()
            conn.close()

# Test function to verify database connection and tables
def test_database_state_manager():
    """Test function to verify everything is working"""
    try:
        manager = DatabaseStateManager()
        logger.info("DatabaseStateManager initialized successfully")
        
        # Test getting last processed index
        index = manager.get_last_processed_index()
        logger.info(f"Last processed index: {index}")
        
        # Test creating a session
        session_id = manager.create_session(0)
        logger.info(f"Created session: {session_id}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing DatabaseStateManager: {e}")
        return False

if __name__ == "__main__":
    # Run test when module is executed directly
    logging.basicConfig(level=logging.INFO)
    test_database_state_manager()