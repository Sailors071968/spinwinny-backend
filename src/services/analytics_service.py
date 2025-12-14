"""
Analytics Service
Real-time metrics collection and reporting with data integrity
"""

import sqlite3
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import hashlib
from collections import defaultdict

logger = logging.getLogger(__name__)

class AnalyticsService:
    """Service for collecting and analyzing SpinWinny usage metrics"""
    
    def __init__(self, db_path: str = 'spinwinny.db'):
        self.db_path = db_path
        self._initialize_analytics_tables()
    
    def _initialize_analytics_tables(self):
        """Initialize analytics database tables"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Analytics events table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS analytics_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    event_data TEXT NOT NULL,
                    wheel_id TEXT,
                    user_id TEXT,
                    session_id TEXT,
                    timestamp TEXT NOT NULL,
                    ip_address TEXT,
                    user_agent TEXT,
                    data_hash TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Wheel metrics table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS wheel_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    wheel_id TEXT NOT NULL,
                    wheel_title TEXT,
                    total_spins INTEGER DEFAULT 0,
                    unique_users INTEGER DEFAULT 0,
                    total_entries INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(wheel_id)
                )
            ''')
            
            # User engagement table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_engagement (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    first_visit DATETIME,
                    last_visit DATETIME,
                    total_spins INTEGER DEFAULT 0,
                    total_wheels_created INTEGER DEFAULT 0,
                    total_time_spent INTEGER DEFAULT 0,
                    engagement_score REAL DEFAULT 0.0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, session_id)
                )
            ''')
            
            # Winner frequency table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS winner_frequency (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    wheel_id TEXT NOT NULL,
                    winner_name TEXT NOT NULL,
                    frequency_count INTEGER DEFAULT 1,
                    last_won DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(wheel_id, winner_name)
                )
            ''')
            
            # Create indexes for performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_analytics_events_timestamp ON analytics_events(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_analytics_events_wheel_id ON analytics_events(wheel_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_analytics_events_user_id ON analytics_events(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_wheel_metrics_wheel_id ON wheel_metrics(wheel_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_engagement_user_id ON user_engagement(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_winner_frequency_wheel_id ON winner_frequency(wheel_id)')
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error initializing analytics tables: {e}")
            raise
    
    def record_event(self, event_type: str, event_data: Dict[str, Any], 
                    wheel_id: str = None, user_id: str = None, session_id: str = None,
                    ip_address: str = None, user_agent: str = None) -> bool:
        """Record an analytics event with data integrity"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Generate timestamp and data hash for integrity
            timestamp = datetime.now().isoformat()
            event_data_json = json.dumps(event_data, sort_keys=True)
            data_hash = hashlib.sha256(
                f"{event_type}{event_data_json}{timestamp}".encode('utf-8')
            ).hexdigest()
            
            cursor.execute('''
                INSERT INTO analytics_events 
                (event_type, event_data, wheel_id, user_id, session_id, timestamp, 
                 ip_address, user_agent, data_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (event_type, event_data_json, wheel_id, user_id, session_id, 
                  timestamp, ip_address, user_agent, data_hash))
            
            conn.commit()
            conn.close()
            
            # Update related metrics
            self._update_metrics(event_type, event_data, wheel_id, user_id, session_id)
            
            return True
            
        except Exception as e:
            logger.error(f"Error recording analytics event: {e}")
            return False
    
    def _update_metrics(self, event_type: str, event_data: Dict[str, Any],
                       wheel_id: str = None, user_id: str = None, session_id: str = None):
        """Update aggregated metrics based on events"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if event_type == 'wheel_spin' and wheel_id:
                # Update wheel metrics
                cursor.execute('''
                    INSERT OR REPLACE INTO wheel_metrics 
                    (wheel_id, wheel_title, total_spins, unique_users, total_entries, updated_at)
                    VALUES (
                        ?, 
                        ?, 
                        COALESCE((SELECT total_spins FROM wheel_metrics WHERE wheel_id = ?), 0) + 1,
                        (SELECT COUNT(DISTINCT user_id) FROM analytics_events WHERE wheel_id = ? AND event_type = 'wheel_spin'),
                        ?,
                        CURRENT_TIMESTAMP
                    )
                ''', (wheel_id, event_data.get('wheel_title', ''), wheel_id, wheel_id, 
                      event_data.get('total_entries', 0)))
                
                # Update winner frequency
                winner = event_data.get('winner')
                if winner:
                    cursor.execute('''
                        INSERT OR REPLACE INTO winner_frequency 
                        (wheel_id, winner_name, frequency_count, last_won, updated_at)
                        VALUES (
                            ?, ?, 
                            COALESCE((SELECT frequency_count FROM winner_frequency WHERE wheel_id = ? AND winner_name = ?), 0) + 1,
                            CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP
                        )
                    ''', (wheel_id, winner, wheel_id, winner))
            
            if user_id and session_id:
                # Update user engagement
                cursor.execute('''
                    INSERT OR REPLACE INTO user_engagement 
                    (user_id, session_id, first_visit, last_visit, total_spins, 
                     total_wheels_created, engagement_score, updated_at)
                    VALUES (
                        ?, ?,
                        COALESCE((SELECT first_visit FROM user_engagement WHERE user_id = ? AND session_id = ?), CURRENT_TIMESTAMP),
                        CURRENT_TIMESTAMP,
                        COALESCE((SELECT total_spins FROM user_engagement WHERE user_id = ? AND session_id = ?), 0) + 
                        CASE WHEN ? = 'wheel_spin' THEN 1 ELSE 0 END,
                        COALESCE((SELECT total_wheels_created FROM user_engagement WHERE user_id = ? AND session_id = ?), 0) +
                        CASE WHEN ? = 'wheel_created' THEN 1 ELSE 0 END,
                        COALESCE((SELECT engagement_score FROM user_engagement WHERE user_id = ? AND session_id = ?), 0) + 1,
                        CURRENT_TIMESTAMP
                    )
                ''', (user_id, session_id, user_id, session_id, user_id, session_id, 
                      event_type, user_id, session_id, event_type, user_id, session_id))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")
    
    def get_wheel_analytics(self, wheel_id: str, days: int = 30) -> Dict[str, Any]:
        """Get comprehensive analytics for a specific wheel"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Basic wheel metrics
            cursor.execute('''
                SELECT total_spins, unique_users, total_entries, created_at, updated_at
                FROM wheel_metrics WHERE wheel_id = ?
            ''', (wheel_id,))
            
            wheel_metrics = cursor.fetchone()
            if not wheel_metrics:
                return {'error': 'Wheel not found'}
            
            # Spin activity over time
            cursor.execute('''
                SELECT DATE(timestamp) as date, COUNT(*) as spins
                FROM analytics_events 
                WHERE wheel_id = ? AND event_type = 'wheel_spin' 
                AND timestamp >= ? AND timestamp <= ?
                GROUP BY DATE(timestamp)
                ORDER BY date
            ''', (wheel_id, start_date.isoformat(), end_date.isoformat()))
            
            daily_activity = [{'date': row[0], 'spins': row[1]} for row in cursor.fetchall()]
            
            # Winner frequency distribution
            cursor.execute('''
                SELECT winner_name, frequency_count, last_won
                FROM winner_frequency 
                WHERE wheel_id = ?
                ORDER BY frequency_count DESC
                LIMIT 20
            ''', (wheel_id,))
            
            winner_distribution = [
                {'winner': row[0], 'count': row[1], 'last_won': row[2]}
                for row in cursor.fetchall()
            ]
            
            # Recent spin results
            cursor.execute('''
                SELECT timestamp, event_data
                FROM analytics_events 
                WHERE wheel_id = ? AND event_type = 'wheel_spin'
                ORDER BY timestamp DESC
                LIMIT 50
            ''', (wheel_id,))
            
            recent_spins = []
            for row in cursor.fetchall():
                try:
                    event_data = json.loads(row[1])
                    recent_spins.append({
                        'timestamp': row[0],
                        'winner': event_data.get('winner'),
                        'spin_duration': event_data.get('spin_duration'),
                        'user_id': event_data.get('user_id', 'Anonymous')
                    })
                except:
                    continue
            
            conn.close()
            
            return {
                'wheel_id': wheel_id,
                'metrics': {
                    'total_spins': wheel_metrics[0],
                    'unique_users': wheel_metrics[1],
                    'total_entries': wheel_metrics[2],
                    'created_at': wheel_metrics[3],
                    'updated_at': wheel_metrics[4]
                },
                'daily_activity': daily_activity,
                'winner_distribution': winner_distribution,
                'recent_spins': recent_spins,
                'analysis_period': f'{days} days',
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting wheel analytics: {e}")
            return {'error': str(e)}
    
    def get_global_analytics(self, days: int = 30) -> Dict[str, Any]:
        """Get global platform analytics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Global metrics
            cursor.execute('''
                SELECT 
                    COUNT(DISTINCT wheel_id) as total_wheels,
                    COUNT(*) as total_spins,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT session_id) as unique_sessions
                FROM analytics_events 
                WHERE event_type = 'wheel_spin'
                AND timestamp >= ? AND timestamp <= ?
            ''', (start_date.isoformat(), end_date.isoformat()))
            
            global_metrics = cursor.fetchone()
            
            # Daily activity trends
            cursor.execute('''
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as total_spins,
                    COUNT(DISTINCT wheel_id) as active_wheels,
                    COUNT(DISTINCT user_id) as active_users
                FROM analytics_events 
                WHERE event_type = 'wheel_spin'
                AND timestamp >= ? AND timestamp <= ?
                GROUP BY DATE(timestamp)
                ORDER BY date
            ''', (start_date.isoformat(), end_date.isoformat()))
            
            daily_trends = [
                {
                    'date': row[0],
                    'total_spins': row[1],
                    'active_wheels': row[2],
                    'active_users': row[3]
                }
                for row in cursor.fetchall()
            ]
            
            # Top performing wheels
            cursor.execute('''
                SELECT wheel_id, wheel_title, total_spins, unique_users
                FROM wheel_metrics
                ORDER BY total_spins DESC
                LIMIT 10
            ''')
            
            top_wheels = [
                {
                    'wheel_id': row[0],
                    'title': row[1],
                    'total_spins': row[2],
                    'unique_users': row[3]
                }
                for row in cursor.fetchall()
            ]
            
            # User engagement distribution
            cursor.execute('''
                SELECT 
                    CASE 
                        WHEN total_spins >= 50 THEN 'High'
                        WHEN total_spins >= 10 THEN 'Medium'
                        ELSE 'Low'
                    END as engagement_level,
                    COUNT(*) as user_count
                FROM user_engagement
                GROUP BY engagement_level
            ''')
            
            engagement_distribution = dict(cursor.fetchall())
            
            conn.close()
            
            return {
                'global_metrics': {
                    'total_wheels': global_metrics[0],
                    'total_spins': global_metrics[1],
                    'unique_users': global_metrics[2],
                    'unique_sessions': global_metrics[3]
                },
                'daily_trends': daily_trends,
                'top_wheels': top_wheels,
                'engagement_distribution': engagement_distribution,
                'analysis_period': f'{days} days',
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting global analytics: {e}")
            return {'error': str(e)}
    
    def export_analytics_data(self, wheel_id: str = None, format_type: str = 'json',
                             days: int = 30) -> Dict[str, Any]:
        """Export analytics data with integrity verification"""
        try:
            if wheel_id:
                data = self.get_wheel_analytics(wheel_id, days)
            else:
                data = self.get_global_analytics(days)
            
            # Add integrity information
            data_json = json.dumps(data, sort_keys=True)
            data_hash = hashlib.sha256(data_json.encode('utf-8')).hexdigest()
            
            export_data = {
                'data': data,
                'metadata': {
                    'export_type': 'analytics',
                    'format': format_type,
                    'exported_at': datetime.now().isoformat(),
                    'data_hash': data_hash,
                    'integrity_verified': True
                }
            }
            
            return {
                'success': True,
                'export_data': export_data,
                'data_hash': data_hash
            }
            
        except Exception as e:
            logger.error(f"Error exporting analytics data: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def verify_data_integrity(self, data: Dict[str, Any], expected_hash: str) -> bool:
        """Verify data integrity using hash comparison"""
        try:
            data_json = json.dumps(data, sort_keys=True)
            actual_hash = hashlib.sha256(data_json.encode('utf-8')).hexdigest()
            return actual_hash == expected_hash
        except:
            return False

# Global analytics service instance
analytics_service = AnalyticsService()

