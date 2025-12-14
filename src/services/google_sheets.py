"""
Google Sheets API Integration Service
Provides real-time bidirectional sync between SpinWinny and Google Sheets
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

logger = logging.getLogger(__name__)

class GoogleSheetsService:
    """Service for Google Sheets API integration with real-time sync"""
    
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file'
    ]
    
    def __init__(self):
        self.credentials_file = 'config/google_credentials.json'
        self.token_file = 'config/google_token.json'
        self.service = None
        self.credentials = None
        
    def get_auth_url(self, redirect_uri: str) -> str:
        """Generate OAuth2 authorization URL"""
        try:
            flow = Flow.from_client_secrets_file(
                self.credentials_file,
                scopes=self.SCOPES,
                redirect_uri=redirect_uri
            )
            
            auth_url, _ = flow.authorization_url(
                access_type='offline',
                include_granted_scopes='true',
                prompt='consent'
            )
            
            return auth_url
            
        except Exception as e:
            logger.error(f"Error generating auth URL: {e}")
            raise
    
    def exchange_code_for_token(self, code: str, redirect_uri: str) -> Dict[str, Any]:
        """Exchange authorization code for access token"""
        try:
            flow = Flow.from_client_secrets_file(
                self.credentials_file,
                scopes=self.SCOPES,
                redirect_uri=redirect_uri
            )
            
            flow.fetch_token(code=code)
            credentials = flow.credentials
            
            # Save credentials for future use
            self._save_credentials(credentials)
            
            return {
                'success': True,
                'access_token': credentials.token,
                'refresh_token': credentials.refresh_token,
                'expires_in': credentials.expiry.timestamp() if credentials.expiry else None
            }
            
        except Exception as e:
            logger.error(f"Error exchanging code for token: {e}")
            return {'success': False, 'error': str(e)}
    
    def _save_credentials(self, credentials: Credentials):
        """Save credentials to file"""
        try:
            os.makedirs('config', exist_ok=True)
            with open(self.token_file, 'w') as token:
                token.write(credentials.to_json())
        except Exception as e:
            logger.error(f"Error saving credentials: {e}")
    
    def _load_credentials(self) -> Optional[Credentials]:
        """Load credentials from file"""
        try:
            if os.path.exists(self.token_file):
                credentials = Credentials.from_authorized_user_file(
                    self.token_file, self.SCOPES
                )
                
                # Refresh if expired
                if credentials and credentials.expired and credentials.refresh_token:
                    credentials.refresh(Request())
                    self._save_credentials(credentials)
                
                return credentials
        except Exception as e:
            logger.error(f"Error loading credentials: {e}")
        
        return None
    
    def initialize_service(self) -> bool:
        """Initialize Google Sheets service"""
        try:
            self.credentials = self._load_credentials()
            if not self.credentials:
                return False
            
            self.service = build('sheets', 'v4', credentials=self.credentials)
            return True
            
        except Exception as e:
            logger.error(f"Error initializing service: {e}")
            return False
    
    def create_spreadsheet(self, title: str, wheel_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new spreadsheet for wheel data"""
        try:
            if not self.service:
                if not self.initialize_service():
                    return {'success': False, 'error': 'Authentication required'}
            
            # Create spreadsheet
            spreadsheet = {
                'properties': {
                    'title': f"SpinWinny - {title}"
                },
                'sheets': [{
                    'properties': {
                        'title': 'Wheel Entries',
                        'gridProperties': {
                            'rowCount': 1000,
                            'columnCount': 10
                        }
                    }
                }]
            }
            
            result = self.service.spreadsheets().create(body=spreadsheet).execute()
            spreadsheet_id = result['spreadsheetId']
            
            # Add headers and initial data
            headers = [['Entry', 'Weight', 'Color', 'Category', 'Active', 'Created', 'Modified']]
            
            # Convert wheel entries to rows
            rows = []
            for i, entry in enumerate(wheel_data.get('entries', [])):
                rows.append([
                    entry,
                    1,  # Default weight
                    '',  # Color (will be filled by SpinWinny)
                    wheel_data.get('category', 'general'),
                    'TRUE',  # Active
                    '',  # Created timestamp
                    ''   # Modified timestamp
                ])
            
            # Write data to sheet
            all_data = headers + rows
            self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range='A1',
                valueInputOption='RAW',
                body={'values': all_data}
            ).execute()
            
            return {
                'success': True,
                'spreadsheet_id': spreadsheet_id,
                'url': f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
            }
            
        except HttpError as e:
            logger.error(f"Google Sheets API error: {e}")
            return {'success': False, 'error': f'API Error: {e.resp.status}'}
        except Exception as e:
            logger.error(f"Error creating spreadsheet: {e}")
            return {'success': False, 'error': str(e)}
    
    def sync_from_sheet(self, spreadsheet_id: str, range_name: str = 'A:G') -> Dict[str, Any]:
        """Sync data from Google Sheet to SpinWinny"""
        try:
            if not self.service:
                if not self.initialize_service():
                    return {'success': False, 'error': 'Authentication required'}
            
            # Read data from sheet
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return {'success': False, 'error': 'No data found in sheet'}
            
            # Parse data (skip header row)
            entries = []
            for row in values[1:]:  # Skip header
                if len(row) > 0 and row[0].strip():  # Entry name exists
                    entry_data = {
                        'name': row[0].strip(),
                        'weight': int(row[1]) if len(row) > 1 and row[1].isdigit() else 1,
                        'color': row[2] if len(row) > 2 else '',
                        'category': row[3] if len(row) > 3 else 'general',
                        'active': row[4].upper() == 'TRUE' if len(row) > 4 else True
                    }
                    
                    if entry_data['active']:  # Only include active entries
                        entries.append(entry_data['name'])
            
            return {
                'success': True,
                'entries': entries,
                'total_rows': len(values) - 1,
                'active_entries': len(entries)
            }
            
        except HttpError as e:
            logger.error(f"Google Sheets API error: {e}")
            return {'success': False, 'error': f'API Error: {e.resp.status}'}
        except Exception as e:
            logger.error(f"Error syncing from sheet: {e}")
            return {'success': False, 'error': str(e)}
    
    def sync_to_sheet(self, spreadsheet_id: str, wheel_data: Dict[str, Any]) -> Dict[str, Any]:
        """Sync data from SpinWinny to Google Sheet"""
        try:
            if not self.service:
                if not self.initialize_service():
                    return {'success': False, 'error': 'Authentication required'}
            
            # Prepare data for sheet
            entries = wheel_data.get('entries', [])
            timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Read existing data to preserve additional columns
            existing_result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range='A:G'
            ).execute()
            
            existing_values = existing_result.get('values', [])
            headers = existing_values[0] if existing_values else [
                'Entry', 'Weight', 'Color', 'Category', 'Active', 'Created', 'Modified'
            ]
            
            # Build new data
            new_data = [headers]
            for entry in entries:
                new_data.append([
                    entry,
                    1,  # Default weight
                    '',  # Color
                    wheel_data.get('category', 'general'),
                    'TRUE',  # Active
                    timestamp,  # Created
                    timestamp   # Modified
                ])
            
            # Clear existing data and write new data
            self.service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range='A:G'
            ).execute()
            
            self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range='A1',
                valueInputOption='RAW',
                body={'values': new_data}
            ).execute()
            
            return {
                'success': True,
                'updated_entries': len(entries),
                'timestamp': timestamp
            }
            
        except HttpError as e:
            logger.error(f"Google Sheets API error: {e}")
            return {'success': False, 'error': f'API Error: {e.resp.status}'}
        except Exception as e:
            logger.error(f"Error syncing to sheet: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_sheet_info(self, spreadsheet_id: str) -> Dict[str, Any]:
        """Get information about a spreadsheet"""
        try:
            if not self.service:
                if not self.initialize_service():
                    return {'success': False, 'error': 'Authentication required'}
            
            result = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            return {
                'success': True,
                'title': result['properties']['title'],
                'url': result['spreadsheetUrl'],
                'sheets': [sheet['properties']['title'] for sheet in result['sheets']]
            }
            
        except HttpError as e:
            logger.error(f"Google Sheets API error: {e}")
            return {'success': False, 'error': f'API Error: {e.resp.status}'}
        except Exception as e:
            logger.error(f"Error getting sheet info: {e}")
            return {'success': False, 'error': str(e)}
    
    def is_authenticated(self) -> bool:
        """Check if user is authenticated"""
        return self._load_credentials() is not None

# Global service instance
google_sheets_service = GoogleSheetsService()

