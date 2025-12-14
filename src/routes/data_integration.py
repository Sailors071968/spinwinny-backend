"""
Data Integration API Routes
Handles Google Sheets sync, CSV import/export, and analytics
"""

from flask import Blueprint, request, jsonify, send_file
from flask_cors import cross_origin
import io
import logging
from typing import Dict, Any
import sqlite3
import os

from ..services.google_sheets import google_sheets_service
from ..services.csv_service import csv_service
from ..security.csv_validator import secure_csv_validator

logger = logging.getLogger(__name__)

data_integration_bp = Blueprint('data_integration', __name__, url_prefix='/api/data')

def get_db_connection():
    """Get database connection"""
    db_path = os.path.join(os.path.dirname(__file__), '..', 'database', 'app.db')
    return sqlite3.connect(db_path)

@data_integration_bp.route('/health', methods=['GET'])
@cross_origin()
def health_check():
    """Health check for data integration services"""
    return jsonify({
        'status': 'healthy',
        'services': {
            'google_sheets': google_sheets_service.is_authenticated(),
            'csv_service': True,
            'database': True
        }
    })

# Google Sheets Integration Routes

@data_integration_bp.route('/google/auth-url', methods=['POST'])
@cross_origin()
def get_google_auth_url():
    """Get Google OAuth2 authorization URL"""
    try:
        data = request.get_json()
        redirect_uri = data.get('redirect_uri', 'http://localhost:5173/auth/google/callback')
        
        auth_url = google_sheets_service.get_auth_url(redirect_uri)
        
        return jsonify({
            'success': True,
            'auth_url': auth_url
        })
        
    except Exception as e:
        logger.error(f"Error getting auth URL: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_integration_bp.route('/google/exchange-token', methods=['POST'])
@cross_origin()
def exchange_google_token():
    """Exchange authorization code for access token"""
    try:
        data = request.get_json()
        code = data.get('code')
        redirect_uri = data.get('redirect_uri', 'http://localhost:5173/auth/google/callback')
        
        if not code:
            return jsonify({
                'success': False,
                'error': 'Authorization code is required'
            }), 400
        
        result = google_sheets_service.exchange_code_for_token(code, redirect_uri)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error exchanging token: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_integration_bp.route('/google/create-sheet', methods=['POST'])
@cross_origin()
def create_google_sheet():
    """Create a new Google Sheet for wheel data"""
    try:
        data = request.get_json()
        title = data.get('title', 'SpinWinny Wheel')
        wheel_data = data.get('wheel_data', {})
        
        result = google_sheets_service.create_spreadsheet(title, wheel_data)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error creating Google Sheet: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_integration_bp.route('/google/sync-from-sheet', methods=['POST'])
@cross_origin()
def sync_from_google_sheet():
    """Sync data from Google Sheet to SpinWinny"""
    try:
        data = request.get_json()
        spreadsheet_id = data.get('spreadsheet_id')
        range_name = data.get('range', 'A:G')
        
        if not spreadsheet_id:
            return jsonify({
                'success': False,
                'error': 'Spreadsheet ID is required'
            }), 400
        
        result = google_sheets_service.sync_from_sheet(spreadsheet_id, range_name)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error syncing from Google Sheet: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_integration_bp.route('/google/sync-to-sheet', methods=['POST'])
@cross_origin()
def sync_to_google_sheet():
    """Sync data from SpinWinny to Google Sheet"""
    try:
        data = request.get_json()
        spreadsheet_id = data.get('spreadsheet_id')
        wheel_data = data.get('wheel_data', {})
        
        if not spreadsheet_id:
            return jsonify({
                'success': False,
                'error': 'Spreadsheet ID is required'
            }), 400
        
        result = google_sheets_service.sync_to_sheet(spreadsheet_id, wheel_data)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error syncing to Google Sheet: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_integration_bp.route('/google/sheet-info', methods=['POST'])
@cross_origin()
def get_google_sheet_info():
    """Get information about a Google Sheet"""
    try:
        data = request.get_json()
        spreadsheet_id = data.get('spreadsheet_id')
        
        if not spreadsheet_id:
            return jsonify({
                'success': False,
                'error': 'Spreadsheet ID is required'
            }), 400
        
        result = google_sheets_service.get_sheet_info(spreadsheet_id)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error getting sheet info: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# CSV Import/Export Routes

@data_integration_bp.route('/csv/validate', methods=['POST'])
@cross_origin()
def validate_csv():
    """Validate CSV file format and content"""
    try:
        data = request.get_json()
        file_content = data.get('content')
        
        if not file_content:
            return jsonify({
                'success': False,
                'error': 'CSV content is required'
            }), 400
        
        result = csv_service.validate_csv_file(file_content)
        
        return jsonify({
            'success': True,
            'validation': result
        })
        
    except Exception as e:
        logger.error(f"Error validating CSV: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_integration_bp.route('/csv/import', methods=['POST'])
@cross_origin()
def import_csv():
    """Import entries from CSV file"""
    try:
        data = request.get_json()
        file_content = data.get('content')
        
        if not file_content:
            return jsonify({
                'success': False,
                'error': 'CSV content is required'
            }), 400
        
        result = csv_service.import_csv(file_content)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error importing CSV: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_integration_bp.route('/csv/export/entries', methods=['POST'])
@cross_origin()
def export_entries():
    """Export wheel entries in various formats"""
    try:
        data = request.get_json()
        entries = data.get('entries', [])
        wheel_data = data.get('wheel_data', {})
        format_type = data.get('format', 'csv').lower()
        
        if format_type not in ['csv', 'json']:
            return jsonify({
                'success': False,
                'error': 'Unsupported format. Use csv or json.'
            }), 400
        
        if format_type == 'csv':
            content = csv_service.export_entries_csv(entries, wheel_data.get('title', ''))
            mimetype = 'text/csv'
        else:  # json
            content = csv_service.export_entries_json(entries, wheel_data)
            mimetype = 'application/json'
        
        filename = csv_service.get_export_filename(format_type, wheel_data.get('title', ''), 'entries')
        
        return jsonify({
            'success': True,
            'content': content,
            'filename': filename,
            'mimetype': mimetype
        })
        
    except Exception as e:
        logger.error(f"Error exporting entries: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_integration_bp.route('/csv/export/results', methods=['POST'])
@cross_origin()
def export_results():
    """Export spin results in various formats"""
    try:
        data = request.get_json()
        wheel_id = data.get('wheel_id')
        format_type = data.get('format', 'csv').lower()
        
        if format_type not in ['csv', 'json', 'pdf']:
            return jsonify({
                'success': False,
                'error': 'Unsupported format. Use csv, json, or pdf.'
            }), 400
        
        # Get results from database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if wheel_id:
            cursor.execute('''
                SELECT timestamp, wheel_id, wheel_title, winner, spin_duration, user_id, session_id
                FROM spin_results 
                WHERE wheel_id = ?
                ORDER BY timestamp DESC
            ''', (wheel_id,))
        else:
            cursor.execute('''
                SELECT timestamp, wheel_id, wheel_title, winner, spin_duration, user_id, session_id
                FROM spin_results 
                ORDER BY timestamp DESC
                LIMIT 1000
            ''')
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'timestamp': row[0],
                'wheel_id': row[1],
                'wheel_title': row[2],
                'winner': row[3],
                'spin_duration': row[4],
                'user_id': row[5],
                'session_id': row[6]
            })
        
        conn.close()
        
        wheel_title = results[0]['wheel_title'] if results else 'All Wheels'
        
        if format_type == 'csv':
            content = csv_service.export_results_csv(results)
            mimetype = 'text/csv'
            filename = csv_service.get_export_filename('csv', wheel_title, 'results')
            
            return jsonify({
                'success': True,
                'content': content,
                'filename': filename,
                'mimetype': mimetype
            })
            
        elif format_type == 'json':
            content = csv_service.export_results_json(results)
            mimetype = 'application/json'
            filename = csv_service.get_export_filename('json', wheel_title, 'results')
            
            return jsonify({
                'success': True,
                'content': content,
                'filename': filename,
                'mimetype': mimetype
            })
            
        else:  # pdf
            pdf_content = csv_service.export_results_pdf(results, wheel_title)
            filename = csv_service.get_export_filename('pdf', wheel_title, 'results')
            
            # Return PDF as base64 for frontend handling
            import base64
            pdf_base64 = base64.b64encode(pdf_content).decode('utf-8')
            
            return jsonify({
                'success': True,
                'content': pdf_base64,
                'filename': filename,
                'mimetype': 'application/pdf',
                'encoding': 'base64'
            })
        
    except Exception as e:
        logger.error(f"Error exporting results: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@data_integration_bp.route('/integrity-check', methods=['POST'])
@cross_origin()
def check_data_integrity():
    """Validate data integrity after import/export operations"""
    try:
        data = request.get_json()
        original_data = data.get('original_data', [])
        processed_data = data.get('processed_data', [])
        
        result = csv_service.validate_data_integrity(original_data, processed_data)
        
        return jsonify({
            'success': True,
            'integrity_check': result
        })
        
    except Exception as e:
        logger.error(f"Error checking data integrity: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

