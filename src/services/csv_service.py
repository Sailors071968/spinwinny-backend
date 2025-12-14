"""
CSV Import/Export Service
Handles bulk data operations with validation and integrity checks
"""

import csv
import json
import pandas as pd
import io
import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch

logger = logging.getLogger(__name__)

class CSVService:
    """Service for CSV import/export operations with validation"""
    
    def __init__(self):
        self.supported_formats = ['csv', 'json', 'pdf']
        self.max_file_size = 10 * 1024 * 1024  # 10MB
        
    def validate_csv_file(self, file_content: str) -> Dict[str, Any]:
        """Validate CSV file format and content"""
        try:
            # Check file size
            if len(file_content.encode('utf-8')) > self.max_file_size:
                return {
                    'valid': False,
                    'error': f'File size exceeds maximum limit of {self.max_file_size // (1024*1024)}MB'
                }
            
            # Parse CSV content
            csv_reader = csv.reader(io.StringIO(file_content))
            rows = list(csv_reader)
            
            if not rows:
                return {'valid': False, 'error': 'CSV file is empty'}
            
            # Validate headers
            headers = [h.strip().lower() for h in rows[0]]
            required_headers = ['entry', 'name']  # Accept either 'entry' or 'name'
            
            if not any(header in headers for header in required_headers):
                return {
                    'valid': False,
                    'error': 'CSV must contain at least one column named "entry" or "name"'
                }
            
            # Validate data rows
            valid_entries = []
            errors = []
            
            for i, row in enumerate(rows[1:], start=2):
                if not row or all(cell.strip() == '' for cell in row):
                    continue  # Skip empty rows
                
                # Find entry name
                entry_name = ''
                for j, header in enumerate(headers):
                    if header in required_headers and j < len(row):
                        entry_name = row[j].strip()
                        break
                
                if not entry_name:
                    errors.append(f'Row {i}: Missing entry name')
                    continue
                
                if len(entry_name) > 100:
                    errors.append(f'Row {i}: Entry name too long (max 100 characters)')
                    continue
                
                valid_entries.append(entry_name)
            
            if not valid_entries:
                return {'valid': False, 'error': 'No valid entries found in CSV'}
            
            return {
                'valid': True,
                'entries': valid_entries,
                'total_rows': len(rows) - 1,
                'valid_entries': len(valid_entries),
                'errors': errors
            }
            
        except csv.Error as e:
            return {'valid': False, 'error': f'CSV parsing error: {str(e)}'}
        except Exception as e:
            logger.error(f"Error validating CSV: {e}")
            return {'valid': False, 'error': f'Validation error: {str(e)}'}
    
    def import_csv(self, file_content: str) -> Dict[str, Any]:
        """Import entries from CSV file"""
        try:
            validation_result = self.validate_csv_file(file_content)
            
            if not validation_result['valid']:
                return {
                    'success': False,
                    'error': validation_result['error']
                }
            
            return {
                'success': True,
                'entries': validation_result['entries'],
                'imported_count': validation_result['valid_entries'],
                'total_rows': validation_result['total_rows'],
                'warnings': validation_result.get('errors', [])
            }
            
        except Exception as e:
            logger.error(f"Error importing CSV: {e}")
            return {'success': False, 'error': str(e)}
    
    def export_entries_csv(self, entries: List[str], wheel_title: str = '') -> str:
        """Export wheel entries to CSV format"""
        try:
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header
            writer.writerow(['Entry', 'Weight', 'Category', 'Active', 'Created'])
            
            # Write entries
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for entry in entries:
                writer.writerow([entry, 1, 'general', 'TRUE', timestamp])
            
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Error exporting entries to CSV: {e}")
            raise
    
    def export_entries_json(self, entries: List[str], wheel_data: Dict[str, Any]) -> str:
        """Export wheel entries to JSON format"""
        try:
            export_data = {
                'wheel': {
                    'title': wheel_data.get('title', 'Untitled Wheel'),
                    'entries': entries,
                    'settings': wheel_data.get('settings', {}),
                    'category': wheel_data.get('category', 'general'),
                    'created_at': datetime.now().isoformat(),
                    'exported_at': datetime.now().isoformat()
                },
                'metadata': {
                    'version': '1.0',
                    'format': 'spinwinny_export',
                    'entry_count': len(entries)
                }
            }
            
            return json.dumps(export_data, indent=2, ensure_ascii=False)
            
        except Exception as e:
            logger.error(f"Error exporting entries to JSON: {e}")
            raise
    
    def export_results_csv(self, results: List[Dict[str, Any]]) -> str:
        """Export spin results to CSV format"""
        try:
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header
            writer.writerow([
                'Timestamp', 'Wheel ID', 'Wheel Title', 'Winner', 
                'Spin Duration', 'User ID', 'Session ID'
            ])
            
            # Write results
            for result in results:
                writer.writerow([
                    result.get('timestamp', ''),
                    result.get('wheel_id', ''),
                    result.get('wheel_title', ''),
                    result.get('winner', ''),
                    result.get('spin_duration', ''),
                    result.get('user_id', ''),
                    result.get('session_id', '')
                ])
            
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Error exporting results to CSV: {e}")
            raise
    
    def export_results_json(self, results: List[Dict[str, Any]]) -> str:
        """Export spin results to JSON format"""
        try:
            export_data = {
                'results': results,
                'metadata': {
                    'exported_at': datetime.now().isoformat(),
                    'total_results': len(results),
                    'format': 'spinwinny_results'
                }
            }
            
            return json.dumps(export_data, indent=2, ensure_ascii=False)
            
        except Exception as e:
            logger.error(f"Error exporting results to JSON: {e}")
            raise
    
    def export_results_pdf(self, results: List[Dict[str, Any]], wheel_title: str = '') -> bytes:
        """Export spin results to PDF format"""
        try:
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            styles = getSampleStyleSheet()
            story = []
            
            # Title
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=18,
                spaceAfter=30,
                alignment=1  # Center alignment
            )
            
            title = f"SpinWinny Results Report - {wheel_title}" if wheel_title else "SpinWinny Results Report"
            story.append(Paragraph(title, title_style))
            story.append(Spacer(1, 20))
            
            # Summary
            summary_style = styles['Normal']
            summary_text = f"""
            <b>Report Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br/>
            <b>Total Results:</b> {len(results)}<br/>
            <b>Wheel:</b> {wheel_title or 'Multiple Wheels'}
            """
            story.append(Paragraph(summary_text, summary_style))
            story.append(Spacer(1, 20))
            
            if results:
                # Results table
                table_data = [['Timestamp', 'Winner', 'Spin Duration', 'User']]
                
                for result in results:
                    table_data.append([
                        result.get('timestamp', '')[:19],  # Truncate timestamp
                        result.get('winner', ''),
                        f"{result.get('spin_duration', 0)}s",
                        result.get('user_id', 'Anonymous')[:10]  # Truncate user ID
                    ])
                
                table = Table(table_data, colWidths=[2*inch, 2*inch, 1*inch, 1.5*inch])
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 12),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                
                story.append(table)
            else:
                story.append(Paragraph("No results to display.", styles['Normal']))
            
            # Build PDF
            doc.build(story)
            buffer.seek(0)
            return buffer.getvalue()
            
        except Exception as e:
            logger.error(f"Error exporting results to PDF: {e}")
            raise
    
    def validate_data_integrity(self, original_data: List[str], processed_data: List[str]) -> Dict[str, Any]:
        """Validate data integrity after import/export operations"""
        try:
            original_set = set(original_data)
            processed_set = set(processed_data)
            
            missing_entries = original_set - processed_set
            extra_entries = processed_set - original_set
            
            integrity_check = {
                'passed': len(missing_entries) == 0 and len(extra_entries) == 0,
                'original_count': len(original_data),
                'processed_count': len(processed_data),
                'missing_entries': list(missing_entries),
                'extra_entries': list(extra_entries),
                'integrity_score': len(processed_set & original_set) / len(original_set) if original_data else 1.0
            }
            
            return integrity_check
            
        except Exception as e:
            logger.error(f"Error validating data integrity: {e}")
            return {
                'passed': False,
                'error': str(e)
            }
    
    def get_export_filename(self, format_type: str, wheel_title: str = '', data_type: str = 'entries') -> str:
        """Generate appropriate filename for exports"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            safe_title = ''.join(c for c in wheel_title if c.isalnum() or c in (' ', '-', '_')).strip()
            safe_title = safe_title.replace(' ', '_')[:20]  # Limit length
            
            if safe_title:
                filename = f"spinwinny_{data_type}_{safe_title}_{timestamp}.{format_type}"
            else:
                filename = f"spinwinny_{data_type}_{timestamp}.{format_type}"
            
            return filename
            
        except Exception as e:
            logger.error(f"Error generating filename: {e}")
            return f"spinwinny_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{format_type}"

# Global service instance
csv_service = CSVService()

