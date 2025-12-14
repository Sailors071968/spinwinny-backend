"""
Secure CSV Validator
Prevents CSV injection, XSS attacks, and validates file integrity
"""

import re
import html
import logging
from typing import List, Dict, Any, Optional, Tuple
import csv
import io
import hashlib

logger = logging.getLogger(__name__)

class SecureCSVValidator:
    """Secure CSV validation with injection prevention and XSS filtering"""
    
    # Dangerous CSV formula prefixes that could execute code
    DANGEROUS_PREFIXES = ['=', '+', '-', '@', '\t=', '\r=', '\n=']
    
    # Dangerous functions that should be blocked
    DANGEROUS_FUNCTIONS = [
        'cmd', 'exec', 'system', 'shell', 'powershell', 'bash',
        'eval', 'javascript', 'vbscript', 'macro', 'formula'
    ]
    
    # XSS patterns to detect and block
    XSS_PATTERNS = [
        r'<script[^>]*>.*?</script>',
        r'javascript:',
        r'vbscript:',
        r'on\w+\s*=',
        r'<iframe[^>]*>',
        r'<object[^>]*>',
        r'<embed[^>]*>',
        r'<link[^>]*>',
        r'<meta[^>]*>',
        r'<style[^>]*>.*?</style>'
    ]
    
    def __init__(self, max_file_size: int = 10 * 1024 * 1024):  # 10MB default
        self.max_file_size = max_file_size
        self.xss_regex = re.compile('|'.join(self.XSS_PATTERNS), re.IGNORECASE | re.DOTALL)
        
    def validate_file_security(self, file_content: str, filename: str = '') -> Dict[str, Any]:
        """Comprehensive security validation of CSV file"""
        try:
            # File size check
            file_size = len(file_content.encode('utf-8'))
            if file_size > self.max_file_size:
                return {
                    'valid': False,
                    'error': f'File size ({file_size} bytes) exceeds maximum limit ({self.max_file_size} bytes)',
                    'security_risk': 'file_size_exceeded'
                }
            
            # File extension check
            if filename and not filename.lower().endswith('.csv'):
                return {
                    'valid': False,
                    'error': 'Invalid file extension. Only .csv files are allowed.',
                    'security_risk': 'invalid_extension'
                }
            
            # Content encoding check
            try:
                file_content.encode('utf-8')
            except UnicodeEncodeError:
                return {
                    'valid': False,
                    'error': 'File contains invalid characters. Please use UTF-8 encoding.',
                    'security_risk': 'encoding_error'
                }
            
            # CSV structure validation
            csv_validation = self._validate_csv_structure(file_content)
            if not csv_validation['valid']:
                return csv_validation
            
            # Security content validation
            security_validation = self._validate_content_security(file_content)
            if not security_validation['valid']:
                return security_validation
            
            # Generate file integrity hash
            file_hash = hashlib.sha256(file_content.encode('utf-8')).hexdigest()
            
            return {
                'valid': True,
                'file_size': file_size,
                'file_hash': file_hash,
                'rows_processed': csv_validation.get('rows_processed', 0),
                'security_checks_passed': True
            }
            
        except Exception as e:
            logger.error(f"Error in security validation: {e}")
            return {
                'valid': False,
                'error': f'Security validation failed: {str(e)}',
                'security_risk': 'validation_error'
            }
    
    def _validate_csv_structure(self, content: str) -> Dict[str, Any]:
        """Validate CSV structure and format"""
        try:
            csv_reader = csv.reader(io.StringIO(content))
            rows = list(csv_reader)
            
            if not rows:
                return {
                    'valid': False,
                    'error': 'CSV file is empty',
                    'security_risk': 'empty_file'
                }
            
            # Check for reasonable number of columns (prevent memory exhaustion)
            max_columns = 50
            for i, row in enumerate(rows):
                if len(row) > max_columns:
                    return {
                        'valid': False,
                        'error': f'Row {i+1} has too many columns ({len(row)}). Maximum allowed: {max_columns}',
                        'security_risk': 'excessive_columns'
                    }
            
            # Check for reasonable number of rows (prevent memory exhaustion)
            max_rows = 10000
            if len(rows) > max_rows:
                return {
                    'valid': False,
                    'error': f'File has too many rows ({len(rows)}). Maximum allowed: {max_rows}',
                    'security_risk': 'excessive_rows'
                }
            
            return {
                'valid': True,
                'rows_processed': len(rows)
            }
            
        except csv.Error as e:
            return {
                'valid': False,
                'error': f'Invalid CSV format: {str(e)}',
                'security_risk': 'malformed_csv'
            }
    
    def _validate_content_security(self, content: str) -> Dict[str, Any]:
        """Validate content for security threats"""
        try:
            csv_reader = csv.reader(io.StringIO(content))
            
            security_issues = []
            
            for row_num, row in enumerate(csv_reader, 1):
                for col_num, cell in enumerate(row, 1):
                    if not cell:
                        continue
                    
                    # Check for CSV injection
                    injection_check = self._check_csv_injection(cell)
                    if injection_check:
                        security_issues.append({
                            'type': 'csv_injection',
                            'location': f'Row {row_num}, Column {col_num}',
                            'content': cell[:50] + '...' if len(cell) > 50 else cell,
                            'risk': injection_check
                        })
                    
                    # Check for XSS payloads
                    xss_check = self._check_xss_payload(cell)
                    if xss_check:
                        security_issues.append({
                            'type': 'xss_payload',
                            'location': f'Row {row_num}, Column {col_num}',
                            'content': cell[:50] + '...' if len(cell) > 50 else cell,
                            'risk': xss_check
                        })
                    
                    # Check cell length (prevent buffer overflow)
                    if len(cell) > 1000:
                        security_issues.append({
                            'type': 'excessive_length',
                            'location': f'Row {row_num}, Column {col_num}',
                            'content': f'Cell length: {len(cell)} characters',
                            'risk': 'potential_buffer_overflow'
                        })
            
            if security_issues:
                return {
                    'valid': False,
                    'error': f'Security threats detected: {len(security_issues)} issues found',
                    'security_risk': 'content_threats',
                    'security_issues': security_issues
                }
            
            return {'valid': True}
            
        except Exception as e:
            logger.error(f"Error in content security validation: {e}")
            return {
                'valid': False,
                'error': f'Content security validation failed: {str(e)}',
                'security_risk': 'validation_error'
            }
    
    def _check_csv_injection(self, cell: str) -> Optional[str]:
        """Check for CSV injection attempts"""
        if not cell:
            return None
        
        # Check for dangerous prefixes
        for prefix in self.DANGEROUS_PREFIXES:
            if cell.strip().startswith(prefix):
                return f'dangerous_prefix_{prefix.strip()}'
        
        # Check for dangerous functions
        cell_lower = cell.lower()
        for func in self.DANGEROUS_FUNCTIONS:
            if func in cell_lower:
                return f'dangerous_function_{func}'
        
        # Check for formula patterns
        if re.search(r'=\s*[A-Z]+\s*\(', cell, re.IGNORECASE):
            return 'formula_pattern'
        
        return None
    
    def _check_xss_payload(self, cell: str) -> Optional[str]:
        """Check for XSS payload attempts"""
        if not cell:
            return None
        
        # Check against XSS patterns
        if self.xss_regex.search(cell):
            return 'xss_pattern_detected'
        
        # Check for encoded payloads
        try:
            decoded = html.unescape(cell)
            if decoded != cell and self.xss_regex.search(decoded):
                return 'encoded_xss_payload'
        except:
            pass
        
        # Check for suspicious character sequences
        suspicious_chars = ['<', '>', '"', "'", '&', '%', '\\']
        suspicious_count = sum(1 for char in suspicious_chars if char in cell)
        if suspicious_count > 3:  # Threshold for suspicion
            return 'suspicious_character_sequence'
        
        return None
    
    def sanitize_cell_content(self, cell: str) -> str:
        """Sanitize cell content to remove security threats"""
        if not cell:
            return cell
        
        # Remove dangerous prefixes
        for prefix in self.DANGEROUS_PREFIXES:
            if cell.strip().startswith(prefix):
                cell = cell.strip()[len(prefix):].strip()
        
        # HTML encode to prevent XSS
        cell = html.escape(cell, quote=True)
        
        # Remove or escape dangerous patterns
        cell = re.sub(r'javascript:', 'javascript_', cell, flags=re.IGNORECASE)
        cell = re.sub(r'vbscript:', 'vbscript_', cell, flags=re.IGNORECASE)
        cell = re.sub(r'on\w+\s*=', '', cell, flags=re.IGNORECASE)
        
        # Limit length
        if len(cell) > 1000:
            cell = cell[:1000] + '...'
        
        return cell
    
    def process_secure_csv(self, content: str, filename: str = '') -> Dict[str, Any]:
        """Process CSV with full security validation and sanitization"""
        try:
            # Security validation
            validation_result = self.validate_file_security(content, filename)
            if not validation_result['valid']:
                return validation_result
            
            # Parse and sanitize content
            csv_reader = csv.reader(io.StringIO(content))
            sanitized_rows = []
            
            for row in csv_reader:
                sanitized_row = [self.sanitize_cell_content(cell) for cell in row]
                sanitized_rows.append(sanitized_row)
            
            # Extract entries (assuming first column contains entry names)
            entries = []
            headers = sanitized_rows[0] if sanitized_rows else []
            
            # Find entry column
            entry_column = 0
            for i, header in enumerate(headers):
                if header.lower() in ['entry', 'name', 'item']:
                    entry_column = i
                    break
            
            # Extract entries
            for row in sanitized_rows[1:]:  # Skip header
                if len(row) > entry_column and row[entry_column].strip():
                    entries.append(row[entry_column].strip())
            
            return {
                'success': True,
                'entries': entries,
                'total_rows': len(sanitized_rows) - 1,  # Exclude header
                'valid_entries': len(entries),
                'file_hash': validation_result['file_hash'],
                'security_validated': True,
                'sanitized': True
            }
            
        except Exception as e:
            logger.error(f"Error processing secure CSV: {e}")
            return {
                'success': False,
                'error': f'CSV processing failed: {str(e)}',
                'security_risk': 'processing_error'
            }

# Global secure validator instance
secure_csv_validator = SecureCSVValidator()

