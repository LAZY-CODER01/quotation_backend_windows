"""
Utility functions for QuoteSnap application.

This module contains common utility functions used throughout
the application for validation, formatting, and data processing.
"""

import re
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
import hashlib
import secrets
import uuid

logger = logging.getLogger(__name__)

def validate_email(email: str) -> bool:
    """
    Validate email address format.
    
    Args:
        email (str): Email address to validate
        
    Returns:
        bool: True if email format is valid
    """
    # TODO: Implement comprehensive email validation
    # - Use regex for basic format checking
    # - Validate domain format
    # - Check for common email providers
    # - Handle international domains
    
    if not email or not isinstance(email, str):
        return False
    
    # Basic email regex pattern
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email.strip()))

def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename for safe file system operations.
    
    Args:
        filename (str): Original filename
        
    Returns:
        str: Sanitized filename safe for file system
    """
    # TODO: Implement filename sanitization
    # - Remove invalid characters
    # - Handle reserved names
    # - Limit filename length
    # - Preserve file extension
    
    if not filename:
        return "untitled"
    
    # Remove invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Replace multiple underscores with single underscore
    sanitized = re.sub(r'_+', '_', sanitized)
    
    # Remove leading/trailing dots and spaces
    sanitized = sanitized.strip('. ')
    
    # Limit length (keep extension)
    if len(sanitized) > 100:
        parts = sanitized.rsplit('.', 1)
        if len(parts) == 2:
            name, ext = parts
            sanitized = name[:90] + '.' + ext
        else:
            sanitized = sanitized[:100]
    
    return sanitized or "untitled"

def generate_unique_id(prefix: str = "") -> str:
    """
    Generate a unique identifier.
    
    Args:
        prefix (str): Optional prefix for the ID
        
    Returns:
        str: Unique identifier string
    """
    # TODO: Implement unique ID generation
    # - Use UUID for guaranteed uniqueness
    # - Include timestamp component
    # - Add optional prefix
    # - Ensure URL-safe characters
    
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_part = str(uuid.uuid4()).replace('-', '')[:8]
    
    if prefix:
        return f"{prefix}_{timestamp}_{unique_part}"
    else:
        return f"{timestamp}_{unique_part}"

def format_currency(amount: Union[int, float], currency: str = "USD") -> str:
    """
    Format currency amount for display.
    
    Args:
        amount (Union[int, float]): Currency amount
        currency (str): Currency code (USD, EUR, etc.)
        
    Returns:
        str: Formatted currency string
    """
    # TODO: Implement currency formatting
    # - Handle different currency symbols
    # - Apply appropriate decimal places
    # - Support international formatting
    # - Handle negative amounts
    
    if amount is None:
        return "N/A"
    
    try:
        amount = float(amount)
        if currency.upper() == "USD":
            return f"${amount:,.2f}"
        elif currency.upper() == "EUR":
            return f"€{amount:,.2f}"
        else:
            return f"{amount:,.2f} {currency}"
    except (ValueError, TypeError):
        return "Invalid Amount"

def truncate_text(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate text to specified length with suffix.
    
    Args:
        text (str): Text to truncate
        max_length (int): Maximum length including suffix
        suffix (str): Suffix to add if truncated
        
    Returns:
        str: Truncated text with suffix if needed
    """
    if not text or len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix

def parse_date_string(date_string: str) -> Optional[datetime]:
    """
    Parse various date string formats into datetime object.
    
    Args:
        date_string (str): Date string to parse
        
    Returns:
        Optional[datetime]: Parsed datetime or None if invalid
    """
    # TODO: Implement flexible date parsing
    # - Handle multiple date formats
    # - Support different locales
    # - Parse relative dates (tomorrow, next week)
    # - Handle time zones
    
    if not date_string:
        return None
    
    # Common date formats to try
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%B %d, %Y",
        "%d %B %Y"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_string.strip(), fmt)
        except ValueError:
            continue
    
    logger.warning(f"Could not parse date string: {date_string}")
    return None

def clean_html(html_content: str) -> str:
    """
    Clean HTML content and extract plain text.
    
    Args:
        html_content (str): HTML content to clean
        
    Returns:
        str: Plain text content
    """
    # TODO: Implement HTML cleaning
    # - Remove HTML tags
    # - Convert entities to text
    # - Preserve line breaks appropriately
    # - Handle special characters
    
    if not html_content:
        return ""
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', html_content)
    
    # Convert common HTML entities
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    text = text.replace('&#39;', "'")
    text = text.replace('&nbsp;', ' ')
    
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    return text

def hash_string(text: str, algorithm: str = "sha256") -> str:
    """
    Generate hash of string using specified algorithm.
    
    Args:
        text (str): Text to hash
        algorithm (str): Hash algorithm to use
        
    Returns:
        str: Hexadecimal hash string
    """
    if not text:
        return ""
    
    if algorithm.lower() == "md5":
        return hashlib.md5(text.encode()).hexdigest()
    elif algorithm.lower() == "sha1":
        return hashlib.sha1(text.encode()).hexdigest()
    elif algorithm.lower() == "sha256":
        return hashlib.sha256(text.encode()).hexdigest()
    else:
        return hashlib.sha256(text.encode()).hexdigest()

def generate_secure_token(length: int = 32) -> str:
    """
    Generate a secure random token.
    
    Args:
        length (int): Length of the token
        
    Returns:
        str: Secure random token
    """
    return secrets.token_urlsafe(length)

def validate_phone_number(phone: str) -> bool:
    """
    Validate phone number format.
    
    Args:
        phone (str): Phone number to validate
        
    Returns:
        bool: True if phone format appears valid
    """
    # TODO: Implement phone number validation
    # - Handle international formats
    # - Support different country codes
    # - Validate digit count
    # - Handle common separators
    
    if not phone:
        return False
    
    # Remove common separators and spaces
    cleaned = re.sub(r'[\s\-\(\)\+\.]', '', phone)
    
    # Check if it contains only digits (with optional + prefix)
    if cleaned.startswith('+'):
        cleaned = cleaned[1:]
    
    # Phone should be 7-15 digits
    return cleaned.isdigit() and 7 <= len(cleaned) <= 15

def extract_numbers(text: str) -> List[float]:
    """
    Extract all numbers from text string.
    
    Args:
        text (str): Text to extract numbers from
        
    Returns:
        List[float]: List of extracted numbers
    """
    if not text:
        return []
    
    # Pattern to match integers and floats
    pattern = r'-?\d+\.?\d*'
    matches = re.findall(pattern, text)
    
    numbers = []
    for match in matches:
        try:
            if '.' in match:
                numbers.append(float(match))
            else:
                numbers.append(float(int(match)))
        except ValueError:
            continue
    
    return numbers

def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes (int): File size in bytes
        
    Returns:
        str: Formatted file size string
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    size = float(size_bytes)
    
    while size >= 1024.0 and i < len(size_names) - 1:
        size /= 1024.0
        i += 1
    
    return f"{size:.1f} {size_names[i]}"

def is_business_hours(dt: Optional[datetime] = None, 
                     start_hour: int = 9, 
                     end_hour: int = 17, 
                     weekdays_only: bool = True) -> bool:
    """
    Check if given datetime falls within business hours.
    
    Args:
        dt (datetime): Datetime to check (default: now)
        start_hour (int): Business day start hour
        end_hour (int): Business day end hour
        weekdays_only (bool): Whether to consider weekdays only
        
    Returns:
        bool: True if within business hours
    """
    if dt is None:
        dt = datetime.now()
    
    # Check weekday if required
    if weekdays_only and dt.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    
    # Check hour range
    return start_hour <= dt.hour < end_hour

def merge_dictionaries(*dicts: Dict) -> Dict:
    """
    Merge multiple dictionaries, with later ones taking precedence.
    
    Args:
        *dicts: Variable number of dictionaries to merge
        
    Returns:
        Dict: Merged dictionary
    """
    result = {}
    for d in dicts:
        if isinstance(d, dict):
            result.update(d)
    return result

def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split list into chunks of specified size.
    
    Args:
        lst (List[Any]): List to chunk
        chunk_size (int): Size of each chunk
        
    Returns:
        List[List[Any]]: List of chunks
    """
    if chunk_size <= 0:
        return [lst]
    
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def retry_on_failure(func, max_retries: int = 3, delay: float = 1.0):
    """
    Decorator to retry function on failure.
    
    Args:
        func: Function to retry
        max_retries (int): Maximum number of retry attempts
        delay (float): Delay between retries in seconds
        
    Returns:
        Function result or raises last exception
    """
    import time
    
    def wrapper(*args, **kwargs):
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < max_retries:
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"All {max_retries + 1} attempts failed")
        
        if last_exception:
            raise last_exception
        else:
            raise Exception("All retry attempts failed")
    
    return wrapper