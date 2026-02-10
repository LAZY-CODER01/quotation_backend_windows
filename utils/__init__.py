"""
File processing utilities for SnapQuote.

Main utility functions for processing different file types.
"""

import logging
from .pdf_processor import pdf_to_markdown
from .excel_processor import excel_to_markdown
from .docx_processor import docx_to_markdown
from .image_processor import image_to_markdown

# Set up logging to catch errors
logger = logging.getLogger(__name__)

__all__ = ['process_attachment', 'pdf_to_markdown', 'excel_to_markdown', 'docx_to_markdown']

def process_attachment(filename: str, content: bytes) -> str:
    """
    Process an attachment based on its file type and return markdown content.
    Safely handles errors so the app doesn't crash on bad files.
    """
    filename_lower = filename.lower()
    
    try:
        if filename_lower.endswith('.pdf'):
            return pdf_to_markdown(content)
        elif filename_lower.endswith(('.xlsx', '.xls')):
            return excel_to_markdown(content, filename=filename)
        elif filename_lower.endswith(('.docx', '.doc')):
            return docx_to_markdown(content)
        elif filename_lower.endswith(('.png', '.jpg', '.jpeg', '.tiff', '.bmp')):
            return image_to_markdown(content)
        else:
            return f"# Unsupported File Type\n\nFile: {filename}\n\n*This file type is not supported for content extraction.*\n"
            
    except Exception as e:
        logger.error(f"Error processing attachment {filename}: {str(e)}")
        return f"# Processing Error\n\nFile: {filename}\n\n*Error extracting content: {str(e)}*\n"