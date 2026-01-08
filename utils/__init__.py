"""
File processing utilities for SnapQuote.

Main utility functions for processing different file types.
"""

from .pdf_processor import pdf_to_markdown
from .excel_processor import excel_to_markdown
from .docx_processor import docx_to_markdown

__all__ = ['pdf_to_markdown', 'excel_to_markdown', 'docx_to_markdown']

def process_attachment(filename: str, content: bytes) -> str:
    """
    Process an attachment based on its file type and return markdown content.
    
    Args:
        filename (str): Name of the file
        content (bytes): File content as bytes
        
    Returns:
        str: Processed content in markdown format
    """
    filename_lower = filename.lower()
    
    if filename_lower.endswith('.pdf'):
        return pdf_to_markdown(content)
    elif filename_lower.endswith(('.xlsx', '.xls')):
        return excel_to_markdown(content)
    elif filename_lower.endswith(('.docx', '.doc')):
        return docx_to_markdown(content)
    else:
        return f"# Unsupported File Type\n\nFile: {filename}\n\n*This file type is not supported for content extraction.*\n"