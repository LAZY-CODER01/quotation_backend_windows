"""
PDF file processing utility for SnapQuote.

Converts PDF content to markdown format.
"""

import PyPDF2
import io
import logging

logger = logging.getLogger(__name__)

def pdf_to_markdown(pdf_content: bytes) -> str:
    """
    Convert PDF content to markdown format.
    
    Args:
        pdf_content (bytes): PDF file content as bytes
        
    Returns:
        str: PDF content converted to markdown format
    """
    try:
        # Create a BytesIO object from the PDF content
        pdf_file = io.BytesIO(pdf_content)
        
        # Create a PDF reader object
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        markdown_content = []
        markdown_content.append("# PDF Document Content\n")
        
        # Extract text from each page
        for page_num, page in enumerate(pdf_reader.pages):
            try:
                text = page.extract_text()
                if text.strip():  # Only add non-empty pages
                    markdown_content.append(f"## Page {page_num + 1}\n")
                    # Clean up the text and format it
                    cleaned_text = text.replace('\n\n', '\n').strip()
                    markdown_content.append(f"{cleaned_text}\n\n")
            except Exception as e:
                logger.warning(f"Error extracting text from page {page_num + 1}: {str(e)}")
                markdown_content.append(f"## Page {page_num + 1}\n")
                markdown_content.append("*[Error extracting text from this page]*\n\n")
        
        if len(markdown_content) == 1:  # Only header, no content extracted
            markdown_content.append("*[No readable text content found in this PDF]*\n")
        
        return "".join(markdown_content)
        
    except Exception as e:
        logger.error(f"Error processing PDF: {str(e)}")
        return f"# PDF Processing Error\n\n*Error: {str(e)}*\n"