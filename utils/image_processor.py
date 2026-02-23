"""
Image file processing utility for SnapQuote.

Converts Image content to markdown format using OCR.
"""

import io
import logging
import pytesseract
from PIL import Image

import sys

logger = logging.getLogger(__name__)

# Add explicit path for Tesseract OCR on Windows if not in PATH
if sys.platform == 'win32':
    pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def image_to_markdown(image_content: bytes) -> str:
    """
    Convert Image content to markdown format using OCR.
    
    Args:
        image_content (bytes): Image file content as bytes
        
    Returns:
        str: Image content converted to markdown format
    """
    try:
        # Create a BytesIO object from the image content
        image_file = io.BytesIO(image_content)
        
        # Open the image using Pillow
        image = Image.open(image_file)
        
        markdown_content = []
        markdown_content.append("# Image Content (OCR)\n")
        
        # Perform OCR using pytesseract
        try:
            text = pytesseract.image_to_string(image)
            
            if text.strip():
                # Clean up the text
                cleaned_text = text.strip()
                markdown_content.append(f"{cleaned_text}\n\n")
            else:
                 markdown_content.append("*[No text detected in image]*\n")

        except Exception as e:
            logger.warning(f"OCR failed for image: {str(e)}")
            if "tesseract is not installed" in str(e).lower() or "not found" in str(e).lower():
                markdown_content.append(f"*[Error performing OCR: OCR Engine (Tesseract) not found. Please install tesseract-ocr]*\n")
            else:
                markdown_content.append(f"*[Error performing OCR: {str(e)}]*\n")

        return "".join(markdown_content)
        
    except Exception as e:
        logger.error(f"Error processing Image: {str(e)}")
        # Check for Tesseract not found error even if generic Exception caught
        if "tesseract is not installed" in str(e).lower() or "not found" in str(e).lower():
             return "# Image Processing Error\n\n*Error: OCR Engine (Tesseract) not found. Please install tesseract-ocr.*\n"
        return f"# Image Processing Error\n\n*Error: {str(e)}*\n"
