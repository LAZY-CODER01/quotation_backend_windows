import os
import logging
import io
import re
from typing import Optional

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import PyPDF2
except ImportError:
    PyPDF2 = None

try:
    from pdf2image import convert_from_path, convert_from_bytes
    import pytesseract
    from PIL import Image
except ImportError:
    convert_from_path = None
    pytesseract = None
    Image = None

logger = logging.getLogger(__name__)

def extract_text_from_file(file_path: str, mime_type: Optional[str] = None) -> str:
    """
    Extract text content from a file (PDF, Excel, Image).
    Autodetects type from extension if mime_type is not provided.
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return ""

    ext = os.path.splitext(file_path)[1].lower()
    
    try:
        if ext == '.pdf':
            return _extract_from_pdf(file_path)
        elif ext in ['.xlsx', '.xls', '.csv']:
            return _extract_from_excel(file_path)
        elif ext in ['.jpg', '.jpeg', '.png', '.bmp', '.tiff']:
            return _extract_from_image(file_path)
        elif ext in ['.txt', '.md']:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        else:
            logger.warning(f"Unsupported file type for text extraction: {ext}")
            return ""
            
    except Exception as e:
        logger.error(f"Error extracting text from {file_path}: {e}")
        return ""

def _extract_from_pdf(file_path: str) -> str:
    text = ""
    # 1. Try standard text extraction
    try:
        if PyPDF2:
            with open(file_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                for page in reader.pages:
                    text += page.extract_text() + "\n"
    except Exception as e:
        logger.warning(f"PyPDF2 extraction failed: {e}")

    # 2. If text is empty or very short, try OCR (Scanned PDF)
    if len(text.strip()) < 50:
        logger.info("PDF text empty or too short. Attempting OCR...")
        try:
            if convert_from_path and pytesseract:
                # limited to first 3 pages to save time/resources for large docs
                images = convert_from_path(file_path, first_page=1, last_page=3) 
                ocr_text = ""
                for img in images:
                    ocr_text += pytesseract.image_to_string(img) + "\n"
                
                if len(ocr_text.strip()) > len(text.strip()):
                    text = ocr_text
            else:
                logger.warning("OCR dependencies (pdf2image/pytesseract) not available.")
        except Exception as e:
             logger.error(f"OCR failed: {e}")
             
    return text

def _extract_from_excel(file_path: str) -> str:
    text = ""
    if not pd:
        logger.error("Pandas not installed.")
        return ""
        
    try:
        # Read all sheets
        xls = pd.ExcelFile(file_path)
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            # Convert to string, handling NaNs
            text += f"--- Sheet: {sheet_name} ---\n"
            text += df.to_string(index=False, na_rep="") + "\n"
    except Exception as e:
        logger.error(f"Excel extraction failed: {e}")
        
    return text

def _extract_from_image(file_path: str) -> str:
    if not pytesseract or not Image:
        logger.error("Pill/Tesseract not installed.")
        return ""
        
    try:
        image = Image.open(file_path)
        return pytesseract.image_to_string(image)
    except Exception as e:
        logger.error(f"Image OCR failed: {e}")
        return ""
