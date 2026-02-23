import PyPDF2
import io
import logging
import pytesseract
from pdf2image import convert_from_bytes
import sys

logger = logging.getLogger(__name__)

if sys.platform == 'win32':
    pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def pdf_to_markdown(pdf_content: bytes) -> str:
    """
    Convert PDF content to markdown format.
    Uses OCR (via pdf2image and pytesseract) if standard text extraction fails.
    
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
                
                # Check if text is scanned (empty or very short)
                # Threshold: < 50 characters might suggest it's just a header/footer or empty
                if not text or len(text.strip()) < 50:
                    logger.info(f"Page {page_num + 1} seems scanned (text len: {len(text) if text else 0}). Attempting OCR...")
                    try:
                        # Convert specific page to image
                        # fmt='jpeg' for speed, dpi=300 for OCR accuracy
                        images = convert_from_bytes(pdf_content, first_page=page_num+1, last_page=page_num+1, fmt='jpeg', dpi=300)
                        
                        ocr_text = ""
                        for img in images:
                            ocr_text += pytesseract.image_to_string(img)
                        
                        if ocr_text.strip():
                            text = ocr_text # Use OCR result if valid
                            markdown_content.append(f"## Page {page_num + 1} (OCR Extracted)\n")
                        else:
                            markdown_content.append(f"## Page {page_num + 1}\n") # Stick with original header
                    except Exception as ocr_e:
                        error_msg = str(ocr_e).lower()
                        if "poppler" in error_msg:
                             logger.warning(f"OCR failed: Poppler not installed. {ocr_e}")
                             markdown_content.append(f"## Page {page_num + 1}\n*[OCR Failed: Poppler (pdf2image) not installed]*\n")
                        elif "tesseract" in error_msg:
                             logger.warning(f"OCR failed: Tesseract not installed. {ocr_e}")
                             markdown_content.append(f"## Page {page_num + 1}\n*[OCR Failed: Tesseract not installed]*\n")
                        else:
                            logger.warning(f"OCR failed for page {page_num + 1}: {str(ocr_e)}")
                            markdown_content.append(f"## Page {page_num + 1}\n*[OCR Failed: {str(ocr_e)}]*\n")
                else:
                    markdown_content.append(f"## Page {page_num + 1}\n")

                if text.strip():  # Only add non-empty content
                    # Clean up the text and format it
                    cleaned_text = text.replace('\n\n', '\n').strip()
                    markdown_content.append(f"{cleaned_text}\n\n")
                else:
                     markdown_content.append("*[No text content found]*\n\n")

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