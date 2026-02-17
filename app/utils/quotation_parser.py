import os
import json
import base64
import logging

from openai import OpenAI
from dotenv import load_dotenv
from app.utils.file_parser import extract_text_from_file

logger = logging.getLogger(__name__)
load_dotenv()

def extract_grand_total(file_path: str) -> dict:
    """
    Routes the file to the correct OpenAI extraction method.
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return {"amount": 0.0, "currency": "AED", "confidence": 0.0}

    ext = os.path.splitext(file_path)[1].lower()
    image_extensions = ['.jpg', '.jpeg', '.png', '.bmp', '.tiff']

    try:
        # Route 1: Direct Image to Vision API
        if ext in image_extensions:
            logger.info("Image detected. Routing to Vision API...")
            return extract_price_from_vision(file_path)

        # Route 2: Text extraction for Excel and PDFs
        text_content = extract_text_from_file(file_path)
        logger.info(f"Extracted text content length: {len(text_content)} characters")
        
        if not text_content.strip():
            logger.warning(f"No text content extracted from {file_path}")
            return {"amount": 0.0, "currency": "AED", "confidence": 0.0}

        return extract_price_from_content(text_content)

    except Exception as e:
        logger.error(f"Failed to extract grand total from {file_path}: {e}")
        return {"amount": 0.0, "currency": "AED", "confidence": 0.0}

def extract_price_from_content(content: str) -> dict:
    """
    Extract total price from text using gpt-4o-mini.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    client = OpenAI(api_key=api_key)

    # Grabs the LAST 15,000 characters to ensure the footer/totals aren't cut off
  
    content_cleaned = content
    print(f"Original content length: {content} characters")
    prompt = f"""
    Analyze the following document content (Quotation, LPO, or CPO) and extract the GRAND TOTAL AMOUNT.
    
    Look for keywords: Grand Total, Total Amount, PO Total, Final Amount, Net Total.

    Rules:
    1. If multiple totals exist, pick the final "Grand Total" or "Total Payable".
    2. Ensure you handle thousands separators (commas).
    3. If no total is found, return 0.0.

    Return as JSON:
    {{
      "amount": number (float),
      "currency": string (e.g. AED, USD, default to AED if not sure),
      "confidence": float (0.0 to 1.0)
    }}

    CONTENT:
    \"\"\"{content_cleaned}\"\"\"
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a data extraction assistant. Output valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0
        )
        
        data = json.loads(response.choices[0].message.content.strip())
        raw_amount = data.get("amount", 0)
        amount = float(str(raw_amount).replace(",", ""))

        return {
            "amount": amount,
            "currency": data.get("currency", "AED"),
            "confidence": data.get("confidence", 0.0)
        }

    except Exception as e:
        logger.error(f"Text extraction failed: {e}")
        return {"amount": 0.0, "currency": "AED", "confidence": 0.0}

def extract_price_from_vision(image_path: str) -> dict:
    """
    Extract total price directly from an image using gpt-4o-mini Vision.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    client = OpenAI(api_key=api_key)

    with open(image_path, "rb") as image_file:
        base64_image = base64.b64encode(image_file.read()).decode('utf-8')

    prompt = """
    Analyze this document image and extract the GRAND TOTAL AMOUNT.
    Look for keywords like Grand Total, Total Amount, PO Total, Net Total.
    
    Return ONLY valid JSON:
    {
      "amount": number (float, remove commas),
      "currency": "string (e.g. AED, USD)",
      "confidence": float (0.0 to 1.0)
    }
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ],
            response_format={"type": "json_object"},
            temperature=0
        )
        
        data = json.loads(response.choices[0].message.content.strip())
        raw_amount = data.get("amount", 0)
        amount = float(str(raw_amount).replace(",", ""))

        return {
            "amount": amount,
            "currency": data.get("currency", "AED"),
            "confidence": data.get("confidence", 0.0)
        }

    except Exception as e:
        logger.error(f"Vision extraction failed: {e}")
        return {"amount": 0.0, "currency": "AED", "confidence": 0.0}