import os
import json
import re
from openai import OpenAI
from dotenv import load_dotenv


# Load environment variables (if using .env file)
print(f"🔄 Loading environment variables from .env file...")
env_loaded = load_dotenv()
print(f"📋 Environment loaded: {env_loaded}")

def normalize_input(text: str) -> str:
    """
    Normalize OCR / email text for better extraction
    """
    text = text.replace("|", "\n")
    text = text.replace(",", "\n")
    text = re.sub(r"\n+", "\n", text)
    return text.strip()



def contains_dbsq_code(text: str) -> bool:
    """
    Detect internal DBSQ codes like:
    DBSQ1111
    dbsq-2025
    Dbsq_8877
    """
    pattern = r"\bdbsq[\s\-_]?\d+\b"
    return re.search(pattern, text, re.IGNORECASE) is not None


def extract_json_from_response(response_text: str) -> dict:
    # Clean up common AI "chatter" if not using JSON mode strictly
    content = response_text.strip()
    if content.startswith("```json"):
        content = content.replace("```json", "", 1).rstrip("```")
    elif content.startswith("```"):
        content = content.replace("```", "", 1).rstrip("```")
    
    try:
        return json.loads(content.strip())
    except json.JSONDecodeError:
        pass
    
    # Remove markdown code blocks with language specifier using robust regex
    # Pattern looks for ``` optionally followed by json, then captures content until checks
    # regex matches: ```json { ... } ``` or ``` { ... } ```
    markdown_pattern = r'```(?:json)?\s*(.*?)\s*```'
    matches = re.findall(markdown_pattern, response_text, re.DOTALL)
    
    if matches:
        # Try parsing the content inside code blocks
        for match in matches:
            try:
                # If there are multiple blocks, return the first valid JSON
                return json.loads(match.strip())
            except json.JSONDecodeError:
                continue
    
    # Try to extract JSON object/array using regex
    # Look for {...} or [...]
    json_pattern = r'(\{(?:[^{}]|(?:\{[^{}]*\}))*\}|\[(?:[^\[\]]|(?:\[[^\[\]]*\]))*\])'
    json_matches = re.findall(json_pattern, response_text, re.DOTALL)
    
    for match in json_matches:
        try:
            return json.loads(match)
        except json.JSONDecodeError:
            continue
    
    # If nothing works, raise an error
    raise ValueError(f"Could not extract valid JSON from response: {response_text[:200]}...")


def extract_hardware_quotation_details(email_content: str):
        
    normalized_text = normalize_input(email_content)
    
    # Check for DBSQ priority code
    is_priority = contains_dbsq_code(normalized_text)
    if is_priority:
        print(f"🚨 DBSQ Code Detected! Forcing VALID processing.")
    """
    Single AI call that validates email and extracts quotation data if valid.
    Returns [IRRELEVANT] for non-quotation emails or JSON for valid requests.
    Uses Structured Outputs for guaranteed JSON parsing.
    """

    # Initialize OpenAI client
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("❌ OPENAI_API_KEY not found in environment variables")
        raise ValueError("OPENAI_API_KEY not found in environment variables")
    
    print(f"🔑 OpenAI API key found (length: {len(api_key)})")
    try:
        client = OpenAI(api_key=api_key)
        print(f"✅ OpenAI client initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize OpenAI client: {str(e)}")
        raise Exception(f"Failed to initialize OpenAI client: {str(e)}")

    # Create unified prompt for validation + extraction
    prompt = f"""
You are an intelligent email processor that handles quotation requests for hardware products, tools, and industrial equipment.

The input text may come from:
- Emails
- Scanned PDFs (OCR output)
- Images converted to text
- WhatsApp-style messages
- Poorly formatted or broken text
- Comma-separated or line-separated item lists


TASK: Analyze the email below and either:
1. Return exactly {{"status": "IRRELEVANT"}} if it's NOT a quotation request
2. Return a JSON object if it IS a valid quotation request



WHAT MAKES AN EMAIL IRRELEVANT (return {{"status": "IRRELEVANT"}}):
- Personal messages or casual conversations
- Marketing/promotional emails  
- System notifications (Google security alerts, etc.)
- Social media notifications
- Order confirmations or shipping updates
- Support tickets or customer service
- General inquiries without specific product requests
- Spam, newsletters, or unrelated content

WHAT MAKES AN EMAIL VALID (return JSON with quotation data):
- Contains request for pricing, quotation, or quote
- Mentions specific hardware products, tools, or equipment
- Has business inquiry tone
- Includes quantities, specifications, or requirements
- Mentions specific hardware products, tools, or equipment (Even if it's just a list)
- Asking for product information or simply listing items with intent to purchase/quote



━━━━━━━━━━━━━━━━━━━━━━
CRITICAL EXTRACTION RULES (VERY IMPORTANT)
━━━━━━━━━━━━━━━━━━━━━━

- EACH PRODUCT MUST BE A SEPARATE ITEM
- Split items even if separated by:
  - commas
  - new lines
  - OCR line breaks
  - bullet points
- NEVER merge multiple products into one Description
- NEVER guess or hallucinate:
  - quantity
  - unit
  - unit price
- If any field is missing, return an empty string ""





METADATA EXTRACTION PRIORITY:
- **Company Name**: 
  - 1. Look in the signature block (Best source).
  - 2. Look for text from logos or headers.
  - 3. Infer from sender's email domain (e.g. from @dbest.com -> Dbest).
  - 4. Default to "Unknown/Individual" if absolutely no trace found.
- **Sender Name**:
  - 1. Extract from "From:" header (e.g. "Ahmed Hassan" <...>)
  - 2. Look for sign-off (e.g. "Regards, Ahmed").
  - 3. Return "" if not found.

IF VALID, return this exact JSON structure:
{{
  "status": "VALID",
  "sender_name": "Name of the sender (e.g. John Doe)",
  "company_name": "Name of the company (e.g. ACME Corp)",
  "email": "Email address of requester (empty string if not found)", 
  "mobile": "Phone number of requester (empty string if not found)",
  "Requirements": [
    {{
      "Description": "Product description and specifications",
      "Quantity": "Quantity if available, otherwise empty string",
      "Unit": "Unit for quantity (pcs/Kg/Litre/etc) if available, otherwise empty string",
      "Unit price": "Unit price if available, otherwise empty string"
    }}
  ]
}}

EMAIL CONTENT:
\"\"\"{normalized_text}\"\"\"

RESPONSE (MUST be valid JSON only, no additional text):"""

    # Inject override rule into prompt if priority
    if is_priority:
        prompt = "IMPORTANT OVERRIDE RULE:\nIf the email contains a DBSQ code, NEVER return IRRELEVANT.\nAlways treat it as VALID and extract all useful tender/procurement items.\n\n" + prompt

    # Make API call with JSON mode enabled for guaranteed JSON output
  
    print(f"🔄 Making OpenAI API call with JSON mode...")
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a JSON generator. Always respond with valid JSON only. Never include markdown formatting or additional text."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},  # Enable JSON mode
            temperature=0
        )
        print(f"✅ OpenAI API call completed successfully")
    except Exception as e:
        print(f"❌ OpenAI API call failed: {str(e)}")
        raise e

    # Extract the response text
    response_text = response.choices[0].message.content.strip()
    print(f"📄 Raw API response: {response_text[:200]}{'...' if len(response_text) > 200 else ''}")

    # Parse JSON from response (handles markdown, code blocks, etc.)
    try:
        parsed_data = extract_json_from_response(response_text)
        
        # Check if email is irrelevant
        if parsed_data.get("status") == "IRRELEVANT":
            if is_priority:
                print(f"⚠️ AI returned IRRELEVANT but DBSQ code found. Overriding to VALID.")
                return {
                    "status": "VALID",
                    "sender_name": "",
                    "company_name": "",
                    "email": "",
                    "mobile": "",
                    "Requirements": []
                }
            return {"status": "NOT_VALID", "reason": "Email is not a quotation request"}
        
        return parsed_data
        
    except (json.JSONDecodeError, ValueError) as e:
        print(f"⚠️ Failed to parse JSON from response: {str(e)}")
        print(f"Raw response: {response_text}")
        return {"status": "ERROR", "reason": "Failed to parse response", "raw_response": response_text}


# Example usage - Single API call handles both validation and extraction
if __name__ == "__main__":
    # Example 1: Valid quotation request (should return JSON with structured requirements)
    valid_email = """
    Dear Supplier,
    
    We are interested in placing a bulk order for screwdriver sets.
    Please share quotation details for both flat-head and Philips-head screwdrivers,
    in sizes ranging from 2mm to 8mm. Quantity required: 200 sets.
    
    Also need Stanley brand precision screwdriver set - 50 pieces at $25 per piece.
    
    Kindly include details for insulated and non-insulated handle designs separately.
    
    Regards,
    Sanat Engineering Works
    Contact: sanat@engworks.com
    Phone: +91-9876543210
    """

    # Example 2: Irrelevant email (should return {"status": "IRRELEVANT"})
    invalid_email = """
    You allowed SnapQuote access to some of your Google Account data
    
    snapquote.v1@gmail.com
    
    If you didn't allow SnapQuote access to some of your Google Account data,
    someone else may be trying to access your Google Account data.
    
    Take a moment now to check your account activity and secure your account.
    © 2025 Google LLC, 1600 Amphitheatre Parkway, Mountain View, CA 94043, USA
    """
   
    sample_text = """
    Masking Tape 2" Plastic Roll, Pencil,
    Oil silicone Gulf 1200
    National Silicone
    Diamond Disc 4.5"
    Hacksaw blade 12"
    Garbage bag 25kg
    """

    result = extract_hardware_quotation_details(sample_text)
    print(json.dumps(result, indent=2))
    print("=== Testing Valid Email (Single API Call) ===")
    valid_result = extract_hardware_quotation_details(valid_email)
    print(json.dumps(valid_result, indent=2))
    
    print("\n=== Testing Invalid Email (Single API Call) ===")
    invalid_result = extract_hardware_quotation_details(invalid_email)
    print(json.dumps(invalid_result, indent=2))


def extract_price_from_content(content: str) -> dict:
    """
    Extract total price/amount from document content using AI.
    Returns JSON with amount and currency.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {"amount": 0.0, "currency": ""}

    client = OpenAI(api_key=api_key)
    
    # Clean content to reduce token usage
    content = normalize_input(content)[:10000] # Limit context window if huge

    prompt = f"""
    Analyze the following document content (Quotation or PO) and extract the GRAND TOTAL AMOUNT.
    
    Look for keywords:
    - Grand Total
    - Total Amount
    - PO Total
    - Final Amount
    - Net Total containing VAT

    Return as JSON:
    {{
      "amount": number (float, 0.0 if not found),
      "currency": string (e.g. AED, USD),
      "confidence": float (0.0 to 1.0)
    }}

    CONTENT:
    \"\"\"{content}\"\"\"
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
        
        result_text = response.choices[0].message.content.strip()
        data = json.loads(result_text)
        
        # Ensure correct types
        amount = float(str(data.get("amount", 0)).replace(",", ""))
        return {
            "amount": amount,
            "currency": data.get("currency", "AED"),
            "confidence": data.get("confidence", 0.0)
        }

    except Exception as e:
        print(f"Price extraction failed: {e}")
        return {"amount": 0.0, "currency": ""}
