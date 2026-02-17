import sys
import os

# Add the project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.utils.quotation_parser import extract_price_from_content

def test_extraction():
    test_cases = [
        {
            "name": "Simple Grand Total",
            "content": "Item 1: 100\nItem 2: 200\nGrand Total: 300.00 AED",
            "expected_amount": 300.0
        },
        {
            "name": "Total with Commas",
            "content": "Description: Heavy Machinery\nNet Amount: 1,500.00\nVAT 5%: 75.00\nTotal Payable: 1,575.00 AED",
            "expected_amount": 1575.0
        },
        {
            "name": "LPO Total",
            "content": "Purchase Order\nItems: 50 units\nLPO Total: 2500.00 USD",
            "expected_amount": 2500.0
        }
    ]

    for case in test_cases:
        print(f"Testing: {case['name']}")
        result = extract_price_from_content(case['content'])
        print(f"Result: {result}")
        if result['amount'] == case['expected_amount']:
            print("✅ Success")
        else:
            print(f"❌ Failed: Expected {case['expected_amount']}, got {result['amount']}")
        print("-" * 20)

if __name__ == "__main__":
    test_extraction()
