import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.ai_email_extraction import contains_dbsq_code, extract_hardware_quotation_details

class TestDBSQOverride(unittest.TestCase):
    def test_contains_dbsq_code(self):
        print("\nTesting contains_dbsq_code...")
        self.assertTrue(contains_dbsq_code("Subject: DBSQ1234 Project"))
        self.assertTrue(contains_dbsq_code("Order dbsq-999"))
        self.assertTrue(contains_dbsq_code("Re: Dbsq_555 update"))
        self.assertTrue(contains_dbsq_code("DBSQ 001"))
        
        self.assertFalse(contains_dbsq_code("Just a normal email"))
        self.assertFalse(contains_dbsq_code("DBSQ without number"))
        self.assertFalse(contains_dbsq_code("12345"))
        print("  contains_dbsq_code passed")

    @patch('app.services.ai_email_extraction.OpenAI')
    @patch('app.services.ai_email_extraction.os.getenv')
    def test_override_logic(self, mock_getenv, mock_openai):
        print("\nTesting override logic...")
        # Setup mocks
        mock_getenv.return_value = "fake-key"
        
        mock_client = MagicMock()
        mock_completion = MagicMock()
        mock_choice = MagicMock()
        mock_message = MagicMock()
        
        # Configure the mock to return "IRRELEVANT"
        mock_message.content = '{"status": "IRRELEVANT"}'
        mock_choice.message = mock_message
        mock_completion.choices = [mock_choice]
        mock_client.chat.completions.create.return_value = mock_completion
        mock_openai.return_value = mock_client

        # Test Case 1: Email WITH DBSQ code -> Should be overridden to VALID
        dbsq_email = "This is a test email with DBSQ1234 code."
        print(f"Testing with email: {dbsq_email}")
        result = extract_hardware_quotation_details(dbsq_email)
        
        self.assertEqual(result['status'], "VALID", "Should result in VALID due to override")
        self.assertEqual(result['Requirements'], [], "Should have empty requirements")
        print("  DBSQ override passed")

        # Test Case 2: Email WITHOUT DBSQ code -> Should remain NOT_VALID (IRRELEVANT)
        normal_email = "This is a junk email."
        print(f"Testing with email: {normal_email}")
        result_normal = extract_hardware_quotation_details(normal_email)
        
        self.assertEqual(result_normal['status'], "NOT_VALID", "Should remain NOT_VALID")
        print("  Normal flow passed")

if __name__ == '__main__':
    unittest.main()
