"""
Configuration settings for QuoteSnap application.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """
    Base configuration class with common settings.
    """
    
    # Flask Configuration
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    DEBUG = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    # Gmail API Configuration
    GMAIL_CREDENTIALS_FILE = os.environ.get('GMAIL_CREDENTIALS_FILE') or 'credentials.json'
    GMAIL_TOKEN_FILE = os.environ.get('GMAIL_TOKEN_FILE') or 'token.json'
    GMAIL_SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.modify'
    ]
    
    # Email Monitoring Configuration
    EMAIL_CHECK_INTERVAL = int(os.environ.get('EMAIL_CHECK_INTERVAL', '60'))
    
    @staticmethod
    def validate_config():
        """
        Validate that all required configuration values are present.
        """
        # Only check if credentials file exists
        if not os.path.exists(Config.GMAIL_CREDENTIALS_FILE):
            raise ValueError(f"Gmail credentials file not found: {Config.GMAIL_CREDENTIALS_FILE}")

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """
    Base configuration class with common settings.
    """
    
    # Flask Configuration
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    DEBUG = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    # CORS Configuration
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', 'http://localhost:5173,http://localhost:3000').split(',')
    FRONTEND_URL = os.environ.get('FRONTEND_URL', 'http://localhost:5173')
    
    # OAuth Redirect Configuration
    OAUTH_REDIRECT_URI = os.environ.get('OAUTH_REDIRECT_URI', 'http://localhost:5000/api/auth/callback')
    
    # Database Configuration
    DATABASE_URL = os.environ.get('DATABASE_URL') or 'sqlite:///database/quotesnap.db'
    
    # Gmail API Configuration
    GMAIL_CREDENTIALS_FILE = os.environ.get('GMAIL_CREDENTIALS_FILE') or 'credentials.json'
    GMAIL_TOKEN_DIRECTORY = os.environ.get('GMAIL_TOKEN_DIRECTORY') or 'tokens'
    GMAIL_SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.modify'
    ]
    
    # OpenAI Configuration
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
    OPENAI_MODEL = os.environ.get('OPENAI_MODEL') or 'gpt-3.5-turbo'
    
    # Email Monitoring Configuration
    EMAIL_CHECK_INTERVAL = int(os.environ.get('EMAIL_CHECK_INTERVAL', '300'))  # 5 minutes
    MAX_EMAILS_PER_CHECK = int(os.environ.get('MAX_EMAILS_PER_CHECK', '50'))
    
    # File Storage Configuration
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER') or 'uploads'
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    
    # Logging Configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL') or 'INFO'
    LOG_FILE = os.environ.get('LOG_FILE') or 'logs/app.log'
    
    # Excel Template Configuration
    EXCEL_TEMPLATE_PATH = os.environ.get('EXCEL_TEMPLATE_PATH') or 'templates/quotation_template.xlsx'
    
    @staticmethod
    def validate_config():
        """
        Validate that all required configuration values are present.
        
        Raises:
            ValueError: If required configuration is missing
        """
        required_files = [
            ('GMAIL_CREDENTIALS_FILE', 'credentials.json')
        ]
        
        required_env_vars = [
            'OPENAI_API_KEY'
        ]
        
        missing_items = []
        
        # Check required files exist
        for env_var, default_path in required_files:
            file_path = os.environ.get(env_var, default_path)
            if not os.path.exists(file_path):
                missing_items.append(f"File '{file_path}' (from {env_var})")
        
        # Check required environment variables
        for var in required_env_vars:
            if not os.environ.get(var):
                missing_items.append(f"Environment variable '{var}'")
        
        if missing_items:
            raise ValueError(f"Missing required configuration: {', '.join(missing_items)}")

class DevelopmentConfig(Config):
    """
    Development environment configuration.
    """
    DEBUG = True
    DATABASE_URL = 'sqlite:///database/quotesnap_dev.db'

class ProductionConfig(Config):
    """
    Production environment configuration.
    """
    DEBUG = False
    # Use more secure settings in production
    
class TestingConfig(Config):
    """
    Testing environment configuration.
    """
    TESTING = True
    DATABASE_URL = 'sqlite:///:memory:'  # In-memory database for testing

# Configuration mapping
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}