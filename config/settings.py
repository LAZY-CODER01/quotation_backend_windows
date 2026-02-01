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
    
    # Flask Configuration (Session usage removed, but KEY kept for other Flask internals if needed)
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    DEBUG = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    # JWT Configuration
    JWT_SECRET = os.environ.get('JWT_SECRET') or 'your-secret-key-change-in-production'
    JWT_ALGO= 'HS256'
    JWT_EXPIRES_HOURS = int(os.environ.get('JWT_EXPIRES_HOURS', '24'))
    
    # Company Gmail Configuration
    COMPANY_GMAIL_ID = 'demo.snapquote@gmail.com'
    
    # CORS Configuration
    CORS_ORIGINS = os.environ.get('CORS_ORIGINS', 'http://localhost:3000,http://localhost:5173').split(',')
    FRONTEND_URL = os.environ.get('FRONTEND_URL', 'http://localhost:3000')
    
    # OAuth Redirect Configuration
    OAUTH_REDIRECT_URI = os.environ.get('OAUTH_REDIRECT_URI', 'http://localhost:3000/admin/gmail/callback')
    
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
    EMAIL_CHECK_INTERVAL = int(os.environ.get('EMAIL_CHECK_INTERVAL', '30'))  # 30 seconds
    MAX_EMAILS_PER_CHECK = int(os.environ.get('MAX_EMAILS_PER_CHECK', '50'))
    
    # File Storage Configuration
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER') or 'uploads'
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    
    # Logging Configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL') or 'INFO'
    LOG_FILE = os.environ.get('LOG_FILE') or 'logs/app.log'
    
    # Excel Template Configuration
    EXCEL_TEMPLATE_PATH = os.environ.get('EXCEL_TEMPLATE_PATH') or 'templates/quotation_template.xlsx'
    CLOUDINARY_CLOUD_NAME = os.getenv('CLOUDINARY_CLOUD_NAME')
    CLOUDINARY_API_KEY = os.getenv('CLOUDINARY_API_KEY')
    CLOUDINARY_API_SECRET = os.getenv('CLOUDINARY_API_SECRET')

    
    # Allowed files
    ALLOWED_EXTENSIONS = {'pdf', 'xlsx', 'xls', 'doc', 'docx', 'png', 'jpg', 'jpeg'}
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