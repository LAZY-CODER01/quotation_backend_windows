import jwt
import os
from datetime import datetime, timedelta, timezone
from config.settings import Config

def create_jwt(user):
    """
    Create a JWT token for a user.
    
    Args:
        user (dict): User dictionary containing 'id', 'username', 'role'.
        
    Returns:
        str: Encoded JWT token.
    """
    secret = Config.JWT_SECRET
    algo = Config.JWT_ALGO
    expires_hours = Config.JWT_EXPIRES_HOURS
    
    payload = {
        "user_id": user["id"],
        "username": user["username"],
        "role": user["role"],
        "exp": datetime.now(timezone.utc) + timedelta(hours=expires_hours),
        "iat": datetime.now(timezone.utc)
    }
    
    return jwt.encode(payload, secret, algorithm=algo)
