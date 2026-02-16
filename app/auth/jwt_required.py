import jwt
from functools import wraps
from flask import request, jsonify, current_app

def jwt_required(roles=None):
    """
    Decorator to protect routes with JWT authentication and optional Role-Based Access Control.
    
    Args:
        roles (list, optional): List of allowed roles (e.g. ['ADMIN', 'EMPLOYEE']). 
                                If None, any valid token is accepted.
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if request.method == "OPTIONS":
                return jsonify({'status': 'ok'}), 200

            auth = request.headers.get("Authorization")

            if not auth or not auth.startswith("Bearer "):
                return jsonify({"error": "Unauthorized: Missing or invalid token format"}), 401

            token = auth.split(" ")[1]
            secret = current_app.config.get('JWT_SECRET')
            algo = current_app.config.get('JWT_ALGORITHM', 'HS256')

            try:
                # Decode and verify token
                payload = jwt.decode(token, secret, algorithms=[algo])
                
                # Attach user info to request for use in endpoint
                request.user = payload
                
            except jwt.ExpiredSignatureError:
                return jsonify({"error": "Token expired"}), 401
            except jwt.InvalidTokenError:
                return jsonify({"error": "Invalid token"}), 401
            except Exception as e:
                return jsonify({"error": f"Authentication error: {str(e)}"}), 401

            # Role-Based Access Control
            if roles:
                user_role = payload.get("role")
                if user_role not in roles:
                    return jsonify({
                        "error": "Forbidden: Insufficient permissions",
                        "required_roles": roles,
                        "user_role": user_role
                    }), 403

            return fn(*args, **kwargs)
        return wrapper
    return decorator
