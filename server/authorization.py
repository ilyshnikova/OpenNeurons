from .env import base
from .helpers import get_user_auth

from functools import wraps
from flask import request, Response

import hashlib

def hash_auth(auth):
    auth_hash = hashlib.md5()
    auth_hash.update(auth.encode('utf-8'))
    return auth_hash.hexdigest()

def check_auth(username, auth, hash_it=False):
    if hash_it:
        auth = hash_auth(auth)
    return auth == get_user_auth(base, username)

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.cookies.get("auth")
        username = request.cookies.get("name")
        if not auth or not check_auth(username, auth):
            return authenticate()
        return f(*args, **kwargs)
    return decorated
