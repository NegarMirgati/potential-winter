from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from app.models.user import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def get_current_user(token: str = Depends(oauth2_scheme)):
    # Placeholder for authentication logic
    user = User(username="testuser", email="test@example.com")
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return user