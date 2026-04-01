"""Authentication router - simple password-based token auth."""
from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from itsdangerous import TimestampSigner, BadSignature, SignatureExpired
from backend.config import APP_PASSWORD, SECRET_KEY

router = APIRouter(prefix="/api/auth", tags=["auth"])
security = HTTPBearer(auto_error=False)
signer = TimestampSigner(SECRET_KEY)

TOKEN_MAX_AGE = 60 * 60 * 24 * 7  # 7 days


class LoginRequest(BaseModel):
    password: str


class LoginResponse(BaseModel):
    token: str
    expires_in: int = TOKEN_MAX_AGE


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> bool:
    """Dependency: verify Bearer token on protected endpoints."""
    if not credentials:
        raise HTTPException(status_code=401, detail="Authentication required")
    try:
        signer.unsign(credentials.credentials, max_age=TOKEN_MAX_AGE)
        return True
    except (BadSignature, SignatureExpired):
        raise HTTPException(status_code=401, detail="Invalid or expired token")


@router.post("/login", response_model=LoginResponse)
def login(req: LoginRequest):
    if req.password != APP_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    token = signer.sign("authenticated").decode()
    return LoginResponse(token=token)
