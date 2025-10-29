from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from app.utils.response_builder import error_response
import traceback

class GlobalErrorHandler(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            return await call_next(request)
        except HTTPException as e:
            # Handles typical FastAPI/HTTP exceptions
            return error_response(
                message=e.detail,
                status_code=e.status_code
            )
        except Exception as e:
            # Handles unexpected errors
            traceback.print_exc()
            return error_response(
                message="Internal server error",
                status_code=500,
                details={"error": str(e)}
            )
