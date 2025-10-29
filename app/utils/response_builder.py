from typing import Any, Optional
from fastapi.responses import JSONResponse

def success_response(
    data: Any = None,
    message: str = "Request successful",
    status_code: int = 200
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "success",
            "message": message,
            "data": data,
        },
    )

def error_response(
    message: str,
    status_code: int = 400,
    details: Optional[Any] = None
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "error",
            "message": message,
            "details": details,
        },
    )
