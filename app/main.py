from fastapi import FastAPI
import uvicorn
from app.middleware.error_handler import GlobalErrorHandler

app = FastAPI(title="Data Hub API")

app.add_middleware(GlobalErrorHandler)

@app.get("/")
def root():
    return {"message": "Welcome to Data Hub!"}

def start():
    """Run the server."""
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)