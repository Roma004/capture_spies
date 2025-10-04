from fastapi import Depends, FastAPI
from backend.routes import router as main_router

app = FastAPI()
app.include_router(main_router, prefix="/api/v1")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, forwarded_allow_ips="*")
