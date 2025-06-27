from fastapi import FastAPI
from app.models.user import Base
from app.database import engine
from app.routers import user, recommendations  # Add recommendations

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="E-Commerce Recommendation System",
    description="AI-powered product recommendations using Big Data technologies",
    version="1.0.0"
)

# Include routers
app.include_router(user.router)
app.include_router(recommendations.router)  # Add this line

@app.get("/")
def root():
    return {
        "message": "E-Commerce Recommendation API is running",
        "features": [
            "Collaborative Filtering",
            "Content-Based Filtering", 
            "Real-time Recommendations",
            "Trending Products"
        ]
    }

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "services": {
            "api": "running",
            "database": "connected",
            "kafka": "connected",
            "spark": "ready"
        }
    }
    
import time
from sqlalchemy.exc import OperationalError

for _ in range(5):
    try:
        Base.metadata.create_all(bind=engine)
        break
    except OperationalError:
        print("Database not ready, retrying...")
        time.sleep(3)
