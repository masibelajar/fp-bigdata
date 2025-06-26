from fastapi import APIRouter, HTTPException
from typing import List, Optional
import requests
import json

router = APIRouter(prefix="/recommendations", tags=["Recommendations"])

@router.get("/user/{user_id}")
async def get_user_recommendations(
    user_id: int, 
    limit: int = 10
):
    """Get personalized recommendations for a user"""
    try:
        # Call Spark ML service (akan kita implement)
        recommendations = await call_spark_recommendation_service(user_id, limit)
        return {
            "user_id": user_id,
            "recommendations": recommendations,
            "algorithm": "collaborative_filtering"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/similar/{asin}")
async def get_similar_products(
    asin: str,
    limit: int = 10
):
    """Get products similar to given product"""
    try:
        similar_products = await call_spark_similar_products_service(asin, limit)
        return {
            "target_product": asin,
            "similar_products": similar_products,
            "algorithm": "content_based"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/trending")
async def get_trending_products(category_id: Optional[int] = None, limit: int = 20):
    """Get trending/popular products"""
    try:
        # Simple popularity-based recommendation
        trending = await get_popular_products(category_id, limit)
        return {
            "trending_products": trending,
            "algorithm": "popularity_based"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def call_spark_recommendation_service(user_id: int, limit: int):
    """Call Spark ML service for recommendations"""
    # Placeholder - akan diintegrasikan dengan Spark service
    return [
        {"asin": "B123456", "predicted_rating": 4.5, "title": "Sample Product 1"},
        {"asin": "B789012", "predicted_rating": 4.3, "title": "Sample Product 2"}
    ]

async def call_spark_similar_products_service(asin: str, limit: int):
    """Call Spark ML service for similar products"""
    # Placeholder - akan diintegrasikan dengan Spark service
    return [
        {"asin": "B111111", "similarity_score": 0.95, "title": "Similar Product 1"},
        {"asin": "B222222", "similarity_score": 0.89, "title": "Similar Product 2"}
    ]

async def get_popular_products(category_id: Optional[int], limit: int):
    """Get popular products from database"""
    # Placeholder - query dari database
    return [
        {"asin": "B333333", "popularity_score": 0.98, "title": "Popular Product 1"},
        {"asin": "B444444", "popularity_score": 0.94, "title": "Popular Product 2"}
    ]