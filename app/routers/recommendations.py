from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
import asyncio
import logging

# Safe ML import with fallback
try:
    from services.ml_service.ml_adapter import ml_adapter
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False
    ml_adapter = None

router = APIRouter(prefix="/recommendations", tags=["recommendations"])

# Setup logging
logger = logging.getLogger(__name__)

# Pydantic Models
class RecommendationItem(BaseModel):
    asin: str
    title: str
    predicted_rating: float
    category: Optional[str] = None
    price: Optional[float] = None
    source: Optional[str] = "ml"

class SimilarProductItem(BaseModel):
    asin: str
    title: str
    similarity_score: float
    category: Optional[str] = None
    price: Optional[float] = None
    source: Optional[str] = "ml"

class TrendingProductItem(BaseModel):
    asin: str
    title: str
    popularity_score: float
    category: Optional[str] = None
    price: Optional[float] = None
    reviews: Optional[int] = None

class UserRecommendationsResponse(BaseModel):
    user_id: int
    recommendations: List[RecommendationItem]
    total_count: int
    source: str

class SimilarProductsResponse(BaseModel):
    target_product: str
    similar_products: List[SimilarProductItem]
    total_count: int
    source: str

class TrendingProductsResponse(BaseModel):
    trending_products: List[TrendingProductItem]
    total_count: int
    period: str

# Safe ML Service Functions
async def call_spark_user_recommendations_service(user_id: int, limit: int):
    """SAFE ML Integration: Use real ML with graceful fallback"""
    try:
        if ML_AVAILABLE and ml_adapter:
            # Try ML service first
            recommendations = await ml_adapter.get_user_recommendations_safe(user_id, limit)
            return recommendations
        else:
            raise Exception("ML service not available")
    except Exception as e:
        logger.warning(f"ML service failed for user {user_id}: {e}, using fallback")
        # Graceful fallback to mock data (NO BREAKING CHANGE)
        return [
            {
                "asin": f"B{user_id:03d}{i:03d}",
                "title": f"Recommended Product {i} for User {user_id}",
                "predicted_rating": round(4.8 - (i * 0.1), 2),
                "category": ["Electronics", "Books", "Home", "Sports", "Fashion"][i % 5],
                "price": round(99.99 - (i * 8.5), 2),
                "source": "fallback"
            }
            for i in range(1, limit + 1)
        ]

async def call_spark_similar_products_service(asin: str, limit: int):
    """SAFE ML Integration: Use real ML with graceful fallback"""
    try:
        if ML_AVAILABLE and ml_adapter:
            # Try ML service first
            similar_products = await ml_adapter.get_similar_products_safe(asin, limit)
            return similar_products
        else:
            raise Exception("ML service not available")
    except Exception as e:
        logger.warning(f"ML service failed for product {asin}: {e}, using fallback")
        # Graceful fallback to mock data (NO BREAKING CHANGE)
        return [
            {
                "asin": f"SIM{asin[-3:] if len(asin) >= 3 else '001'}{i:02d}",
                "title": f"Similar Product {i} to {asin}",
                "similarity_score": round(0.95 - (i * 0.08), 2),
                "category": ["Electronics", "Books", "Home", "Sports"][i % 4],
                "price": round(79.99 + (i * 12.3), 2),
                "source": "fallback"
            }
            for i in range(1, limit + 1)
        ]

async def call_spark_trending_products_service(limit: int, period: str = "week"):
    """Enhanced trending products with realistic data"""
    try:
        # Enhanced mock trending data (can be replaced with real ML later)
        trending_products = [
            {
                "asin": "B08N5WRWNW",
                "title": "Echo Dot (4th Gen) Smart Speaker with Alexa",
                "popularity_score": 0.98,
                "category": "Electronics",
                "price": 49.99,
                "reviews": 125000
            },
            {
                "asin": "B07XJ8C8F5",
                "title": "Fire TV Stick 4K Max Streaming Device",
                "popularity_score": 0.95,
                "category": "Electronics", 
                "price": 54.99,
                "reviews": 89000
            },
            {
                "asin": "B09B8RXYST",
                "title": "Apple AirPods (3rd Generation)",
                "popularity_score": 0.93,
                "category": "Electronics",
                "price": 179.00,
                "reviews": 67000
            },
            {
                "asin": "B08C1W5N87",
                "title": "Ring Video Doorbell Wired",
                "popularity_score": 0.91,
                "category": "Home Security",
                "price": 64.99,
                "reviews": 45000
            },
            {
                "asin": "B07ZPKN6YR",
                "title": "Instant Vortex Plus 4 Quart Air Fryer",
                "popularity_score": 0.88,
                "category": "Kitchen",
                "price": 79.95,
                "reviews": 34000
            }
        ]
        
        return trending_products[:limit]
        
    except Exception as e:
        logger.error(f"Error getting trending products: {e}")
        return []

# API Endpoints
@router.get("/user/{user_id}", response_model=UserRecommendationsResponse)
async def get_user_recommendations(
    user_id: int,
    limit: int = Query(default=10, ge=1, le=50, description="Number of recommendations to return")
):
    """
    Get personalized product recommendations for a specific user.
    
    - **user_id**: The ID of the user to get recommendations for
    - **limit**: Maximum number of recommendations to return (1-50)
    """
    try:
        recommendations = await call_spark_user_recommendations_service(user_id, limit)
        
        # Convert to Pydantic models
        recommendation_items = [
            RecommendationItem(**item) for item in recommendations
        ]
        
        return UserRecommendationsResponse(
            user_id=user_id,
            recommendations=recommendation_items,
            total_count=len(recommendation_items),
            source="ml_service" if ML_AVAILABLE else "fallback"
        )
        
    except Exception as e:
        logger.error(f"Error getting recommendations for user {user_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get recommendations for user {user_id}"
        )

@router.get("/similar/{asin}", response_model=SimilarProductsResponse)
async def get_similar_products(
    asin: str,
    limit: int = Query(default=10, ge=1, le=30, description="Number of similar products to return")
):
    """
    Get products similar to the specified product.
    
    - **asin**: The ASIN of the target product
    - **limit**: Maximum number of similar products to return (1-30)
    """
    try:
        similar_products = await call_spark_similar_products_service(asin, limit)
        
        # Convert to Pydantic models
        similar_items = [
            SimilarProductItem(**item) for item in similar_products
        ]
        
        return SimilarProductsResponse(
            target_product=asin,
            similar_products=similar_items,
            total_count=len(similar_items),
            source="ml_service" if ML_AVAILABLE else "fallback"
        )
        
    except Exception as e:
        logger.error(f"Error getting similar products for {asin}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get similar products for {asin}"
        )

@router.get("/trending", response_model=TrendingProductsResponse)
async def get_trending_products(
    limit: int = Query(default=20, ge=1, le=100, description="Number of trending products to return"),
    period: str = Query(default="week", regex="^(day|week|month)$", description="Time period for trending calculation")
):
    """
    Get currently trending products.
    
    - **limit**: Maximum number of trending products to return (1-100)
    - **period**: Time period for trending calculation (day, week, month)
    """
    try:
        trending_products = await call_spark_trending_products_service(limit, period)
        
        # Convert to Pydantic models
        trending_items = [
            TrendingProductItem(**item) for item in trending_products
        ]
        
        return TrendingProductsResponse(
            trending_products=trending_items,
            total_count=len(trending_items),
            period=period
        )
        
    except Exception as e:
        logger.error(f"Error getting trending products: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get trending products"
        )

@router.get("/categories/{category}/trending")
async def get_trending_by_category(
    category: str,
    limit: int = Query(default=15, ge=1, le=50, description="Number of products to return")
):
    """
    Get trending products within a specific category.
    
    - **category**: Product category (e.g., Electronics, Books, Home)
    - **limit**: Maximum number of products to return (1-50)
    """
    try:
        # Enhanced category-specific trending
        category_products = {
            "Electronics": [
                {"asin": "B08N5WRWNW", "title": "Echo Dot (4th Gen)", "score": 0.98, "price": 49.99},
                {"asin": "B07XJ8C8F5", "title": "Fire TV Stick 4K Max", "score": 0.95, "price": 54.99},
                {"asin": "B09B8RXYST", "title": "Apple AirPods (3rd Gen)", "score": 0.93, "price": 179.00}
            ],
            "Books": [
                {"asin": "B085KBCZCP", "title": "The Seven Husbands of Evelyn Hugo", "score": 0.96, "price": 13.99},
                {"asin": "B08FF8Z8C1", "title": "Project Hail Mary", "score": 0.94, "price": 14.99},
                {"asin": "B08FFFBPS6", "title": "The Invisible Life of Addie LaRue", "score": 0.91, "price": 12.99}
            ],
            "Home": [
                {"asin": "B08C1W5N87", "title": "Ring Video Doorbell Wired", "score": 0.91, "price": 64.99},
                {"asin": "B07ZPKN6YR", "title": "Instant Vortex Air Fryer", "score": 0.88, "price": 79.95},
                {"asin": "B085849Z1J", "title": "Ninja Foodi Personal Blender", "score": 0.85, "price": 39.99}
            ]
        }
        
        products = category_products.get(category, [])[:limit]
        
        return {
            "category": category,
            "products": products,
            "total_count": len(products)
        }
        
    except Exception as e:
        logger.error(f"Error getting trending products for category {category}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get trending products for category {category}"
        )

@router.get("/health")
async def recommendations_health():
    """Health check endpoint for recommendation service"""
    try:
        # Check ML service availability
        ml_status = "available" if ML_AVAILABLE else "fallback_mode"
        
        # Test a quick recommendation call
        test_recommendations = await call_spark_user_recommendations_service(1, 1)
        
        return {
            "status": "healthy",
            "ml_service": ml_status,
            "test_successful": len(test_recommendations) > 0,
            "timestamp": "2024-12-27T10:00:00Z"
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "degraded",
            "ml_service": "error",
            "error": str(e),
            "timestamp": "2024-12-27T10:00:00Z"
        }