import asyncio
import subprocess
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

class LocalMLAdapter:
    def __init__(self):
        self.conda_env = "bigdata-ml"
        self.project_root = Path(__file__).parent.parent.parent
        self.ml_script_path = self.project_root / "services/spark_processor/src/recommendation_engine.py"
        
    async def initialize_engine(self) -> Dict:
        """Initialize ML engine in conda environment"""
        try:
            cmd = [
                "conda", "run", "-n", self.conda_env, "python", "-c",
                "from services.spark_processor.src.recommendation_engine import AmazonRecommendationEngine; "
                "engine = AmazonRecommendationEngine(); "
                "print('ML_ENGINE_READY')"
            ]
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                cwd=str(self.project_root),
                timeout=30
            )
            
            if result.returncode == 0 and "ML_ENGINE_READY" in result.stdout:
                return {"status": "success", "message": "ML engine initialized"}
            else:
                return {"status": "error", "message": f"Init failed: {result.stderr}"}
                
        except Exception as e:
            return {"status": "error", "message": f"Exception: {str(e)}"}
    
    async def get_user_recommendations(self, user_id: int, limit: int = 10) -> Dict:
        """Get recommendations for user using local ML engine"""
        try:
            # Create Python script to run ML recommendation
            ml_command = f'''
import sys
sys.path.append(r"{self.project_root}")
from services.spark_processor.src.recommendation_engine import AmazonRecommendationEngine
import json

try:
    engine = AmazonRecommendationEngine()
    recommendations = engine.get_user_recommendations({user_id}, {limit})
    result = {{
        "user_id": {user_id},
        "recommendations": recommendations,
        "source": "local_ml_engine",
        "status": "success"
    }}
    print(json.dumps(result))
except Exception as e:
    fallback = {{
        "user_id": {user_id},
        "recommendations": [
            {{"product_id": f"FALLBACK_{{i}}", "score": 0.8 - i*0.1, "title": f"Recommended Product {{i+1}}"}}
            for i in range({limit})
        ],
        "source": "fallback",
        "status": "fallback",
        "error": str(e)
    }}
    print(json.dumps(fallback))
'''
            
            cmd = ["conda", "run", "-n", self.conda_env, "python", "-c", ml_command]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(self.project_root),
                timeout=60
            )
            
            if result.returncode == 0:
                try:
                    return json.loads(result.stdout.strip())
                except json.JSONDecodeError:
                    return self._get_fallback_recommendations(user_id, limit, "json_decode_error")
            else:
                return self._get_fallback_recommendations(user_id, limit, result.stderr)
                
        except Exception as e:
            return self._get_fallback_recommendations(user_id, limit, str(e))
    
    async def get_similar_products(self, product_id: str, limit: int = 5) -> Dict:
        """Get similar products using local ML engine"""
        try:
            ml_command = f'''
import sys
sys.path.append(r"{self.project_root}")
from services.spark_processor.src.recommendation_engine import AmazonRecommendationEngine
import json

try:
    engine = AmazonRecommendationEngine()
    similar_products = engine.get_similar_products("{product_id}", {limit})
    result = {{
        "product_id": "{product_id}",
        "similar_products": similar_products,
        "source": "local_ml_engine",
        "status": "success"
    }}
    print(json.dumps(result))
except Exception as e:
    fallback = {{
        "product_id": "{product_id}",
        "similar_products": [
            {{"product_id": f"SIM_{{i}}", "similarity": 0.9 - i*0.1, "title": f"Similar Product {{i+1}}"}}
            for i in range({limit})
        ],
        "source": "fallback",
        "status": "fallback",
        "error": str(e)
    }}
    print(json.dumps(fallback))
'''
            
            cmd = ["conda", "run", "-n", self.conda_env, "python", "-c", ml_command]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(self.project_root),
                timeout=60
            )
            
            if result.returncode == 0:
                try:
                    return json.loads(result.stdout.strip())
                except json.JSONDecodeError:
                    return self._get_fallback_similar_products(product_id, limit, "json_decode_error")
            else:
                return self._get_fallback_similar_products(product_id, limit, result.stderr)
                
        except Exception as e:
            return self._get_fallback_similar_products(product_id, limit, str(e))
    
    def _get_fallback_recommendations(self, user_id: int, limit: int, error: str) -> Dict:
        """Fallback recommendations when ML engine fails"""
        return {
            "user_id": user_id,
            "recommendations": [
                {
                    "product_id": f"FALLBACK_{i}",
                    "score": 0.8 - i*0.1,
                    "title": f"Recommended Product {i+1}",
                    "category": "Electronics"
                }
                for i in range(limit)
            ],
            "source": "fallback",
            "status": "fallback",
            "error": error
        }
    
    def _get_fallback_similar_products(self, product_id: str, limit: int, error: str) -> Dict:
        """Fallback similar products when ML engine fails"""
        return {
            "product_id": product_id,
            "similar_products": [
                {
                    "product_id": f"SIM_{i}",
                    "similarity": 0.9 - i*0.1,
                    "title": f"Similar Product {i+1}",
                    "category": "Electronics"
                }
                for i in range(limit)
            ],
            "source": "fallback",
            "status": "fallback",
            "error": error
        }

# Global ML adapter instance
ml_adapter = LocalMLAdapter()