from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, when, count, avg, desc
from pyspark.sql.types import *
import os

class RecommendationEngine:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.model = None
        
    def _create_spark_session(self):
        return SparkSession.builder \
            .appName("ECommerceRecommendationEngine") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    
    def load_and_prepare_data(self, products_path="/opt/bitnami/spark/data/amazon_products.csv"):
        """Load and prepare Amazon products data"""
        # Load products data
        products_df = self.spark.read.csv(products_path, header=True, inferSchema=True)
        
        # Create synthetic user interactions for ML training
        user_interactions = self._generate_synthetic_interactions(products_df)
        
        return products_df, user_interactions
    
    def _generate_synthetic_interactions(self, products_df, num_users=10000):
        """Generate realistic user-product interactions based on product popularity"""
        from pyspark.sql.functions import rand, floor, when
        
        # Create user IDs
        users_df = self.spark.range(1, num_users + 1).withColumnRenamed("id", "user_id")
        
        # Sample popular products (high stars, many reviews)
        popular_products = products_df.filter(
            (col("stars") >= 4.0) & (col("reviews") > 10)
        ).sample(0.3)  # 30% of popular products
        
        # Cross join users with popular products (controlled sample)
        interactions = users_df.crossJoin(
            popular_products.select("asin", "stars", "reviews").sample(0.01)  # 1% sample
        )
        
        # Generate ratings based on product stars + some noise
        interactions = interactions.withColumn(
            "rating",
            when(col("stars") >= 4.5, 5)
            .when(col("stars") >= 4.0, 4)
            .when(col("stars") >= 3.5, 3)
            .when(col("stars") >= 3.0, 2)
            .otherwise(1)
        ).withColumn(
            "rating",
            when(rand() < 0.2, col("rating") - 1).otherwise(col("rating"))  # Add some noise
        ).filter(col("rating") > 0)
        
        return interactions.select("user_id", "asin", "rating")
    
    def train_collaborative_filtering(self, interactions_df):
        """Train ALS collaborative filtering model"""
        # Index string IDs to numeric
        user_indexer = StringIndexer(inputCol="user_id", outputCol="user_index")
        item_indexer = StringIndexer(inputCol="asin", outputCol="item_index")
        
        # Transform data
        indexed_data = user_indexer.fit(interactions_df).transform(interactions_df)
        indexed_data = item_indexer.fit(indexed_data).transform(indexed_data)
        
        # Split data
        train_data, test_data = indexed_data.randomSplit([0.8, 0.2], seed=42)
        
        # Configure ALS
        als = ALS(
            userCol="user_index",
            itemCol="item_index", 
            ratingCol="rating",
            coldStartStrategy="drop",
            implicitPrefs=False,
            rank=50,
            maxIter=10,
            regParam=0.1
        )
        
        # Train model
        self.model = als.fit(train_data)
        
        # Evaluate
        predictions = self.model.transform(test_data)
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
        rmse = evaluator.evaluate(predictions)
        
        print(f"✅ Model trained successfully! RMSE: {rmse:.3f}")
        
        # Save model
        self.model.write().overwrite().save("/tmp/als_model")
        
        return self.model, rmse
    
    def generate_recommendations(self, user_id, num_recommendations=10):
        """Generate product recommendations for a user"""
        if not self.model:
            print("❌ Model not trained yet!")
            return None
            
        # Get recommendations
        user_df = self.spark.createDataFrame([(user_id,)], ["user_index"])
        recommendations = self.model.recommendForUserSubset(user_df, num_recommendations)
        
        return recommendations
    
    def get_similar_products(self, products_df, target_asin, num_similar=10):
        """Content-based: Find similar products based on features"""
        target_product = products_df.filter(col("asin") == target_asin).collect()
        
        if not target_product:
            return None
            
        target_category = target_product[0]["category_id"]
        target_price = target_product[0]["price"]
        
        # Find products in same category with similar price
        similar_products = products_df.filter(
            (col("category_id") == target_category) &
            (col("asin") != target_asin) &
            (col("price").between(target_price * 0.7, target_price * 1.3))
        ).orderBy(desc("stars"), desc("reviews")).limit(num_similar)
        
        return similar_products

# Main execution
if __name__ == "__main__":
    engine = RecommendationEngine()
    
    # Load data
    products_df, interactions_df = engine.load_and_prepare_data()
    
    # Train model
    model, rmse = engine.train_collaborative_filtering(interactions_df)
    
    # Example: Get recommendations for user 1
    recommendations = engine.generate_recommendations(1, 10)
    print("✅ Recommendation engine ready!")