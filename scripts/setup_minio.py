from minio import Minio
from minio.error import S3Error
import json
import io
from datetime import datetime
import sys
import time

def wait_for_minio(max_retries=30):
    """Wait for MinIO to be ready"""
    print("🔄 Waiting for MinIO to start...")
    
    for attempt in range(max_retries):
        try:
            client = Minio(
                "localhost:9000",
                access_key="admin",
                secret_key="password123", 
                secure=False
            )
            
            # Test connection
            client.list_buckets()
            print("✅ MinIO is ready!")
            return client
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⏳ MinIO not ready yet (attempt {attempt + 1}/{max_retries}), waiting...")
                time.sleep(2)
            else:
                print(f"❌ MinIO failed to start after {max_retries} attempts: {e}")
                return None
    
    return None

def create_buckets(client):
    """Create required MinIO buckets"""
    buckets = [
        "datalake",
        "landing-zone", 
        "processed",
        "ml-models",
        "backups",
        "real-time-data"
    ]
    
    print("🗄️ Creating MinIO buckets...")
    
    for bucket_name in buckets:
        try:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"✅ Created bucket: {bucket_name}")
            else:
                print(f"ℹ️ Bucket already exists: {bucket_name}")
        except Exception as e:
            print(f"❌ Error creating bucket {bucket_name}: {e}")
            return False
    
    return True

def upload_sample_data(client):
    """Upload comprehensive sample data"""
    print("📊 Uploading sample data...")
    
    # Sample Amazon products
    products_data = [
        {
            "asin": "B001LAPTOP",
            "title": "High Performance Laptop Computer",
            "category": "Electronics",
            "price": 1299.99,
            "stars": 4.5,
            "reviews": 2847,
            "brand": "TechCorp",
            "description": "Latest generation laptop with 16GB RAM",
            "timestamp": datetime.now().isoformat()
        },
        {
            "asin": "B002MOUSE",
            "title": "Wireless Ergonomic Mouse",
            "category": "Electronics", 
            "price": 49.99,
            "stars": 4.3,
            "reviews": 1205,
            "brand": "ComfortTech",
            "description": "Wireless mouse with precision tracking",
            "timestamp": datetime.now().isoformat()
        },
        {
            "asin": "B003COFFEE",
            "title": "Smart Coffee Maker Pro",
            "category": "Kitchen",
            "price": 189.99,
            "stars": 4.7,
            "reviews": 892,
            "brand": "BrewMaster",
            "description": "WiFi enabled programmable coffee maker",
            "timestamp": datetime.now().isoformat()
        },
        {
            "asin": "B004HEADPHONES",
            "title": "Noise Cancelling Headphones",
            "category": "Electronics",
            "price": 299.99,
            "stars": 4.6,
            "reviews": 3421,
            "brand": "AudioPlus",
            "description": "Professional grade noise cancelling",
            "timestamp": datetime.now().isoformat()
        },
        {
            "asin": "B005BOOK",
            "title": "Data Science and Machine Learning Guide",
            "category": "Books",
            "price": 45.99,
            "stars": 4.8,
            "reviews": 567,
            "brand": "TechBooks",
            "description": "Comprehensive guide to modern data science",
            "timestamp": datetime.now().isoformat()
        }
    ]
    
    # Sample user interactions
    interactions_data = [
        {"user_id": 1, "asin": "B001LAPTOP", "rating": 5.0, "timestamp": datetime.now().isoformat()},
        {"user_id": 1, "asin": "B002MOUSE", "rating": 4.0, "timestamp": datetime.now().isoformat()},
        {"user_id": 2, "asin": "B001LAPTOP", "rating": 4.0, "timestamp": datetime.now().isoformat()},
        {"user_id": 2, "asin": "B004HEADPHONES", "rating": 5.0, "timestamp": datetime.now().isoformat()},
        {"user_id": 3, "asin": "B003COFFEE", "rating": 4.5, "timestamp": datetime.now().isoformat()},
        {"user_id": 3, "asin": "B005BOOK", "rating": 4.8, "timestamp": datetime.now().isoformat()},
    ]
    
    try:
        # Upload products data
        products_json = json.dumps(products_data, indent=2)
        products_bytes = io.BytesIO(products_json.encode('utf-8'))
        
        client.put_object(
            "landing-zone",
            "products/amazon_products.json",
            products_bytes,
            len(products_json.encode('utf-8')),
            content_type="application/json"
        )
        print("✅ Uploaded products data")
        
        # Upload interactions data
        interactions_json = json.dumps(interactions_data, indent=2)
        interactions_bytes = io.BytesIO(interactions_json.encode('utf-8'))
        
        client.put_object(
            "landing-zone",
            "interactions/user_interactions.json", 
            interactions_bytes,
            len(interactions_json.encode('utf-8')),
            content_type="application/json"
        )
        print("✅ Uploaded interactions data")
        
        # Upload sample config
        config_data = {
            "ml_config": {
                "als_max_iter": 10,
                "als_reg_param": 0.1,
                "recommendation_limit": 10,
                "similarity_threshold": 0.7
            },
            "api_config": {
                "lite_mode": True,
                "max_recommendations": 10,
                "cache_ttl": 300
            },
            "updated": datetime.now().isoformat()
        }
        
        config_json = json.dumps(config_data, indent=2)
        config_bytes = io.BytesIO(config_json.encode('utf-8'))
        
        client.put_object(
            "ml-models",
            "config/ml_config.json",
            config_bytes,
            len(config_json.encode('utf-8')),
            content_type="application/json"
        )
        print("✅ Uploaded ML config")
        
        return True
        
    except Exception as e:
        print(f"❌ Error uploading data: {e}")
        return False

def verify_setup(client):
    """Verify MinIO setup is complete"""
    print("🔍 Verifying setup...")
    
    try:
        # List all buckets
        buckets = client.list_buckets()
        print(f"📦 Found {len(buckets)} buckets:")
        for bucket in buckets:
            print(f"   - {bucket.name}")
        
        # List objects in landing-zone
        objects = list(client.list_objects("landing-zone", recursive=True))
        print(f"📄 Found {len(objects)} files in landing-zone:")
        for obj in objects:
            print(f"   - {obj.object_name} ({obj.size} bytes)")
        
        print("✅ MinIO setup verification complete!")
        return True
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

def main():
    """Main setup function"""
    print("🚀 Starting MinIO setup automation...")
    
    # Wait for MinIO
    client = wait_for_minio()
    if not client:
        sys.exit(1)
    
    # Create buckets
    if not create_buckets(client):
        sys.exit(1)
    
    # Upload sample data
    if not upload_sample_data(client):
        sys.exit(1)
    
    # Verify setup
    if not verify_setup(client):
        sys.exit(1)
    
    print("🎉 MinIO setup completed successfully!")
    print("\n📋 Setup Summary:")
    print("   ✅ 6 buckets created")
    print("   ✅ Sample products uploaded") 
    print("   ✅ User interactions uploaded")
    print("   ✅ ML configuration uploaded")
    print("\n🌐 Access MinIO Console: http://localhost:9001")
    print("   Username: admin")
    print("   Password: password123")

if __name__ == "__main__":
    main()