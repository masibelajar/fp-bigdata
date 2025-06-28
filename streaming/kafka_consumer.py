from kafka import KafkaConsumer
import json
import time
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import io
import pandas as pd
from collections import defaultdict

class CSVDataConsumer:
    def __init__(self):
        # Kafka consumer
        self.consumer = KafkaConsumer(
            'amazon-csv-reviews',
            'user-interactions',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            group_id='csv-data-consumer',
            auto_offset_reset='latest'
        )
        
        # MinIO client
        self.minio_client = Minio(
            'localhost:9000',
            access_key='admin',
            secret_key='password123',
            secure=False
        )
        
        # Setup buckets
        self.setup_minio_buckets()
        
        # Enhanced statistics
        self.csv_reviews_processed = 0
        self.interactions_processed = 0
        self.files_stored = 0
        self.user_profiles = defaultdict(lambda: {
            'reviews': 0, 
            'interactions': 0, 
            'products': set(), 
            'avg_rating': 0,
            'total_rating': 0,
            'preferences': defaultdict(int)
        })
        self.product_profiles = defaultdict(lambda: {
            'reviews': 0, 
            'interactions': 0, 
            'users': set(), 
            'avg_rating': 0,
            'total_rating': 0,
            'categories': set()
        })
        
    def setup_minio_buckets(self):
        """Create MinIO buckets for CSV data"""
        buckets = [
            'csv-amazon-reviews', 
            'csv-user-interactions', 
            'csv-recommendations',
            'csv-analytics',
            'csv-user-profiles',
            'csv-product-profiles'
        ]
        
        for bucket in buckets:
            try:
                if not self.minio_client.bucket_exists(bucket):
                    self.minio_client.make_bucket(bucket)
                    print(f"✅ Created bucket: {bucket}")
                else:
                    print(f"📦 Bucket exists: {bucket}")
            except S3Error as e:
                print(f"❌ Bucket error {bucket}: {e}")
    
    def store_to_minio(self, data, bucket, object_name):
        """Store data to MinIO with error handling"""
        try:
            # Convert sets to lists for JSON serialization
            def convert_sets(obj):
                if isinstance(obj, set):
                    return list(obj)
                elif isinstance(obj, dict):
                    return {k: convert_sets(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_sets(item) for item in obj]
                return obj
            
            serializable_data = convert_sets(data)
            json_data = json.dumps(serializable_data, indent=2, default=str)
            data_bytes = json_data.encode('utf-8')
            
            self.minio_client.put_object(
                bucket,
                object_name,
                io.BytesIO(data_bytes),
                length=len(data_bytes),
                content_type='application/json'
            )
            
            self.files_stored += 1
            return True
            
        except Exception as e:
            print(f"❌ MinIO storage error: {e}")
            return False
    
    def process_csv_review(self, review):
        """Process CSV review data with enhanced analytics"""
        self.csv_reviews_processed += 1
        
        # Extract key information
        user_id = review.get('user_id')
        product_id = review.get('product_id')
        rating = review.get('rating')
        review_text = review.get('review_text', '')
        
        # Enhanced processing
        processed_review = {
            **review,
            'processed_at': datetime.now().isoformat(),
            'processing_id': f"csv_proc_{self.csv_reviews_processed:06d}",
            'sentiment_analysis': self.enhanced_sentiment_analysis(review_text),
            'review_length': len(review_text),
            'recommendation_weight': self.calculate_recommendation_weight(rating, review_text),
            'quality_score': self.calculate_quality_score(review)
        }
        
        # Update user profile
        if user_id:
            profile = self.user_profiles[user_id]
            profile['reviews'] += 1
            
            if rating and rating.replace('.', '').isdigit():
                rating_val = float(rating)
                profile['total_rating'] += rating_val
                profile['avg_rating'] = profile['total_rating'] / profile['reviews']
                
            if product_id:
                profile['products'].add(product_id)
                
            # Extract preferences from metadata
            if 'metadata' in review:
                for key, value in review['metadata'].items():
                    if value and len(str(value)) < 50:  # Reasonable category length
                        profile['preferences'][str(value)] += 1
        
        # Update product profile
        if product_id:
            profile = self.product_profiles[product_id]
            profile['reviews'] += 1
            
            if rating and rating.replace('.', '').isdigit():
                rating_val = float(rating)
                profile['total_rating'] += rating_val
                profile['avg_rating'] = profile['total_rating'] / profile['reviews']
                
            if user_id:
                profile['users'].add(user_id)
        
        # Store processed review
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"csv_reviews/{timestamp}_{processed_review['processing_id']}.json"
        
        if self.store_to_minio(processed_review, 'csv-amazon-reviews', object_name):
            print(f"📊 Processed CSV review #{self.csv_reviews_processed} - User: {user_id}, Product: {product_id}")
            
            # Generate recommendations periodically
            if self.csv_reviews_processed % 10 == 0 and user_id:
                self.generate_csv_based_recommendations(user_id)
            
            return True
        return False
    
    def process_interaction_event(self, event):
        """Process user interaction events"""
        self.interactions_processed += 1
        
        user_id = event.get('user_id')
        product_id = event.get('product_id')
        action = event.get('action')
        
        # Enhanced interaction processing
        processed_interaction = {
            **event,
            'processed_at': datetime.now().isoformat(),
            'processing_id': f"int_proc_{self.interactions_processed:06d}",
            'action_weight': self.get_action_weight(action),
            'recommendation_trigger': action in ['purchase', 'like', 'cart_add'],
            'sequence_number': self.interactions_processed
        }
        
        # Update user interaction profile
        if user_id:
            self.user_profiles[user_id]['interactions'] += 1
        
        # Update product interaction profile
        if product_id:
            self.product_profiles[product_id]['interactions'] += 1
        
        # Store interaction
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"interactions/{timestamp}_{event['event_id']}.json"
        
        if self.store_to_minio(processed_interaction, 'csv-user-interactions', object_name):
            print(f"🎯 Processed interaction #{self.interactions_processed} - {action} by {user_id} on {product_id}")
            return True
        return False
    
    def enhanced_sentiment_analysis(self, text):
        """Enhanced sentiment analysis"""
        if not text:
            return {'score': 0.5, 'confidence': 0}
        
        # Extended word lists
        positive_words = [
            'excellent', 'amazing', 'great', 'good', 'love', 'perfect', 'best', 'awesome',
            'fantastic', 'wonderful', 'outstanding', 'superb', 'brilliant', 'impressive'
        ]
        negative_words = [
            'terrible', 'awful', 'bad', 'hate', 'worst', 'horrible', 'disappointing',
            'useless', 'poor', 'cheap', 'broken', 'defective', 'waste'
        ]
        
        text_lower = text.lower()
        words = text_lower.split()
        
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        total_words = len(words)
        
        if pos_count + neg_count == 0:
            return {'score': 0.5, 'confidence': 0}
        
        sentiment_score = pos_count / (pos_count + neg_count) if (pos_count + neg_count) > 0 else 0.5
        confidence = (pos_count + neg_count) / total_words if total_words > 0 else 0
        
        return {
            'score': round(sentiment_score, 3),
            'confidence': round(confidence, 3),
            'positive_words': pos_count,
            'negative_words': neg_count
        }
    
    def calculate_recommendation_weight(self, rating, review_text):
        """Calculate how much this review should influence recommendations"""
        weight = 1.0
        
        if rating and rating.replace('.', '').isdigit():
            rating_val = float(rating)
            weight *= (rating_val / 5.0)  # Higher ratings get more weight
        
        if review_text:
            # Longer reviews get slightly more weight
            length_factor = min(len(review_text) / 100, 2.0)  # Cap at 2x weight
            weight *= (1 + length_factor * 0.2)
        
        return round(weight, 3)
    
    def calculate_quality_score(self, review):
        """Calculate overall quality score of the review"""
        score = 0.5  # Base score
        
        # Rating presence
        if review.get('rating'):
            score += 0.2
        
        # Review text presence and length
        review_text = review.get('review_text', '')
        if review_text:
            score += 0.2
            if len(review_text) > 50:
                score += 0.1
        
        # Metadata richness
        if review.get('metadata'):
            metadata_count = len([v for v in review['metadata'].values() if v])
            score += min(metadata_count * 0.05, 0.2)
        
        return round(min(score, 1.0), 3)
    
    def get_action_weight(self, action):
        """Get weight for different actions"""
        weights = {
            'view': 1.0,
            'click': 2.0,
            'cart_add': 3.0,
            'purchase': 5.0,
            'like': 4.0,
            'share': 3.5
        }
        return weights.get(action, 1.0)
    
    def generate_csv_based_recommendations(self, user_id):
        """Generate recommendations based on CSV data patterns"""
        user_profile = self.user_profiles[user_id]
        user_products = user_profile['products']
        
        if not user_products:
            return
        
        # Find similar users (collaborative filtering)
        similar_users = []
        for uid, profile in self.user_profiles.items():
            if uid != user_id and profile['products']:
                # Calculate Jaccard similarity
                intersection = len(user_products.intersection(profile['products']))
                union = len(user_products.union(profile['products']))
                similarity = intersection / union if union > 0 else 0
                
                if similarity > 0.1:  # Minimum similarity threshold
                    similar_users.append((uid, similarity, profile))
        
        # Sort by similarity
        similar_users.sort(key=lambda x: x[1], reverse=True)
        
        # Generate recommendations
        recommended_products = {}
        
        for uid, similarity, profile in similar_users[:5]:  # Top 5 similar users
            for product in profile['products']:
                if product not in user_products:
                    if product not in recommended_products:
                        recommended_products[product] = 0
                    recommended_products[product] += similarity
        
        # Sort recommendations by score
        top_recommendations = sorted(recommended_products.items(), key=lambda x: x[1], reverse=True)[:5]
        
        if top_recommendations:
            recommendations = {
                'user_id': user_id,
                'generated_at': datetime.now().isoformat(),
                'method': 'csv_collaborative_filtering',
                'user_profile_summary': {
                    'total_reviews': user_profile['reviews'],
                    'avg_rating': round(user_profile['avg_rating'], 2),
                    'products_reviewed': len(user_profile['products'])
                },
                'recommendations': []
            }
            
            for product_id, score in top_recommendations:
                product_profile = self.product_profiles[product_id]
                rec = {
                    'product_id': product_id,
                    'confidence': round(min(score, 1.0), 3),
                    'predicted_rating': round(product_profile['avg_rating'], 2) if product_profile['avg_rating'] else 4.0,
                    'total_reviews': product_profile['reviews'],
                    'unique_users': len(product_profile['users']),
                    'reason': f'Similar users (similarity: {round(score, 2)}) liked this product'
                }
                recommendations['recommendations'].append(rec)
            
            # Store recommendations
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            object_name = f"recommendations/{timestamp}_user_{user_id}.json"
            
            if self.store_to_minio(recommendations, 'csv-recommendations', object_name):
                print(f"🎯 Generated {len(recommendations['recommendations'])} CSV-based recommendations for user {user_id}")
    
    def generate_comprehensive_analytics(self):
        """Generate comprehensive analytics from CSV data"""
        analytics = {
            'generated_at': datetime.now().isoformat(),
            'data_source': 'csv_amazon_dataset',
            'processing_summary': {
                'csv_reviews_processed': self.csv_reviews_processed,
                'interactions_processed': self.interactions_processed,
                'files_stored': self.files_stored,
                'unique_users': len(self.user_profiles),
                'unique_products': len(self.product_profiles)
            },
            'user_analytics': self.analyze_users(),
            'product_analytics': self.analyze_products(),
            'recommendation_analytics': self.analyze_recommendations()
        }
        
        # Store analytics
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"comprehensive_analytics_{timestamp}.json"
        
        if self.store_to_minio(analytics, 'csv-analytics', object_name):
            print(f"📈 Generated comprehensive analytics report")
            return analytics
        return None
    
    def analyze_users(self):
        """Analyze user patterns"""
        if not self.user_profiles:
            return {}
        
        # Top users by activity
        top_users = sorted(self.user_profiles.items(), 
                          key=lambda x: x[1]['reviews'] + x[1]['interactions'], 
                          reverse=True)[:10]
        
        # Rating distribution
        ratings = [profile['avg_rating'] for profile in self.user_profiles.values() if profile['avg_rating'] > 0]
        
        return {
            'total_users': len(self.user_profiles),
            'avg_reviews_per_user': round(sum(p['reviews'] for p in self.user_profiles.values()) / len(self.user_profiles), 2),
            'avg_rating_given': round(sum(ratings) / len(ratings), 2) if ratings else 0,
            'top_users': [
                {
                    'user_id': uid,
                    'reviews': profile['reviews'],
                    'interactions': profile['interactions'],
                    'avg_rating': round(profile['avg_rating'], 2),
                    'products_count': len(profile['products'])
                }
                for uid, profile in top_users[:5]
            ]
        }
    
    def analyze_products(self):
        """Analyze product patterns"""
        if not self.product_profiles:
            return {}
        
        # Top products by popularity
        top_products = sorted(self.product_profiles.items(), 
                             key=lambda x: x[1]['reviews'] + x[1]['interactions'], 
                             reverse=True)[:10]
        
        # Rating distribution
        ratings = [profile['avg_rating'] for profile in self.product_profiles.values() if profile['avg_rating'] > 0]
        
        return {
            'total_products': len(self.product_profiles),
            'avg_reviews_per_product': round(sum(p['reviews'] for p in self.product_profiles.values()) / len(self.product_profiles), 2),
            'avg_product_rating': round(sum(ratings) / len(ratings), 2) if ratings else 0,
            'top_products': [
                {
                    'product_id': pid,
                    'reviews': profile['reviews'],
                    'interactions': profile['interactions'],
                    'avg_rating': round(profile['avg_rating'], 2),
                    'unique_users': len(profile['users'])
                }
                for pid, profile in top_products[:5]
            ]
        }
    
    def analyze_recommendations(self):
        """Analyze recommendation patterns"""
        return {
            'recommendations_generated': self.csv_reviews_processed // 10,  # Every 10 reviews
            'avg_recommendations_per_user': 5,  # Fixed for our algorithm
            'recommendation_method': 'collaborative_filtering_csv_based',
            'data_freshness': 'real_time_from_csv'
        }
    
    def start_consuming(self, max_messages=150):
        """Start consuming CSV-based messages"""
        print("🎧 Starting CSV Data Consumer...")
        print("📊 Processing real Amazon CSV data and interactions...")
        
        try:
            for message in self.consumer:
                topic = message.topic
                data = message.value
                
                print(f"\n📨 CSV Message from '{topic}'")
                
                if topic == 'amazon-csv-reviews':
                    success = self.process_csv_review(data)
                elif topic == 'user-interactions':
                    success = self.process_interaction_event(data)
                else:
                    print(f"⚠️ Unknown topic: {topic}")
                    continue
                
                # Generate analytics periodically
                total_processed = self.csv_reviews_processed + self.interactions_processed
                if total_processed % 25 == 0:
                    self.generate_comprehensive_analytics()
                
                # Show detailed stats
                if total_processed % 15 == 0:
                    print(f"\n📊 CSV DATA PROCESSING STATS:")
                    print(f"   📝 CSV Reviews: {self.csv_reviews_processed}")
                    print(f"   🎯 Interactions: {self.interactions_processed}")
                    print(f"   👥 Unique Users: {len(self.user_profiles)}")
                    print(f"   📦 Unique Products: {len(self.product_profiles)}")
                    print(f"   💾 Files Stored: {self.files_stored}")
                
                if total_processed >= max_messages:
                    break
                    
        except KeyboardInterrupt:
            print("\n⏹️ CSV Consumer stopped")
        finally:
            # Generate final comprehensive analytics
            final_analytics = self.generate_comprehensive_analytics()
            if final_analytics:
                print(f"\n📊 FINAL CSV DATA ANALYTICS:")
                summary = final_analytics['processing_summary']
                print(f"   📝 Total CSV Reviews: {summary['csv_reviews_processed']}")
                print(f"   🎯 Total Interactions: {summary['interactions_processed']}")
                print(f"   👥 Unique Users: {summary['unique_users']}")
                print(f"   📦 Unique Products: {summary['unique_products']}")
                print(f"   💾 Files Stored: {summary['files_stored']}")
            
            self.consumer.close()

if __name__ == "__main__":
    consumer = CSVDataConsumer()
    
    print("🎬 CSV AMAZON DATA CONSUMER")
    print("=" * 40)
    print("💡 Run kafka_producer_csv.py to stream CSV data")
    print("⏹️ Press Ctrl+C to stop")
    
    consumer.start_consuming(max_messages=150)