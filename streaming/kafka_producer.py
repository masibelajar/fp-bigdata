import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import os
import glob

class CSVDataProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        self.data_path = "../data/raw/"
        
    def discover_csv_files(self):
        """Discover all CSV files in raw data directory"""
        csv_files = glob.glob(os.path.join(self.data_path, "*.csv"))
        
        print(f"🔍 Found {len(csv_files)} CSV files:")
        
        file_info = []
        for file_path in csv_files:
            try:
                # Get basic file info
                file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                filename = os.path.basename(file_path)
                
                # Try to read first few rows to understand structure
                df_sample = pd.read_csv(file_path, nrows=5)
                
                info = {
                    'filename': filename,
                    'path': file_path,
                    'size_mb': round(file_size, 2),
                    'columns': list(df_sample.columns),
                    'sample_data': df_sample.head(2).to_dict('records')
                }
                
                file_info.append(info)
                
                print(f"📄 {filename}")
                print(f"   📐 Size: {info['size_mb']} MB")
                print(f"   📊 Columns: {len(info['columns'])}")
                print(f"   🏷️ Headers: {info['columns'][:5]}...")
                print()
                
            except Exception as e:
                print(f"❌ Error reading {file_path}: {e}")
                
        return file_info
    
    def load_csv_dataset(self, filename=None, max_rows=1000):
        """Load a specific CSV or auto-select the best one"""
        if filename:
            file_path = os.path.join(self.data_path, filename)
        else:
            # Auto-select largest CSV file (likely the main dataset)
            csv_files = glob.glob(os.path.join(self.data_path, "*.csv"))
            if not csv_files:
                print("❌ No CSV files found in data/raw/")
                return None
            
            # Select largest file
            largest_file = max(csv_files, key=os.path.getsize)
            file_path = largest_file
            filename = os.path.basename(largest_file)
        
        print(f"📖 Loading dataset: {filename}")
        
        try:
            # Try different encodings
            for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, nrows=max_rows)
                    print(f"✅ Successfully loaded with {encoding} encoding")
                    print(f"📊 Dataset shape: {df.shape}")
                    print(f"📋 Columns: {list(df.columns)}")
                    
                    # Show sample data
                    print(f"\n📝 Sample data:")
                    print(df.head(3).to_string())
                    
                    return df, filename
                    
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    print(f"⚠️ Error with {encoding}: {e}")
                    continue
            
            print(f"❌ Could not load {filename} with any encoding")
            return None, None
            
        except Exception as e:
            print(f"❌ Error loading {filename}: {e}")
            return None, None
    
    def identify_amazon_columns(self, df):
        """Identify Amazon-style columns in the dataset"""
        column_mapping = {
            'user_id': ['user_id', 'userId', 'reviewerID', 'reviewer_id', 'customer_id'],
            'product_id': ['product_id', 'productId', 'asin', 'ASIN', 'item_id', 'product'],
            'rating': ['rating', 'overall', 'score', 'stars', 'review_score'],
            'review_text': ['review_text', 'reviewText', 'summary', 'text', 'review', 'comment'],
            'timestamp': ['timestamp', 'unixReviewTime', 'reviewTime', 'date', 'review_date'],
            'helpful': ['helpful', 'helpfulness', 'helpful_votes'],
            'title': ['title', 'product_title', 'name', 'item_name']
        }
        
        identified_cols = {}
        for key, possible_names in column_mapping.items():
            for name in possible_names:
                if name in df.columns:
                    identified_cols[key] = name
                    break
        
        print(f"🎯 Identified Amazon columns:")
        for key, col_name in identified_cols.items():
            print(f"   {key}: {col_name}")
        
        return identified_cols
    
    def stream_csv_reviews(self, df, filename, cols, num_events=100):
        """Stream real CSV review data to Kafka"""
        print(f"📤 Streaming {min(num_events, len(df))} reviews from {filename}...")
        
        events_sent = 0
        
        for idx, row in df.iterrows():
            if events_sent >= num_events:
                break
            
            try:
                # Create review event from CSV data
                review_event = {
                    'event_id': f"csv_{filename}_{idx}",
                    'event_type': 'amazon_review',
                    'dataset_source': filename,
                    'row_index': idx,
                    'timestamp': datetime.now().isoformat(),
                    'processing_time': datetime.now().isoformat()
                }
                
                # Add identified columns
                for key, col_name in cols.items():
                    if col_name in df.columns and pd.notna(row[col_name]):
                        review_event[key] = str(row[col_name])
                
                # Add all other columns as metadata
                review_event['metadata'] = {}
                for col in df.columns:
                    if col not in cols.values() and pd.notna(row[col]):
                        review_event['metadata'][col] = str(row[col])
                
                # Determine Kafka key
                key = review_event.get('user_id', f"user_{idx}")
                
                # Send to Kafka
                self.producer.send('amazon-csv-reviews', key=key, value=review_event)
                
                events_sent += 1
                
                if events_sent % 20 == 0:
                    print(f"📊 Streamed {events_sent}/{num_events} reviews from {filename}")
                
                time.sleep(0.05)  # Small delay to see streaming effect
                
            except Exception as e:
                print(f"⚠️ Error processing row {idx}: {e}")
                continue
        
        self.producer.flush()
        print(f"✅ Successfully streamed {events_sent} reviews from {filename}!")
        return events_sent
    
    def generate_interaction_events(self, df, cols, num_events=50):
        """Generate user interaction events based on review data"""
        print(f"🎯 Generating {num_events} interaction events from review patterns...")
        
        events_sent = 0
        
        for idx, row in df.iterrows():
            if events_sent >= num_events:
                break
            
            try:
                user_id = str(row[cols['user_id']]) if 'user_id' in cols else f"user_{idx}"
                product_id = str(row[cols['product_id']]) if 'product_id' in cols else f"prod_{idx}"
                rating = None
                
                if 'rating' in cols and pd.notna(row[cols['rating']]):
                    rating = float(row[cols['rating']])
                
                # Generate multiple events per review based on rating
                actions = ['view']  # Everyone views
                
                if rating:
                    if rating >= 3:
                        actions.append('click')
                    if rating >= 4:
                        actions.extend(['cart_add', 'purchase'])
                    if rating >= 4.5:
                        actions.append('like')
                
                for action in actions:
                    if events_sent >= num_events:
                        break
                    
                    interaction_event = {
                        'event_id': f"interaction_{events_sent:06d}",
                        'user_id': user_id,
                        'product_id': product_id,
                        'action': action,
                        'timestamp': datetime.now().isoformat(),
                        'source': 'csv_derived',
                        'original_rating': rating,
                        'confidence': 0.9 if rating and rating >= 4 else 0.7
                    }
                    
                    # Send to Kafka
                    self.producer.send('user-interactions', key=user_id, value=interaction_event)
                    
                    events_sent += 1
                    
                    if events_sent % 15 == 0:
                        print(f"📈 Generated {events_sent}/{num_events} interaction events")
                    
                    time.sleep(0.03)
                
            except Exception as e:
                print(f"⚠️ Error generating interactions from row {idx}: {e}")
                continue
        
        self.producer.flush()
        print(f"✅ Generated {events_sent} realistic interaction events!")
        return events_sent

def main():
    producer = CSVDataProducer()
    
    print("🎬 CSV AMAZON DATA STREAMING DEMO")
    print("=" * 50)
    
    # Discover available CSV files
    file_info = producer.discover_csv_files()
    
    if not file_info:
        print("❌ No CSV files found in data/raw/")
        return
    
    print(f"📂 Available datasets:")
    for i, info in enumerate(file_info):
        print(f"   {i+1}. {info['filename']} ({info['size_mb']} MB)")
    
    # Auto-select or let user choose
    print(f"\n🎯 Auto-selecting largest dataset...")
    largest_dataset = max(file_info, key=lambda x: x['size_mb'])
    selected_file = largest_dataset['filename']
    
    print(f"📊 Selected: {selected_file}")
    
    # Load the dataset
    df, filename = producer.load_csv_dataset(selected_file, max_rows=500)  # Load 500 rows for demo
    
    if df is None:
        print("❌ Failed to load dataset!")
        return
    
    # Identify Amazon-style columns
    cols = producer.identify_amazon_columns(df)
    
    if not cols:
        print("⚠️ No Amazon-style columns detected, using generic approach...")
        cols = {
            'user_id': df.columns[0] if len(df.columns) > 0 else None,
            'product_id': df.columns[1] if len(df.columns) > 1 else None,
            'rating': df.columns[2] if len(df.columns) > 2 else None
        }
    
    print(f"\n🚀 Starting to stream real CSV data...")
    
    # Stream reviews
    reviews_sent = producer.stream_csv_reviews(df, filename, cols, num_events=75)
    
    print(f"\n" + "="*30)
    
    # Generate interaction events
    interactions_sent = producer.generate_interaction_events(df, cols, num_events=50)
    
    print(f"\n🎉 CSV DATA STREAMING COMPLETED!")
    print(f"📊 Total reviews streamed: {reviews_sent}")
    print(f"🎯 Total interactions generated: {interactions_sent}")
    print(f"📈 Check consumer to see real Amazon CSV data processing")

if __name__ == "__main__":
    main()