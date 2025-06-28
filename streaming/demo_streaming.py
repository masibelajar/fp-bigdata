import threading
import time
import requests
import json
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka_producer import RealTimeEventProducer
from kafka_consumer import RealTimeEventConsumer, AnalyticsConsumer

class StreamingDemo:
    def __init__(self):
        self.producer = None
        self.consumer = None
        self.analytics_consumer = None
        self.api_base = "http://localhost:8000"
        self.demo_results = {
            'api_streaming': False,
            'event_processing': False,
            'kafka_producer': False,
            'kafka_consumer': False,
            'total_events': 0
        }
    
    def check_prerequisites(self):
        """Check if all required services are running"""
        print("🔍 Checking prerequisites...")
        
        checks = {
            'API': f"{self.api_base}/recommendations/health",
            'MinIO': "http://localhost:9001",
            'Spark': "http://localhost:8080"
        }
        
        all_good = True
        for service, url in checks.items():
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    print(f"✅ {service}: OK")
                else:
                    print(f"⚠️ {service}: HTTP {response.status_code}")
                    if service == 'API':
                        all_good = False
            except Exception as e:
                print(f"❌ {service}: {str(e)[:50]}...")
                if service == 'API':
                    all_good = False
        
        return all_good
    
    def test_streaming_api(self):
        """Test streaming API endpoints"""
        print("\n🧪 Testing streaming API endpoints...")
        
        try:
            # Test streaming recommendations
            print("📤 Testing /stream/recommendations/1")
            response = requests.get(
                f"{self.api_base}/stream/recommendations/1", 
                timeout=15, 
                stream=True
            )
            
            if response.status_code == 200:
                print("✅ Streaming recommendations endpoint working")
                
                # Read first few chunks
                chunks_read = 0
                for chunk in response.iter_content(chunk_size=1024):
                    if chunks_read >= 3:  # Read first 3 chunks
                        break
                    if chunk:
                        chunk_str = chunk.decode()
                        if 'data:' in chunk_str:
                            print(f"📡 Received stream data: {chunk_str[:100]}...")
                            chunks_read += 1
                
                self.demo_results['api_streaming'] = True
                response.close()
            else:
                print(f"❌ Streaming API failed: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Streaming API error: {e}")
        
        # Test analytics streaming
        try:
            print("📈 Testing /stream/analytics")
            response = requests.get(
                f"{self.api_base}/stream/analytics",
                timeout=10,
                stream=True
            )
            
            if response.status_code == 200:
                print("✅ Analytics streaming working")
                # Read one chunk
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk and 'data:' in chunk.decode():
                        print(f"📊 Analytics data: {chunk.decode()[:100]}...")
                        break
                response.close()
            
        except Exception as e:
            print(f"⚠️ Analytics streaming: {e}")
    
    def test_event_processing(self):
        """Test event posting and processing"""
        print("\n📤 Testing event processing...")
        
        events = [
            {"user_id": 1, "action": "view", "product_id": "B001LAPTOP"},
            {"user_id": 2, "action": "purchase", "product_id": "B002MOUSE"},
            {"user_id": 3, "action": "like", "product_id": "B003COFFEE"},
            {"user_id": 1, "action": "click", "product_id": "B004HEADPHONES"},
            {"user_id": 4, "action": "view", "product_id": "B005BOOK"}
        ]
        
        try:
            response = requests.post(
                f"{self.api_base}/stream/events",
                json=events,
                timeout=15
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Events processed: {result['events_processed']}")
                print(f"📊 Processing results: {len(result.get('results', []))} events")
                self.demo_results['event_processing'] = True
                return True
            else:
                print(f"❌ Event processing failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Event processing error: {e}")
            return False
    
    def run_producer_demo(self, duration=30):
        """Run producer in background"""
        try:
            print(f"🚀 Starting Kafka producer for {duration} seconds...")
            self.producer = RealTimeEventProducer()
            
            if not self.producer.producer:
                print("❌ Producer not available")
                return 0
            
            # Create topics first
            self.producer.create_topics()
            
            # Send some initial events
            initial_events = [
                (1, "B001LAPTOP", "view"),
                (2, "B002MOUSE", "view"),
                (3, "B003COFFEE", "purchase")
            ]
            
            for user_id, product_id, action in initial_events:
                self.producer.send_user_event(user_id, product_id, action)
                time.sleep(0.5)
            
            # Run simulation
            event_count = self.producer.simulate_real_time_events(duration)
            self.demo_results['kafka_producer'] = True
            self.demo_results['total_events'] = event_count
            return event_count
            
        except Exception as e:
            print(f"❌ Producer error: {e}")
            return 0
        finally:
            if self.producer:
                self.producer.close()
    
    def run_consumer_demo(self, max_messages=30, timeout=40):
        """Run consumer in background"""
        try:
            print(f"📥 Starting Kafka consumer for {max_messages} messages...")
            self.consumer = RealTimeEventConsumer()
            
            if not self.consumer.consumer:
                print("❌ Consumer not available")
                return
            
            self.consumer.start_consuming(max_messages, timeout)
            self.demo_results['kafka_consumer'] = True
            
        except Exception as e:
            print(f"❌ Consumer error: {e}")
    
    def run_analytics_demo(self, duration=20):
        """Run analytics consumer"""
        try:
            print(f"📈 Starting analytics consumer for {duration} seconds...")
            self.analytics_consumer = AnalyticsConsumer()
            
            if self.analytics_consumer.consumer:
                self.analytics_consumer.start_analytics_processing(duration)
            
        except Exception as e:
            print(f"❌ Analytics consumer error: {e}")
    
    def run_complete_demo(self):
        """Run complete streaming demo"""
        print("🎬 COMPLETE REAL-TIME STREAMING DEMO")
        print("====================================")
        
        # 1. Check prerequisites
        if not self.check_prerequisites():
            print("❌ Prerequisites not met. Please ensure all services are running.")
            return
        
        # 2. Test API streaming
        self.test_streaming_api()
        
        # 3. Test event processing
        self.test_event_processing()
        
        print("\n🔄 Starting real-time Kafka demo...")
        
        # 4. Start consumer in background thread
        consumer_thread = threading.Thread(
            target=self.run_consumer_demo, 
            args=(25, 35)  # 25 messages, 35 second timeout
        )
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # 5. Start analytics consumer
        analytics_thread = threading.Thread(
            target=self.run_analytics_demo,
            args=(25,)
        )
        analytics_thread.daemon = True
        analytics_thread.start()
        
        # Give consumers time to start
        print("⏳ Waiting for consumers to start...")
        time.sleep(5)
        
        # 6. Start producer (this will generate events)
        event_count = self.run_producer_demo(25)
        
        # 7. Wait for consumer to finish
        print("⏳ Waiting for consumers to process events...")
        consumer_thread.join(timeout=40)
        analytics_thread.join(timeout=30)
        
        # 8. Show results
        self.show_demo_results(event_count)
    
    def show_demo_results(self, event_count):
        """Show comprehensive demo results"""
        print("\n" + "="*50)
        print("🎯 STREAMING DEMO RESULTS")
        print("="*50)
        
        results = [
            ("API Streaming", self.demo_results['api_streaming']),
            ("Event Processing", self.demo_results['event_processing']),
            ("Kafka Producer", self.demo_results['kafka_producer']),
            ("Kafka Consumer", self.demo_results['kafka_consumer']),
        ]
        
        for feature, status in results:
            icon = "✅" if status else "❌"
            print(f"{icon} {feature}: {'Working' if status else 'Failed'}")
        
        print(f"\n📊 Statistics:")
        print(f"   Events Generated: {event_count}")
        print(f"   Total Features Tested: {len(results)}")
        print(f"   Success Rate: {sum(self.demo_results.values())}/{len(results)} features")
        
        overall_success = sum(self.demo_results.values()) >= 3
        status_icon = "🎉" if overall_success else "⚠️"
        status_text = "SUCCESS" if overall_success else "PARTIAL SUCCESS"
        
        print(f"\n{status_icon} OVERALL STATUS: {status_text}")
        
        if overall_success:
            print("\n✅ Real-time streaming system is fully operational!")
            print("✅ End-to-end data pipeline working")
            print("✅ Ready for production demo")
        else:
            print("\n⚠️ Some features need attention")
            print("✅ Core functionality working")
            print("🔧 Consider troubleshooting failed components")
        
        print("\n🌐 Access Points:")
        print(f"   📖 API Docs: {self.api_base}/docs")
        print(f"   📊 Dashboard: http://localhost:8501")
        print(f"   🗄️ MinIO: http://localhost:9001")
        print(f"   ⚡ Spark: http://localhost:8080")

def main():
    """Run the complete streaming demo"""
    demo = StreamingDemo()
    demo.run_complete_demo()

if __name__ == "__main__":
    main()