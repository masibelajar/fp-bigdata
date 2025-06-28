#!/bin/bash

echo "🎬 STREAMING SYSTEM TEST SCRIPT"
echo "==============================="

# Function to check service
check_service() {
    local service_name=$1
    local url=$2
    echo -n "Checking $service_name... "
    
    if curl -s "$url" > /dev/null 2>&1; then
        echo "✅ OK"
        return 0
    else
        echo "❌ FAILED"
        return 1
    fi
}

# Check prerequisites
echo "🔍 Checking prerequisites..."
check_service "API" "http://localhost:8000/recommendations/health"
check_service "MinIO" "http://localhost:9001"
check_service "Kafka" "http://localhost:9092"

echo ""

# Test streaming API endpoints
echo "🧪 Testing streaming API endpoints..."

echo -n "Testing streaming recommendations... "
if curl -s -N "http://localhost:8000/stream/recommendations/1" -m 10 | head -n 3 | grep -q "data:"; then
    echo "✅ OK"
else
    echo "❌ FAILED"
fi

echo -n "Testing event processing... "
if curl -s -X POST "http://localhost:8000/stream/events" \
    -H "Content-Type: application/json" \
    -d '[{"user_id": 1, "action": "view", "product_id": "B001LAPTOP"}]' | grep -q "success"; then
    echo "✅ OK"
else
    echo "❌ FAILED"
fi

echo -n "Testing analytics stream... "
if curl -s -N "http://localhost:8000/stream/analytics" -m 5 | head -n 2 | grep -q "data:"; then
    echo "✅ OK"
else
    echo "❌ FAILED"
fi

echo ""

# Install streaming dependencies
echo "📦 Installing streaming dependencies..."
pip install -r requirements.txt

echo ""

# Test Kafka producer
echo "🚀 Testing Kafka producer..."
python kafka_producer.py &
PRODUCER_PID=$!
sleep 10
kill $PRODUCER_PID 2>/dev/null

echo ""

# Test complete demo
echo "🎬 Running complete streaming demo..."
python demo_streaming.py

echo ""
echo "✅ Streaming system test completed!"
echo "🌐 Open http://localhost:8000/docs to see all endpoints"