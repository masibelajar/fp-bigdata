#!/bin/bash
echo "🎬 COMPLETE BIGDATA STREAMING SYSTEM TEST"
echo "========================================="

echo "📊 1. Checking Infrastructure..."
docker-compose ps

echo -e "\n🔍 2. Testing API Health..."
curl -s http://localhost:8000/health | python -m json.tool

echo -e "\n👤 3. Testing User Recommendations..."
curl -s "http://localhost:8000/recommendations/user/1?limit=3" | python -m json.tool

echo -e "\n📈 4. Testing Trending Products..."
curl -s "http://localhost:8000/recommendations/trending?limit=3" | python -m json.tool

echo -e "\n📊 5. Testing Analytics..."
curl -s "http://localhost:8000/analytics/user_behavior" | python -m json.tool

echo -e "\n🌊 6. Testing Streaming Recommendations (10 seconds)..."
timeout 10s curl -N "http://localhost:8000/stream/recommendations/1" || true

echo -e "\n📈 7. Testing Analytics Stream (5 seconds)..."
timeout 5s curl -N "http://localhost:8000/stream/analytics" || true

echo -e "\n📤 8. Testing Event Processing..."
curl -X POST "http://localhost:8000/stream/events" \
  -H "Content-Type: application/json" \
  -d '[{"user_id": 1, "action": "view", "product_id": "B001LAPTOP"}]' | python -m json.tool

echo -e "\n🎯 9. Testing System Metrics..."
curl -s "http://localhost:8000/system/metrics" | python -m json.tool

echo -e "\n✅ ALL TESTS COMPLETED!"
echo "🌐 Access Points:"
echo "   📖 API Docs: http://localhost:8000/docs"
echo "   📊 Dashboard: http://localhost:8501"
echo "   🗄️ MinIO: http://localhost:9001"
echo "   ⚡ Spark: http://localhost:8080"