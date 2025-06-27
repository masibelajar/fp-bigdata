import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict
import os

# Configuration
# This now correctly reads the environment variable from docker-compose,
# and falls back to localhost for local testing.
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

st.set_page_config(
    page_title="E-Commerce Recommendation Dashboard",
    page_icon="🛒",
    layout="wide"
)

def main():
    st.title("🛒 E-Commerce Recommendation Dashboard")
    st.markdown("AI-Powered Product Recommendations using Big Data")

    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a page",
        ["🏠 Home", "👤 User Recommendations", "🔍 Product Analysis", "📊 Analytics"]
    )

    if page == "🏠 Home":
        show_home_page()
    elif page == "👤 User Recommendations":
        show_user_recommendations()
    elif page == "🔍 Product Analysis":
        show_product_analysis()
    elif page == "📊 Analytics":
        show_analytics()

def show_home_page():
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Products", "1.4M", "📦")
    with col2:
        st.metric("Active Users", "10K", "👥")
    with col3:
        st.metric("Recommendations Today", "15.2K", "🎯")

    st.subheader("🔥 Trending Products")
    try:
        response = requests.get(f"{API_BASE_URL}/recommendations/trending")
        response.raise_for_status() # Check for HTTP errors
        trending = response.json()["trending_products"]
        df = pd.DataFrame(trending)
        st.dataframe(df, use_container_width=True)
    except requests.exceptions.RequestException as e:
        st.warning(f"API connection failed: {e}. Showing demo data.")
        show_demo_trending_products()


def show_user_recommendations():
    st.subheader("👤 Personalized Recommendations")

    user_id = st.number_input("Enter User ID", min_value=1, max_value=10000, value=1)
    num_recommendations = st.slider("Number of recommendations", 5, 20, 10)

    if st.button("Get Recommendations"):
        with st.spinner("Generating recommendations..."):
            try:
                response = requests.get(
                    f"{API_BASE_URL}/recommendations/user/{user_id}",
                    params={"limit": num_recommendations}
                )
                response.raise_for_status() # Check for HTTP errors
                recommendations = response.json()["recommendations"]

                st.success(f"Found {len(recommendations)} recommendations for User {user_id}")

                # Display recommendations
                for i, rec in enumerate(recommendations, 1):
                    with st.expander(f"#{i} - {rec['title']} (Rating: {rec['predicted_rating']:.1f})"):
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.write(f"**ASIN:** {rec['asin']}")
                            st.write(f"**Predicted Rating:** {rec['predicted_rating']:.2f}")
                        with col2:
                            st.write(f"**Title:** {rec['title']}")
            except requests.exceptions.RequestException as e:
                st.warning(f"API connection failed: {e}. Showing demo recommendations.")
                show_demo_recommendations(user_id)

def show_product_analysis():
    st.subheader("🔍 Product Similarity Analysis")

    asin = st.text_input("Enter Product ASIN", value="B014TMV5YE")

    if st.button("Find Similar Products"):
        with st.spinner("Finding similar products..."):
            try:
                response = requests.get(f"{API_BASE_URL}/recommendations/similar/{asin}")
                response.raise_for_status() # Check for HTTP errors
                similar_products = response.json()["similar_products"]

                st.success(f"Found {len(similar_products)} similar products")

                # Create DataFrame for visualization
                df = pd.DataFrame(similar_products)

                # Bar chart of similarity scores
                fig = px.bar(
                    df,
                    x="title",
                    y="similarity_score",
                    title="Product Similarity Scores"
                )
                # THIS IS THE CORRECTED LINE:
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)

                # Detailed table
                st.dataframe(df, use_container_width=True)
            except requests.exceptions.RequestException as e:
                st.warning(f"API connection failed: {e}. Showing demo similar products.")
                show_demo_similar_products(asin)

def show_analytics():
    st.subheader("📊 Recommendation Analytics")

    # Create demo analytics charts
    col1, col2 = st.columns(2)

    with col1:
        # User engagement over time
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        engagement = [100 + i*5 + (i%7)*20 for i in range(30)]

        fig = px.line(
            x=dates,
            y=engagement,
            title="Daily User Engagement",
            labels={'x': 'Date', 'y': 'Active Users'}
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Recommendation algorithm performance
        algorithms = ['Collaborative Filtering', 'Content-Based', 'Popularity-Based']
        accuracy = [0.85, 0.78, 0.65]

        fig = px.bar(
            x=algorithms,
            y=accuracy,
            title="Algorithm Accuracy Comparison",
            labels={'x': 'Algorithm', 'y': 'Accuracy Score'}
        )
        st.plotly_chart(fig, use_container_width=True)

    # Category performance
    st.subheader("Category Performance")
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports']
    performance_data = {
        'Category': categories,
        'Recommendations': [5234, 3421, 2876, 4123, 1987],
        'Click-through Rate': [0.12, 0.08, 0.15, 0.09, 0.11],
        'Conversion Rate': [0.03, 0.02, 0.04, 0.025, 0.028]
    }

    df = pd.DataFrame(performance_data)
    st.dataframe(df, use_container_width=True)

# Demo functions for when API is not available
def show_demo_trending_products():
    demo_data = [
        {"asin": "B123456", "title": "Wireless Headphones", "popularity_score": 0.95},
        {"asin": "B234567", "title": "Smart Watch", "popularity_score": 0.89},
        {"asin": "B345678", "title": "Laptop Stand", "popularity_score": 0.84}
    ]
    df = pd.DataFrame(demo_data)
    st.dataframe(df, use_container_width=True)

def show_demo_recommendations(user_id):
    demo_recommendations = [
        {"asin": "B111111", "title": "Product 1", "predicted_rating": 4.5},
        {"asin": "B222222", "title": "Product 2", "predicted_rating": 4.3},
        {"asin": "B333333", "title": "Product 3", "predicted_rating": 4.1}
    ]

    for i, rec in enumerate(demo_recommendations, 1):
        st.write(f"#{i} - {rec['title']} (Rating: {rec['predicted_rating']:.1f})")

def show_demo_similar_products(asin):
    demo_similar = [
        {"asin": "B444444", "title": "Similar Product 1", "similarity_score": 0.95},
        {"asin": "B555555", "title": "Similar Product 2", "similarity_score": 0.89},
        {"asin": "B666666", "title": "Similar Product 3", "similarity_score": 0.84}
    ]
    df = pd.DataFrame(demo_similar)
    st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    main()