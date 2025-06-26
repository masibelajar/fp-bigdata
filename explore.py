# Amazon Products Dataset Exploration
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Fix Windows path issue - use raw string or forward slashes
# Option 1: Raw string (recommended)
data_path = r'C:\Users\jobir\Downloads\amazondataset'

# Option 2: Forward slashes (also works on Windows)
# data_path = 'C:/Users/jobir/Downloads/amazondataset'

# Option 3: Use pathlib (most robust)
data_path = Path('C:/Users/jobir/Downloads/amazondataset')

print("🔍 Amazon Products Dataset Exploration")
print("=" * 50)

# Load both datasets
try:
    # Load main products dataset
    df_products = pd.read_csv(data_path / 'amazon_products.csv')
    print(f"✅ Products dataset loaded successfully!")
    print(f"   Shape: {df_products.shape}")
    
    # Load categories dataset
    df_categories = pd.read_csv(data_path / 'amazon_categories.csv')
    print(f"✅ Categories dataset loaded successfully!")
    print(f"   Shape: {df_categories.shape}")
    
except FileNotFoundError as e:
    print(f"❌ Error loading files: {e}")
    print("Please check the file paths and ensure files exist.")
    exit()

print("\n" + "="*60)
print("📊 PRODUCTS DATASET ANALYSIS")
print("="*60)

# Basic info about products dataset
print(f"Dataset Info:")
print(f"├── Shape: {df_products.shape}")
print(f"├── Size: {df_products.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
print(f"└── Columns ({len(df_products.columns)}): {list(df_products.columns)}")

print(f"\nData Types:")
for col, dtype in df_products.dtypes.items():
    print(f"├── {col}: {dtype}")

print(f"\nMissing Values:")
missing_values = df_products.isnull().sum()
for col, count in missing_values.items():
    if count > 0:
        percentage = (count / len(df_products)) * 100
        print(f"├── {col}: {count:,} ({percentage:.1f}%)")

print(f"\nDuplicate Rows: {df_products.duplicated().sum():,}")

# Sample data
print(f"\n📋 Sample Products Data (First 5 rows):")
print(df_products.head())

print("\n" + "="*60)
print("🏷️ CATEGORIES DATASET ANALYSIS")
print("="*60)

# Basic info about categories dataset
print(f"Dataset Info:")
print(f"├── Shape: {df_categories.shape}")
print(f"├── Size: {df_categories.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
print(f"└── Columns ({len(df_categories.columns)}): {list(df_categories.columns)}")

print(f"\nData Types:")
for col, dtype in df_categories.dtypes.items():
    print(f"├── {col}: {dtype}")

print(f"\nMissing Values:")
missing_values_cat = df_categories.isnull().sum()
for col, count in missing_values_cat.items():
    if count > 0:
        percentage = (count / len(df_categories)) * 100
        print(f"├── {col}: {count:,} ({percentage:.1f}%)")

# Sample categories data
print(f"\n📋 Sample Categories Data (First 10 rows):")
print(df_categories.head(10))

print("\n" + "="*60)
print("🔗 RELATIONSHIP ANALYSIS")
print("="*60)

# Check if there's a relationship between the two datasets
print("Analyzing relationship between products and categories...")

# Common columns check
common_cols = set(df_products.columns) & set(df_categories.columns)
print(f"Common columns: {common_cols}")

# Check if there's a category_id or similar linking column
potential_links = []
for col in df_products.columns:
    if 'category' in col.lower() or 'cat' in col.lower():
        potential_links.append(col)

print(f"Potential linking columns in products: {potential_links}")

print("\n" + "="*60)
print("📈 STATISTICAL SUMMARY")
print("="*60)

# Numerical columns analysis for products
numerical_cols = df_products.select_dtypes(include=[np.number]).columns
if len(numerical_cols) > 0:
    print("Products - Numerical columns summary:")
    print(df_products[numerical_cols].describe())

# Categorical columns analysis
categorical_cols = df_products.select_dtypes(include=['object']).columns
print(f"\nProducts - Categorical columns ({len(categorical_cols)}):")
for col in categorical_cols[:5]:  # Show first 5 categorical columns
    unique_count = df_products[col].nunique()
    print(f"├── {col}: {unique_count:,} unique values")
    if unique_count <= 10:
        print(f"    Values: {df_products[col].value_counts().head().to_dict()}")
    else:
        print(f"    Top 3: {df_products[col].value_counts().head(3).to_dict()}")

print("\n" + "="*60)
print("💾 DATA SAMPLING FOR DEVELOPMENT")
print("="*60)

# Create smaller samples for development
sample_size = min(100000, len(df_products))  # 100K or full dataset if smaller
df_products_sample = df_products.sample(n=sample_size, random_state=42)

# Save sample data
sample_path = data_path / 'sample'
sample_path.mkdir(exist_ok=True)

df_products_sample.to_csv(sample_path / 'amazon_products_sample.csv', index=False)
df_categories.to_csv(sample_path / 'amazon_categories_sample.csv', index=False)

print(f"✅ Created sample datasets:")
print(f"├── Products sample: {len(df_products_sample):,} rows")
print(f"├── Categories: {len(df_categories):,} rows")
print(f"└── Saved to: {sample_path}")

print("\n" + "="*60)
print("🎯 BIG DATA 5V's VALIDATION")
print("="*60)

# Volume
total_size_mb = (df_products.memory_usage(deep=True).sum() + 
                df_categories.memory_usage(deep=True).sum()) / 1024**2
print(f"📊 Volume: {total_size_mb:.2f} MB ({len(df_products):,} products)")

# Variety
data_types = set(df_products.dtypes.astype(str)) | set(df_categories.dtypes.astype(str))
print(f"🎨 Variety: {len(data_types)} different data types")

# Velocity (simulated)
print(f"⚡ Velocity: Real-time streaming capability (to be implemented)")

# Veracity
missing_percentage = (df_products.isnull().sum().sum() / 
                     (len(df_products) * len(df_products.columns))) * 100
print(f"✅ Veracity: {missing_percentage:.1f}% missing data")

# Value
print(f"💎 Value: E-commerce analytics & recommendation system potential")

print("\n" + "="*60)
print("✅ EXPLORATION COMPLETE!")
print("="*60)
print("Next steps:")
print("1. Set up Docker infrastructure")
print("2. Create data ingestion pipeline")
print("3. Build ETL processing with Spark")
print("4. Implement streaming simulation")