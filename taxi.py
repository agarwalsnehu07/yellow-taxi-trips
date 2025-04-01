import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns

# Step 1: Download Data (Manually from NYC TLC website)
file_path = ("C:\Users\DELL\Downloads\yellow_tripdata_2023-01.csv", low memory=false)

# Step 2: Create PostgreSQL Database (Run in PostgreSQL )
# CREATE DATABASE nyc_taxi_db;

# Step 3: Define Database Connection
db_name = "nyc_taxi_db"
db_user = "postgres"
db_password = "1234"
db_host = "localhost"
db_port = "5432"
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Step 4: Define Table Schema
create_table_query = """
CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
    trip_id SERIAL PRIMARY KEY,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT
);
"""
with engine.connect() as conn:
    conn.execute(create_table_query)

# Step 5: Load Data into DataFrame
df = pd.read_csv(file_path, usecols=["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "fare_amount", "tip_amount"], parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

# Step 6: Load Data into PostgreSQL
df.to_sql("yellow_taxi_trips", engine, if_exists="append", index=False)

# Step 7: Feature Engineering
df["trip_duration_min"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60
df["speed_mph"] = df["trip_distance"] / (df["trip_duration_min"] / 60)
df["time_of_day"] = pd.cut(df["tpep_pickup_datetime"].dt.hour, bins=[0, 6, 12, 18, 24], labels=["Night", "Morning", "Afternoon", "Evening"], right=False)
df["is_weekend"] = df["tpep_pickup_datetime"].dt.weekday >= 5

# Step 8: Save Processed Data Back to PostgreSQL
df.to_sql("yellow_taxi_trips", engine, if_exists="replace", index=False)

# Step 9: Visualization
# Aggregate daily revenue
daily_revenue = df.groupby(df["tpep_pickup_datetime"].dt.date)["fare_amount"].sum()
daily_revenue = daily_revenue.to_frame().reset_index()
daily_revenue.columns = ["date", "total_revenue"]

# Compute 7-day moving average
daily_revenue["7_day_MA"] = daily_revenue["total_revenue"].rolling(window=7).mean()

# Plot the revenue trend
plt.figure(figsize=(12, 6))
sns.lineplot(data=daily_revenue, x="date", y="total_revenue", label="Daily Revenue", marker="o")
sns.lineplot(data=daily_revenue, x="date", y="7_day_MA", label="7-Day Moving Average", linestyle="dashed", color="red")

# Annotate peaks and troughs
peaks = daily_revenue.nlargest(3, "total_revenue")
troughs = daily_revenue.nsmallest(3, "total_revenue")
for _, row in peaks.iterrows():
    plt.annotate(f"Peak: {row['total_revenue']:.0f}", (row["date"], row["total_revenue"]), textcoords="offset points", xytext=(0,10), ha="center", fontsize=10, color="green")
for _, row in troughs.iterrows():
    plt.annotate(f"Trough: {row['total_revenue']:.0f}", (row["date"], row["total_revenue"]), textcoords="offset points", xytext=(0,-15), ha="center", fontsize=10, color="red")

plt.xlabel("Date")
plt.ylabel("Total Revenue ($)")
plt.title("Daily Total Revenue with 7-Day Moving Average")
plt.xticks(rotation=45)
plt.legend()
plt.show()
