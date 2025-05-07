import requests
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import plotly.express as px


# 1. Function to fetch weather data , Since im from salem TN, i took salem data from open meteo
def fetch_weather_data(latitude=11.6538, longitude=78.1554):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m"
    response = requests.get(url)
    if response.status_code == 200:
        print("✅ Data fetched successfully!")
        return response.json()
    else:
        raise Exception("❌ Failed to fetch data")


# 2. Function to process JSON and return DataFrame
def process_weather_data(data):
    temperature = data["hourly"]["temperature_2m"]
    datetime = pd.to_datetime(data["hourly"]["time"])

    df = pd.DataFrame({
        "date": datetime.date,
        "time": datetime.time,
        "temperature": temperature
    })
    return df


# 3. Function to plot temperature vs time
def plot_temperature(df):
    df_plot = df.copy()
    df_plot["datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str))
    fig = px.line(df_plot, x='datetime', y='temperature', title='Hourly Temperature Forecast - Salem')
    fig.update_xaxes(title="Date and Time")
    fig.update_yaxes(title="Temperature (°C)")
    fig.show()


# 4. Function to upload DataFrame to MySQL
def upload_to_mysql(df, table_name="weather_forecast"):
    username = 'root'
    password = "password"
    host = "localhost"
    port = "3306"
    database = "mydb"

    engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")
    df.to_sql(name=table_name, con=engine, if_exists="replace", index=False,
              dtype={
                  "date": sqlalchemy.types.Date(),
                  "time": sqlalchemy.types.Time(),
                  "temperature": sqlalchemy.types.Float()
              })
    print(f"✅ Data uploaded to MySQL table '{table_name}' successfully.")


# 5. Main function to call all

raw_data = fetch_weather_data()
df = process_weather_data(raw_data)
plot_temperature(df)
upload_to_mysql(df)
