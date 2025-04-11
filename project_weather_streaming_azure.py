import requests
import json
from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime, timedelta

# Configuration de la connexion à Event Hub
EVENT_HUB_CONNECTION_STRING = "Connection_key"  
EVENT_HUB_NAME = "eventhub1"

# Créer le client Event Hub
producer = EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME)

# Function to handle the API response
def handle_response(response):
    try:
        response_json = response.json()  # Tente de convertir la réponse en JSON
        if response.status_code == 200:
            return response_json
        else:
            return {"error": f"Error {response.status_code}: {response_json}"}  # Retourne un dictionnaire avec l'erreur
    except json.JSONDecodeError:  # Si la réponse n'est pas du JSON
        return {"error": f"Invalid JSON response: {response.text}"}


# Function to get current weather and air quality data
def get_current_weather(base_url, api_key, location):
    current_weather_url = f"{base_url}/current.json"
    params = {
        "key": api_key,
        "q": location
    }
    
    response = requests.get(current_weather_url, params=params)
    return handle_response(response)

# Function to get Forecast Data
def get_forecast_weather(base_url, api_key, location, days):
    forecast_url = f"{base_url}/forecast.json"
    params = {
        "key": api_key,
        "q": location,
        "days": days
    }

    response = requests.get(forecast_url, params=params)
    return handle_response(response)

# Function to get Alerts
def get_alerts(base_url, api_key, location):
    alerts_url = f"{base_url}/alerts.json"
    params = {
        "key": api_key,
        "q": location,
        "alerts": "yes"
    }

    response = requests.get(alerts_url, params=params)
    return handle_response(response)

   
# Flatten and merge the data
def flatten_data(current_weather, forecast_weather, alerts):

    print("DEBUG - Current Weather:", type(current_weather), current_weather)
    print("DEBUG - Forecast Weather:", type(forecast_weather), forecast_weather)
    print("DEBUG - Alerts:", type(alerts), alerts)

    location = current_weather.get("location", {})
    current = current_weather.get("current", {})
    condition = current.get("condition", {})
    air_quality = current.get("air_quality", {})
    
    forecast_days = forecast_weather.get("forecast", {}).get("forecastday", [])
    alert_list = alerts.get("alerts", {}).get("alert", [])


    return {
        **{  # Infos générales
            "region": location.get("region"),
            "name": location.get("name"),
            "country": location.get("country"),
            "lat": location.get("lat"),
            "lon": location.get("lon"),
            "tz_id": location.get("tz_id"),
            "localtime": location.get("localtime"),
        },
        **{  # Conditions météo actuelles
            "text": condition.get("text"),
            "wind_kph": current.get("wind_kph"),
            "wind_degree": current.get("wind_degree"),
            "wind_dir": current.get("wind_dir"),
            "pressure_in": current.get("pressure_in"),
            "precip_in": current.get("precip_in"),
            "humidity": current.get("humidity"),
            "cloud": current.get("cloud"),
            "feelslike_c": current.get("feelslike_c"),
            "uv": current.get("uv"),
        },
        "air_quality": {key: air_quality.get(key) for key in 
            ["co", "no2", "o3", "so2", "pm2_5", "pm10", "us-epa-index", "gb-defra-index"]
        },
        "alerts": [
            {key: alert.get(key) for key in ["headline", "severity", "desc", "instruction"]}
            for alert in alert_list
        ],
        "forecast": [
            {
                "date": day.get("date"),
                "maxtemp_c": day.get("day", {}).get("maxtemp_c"),
                "mintemp_c": day.get("day", {}).get("mintemp_c"),
                "condition": day.get("day", {}).get("condition", {}).get("text"),
            }
            for day in forecast_days
        ]
    }



# Main program
def fetch_weather_data():
    base_url = "http://api.weatherapi.com/v1"
    location = "Paris"  
    api_key = "API_key"  

    # Getting data from API
    current_weather = get_current_weather(base_url, api_key, location)
    forecast_weather = get_forecast_weather(base_url, api_key, location, 3)
    alerts = get_alerts(base_url, api_key, location)


 # check if an error is present in the responses
    if "error" in current_weather or "error" in forecast_weather or "error" in alerts:
        print("Erreur lors de la récupération des données météo")
        print("current_weather:", current_weather)
        print("forecast_weather:", forecast_weather)
        print("alerts:", alerts)
        return  # Stop the program if error
    
    # Merging data
    merged_data = flatten_data(current_weather, forecast_weather, alerts)
    return merged_data

def send_event(event):
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)


# the main program
def process_batch(batch_df, batch_id):
    try:
        #fetch_weather_data
        weather_data = fetch_weather_data()
        send_event(weather_data)
        
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")
        #raise e

#Set up streaming soure source
streaming_df = spark.readStream.format("rate").option("rowPerSecond", 1).load()

query = streaming_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="30 seconds") \
    .start()
try:
    query.awaitTermination()
finally:
    producer.close()

    
        
