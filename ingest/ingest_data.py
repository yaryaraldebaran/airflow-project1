import requests
from requests.exceptions import HTTPError
import json 
import datetime

def ingest_openweather(lon, lat):
    appID = "e353fdf2a125b862dad3a573ffadefbb"
    url = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appId={appID}&units=metric".format(lon=lon,lat=lat,appID=appID)
    try:
        response = requests.get(url)
        return response.text
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')


def extract_temperature(response):
    data = json.loads(response)
    data_temperature = data["main"]
    temp = data_temperature["temp"]
    # feels_like = data_temp["feels_like"]
    # temp_min = data_temp["temp_min"]
    # temp_max = data_temp["temp_max"]
    # pressure = data_temp["pressure"]
    # humidity = data_temp["humidity"]
    return data_temperature

def extract_sky_conditions(response):
    data = json.loads(response)
    data_sky = {
        "clouds":{},
        "sys":{}
    }
    data_sky["clouds"].update(data["clouds"])
    data_sky["sys"].update(data["sys"])
    # data_sky["wind_all"] = data_sky["all"]
    # del data_sky["all"]
    return data_sky
def extract_time_dim(response):
    data = json.loads(response)
    unix_time = data["dt"]
    time = datetime.datetime.fromtimestamp(unix_time)
    return time


if __name__=="__main__":
    # print(extract_sky_conditions(ingest_openweather(lat=-6.404218916,lon=106.7903278)))
    print(extract_time_dim(ingest_openweather(lat=-6.404218916,lon=106.7903278)))
