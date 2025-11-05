import os
import time
import requests
from prometheus_client import start_http_server, Gauge, Info

# 1. Настройки
API_KEY = os.getenv("OPENWEATHER_API_KEY", "6bea3a864d4087d591a70398c28feb07")
UPDATE_INTERVAL = 20  # сек

# 2. Города, которые будем собирать
CITIES = {
    "Astana": (51.1694, 71.4491),
    "Almaty": (43.2389, 76.8897),
    "Dubai": (25.276987, 55.296249),
    "London": (51.5072, -0.1276),
}

# 3. Метрики (с лейблом city)
m_temp = Gauge("owm_temperature_celsius", "Current temperature from OpenWeather, C", ["city"])
m_feels = Gauge("owm_feels_like_celsius", "Feels like temperature, C", ["city"])
m_hum = Gauge("owm_humidity_percent", "Humidity, %", ["city"])
m_press = Gauge("owm_pressure_hpa", "Pressure, hPa", ["city"])
m_wind = Gauge("owm_wind_speed_ms", "Wind speed, m/s", ["city"])
m_clouds = Gauge("owm_clouds_percent", "Cloudiness, %", ["city"])
m_visibility = Gauge("owm_visibility_m", "Visibility, m", ["city"])
m_rain_1h = Gauge("owm_rain_1h_mm", "Rain volume for the last 1 hour, mm", ["city"])
m_snow_1h = Gauge("owm_snow_1h_mm", "Snow volume for the last 1 hour, mm", ["city"])

# те, что без города (общие)
m_api_up = Gauge("owm_api_up", "Weather API status 1=up 0=down")
m_last_update = Gauge("owm_last_update_unixtime", "Last successful update timestamp")

info_exporter = Info("owm_exporter_info", "Info about this exporter")
info_exporter.info({
    "source": "openweather",
    "author": "student",
    "cities": ",".join(CITIES.keys())
})


def fetch_city_weather(city_name: str, lat: float, lon: float):

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY,
        "units": "metric",
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    main = data.get("main", {})
    wind = data.get("wind", {})
    clouds = data.get("clouds", {})
    rain = data.get("rain", {})
    snow = data.get("snow", {})

    m_temp.labels(city=city_name).set(main.get("temp", 0))
    m_feels.labels(city=city_name).set(main.get("feels_like", 0))
    m_hum.labels(city=city_name).set(main.get("humidity", 0))
    m_press.labels(city=city_name).set(main.get("pressure", 0))
    m_wind.labels(city=city_name).set(wind.get("speed", 0))
    m_clouds.labels(city=city_name).set(clouds.get("all", 0))
    m_visibility.labels(city=city_name).set(data.get("visibility", 0))
    m_rain_1h.labels(city=city_name).set(rain.get("1h", 0))
    m_snow_1h.labels(city=city_name).set(snow.get("1h", 0))


def fetch_and_update_all():
    """Обходит все города и обновляет метрики."""
    ok = True
    for city_name, (lat, lon) in CITIES.items():
        try:
            fetch_city_weather(city_name, lat, lon)
        except Exception as e:
            # если по одному городу ошибка — просто пометим, что API не ок
            ok = False
            print(f"[WARN] cannot fetch {city_name}: {e}")

    # общие флаги
    m_api_up.set(1 if ok else 0)
    m_last_update.set(time.time())


if __name__ == "__main__":
    # http://localhost:8000/metrics
    start_http_server(9101)
    print("Custom OpenWeather exporter started on :8000")
    while True:
        fetch_and_update_all()
        time.sleep(UPDATE_INTERVAL)
