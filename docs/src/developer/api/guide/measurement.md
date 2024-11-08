---
title: Documentation
summary: Tomorrow Now GAP
  - Irwan Fathurrahman
date: 2024-06-18
some_url: https://github.com/kartoza/tomorrownow_gap.git
copyright: Copyright 2024, Kartoza
contact:
license: This program is free software; you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
---

# OSIRIS II Global Access Platform

**Project Overview**

TomorrowNow.org is partnering with the Bill and Melinda Gates Foundation (BMGF) to develop and assess new weather technologies to support the seed breeding ecosystem in East Africa. The "Next-Gen" project focuses on adopting new or next-generation technologies to improve data access and quality.

**Goals**

The project aims to address two key challenges limiting the uptake of weather data in Africa:

1. **Data Access**: Provide curated datasets from top weather data providers, streamlined APIs, and a global access strategy to ensure long-term, low-cost access to weather data.

2. **Data Quality**: Localise forecast models using a network of ground observation stations, apply bias adjustment techniques, and produce analysis-ready datasets using best-practice quality control methods.

**Objectives**

* Improve data quality by measuring and benchmarking data quality and cost across top models for historical climate reanalysis, short-term weather forecasting, and S2S weather forecasting.

* Enhance data access through a global access strategy and partnerships with data providers.

**Impact**

By addressing data access and quality challenges, the project aims to accelerate the adoption of weather intelligence across the smallholder farming ecosystem in East Africa.

TomorrowNow provides access to the data through a RESTful API, available at https://tngap.sta.do.kartoza.com/api/v1/docs/


## GAP Input Data Table

| Product | Provider | Resolution | Source | Version | API product_type |
|---------|----------|------------|--------|---------|------------------|
| **Historical Data** |
| CBAM Daily Historical Reanalysis (2012 - 2023) | Tomorrow.io | 4km² | Tomorrow.io CBAM 1F enhanced bias-corrected reanalysis | 2012-2023 | cbam_historical_analysis |
| CBAM Daily Historical Reanalysis (Bias-Corrected) (2012-2023) | Tomorrow.io | 4km² | Tomorrow.io CBAM 1F enhanced bias-corrected reanalysis | 2012-2023 | cbam_historical_analysis_bias_adjust |
| Ground Observations (TAHMO stations) | TAHMO weather stations | 300+ stations across East Africa | TAHMO Gap Filled Data (csv) | 2018-2024 | tahmo_ground_observation |
| Ground Observations (Arable stations) | Arable weather stations | 300+ stations across East Africa | Arable (API) | | arable_ground_observation |
| Disdrometer Observation Data | disdrometers | | Tahmo (API) | | disdrometer_ground_observation |
| Radiosonde Observations (Windborne) | WindBorne Systems | 100 weather balloons| Windborne Systems | | windborne_radiosonde_observation |
| **Weather Forecasts** |
| CBAM Short-Term weather forecast (14-days) | Tomorrow.io | 4km² | Tomorrow.io CBAM satellite enhanced short-term weather forecasts | | cbam_shortterm_forecast |
| Salient Seasonal weather forecast (3-months) | Salient | 9km² | Salient (API) | v9 | salient_seasonal_forecast |
| |


## Attributes Table

| Product | Name | Description | Unit | API attribute name |
|---------|------|-------------|------|---------------------|
| **Salient Seasonal weather forecast (3-months)** |
| Salient Seasonal weather forecast (3-months) | Temperature | | °C | temperature |
| Salient Seasonal weather forecast (3-months) | Temperature Climatology | | °C | temperature_clim |
| Salient Seasonal weather forecast (3-months) | Temperature Anomaly | | °C | temperature_anom |
| Salient Seasonal weather forecast (3-months) | Precipitation | | mm day-1 | precipitation |
| Salient Seasonal weather forecast (3-months) | Precipitation Anomaly | | mm day-1 | precipitation_anom |
| Salient Seasonal weather forecast (3-months) | Precipitation Climatology | | mm day-1 | precipitation_clim |
| Salient Seasonal weather forecast (3-months) | Minimum Temperature | | °C | min_temperature |
| Salient Seasonal weather forecast (3-months) | Minimum Temperature Climatology | | °C | min_temperature_clim |
| Salient Seasonal weather forecast (3-months) | Minimum Temperature Anomaly | | °C | min_temperature_anom |
| Salient Seasonal weather forecast (3-months) | Maximum Temperature | | °C | max_temperature |
| Salient Seasonal weather forecast (3-months) | Maximum Temperature Climatology | | °C | max_temperature_clim |
| Salient Seasonal weather forecast (3-months) | Maximum Temperature Anomaly | | °C | max_temperature_anom |
| Salient Seasonal weather forecast (3-months) | Relative Humidity | | % | relative_humidty |
| Salient Seasonal weather forecast (3-months) | Relative Humidity Climatology | | % | relative_humidty_clim |
| Salient Seasonal weather forecast (3-months) | Relative Humidity Anomaly | | % | relative_humidty_anom |
| Salient Seasonal weather forecast (3-months) | Downward Solar Radiation | | kWh m-2 day-1 | solar_radiation |
| Salient Seasonal weather forecast (3-months) | Downward Solar Radiation Climatology | | kWh m-2 day-1 | solar_radiation_clim |
| Salient Seasonal weather forecast (3-months) | Downward Solar Radiation Anomaly | | kWh m-2 day-1 | solar_radiation_anom |
| Salient Seasonal weather forecast (3-months) | Wind Speed Climatology | | m/s | wind_speed |
| Salient Seasonal weather forecast (3-months) | Wind Speed Climatology | | m/s | wind_speed_clim |
| Salient Seasonal weather forecast (3-months) | Wind Speed Climatology | | m/s | wind_speed_anom |
| **CBAM Short-Term weather forecast (14-days)** |
| CBAM Short-Term weather forecast (14-days) | Total Rainfall | | mm | total_rainfall |
| CBAM Short-Term weather forecast (14-days) | Total Evapotranspiration Flux | | mm | total_evapotranspiration_flux |
| CBAM Short-Term weather forecast (14-days) | Max Temperature | | °C | max_temperature |
| CBAM Short-Term weather forecast (14-days) | Min Temperature | | °C | min_temperature |
| CBAM Short-Term weather forecast (14-days) | Precipitation Probability | | % | precipitation_probability |
| CBAM Short-Term weather forecast (14-days) | Humidity Maximum | | % | humidity_maximum |
| CBAM Short-Term weather forecast (14-days) | Humidity Minimum | | % | humidity_minimum |
| CBAM Short-Term weather forecast (14-days) | Wind Speed Average | | m/s | wind_speed_avg |
| CBAM Short-Term weather forecast (14-days) | Solar radiation | | Wh/m2 | solar_radiation |
| **CBAM Daily Historical Reanalysis (2012 - 2023)** |
| CBAM Daily Historical Reanalysis (2012 - 2023) | Min Total Temperature | Minimum temperature (0000:2300) | °C | min_temperature |
| CBAM Daily Historical Reanalysis (2012 - 2023) | Min Day Temperature | Minimum day-time temperature (0600:1800) | °C | min_day_temperature |
| CBAM Daily Historical Reanalysis (2012 - 2023) | Total Rainfall | Total rainfall (0000:2300) | mm | total_rainfall |
| CBAM Daily Historical Reanalysis (2012 - 2023) | Max Day Temperature | Maximum day-time temperature (0600:1800) | °C | max_day_temperature |
| CBAM Daily Historical Reanalysis (2012 - 2023) | Min Night Temperature | Minimum night-time temperature (1900:0500) | °C | min_night_temperature |
| CBAM Daily Historical Reanalysis (2012 - 2023) | Total Solar Irradiance | Total solar irradiance reaching the surface (0000:2300) | MJ/sqm | total_solar_irradiance |
| CBAM Daily Historical Reanalysis (2012 - 2023) | Average Solar Irradiance | Average hourly solar irradiance reaching the surface (0600:1800) | MJ/sqm | average_solar_irradiance |
| CBAM Daily Historical Reanalysis (2012 - 2023) | Max Night Temperature | Maximum night-time temperature (1900:0500) | °C | max_night_temperature |
| CBAM Daily Historical Reanalysis (2012 - 2023) | Max Total Temperature | Maximum temperature (0000:2300) | °C | max_temperature |
| CBAM Daily Historical Reanalysis (2012 - 2023) | Total Evapotranspiration Flux | Total Evapotranspiration flux with respect to grass cover (0000:2300) | mm | total_evapotranspiration_flux |
| **CBAM Daily Historical Reanalysis (Bias-Corrected) (2012-2023)** |
| CBAM Daily Historical Reanalysis (Bias-Corrected) (2012-2023) | Min Total Temperature | Minimum temperature (0000:2300) | °C | min_temperature |
| CBAM Daily Historical Reanalysis (Bias-Corrected) (2012-2023) | Total Rainfall | Total rainfall (0000:2300) | mm | total_rainfall |
| CBAM Daily Historical Reanalysis (Bias-Corrected) (2012-2023) | Total Solar Irradiance | Total solar irradiance reaching the surface (0000:2300) | MJ/sqm | total_solar_irradiance |
| CBAM Daily Historical Reanalysis (Bias-Corrected) (2012-2023) | Max Total Temperature | Maximum temperature (0000:2300) | °C | max_temperature |
| **Ground Observations (TAHMO stations)** |
| Ground Observations (TAHMO stations) | Precipitation | | mm/day | precipitation |
| Ground Observations (TAHMO stations) | Solar radiation | | Wh/m2 | solar_radiation |
| Ground Observations (TAHMO stations) | Relative Humidity Max | | % | max_relative_humidity |
| Ground Observations (TAHMO stations) | Relative Humidity Min | | % | min_relative_humidity |
| Ground Observations (TAHMO stations) | Air Temperature Average | | °C | average_air_temperature |
| Ground Observations (TAHMO stations) | Air Temperature Max | | °C | max_air_temperature |
| Ground Observations (TAHMO stations) | Air Temperature Min | | °C | min_air_temperature |
| **Ground Observations (Arable stations)** |
| Ground Observations (Arable stations) | Total Evapotranspiration Flux | | mm | total_evapotranspiration_flux |
| Ground Observations (Arable stations) | Relative Humidity Max | | % | max_relative_humidity |
| Ground Observations (Arable stations) | Max Day Temperature | | °C | max_day_temperature |
| Ground Observations (Arable stations) | Relative Humidity Mean | | % | mean_relative_humidity |
| Ground Observations (Arable stations) | Mean Day Temperature | | °C | mean_day_temperature |
| Ground Observations (Arable stations) | Relative Humidity Min | | % | min_relative_humidity |
| Ground Observations (Arable stations) | Min Day Temperature | | °C | min_day_temperature |
| Ground Observations (Arable stations) | Precipitation Total | | mm | precipitation_total |
| Ground Observations (Arable stations) | Precipitation | | mm/day | precipitation |
| Ground Observations (Arable stations) | Sea Level Pressure | | kPa | sea_level_pressure |
| Ground Observations (Arable stations) | Wind Heading | | degree | wind_heading |
| Ground Observations (Arable stations) | Wind Speed | | m/s | wind_speed |
| Ground Observations (Arable stations) | Wind Speed Max | | m/s | wind_speed_max |
| Ground Observations (Arable stations) | Wind Speed Min | | m/s | wind_speed_min |
| **Disdrometer Observation Data** |
| Disdrometer Observation Data | Atmospheric Pressure | | kPa | atmospheric_pressure |
| Disdrometer Observation Data | Depth of Water | | mm | depth_of_water |
| Disdrometer Observation Data | Electrical Conductivity of Precipitation | | mS/cm | electrical_conductivity_of_precipitation |
| Disdrometer Observation Data | Electrical Conductivity of Water | | mS/cm | electrical_conductivity_of_water |
| Disdrometer Observation Data | Lightning Distance | | km | lightning_distance |
| Disdrometer Observation Data | Shortwave Radiation | | W/m2 | shortwave_radiation |
| Disdrometer Observation Data | Soil Moisture Content | | m3/m3 | soil_moisture_content |
| Disdrometer Observation Data | Soil Temperature | | °C | soil_temperature |
| Disdrometer Observation Data | Surface Air Temperature | | °C | surface_air_temperature |
| Disdrometer Observation Data | Wind Speed | | m/s | wind_speed |
| Disdrometer Observation Data | Wind Gusts | | m/s | wind_gusts |
| Disdrometer Observation Data | Precipitation Total | | mm | precipitation_total |
| Disdrometer Observation Data | Precipitation | | mm/day | precipitation |
| Disdrometer Observation Data | Relative Humidity | | % | relative_humidity |
| Disdrometer Observation Data | Wind Heading | | degree | wind_heading |
| **Radiosonde Observations (Windborne)** |
| Radiosonde Observations (Windborne) | Temperature | | °C | temperature |
| Radiosonde Observations (Windborne) | Atmospheric Pressure | | hPa | atmospheric_pressure |
| Radiosonde Observations (Windborne) | Specific Humidity | | mg/kg | specific_humidity |
| Radiosonde Observations (Windborne) | Relative Humidity | | % | relative_humidity |
| |

In order to use the API, the user must be authenticated and must have authorisation to access the data.

Let's see how to use the API and what sequence of API calls can lead us to get data for analysis.

Once you open the above link the Swagger will open. Click on the 1️⃣ `Authorize` button, to open the authorisation form.

![Authorise](./img/api-guide-1.png)

To authorize, please enter your `Username` and `Password` Once you have entered your credentials, click the `Authorize` button to complete the authorisation process.

![Authorise form](./img/api-guide-2.png)

Click on the close button or cross button to close the authorisation form.

![Close](./img/api-guide-3.png)

**Examples of Usage of the OSIRIS II API**

Please note that the data in the examples provided below DO NOT reflect the actual data in TomorrowNow.

## Accessing the OSIRIS II API

To use the API click on the Weather & Climate Data 1️⃣.

![Measurement API](./img/api-guide-4.png)

**Weather & Climate Data API:**

Click on the GET API it will show the parameters to enter to get the data. Click on the 1️⃣ `Try it out` button, to fill the detailed in the 2️⃣ available request parameters. After filling the details click on the 3️⃣ `Execute` button, to run the API.

![GET API](./img/api-guide-5.png)
![GET API](./img/api-guide-6.png)

**Example of response:**

![GET API RESPONSE](./img/api-guide-7.png)

**Available format types**

### JSON 

This type is only available for querying by single point.
![JSON](./img/api-guide-7.png)

###  CSV

The user can download the file to check the response
![CSV](./img/api-guide-12.png)

### NETCDF

The user can download the file to check the response
![netcdf](./img/api-guide-13.png)

To read/write the netcdf file user can refer to below link 
https://docs.xarray.dev/en/stable/user-guide/io.html#netcdf

**Example of codes to access the API**

### Python

```python
import requests
from requests.auth import HTTPBasicAuth

url = "https://tngap.sta.do.kartoza.com/api/v1/measurement/?lat=-1.404244&lon=35.008688&attributes=max_temperature,min_temperature&start_date=2019-11-01&end_date=2019-11-02&product=cbam_historical_analysis&output_type=json"

payload={}
headers = {}
basic = HTTPBasicAuth('YOUR_USERNAME', 'YOUR_PASSWORD')

response = requests.request("GET", url, auth=basic, headers=headers, data=payload)

print(response.json())
```

### CURL

```
curl --location 'https://tngap.sta.do.kartoza.com/api/v1/measurement/?lat=-1.404244&lon=35.008688&attributes=max_temperature%2Cmin_temperature&start_date=2019-11-01&end_date=2019-11-02&product=cbam_historical_analysis&output_type=json' -u 'YOUR_USERNAME:YOUR_PASSWORD' -H 'User-Agent: PostmanRuntime/7.42.0'
```

### JavaScript-JQuery

```js
var settings = {
  "url": "https://tngap.sta.do.kartoza.com/api/v1/measurement/?lat=-1.404244&lon=35.008688&attributes=max_temperature,min_temperature&start_date=2019-11-01&end_date=2019-11-02&product=cbam_historical_analysis&output_type=json",
  "method": "GET",
  "timeout": 0,
  "headers": {
    "Authorization": "Basic *****"
  },
};

$.ajax(settings).done(function (response) {
  console.log(response);
});
```


**Upload Location API:**

Using the Location API, you can upload the geometry to query the data by polygon or list of point. You can upload the geometry in one of format: geojson/shapefile/gpkg. The file must be in WGS84 or CRS 4326. The uploaded location will have expiry date time (2 months). Once the server removes your geometry after the expiry time, you need to reupload your geometry.

Note: when using shapefile, the .shp, .dbf, .shx files must be in the zip root directory.


Click on the 1️⃣ Upload Location POST API to view the usage option. Click on the 2️⃣ `Try it out` button, to enable the fields to enter the attributes.

![POST API](./img/api-guide-9.png)

Fill the location_name and select your file to upload in the 1️⃣ available fields. After filling the details click on the 2️⃣ `Execute` button, to run the API.

![POST API](./img/api-guide-10.png)

**Example of response:**

![POST API RESPONSE](./img/api-guide-11.png)

You can see the expiry date time for your geometry in the `expired_on` field.

## API Postman Collection

You can download the postman collection below and import the API collection using your postman. Once imported, you need to set the variable `gap_api_username` and `gap_api_password` using your credentials.

[Download](./assets/tngap_api.postman_collection.zip)


## Error codes

| Response code | Message | Reason |
|---------------|---------|--------|
| 400 | Unknown geometry type! | Use geometry with type Polygon/Multipolygon/MultiPoint to make a request using POST method |
| 400 | Output format json is only available for single point query! | JSON output is only available for GET method with singe point query. Please use csv/netcdf output format! |
| 400 | No matching attribute found! | The attribute list cannot be found in the product type. |
| 400 | Attribute with ensemble cannot be mixed with non-ensemble | When requesting for product type salient_seasonal_forecast and output is csv, the attribute that is in ensemble (50-values) cannot be requested with the attribute that does not have ensemble. Please use netcdf output format instead! |
| 400 | Incorrect output type | Use either json, csv, netcdf or ascii |
| 404 | No weather data is found for given queries | |
| 429 | Too many requests | You have hit the rate limit |
