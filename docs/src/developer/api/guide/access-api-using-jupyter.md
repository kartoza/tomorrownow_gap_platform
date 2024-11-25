---
title: Documentation
summary: Tomorrow Now GAP
  - Danang Massandy
date: 2024-11-25
some_url: https://github.com/kartoza/tomorrownow_gap.git
copyright: Copyright 2024, Kartoza
contact:
license: This program is free software; you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.
---

# Accessing GAP API using Jupyter Notebook

This tutorial demonstrates how to download a NetCDF file from GAP API, save it locally, and reading it using Python libraries in a Jupyter Notebook. We'll use requests for the download and xarray for processing.

## Requirements

1. **Python Libraries**:
- requests for downloading files from an API.
- xarray for reading and working with NetCDF files.
- netCDF4 as a backend for xarray to handle NetCDF data.
- matplotlib (Optional) for plotting the data.

Install these libraries using the following commands:

```bash
pip install requests xarray netCDF4 matplotlib
```

2. **Jupyter Notebook**: If Jupyter is not already installed, you can install it with:

```bash
pip install notebook
```


## Running the Code

1. **Download the sample code**

- You can download the full code from [here](https://github.com/kartoza/tomorrownow_gap/blob/main/examples/sample.ipynb).
- There is also a python sample code that you can download from [here](https://github.com/kartoza/tomorrownow_gap/blob/main/examples/sample.py). You can use this sample code to run without jupyter notebook by running: `python sample.py`.


2. **Open Jupyter Notebook**: Launch the notebook by running:

```bash
jupyter notebook
```

3. **Open the downloaded sample code**
- Select the file `sample.ipynb`
- Click Open

4. **Set the username and password**

```python
# Set your username and password
username = ""
password = ""
```

3. **Define your query parameters**

- The sample code shows query to CBAM Historical Analysis dataset and save the output to netcdf file.
- The query is using bounding box of Kenya, you can also define another area bounding box or use the lat/lon for single query. Another option is to upload a shapefile for custom bounding box/polygon, then use location_name in the query_params.

```python
# product type and attribute list can be viewed in
# https://kartoza.github.io/tomorrownow_gap/developer/api/guide/measurement/#gap-input-data-table
product = 'cbam_historical_analysis'
# Define the attributes to fetch
attribs = [
    "max_temperature",
    "min_temperature"
]
# start and end dates in format YYYY-MM-DD
start_date = '2020-01-01'
end_date = '2020-01-03'
# available output type: json, csv, netcdf
# Note that json output is only for single location query
output_type = 'netcdf'
# area bounding box (long min, lat min, long max, lat max)
bbox = '33.9, -4.67, 41.89, 5.5'
# for single point query, we can use lat and lon parameters
# lat = '',
# lon = ''
# for custom polygon/bounding box, you can upload a shapefile and provides the location_name
# location_name = ''
```

4. **Set the output file path**

```python
# Set the output file path
local_filename = "data.nc"
```

5. **Run the code by clicking the :arrow_forward: button or `Run > Run All Cells`.**


## Expected Output

**If the download is successful:**
1. The file will be saved locally with the specified filename (e.g., data.nc).

2. The ncdf4 library will print metadata and a list of variables in the NetCDF file, e.g.:

```
Reading NetCDF file with xarray...
<xarray.Dataset> Size: 2MB
Dimensions:          (date: 3, lat: 285, lon: 222)
Coordinates:
  * date             (date) datetime64[ns] 24B 2020-01-01 2020-01-02 2020-01-03
  * lat              (lat) float64 2kB -4.662 -4.626 -4.59 ... 5.418 5.454 5.489
  * lon              (lon) float64 2kB 33.92 33.95 33.99 ... 41.8 41.84 41.87
Data variables:
    max_temperature  (date, lat, lon) float32 759kB ...
    min_temperature  (date, lat, lon) float32 759kB ...
```

3. If matplotlib is installed, then the chart of max_temperature will be plotted.

**If the download fails:**

The script will display an error message with the HTTP status code, e.g.:

```
Failed to download file. HTTP Status Code: 404
```
