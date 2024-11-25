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

# Accessing GAP API using R

This tutorial demonstrates how to download a NetCDF file from GAP API, save it locally, and read it using R. We'll use httr for downloading the file and the ncdf4 library for reading NetCDF data.

## Requirements

1. **R Environment**:

    - Install R and RStudio (optional but recommended for running scripts interactively).

2. **R Libraries**:

    - curl: Dependency for httr
    - httr: For making HTTP requests to download the file.
    - ncdf4: For reading NetCDF files.
    - fields: (Optional), for plotting

    Install these packages using the following commands in R:

    ```R
    install.packages("curl")
    install.packages("httr")
    install.packages("ncdf4")
    install.packages("fields")
    ```

    If `ncdf4` package installation failed, then you can need to install `libnetcdf-dev` library on your operating system. Below is the example to install the package on Ubuntu. Once installed, run `install.packages("ncdf4")` again on RStudio. 

    ```bash
    sudo apt-get update -y
    sudo apt-get install -y libnetcdf-dev
    ```

    For windows, you can follow the installation guide of ncdf4 from this [link](https://cirrus.ucsd.edu/~pierce/ncdf/#:~:text=Installation&text=Windows%3A%20Use%20the%20R%20graphical,9.).


## Running the Code

1. **Download the sample code**

    - You can download the full code from [here](https://github.com/kartoza/tomorrownow_gap/blob/main/examples/sample.R).
    - Once you have downloaded the sample code, then you can open the file from RStudio (File -> Open File).

2. **Set the username and password**

    ```R
    # Set your username and password
    username <- '<YOUR_USERNAME>'
    password <- '<YOUR_PASSWORD>'
    ```

3. **Define your query parameters**

    - The sample code shows query to CBAM Historical Analysis dataset and save the output to netcdf file.
    - The query is using bounding box of Kenya, you can also define another area bounding box or use the lat/lon for single query. Another option is to upload a shapefile for custom bounding box/polygon, then use location_name in the query_params.

    ```R
    # Define the request parameters
    query_params <- list(
        # product type and attribute list can be viewed in
        # https://kartoza.github.io/tomorrownow_gap/developer/api/guide/measurement/#gap-input-data-table
        product = 'cbam_historical_analysis',
        # comma separated string (without space)
        attributes = 'max_temperature,min_temperature',
        # start and end dates in format YYYY-MM-DD
        start_date = '2020-01-01',
        end_date = '2020-01-03',
        # available output type: json, csv, netcdf
        # Note that json output is only for single location query
        output_type = 'netcdf',
        # area bounding box (long min, lat min, long max, lat max)
        bbox = '33.9, -4.67, 41.89, 5.5'
        # for single point query, we can use lat and lon parameters
        # lat = '',
        # lon = ''
        # for custom polygon/bounding box, you can upload a shapefile and provides the location_name
        # location_name = ''
    )
    ```

4. **Set the output file path**

    ```R
    # Set the output file path
    output_file <- "data.nc"
    ```

5. **Run the code using**: `Code -> Run Region -> Run All`.


## Expected Output

**If the download is successful:**


1. The file will be saved locally with the specified filename (e.g., data.nc).

2. The ncdf4 library will print metadata and a list of variables in the NetCDF file, e.g.:

    ```
    File data.nc (NC_FORMAT_NETCDF4):

        2 variables (excluding dimension variables):
            float max_temperature[lon,lat,date]   (Contiguous storage)  
                _FillValue: NaN
                Description: Maximum temperature (0000:2300)
                Units: Deg C
            float min_temperature[lon,lat,date]   (Contiguous storage)  
                _FillValue: NaN
                Description: Minimum temperature (0000:2300)
                Units: Deg C

        3 dimensions:
            date  Size:3 
                units: days since 2012-01-01
                calendar: proleptic_gregorian
            lat  Size:285 
                _FillValue: NaN
                dtype: float32
                long_name: Latitude
                units: degrees_north
            lon  Size:222 
                _FillValue: NaN
                dtype: float32
                long_name: Longitude
                units: degrees_east
    ```

**If the download fails:**


The script will display an error message with the HTTP status code, e.g.:

```
Failed to download file. HTTP Status Code: 404
```

## Exploring the Dataset

After successfully loading the file:

1. List All Variables:

    ```R
    print(variables)
    ```

2. Extract Data from a Variable:

    ```R
    nc <- nc_open(local_filename)
    max_temperature <- ncvar_get(nc, "max_temperature")
    nc_close(nc)
    ```

3. Visualize Data

    - Install the fields package for plotting:

        ```R
        install.packages("fields")
        library(fields)
        image.plot(temperature[,,1])  # Plot the first time step
        ```
