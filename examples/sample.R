# library for HTTP request
library(httr)

# library for reading netcdf file
library(ncdf4)

# (optional) for plotting the data
library(fields)

# GAP API Base URL
base_url <- 'https://tngap.sta.do.kartoza.com/api/v1/measurement/'

# Set your username and password
username <- ''
password <- ''

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

# Set the output file path
output_file <- "data.nc"

# Trigger the API call
response <- GET(
    url = base_url,
    authenticate(username, password),
    query = query_params,
    user_agent("httr"),
    write_disk(output_file, overwrite = TRUE)
)
# Check the response
if (status_code(response) == 200) {
    cat("File downloaded successfully as", output_file, "\n")
} else {
    cat("Failed to download. HTTP Status Code:", status_code(response), "\n")
}

# Optional, read the netcdf file
nc <- nc_open(output_file)
# print dataset
print(nc)

# print variables
variables <- names(nc$var)
print(variables)


# get max_temperature
max_temperature <- ncvar_get(nc, "max_temperature")
nc_close(nc) # cleanup resource

# plot max_temperature variable
image.plot(max_temperature[,,1])
