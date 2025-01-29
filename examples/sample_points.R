# library for HTTP request
library(httr)

# working directory for output data
# NOTE: on windows, the path should use double backslashes ("\\")
working_dir <- '~/Documents/gap_data'
dir.create(working_dir, showWarnings = FALSE)

# GAP API Base URL
base_url <- 'https://tngap.sta.do.kartoza.com/api/v1/measurement/'

# Set your username and password
username <- ''
password <- ''

# the csv has columns: locationId,latitude,longitude
csv_file_path <- file.path(working_dir, 'points.csv')

# Load CSV into a DataFrame
input_points <- read.csv(csv_file_path, header = TRUE)

# Initialize an empty list to store dataframes
dataframes <- list()

for (i in 1:nrow(input_points)) {
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
        output_type = 'csv',
        # area bounding box (long min, lat min, long max, lat max)
        # bbox = '33.9, -4.67, 41.89, 5.5'
        # for single point query, we can use lat and lon parameters
        lat = input_points$latitude[i],
        lon = input_points$longitude[i]
        # for custom polygon/bounding box, you can upload a shapefile and provides the location_name
        # location_name = ''
    )

    # Set the output file path
    output_file <- file.path(working_dir, paste0("data", "_lat", input_points$latitude[i], "_lon", input_points$longitude[i], ".csv"))
    print(output_file)
    
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

        # Load the CSV into a dataframe
        df <- read.csv(output_file)

        # add locationId to the df
        df$locationId <- input_points$locationId[i]

        # Add the dataframe to the list
        dataframes[[i]] <- df

        # Clean up the temporary file if needed
        # unlink(output_file)
    } else {
        cat("Failed to download. HTTP Status Code:", status_code(response), "\n")
        cat(content(response, as = "text"))
    }
}

print(dataframes)
