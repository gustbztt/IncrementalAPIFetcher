# API Data Extraction and Incremental Update

This project extracts data from an API, performs incremental updates based on the last date of the data, and saves the processed data to a file.

## Description

The **API Data Extraction and Incremental Update** project automates the process of extracting data from an API, performing incremental updates based on the last date of the data in the files in a specified folder, and saving the processed data to a file. It consists of the following components:

- **CodeRunner.py**: The main script that orchestrates the data extraction and update process.
- **IncrementalUpdate.py**: A class that handles the incremental update logic by tracking file modifications and extracting the latest data.
- **UrlDataExtractor.py**: A class that interacts with the API, retrieves the data, and performs data processing.
- **config folder**: Contains configuration files required for the project, including `conn_dremio.py` and `constants_api.py`.

## Dependencies

The project relies on the following dependencies:

- **pandas**: Used for data manipulation and analysis.
- **pyarrow**: Used for saving the `.parquet` files.
- **requests**: Used to handle JSON requests.
- **os**: Used to handle folders and file paths.
- **pyodbc**: Used to make a connection to Dremio.

## Configuration

The project requires the following configuration:

- Connection details for the API in `conn_dremio.py`.
- API-specific constants and settings in `constants_api.py`.

Please ensure that these configuration files are appropriately set before running the script.


## Author
- Gustavo Barzotto
