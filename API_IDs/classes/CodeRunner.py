import time
import os
from classes.UrlDataExtractor import UrlDataExtractor
from classes.IncrementalUpdate import IncrementalUpdate

class CodeRunner:
    def __init__(self, folder_path, url_base, headers, desired_fields, date_column_from_api, date_column_from_query):
        """
        Class for running the code, performing incremental updates, extracting data from URLs, and saving the results.

        Args:
            folder_path (str): Path to the folder where the results will be saved.
            url_base (str): Base URL for data extraction.
            headers (dict): Headers for the HTTP requests.
            desired_fields (list): List of field names to extract from the data.
            date_column_from_api (str): Name of the column containing dates in the API data.
            date_column_from_query (str): Name of the column containing dates in the query results.
        """
        self.folder_path = folder_path
        self.url_base = url_base
        self.headers = headers
        self.desired_fields = desired_fields
        self.date_column_from_api = date_column_from_api
        self.date_column_from_query = date_column_from_query
        self.incremental_update = IncrementalUpdate(self.folder_path, self.date_column_from_api, self.date_column_from_query)
        self.url_data_extractor = UrlDataExtractor(self.url_base, self.headers, self.desired_fields)

    def run_code(self):
        """
        Run the code to perform incremental updates, extract data from URLs, and save the results.
        """
        start_time = time.time()

        self.dataframe = self.incremental_update.get_sql_query()
        self.last_date = self.incremental_update.get_latest_date_from_dataframe()

        self.url_list = self.url_data_extractor.get_url_list(self.dataframe, self.date_column_from_query)
        self.df_treated = self.url_data_extractor.get_treated_data()

        elapsed_time = time.time() - start_time
        print(f"Code execution time: {elapsed_time} seconds")

        self.save_file()

    def save_file(self):
        """
        Save the treated DataFrame to a Parquet file in the specified folder.
        """
        timestamp = time.strftime("%Y%m%d%H%M%S")
        file_name = f"data_{timestamp}.parquet"
        file_path = os.path.join(self.folder_path, file_name)

        try:
            # Save the DataFrame to Parquet format
            self.df_treated.to_parquet(file_path, index=False)
            print(f"File saved to: {file_path}")
        except Exception as e:
            print(f"Error saving file: {str(e)}")
