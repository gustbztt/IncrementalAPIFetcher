import requests
import pandas as pd
from typing import List, Dict
from config.desired_fields import desired_fields

class UrlDataExtractor:

    '''
    A class to extract and treat data from a list of URLs.

    Attributes:
        url_base (str): The base URL used to build the full URLs.
        headers (dict): A dictionary of HTTP headers to use when making the requests.
        desired_fields (List[str]): A list of strings representing the desired fields from the JSON response.

    Methods:
        get_url_list(dataframe: pd.DataFrame, column_name: str) -> List[str]:
            Returns a list of URLs built from a DataFrame column.

        get_desired_fields_from_json(json_data: Dict[str, any]) -> Dict[str, any]:
            Returns a dictionary containing only the desired fields from a JSON response.

        get_json_from_url(url: str) -> Dict[str, any]:
            Returns a dictionary containing only the desired fields from a JSON response retrieved from a given URL.

        get_treated_data() -> pd.DataFrame:
            Returns a DataFrame with the desired fields extracted from all URLs.

    Usage:
        1. Initialize an instance of the class with the following parameters:
            - url_base (str): The base URL to which the IDs will be appended.
            - headers (dict): Headers to be sent with each GET request.
            - desired_fields (List[str]): A list of strings representing the desired fields.
        
        2. Call the get_url_list() method with the following parameters:
            - dataframe (pd.DataFrame): The pandas DataFrame containing the IDs.
            - column_name (str): The name of the column containing the IDs.
            
        3. Call the get_treated_data() method with no parameters to get a pandas DataFrame 
           containing the extracted data.

    '''

    def __init__(self, url_base, headers, desired_fields):
        # Initializes a new instance of the UrlDataExtractor class.
        self.url_base = url_base
        self.headers = headers
        self.desired_fields = desired_fields

    def get_url_list(self, dataframe, column_name):
        # Builds a list of URLs based on a DataFrame column.
        self.urls = []
        for id in dataframe[column_name]:
            # Cria a URL da solicitação GET com o ID atual
            url = self.url_base + id
            self.urls.append(url)
        self.urls = self.urls[0:10000]
        return self.urls
    
    def get_desired_fields_from_json(self, json_data):
        # Extracts only the desired fields from a JSON response.
        results = {}
        for key, value in json_data.items():
            if key in self.desired_fields:
                results[key] = value
            elif isinstance(value, dict):
                nested_results = self.get_desired_fields_from_json(value)
                if nested_results:
                    results.update(nested_results)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        nested_results = self.get_desired_fields_from_json(item)
                        if nested_results:
                            results.update(nested_results)
        return results

    
    def get_json_from_url(self, url):
        # Retrieves and extracts the desired fields from a JSON response from a given URL.
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data_json = response.json()
            extracted_data = self.get_desired_fields_from_json(data_json)
            return extracted_data
        else:
            return print(f"Error: {response.status_code}")
    
    def get_treated_data(self):
        """
        Requests JSON data from each URL in self.urls and extracts only the desired fields
        specified in self.desired_fields. The extracted data is then organized into a
        pandas DataFrame.

        Returns:
        --------
        df: pd.DataFrame
            A DataFrame containing the extracted data from the URLs.
        """
        data_list = []
        for url in self.urls:
            data = self.get_json_from_url(url)
            data_list.append(data)
        df = pd.DataFrame(data_list, columns=desired_fields)
        return df
