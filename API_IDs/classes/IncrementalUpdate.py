import os
import pandas as pd

class IncrementalUpdate:
    def __init__(self, folder_path, date_column_from_api, date_column_from_query):
        """
        Class for performing incremental updates based on file modification dates.

        Args:
            folder_path (str): Path to the folder containing the files.
            date_column_from_api (str): Name of the column containing dates in the API data.
            date_column_from_query (str): Name of the column containing dates in the query results.
        """
        self.folder_path = folder_path
        self.date_column_from_api = date_column_from_api
        self.date_column_from_query = date_column_from_query

    def get_latest_date_from_files(self):
        """
        Get the latest date from the files in the specified folder.

        Returns:
            latest_datetime (pandas.Timestamp or None): Latest date/time found in the files, or None if no valid dates found.
        """
        latest_datetime = None

        for file_name in os.listdir(self.folder_path):
            file_path = os.path.join(self.folder_path, file_name)
            if os.path.isfile(file_path):
                try:
                    df = pd.read_parquet(file_path)  # Assuming the files are in CSV format
                    if self.date_column_from_api in df.columns:
                        column_data = pd.to_datetime(df[self.date_column_from_api])
                        max_datetime = column_data.max()
                        if latest_datetime is None or max_datetime > latest_datetime:
                            latest_datetime = max_datetime
                except pd.errors.ParserError:
                    print(f"Error reading file: {file_path}")

        return latest_datetime

    def get_sql_query(self):
        """
        Generate the SQL query based on the latest date and retrieve the data.

        Returns:
            df_fluxos (pandas.DataFrame): DataFrame containing the retrieved data.
        """
        from config.conn_dremio import cnxn

        latest_date = self.get_latest_date_from_files()

        if latest_date is not None:
            formatted_date = latest_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

            sql_query = f"SELECT (insert your table fields here, including the API url) FROM (table) WHERE (date_field) > '{formatted_date}'"
            
            self.df_fluxos = pd.read_sql(sql_query, cnxn) 
            
            return self.df_fluxos
        else:
            sql_query = f"SELECT (insert your table fields here, including the API url) FROM BI.BI191"
            self.df_fluxos = pd.read_sql(sql_query, cnxn) 
            return self.df_fluxos
        

    def get_latest_date_from_dataframe(self):
        """
        Get the latest date from the DataFrame.

        Returns:
            latest_date (str or None): Latest date in the specified column format 'dd_mm_yyyy', or None if the column is not found.
        """
        if self.date_column_from_query in self.df_fluxos.columns:
            column_data = pd.to_datetime(self.df_fluxos[self.date_column_from_query])
            latest_date = column_data.max()
            latest_date = latest_date.strftime('%d_%m_%Y')
            return latest_date
        else:
            print(f"'{self.date_column_from_query}' column not found in the DataFrame.")
            return None
