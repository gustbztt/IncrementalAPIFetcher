from config.constants_api_fluxo import url_base, headers, folder_path, date_column_from_api, date_column_from_query
from classes.CodeRunner import CodeRunner   
from config.desired_fields import desired_fields

code_runner = CodeRunner(folder_path, url_base, headers, desired_fields, date_column_from_api, date_column_from_query)
code_runner.run_code()
