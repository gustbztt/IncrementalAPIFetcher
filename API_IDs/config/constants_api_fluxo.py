# Define a URL base da API
url_base = 'insert base url'

# Define requisitos para Execução de Fluxo na API
api_secret_key = 'insert api secret key'
api_access_key = 'insert api access key'
billing_company_id = "insert billing company id"
content_type = "application/json"

headers = {
"SecretKey": api_secret_key,
"AccessKey": api_access_key,
"X-Billing-Company-Id": billing_company_id,
"Content-Type": content_type
            }

folder_path = r'insert folder where you want to save your data'
date_column_from_api = 'insert name of the column where you have date (for incremental update)'
date_column_from_query = 'insert date column from sql query'
