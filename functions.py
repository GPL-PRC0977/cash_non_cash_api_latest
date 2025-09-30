from google.cloud import secretmanager
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import bigquery
import os
import json
import uuid
import requests
from dotenv import load_dotenv

load_dotenv()

gemini_api_url = 'https://us-west1-pgc-dma-dev-sandbox.cloudfunctions.net/cash-non-cash-gemini-test'

def get_credentials_from_secret_manager(project_id: str, secret_id: str, scopes: list = None):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    service_account_info = json.loads(response.payload.data.decode("UTF-8"))

    if scopes:
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=scopes
        )
    else:
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )

    return credentials

def get_drive_service():
    creds = get_credentials_from_secret_manager(
        os.getenv("BQ_PROJECT_NAME"), 
        os.getenv("GDRIVE_FOLDER_SECRET_FROM_SECRET_MANAGER"), 
        os.getenv("SCOPE"))
    service = build('drive', 'v3', credentials=creds)
    return service

BQ_WRITER_CREDENTIALS = get_credentials_from_secret_manager(
    os.getenv("BQ_PROJECT_NAME"), 
    os.getenv("BQ_DATA_WRITER")
)

BQ_READER_CREDENTIALS = get_credentials_from_secret_manager(
    os.getenv("BQ_PROJECT_NAME"),
    os.getenv("BQ_DATA_READER")
)

API_CREDENTIALS = get_credentials_from_secret_manager(
    os.getenv("BQ_PROJECT_NAME"),
    os.getenv("API_SECRET_ID_FROM_SECRET_MANAGER")
)

BQ_CLIENT_WRITER = bigquery.Client(
    credentials=BQ_WRITER_CREDENTIALS, 
    project=BQ_WRITER_CREDENTIALS.project_id
)

BQ_CLIENT_READER = bigquery.Client(
    credentials=BQ_READER_CREDENTIALS, 
    project=BQ_READER_CREDENTIALS.project_id
)

def is_valid_api_key(api_key):
    api_client = bigquery.Client(
        credentials=API_CREDENTIALS, project=API_CREDENTIALS.project_id)

    query = f"""
        SELECT 1 FROM `{os.getenv("API_PROJECT_TABLE_NAME")}`
        WHERE api_key = @api_key
        AND active = 1
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("api_key", "STRING", api_key)
        ]
    )
    results = api_client.query(query, job_config=job_config).result()
    return any(results)
    
def save_file_info_to_bq(orig_file_name, file_name, ir_type, ir_description, uploaded_by):
    try:
        file_id = str(uuid.uuid4())

        query = f"""
            insert into `{os.getenv("BQ_PROJECT_NAME")}.cash_non_cash.store_upload_master` (
                    file_id,
                    file_original_name,
                    file_new_name,
                    date_uploaded,
                    uploaded_by,
                    ir_type,
                    ir_description
                )
                values
                (
                    @file_id,
                    @file_name,
                    @new_file_name,
                    CURRENT_DATETIME(),
                    @uploaded_by,
                    @ir_type,
                    @ir_description
                )
            """
        query_parameters = [
            bigquery.ScalarQueryParameter("file_id", "STRING", file_id),
            bigquery.ScalarQueryParameter(
                "file_name", "STRING", orig_file_name),
            bigquery.ScalarQueryParameter(
                "new_file_name", "STRING", file_name),
            bigquery.ScalarQueryParameter("uploaded_by", "STRING", uploaded_by),
            bigquery.ScalarQueryParameter("ir_type", "STRING", ir_type),
            bigquery.ScalarQueryParameter("ir_description", "STRING", ir_description)
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

        query_job = BQ_CLIENT_WRITER.query(query, job_config=job_config)

        query_job.result()
        
        print("Successfully save to BQ.")

    except Exception as e:
        print(f"Error: {e}")

def gemini_processing(filepath, new_file_name):
    try:
        print(f"Gemini processing has started for file: {filepath}")
        with open(filepath, "rb") as f:
            files = {"file": (new_file_name, f)}
            requests.post(gemini_api_url, files=files)

        # return {'status':'success', 'message':'Sent to gemini for processing.'}

    except Exception as e:
        print(f"Gemini processing error: {e}")
        # return {"status": False, "message": str(e)}
    