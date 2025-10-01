<<<<<<< HEAD
from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
import uuid
from functions import save_file_info_to_bq, get_drive_service, is_valid_api_key, gemini_processing, BQ_CLIENT_READER, BQ_READER_CREDENTIALS, BQ_CLIENT_WRITER
from googleapiclient.http import MediaFileUpload
=======
# Requirements
# * pip install flask
# * pip install google-cloud-secret-manager
# * pip install Authlib
# * pip install python-dotenv

from flask import Flask, render_template, jsonify, request, url_for, session, redirect
from werkzeug.utils import secure_filename
import requests
from dotenv import load_dotenv
from google.cloud import secretmanager
import os
import json
from authlib.integrations.flask_client import OAuth # type: ignore
import google.auth
import threading
from datetime import datetime

# * This is use to make the google authentication callback work
# * Without this, the google authentication will not work after deploying in cloud run
from werkzeug.middleware.proxy_fix import ProxyFix
>>>>>>> ae8a73ac24edc2d688ee7ffce237359f35b53f15

load_dotenv()

app = Flask(__name__)
<<<<<<< HEAD

TEMP_FOLDER=os.getenv("TEMP_FOLDER")

os.makedirs(TEMP_FOLDER, exist_ok=True)
app.config["TEMP_FOLDER"] = TEMP_FOLDER

@app.route('/upload_ir', methods=['POST'])
def upload():
    try:
        api_key = request.headers.get('X-API-Key')
        if not api_key or not is_valid_api_key(api_key):
            return jsonify({"status": "error",
                            "message": "Unauthorized. Invalid API key."}), 401

        if 'file' not in request.files:
            return jsonify({'error': 'No file part in the request.'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected.'}), 400

        ir_type = request.form.get('ir_type')
        ir_description = request.form.get('ir_description')
        uploaded_by = request.form.get('uploaded_by')

        # Generate unique filename
        name, ext = os.path.splitext(file.filename)
        uid = str(uuid.uuid4())
        parts = uid.split('-')
        collected_parts = [p[:4] for p in parts]
        short_parts = ("-".join(collected_parts))
        new_file_name = f"{name}-{short_parts}{ext}"

        filename = secure_filename(new_file_name)
        filepath = os.path.join(app.config['TEMP_FOLDER'], filename)
        file.save(filepath)

        # Step 2: Save info to BQ
        save_file_info_to_bq(file.filename, filename, ir_type, ir_description, uploaded_by)

        # Step 3: Upload to Drive
        service = get_drive_service()
        file_metadata = {
            'name': filename,
            'parents': [os.getenv("UPLOAD_FOLDER_ID")]
        }
        media = MediaFileUpload(filepath, resumable=True)

        try:
            uploaded_file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink',
                supportsAllDrives=True
            ).execute()
        finally:
            if media._fd:
                media._fd.close()
                
        # Step 4: Gemini processing
        gemini_processing(filepath,filename)

        # Step 5: Cleanup
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                print(f"Deleted: {filepath}")
                delete_status = f"Deleted: {filepath}"
            else:
                print(f"File not found for cleanup: {filepath}")
        except Exception as cleanup_err:
            print(f"Cleanup failed: {cleanup_err}")
            delete_status = f"Cleanup failed: {cleanup_err}"

        return jsonify(
            {'message': f'{uploaded_file["name"]}',
            'cleanup_status': delete_status}
        ), 200

    except Exception as e:
        print(f"Error uploading file: {str(e)}")
        return jsonify({'message': f"Error uploading file: {str(e)}"}), 500
    

@app.route('/upload_bulk_to_gdrive', methods=['POST'])
def upload_bulk_to_gdrive():
    try:
        api_key = request.headers.get('X-API-Key')
        if not api_key or not is_valid_api_key(api_key):
            return jsonify({"status": "error",
                            "message": "Unauthorized. Invalid API key."}), 401
            
        print("starting bulk gdrive upload...")
        if 'bulk_file' not in request.files:
            return jsonify({'error': 'No file part in the request.'}), 400

        files = request.files.getlist('bulk_file')
        uploaded_by = request.form.get('uploaded_by')

        print(f"Uploaded By: {uploaded_by}")
        
        for file in files:
            filename = file.filename
            filepath = os.path.join(app.config['TEMP_FOLDER'], filename)
            print(f"Filename: {filename}")

            name, ext = os.path.splitext(filename)
            uid = str(uuid.uuid4())
            parts = uid.split('-')
            collected_parts = [p[:4] for p in parts]
            short_parts = ("-".join(collected_parts))
            new_created_file_name = f"{name}-{short_parts}{ext}"
            
            new_file_name = secure_filename(new_created_file_name)
            
            file.save(filepath)
            
            service = get_drive_service()
            file_metadata = {
                'name': new_file_name,
                'parents': [os.getenv("UPLOAD_FOLDER_ID")]
            }
            media = MediaFileUpload(filepath, resumable=True)
            
            try:
                service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id, name, webViewLink',
                    supportsAllDrives=True
                ).execute()
            finally:
                save_file_info_to_bq(filename,new_file_name,"","",uploaded_by)
                gemini_processing(filepath, new_file_name)
                if media._fd:
                    media._fd.close()
                    
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
            except Exception as cleanup_err:
                print(f"Cleanup failed: {cleanup_err}")
                
        return jsonify(
            {
                'message': 'Upload complete.',
                'status': 'success'
            }
        ),200
                   
    except Exception as e:
        return jsonify({"status": "failed"}), 500

@app.route('/get_app_master_data', methods=['GET'])
def get_app_master_data():
    try:
        print("Entering get_app_master_data endpoint.")
        
        api_key = request.headers.get('X-API-Key')
        if not api_key or not is_valid_api_key(api_key):
            return jsonify({"status": "error",
                            "message": "Unauthorized. Invalid API key."}), 401

        query = f"""            
            WITH cte_master AS (
                SELECT
                    file_id,
                    file_new_name,
                    file_original_name,
                    date_uploaded,
                    ROW_NUMBER() OVER (
                    PARTITION BY file_original_name
                    ORDER BY date_uploaded DESC
                    ) AS rn
                FROM `pgc-dma-dev-sandbox.cash_non_cash.store_upload_master`
                )
                SELECT 
                cte.file_id,
                cte.file_new_name,
                cte.file_original_name,
                FORMAT_TIMESTAMP("%m/%d/%Y %I:%M:%S %p", TIMESTAMP(cte.date_uploaded), "Asia/Manila") AS date_uploaded,
                master.uploaded_by,
                master.ir_type,
                master.ir_description,
                COALESCE(extracts.error, '') AS error,
                LOWER(extracts.document_type) AS document_type
                FROM cte_master AS cte
                LEFT JOIN `pgc-dma-dev-sandbox.cash_non_cash.store_upload_master` AS master
                ON cte.file_id = master.file_id
                LEFT JOIN `pgc-dma-dev-sandbox.cash_non_cash.data_extracts` AS extracts
                ON REGEXP_EXTRACT(extracts.file_name, r'[^/]+$') = cte.file_new_name
                WHERE rn = 1 order by cte.date_uploaded desc;


        """
        
        print(f"Query: {query}")
        

        query_job = BQ_CLIENT_WRITER.query(query)
        results = query_job.result()

        rows = [dict(row) for row in results]
        if not rows:
            return jsonify({"message": "No data found"})
        return jsonify(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    from googleapiclient.http import MediaFileUpload
    app.run(debug=True, port=5001)

# if __name__ == '__main__':
#     app.run(debug=True)
=======
app.secret_key = 'your_secret_key'

app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

secret_project_id = os.getenv('EP_PROJECT_ID')
secret_id = os.getenv('SECRET_ID')


domain = "https://cash-non-cash-api-740032229271.us-west1.run.app" # TODO: Change this to prod before deploying

# domain = "http://127.0.0.1:5001"


def get_api_key(project_id: str, secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=secret_name)
    return response.payload.data.decode("UTF-8")

api_key = get_api_key(project_id=secret_project_id, secret_id=secret_id)


def get_oauth_config_from_secret(project_id: str, secret_id: str) -> dict:
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    response = secret_client.access_secret_version(
        request={"name": secret_name})
    secret_payload = response.payload.data.decode("UTF-8")

    oauth_config = json.loads(secret_payload)
    return oauth_config


oauth_secrets = get_oauth_config_from_secret(
    os.getenv('EP_PROJECT_ID'), "cash-non-cash-google-oauth")

client_id = oauth_secrets["GOOGLE_CLIENT_ID"]
client_secret = oauth_secrets["GOOGLE_CLIENT_SECRET"]
redirect_uri = oauth_secrets["GOOGLE_REDIRECT_URI"]

print(f"Client ID: {client_id}")
print(f"Client Secret: {client_secret}")
print(f"Redirect URI: {redirect_uri}")

oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=client_id,
    client_secret=client_secret,
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={
        'scope': 'openid email profile'
    },
    redirect_uri=redirect_uri,
)


@app.route('/login')
def login():
    redirect_uri = url_for('callback', _external=True)
    return google.authorize_redirect(redirect_uri)


@app.route('/callback')
def callback():
    token = google.authorize_access_token()
    user_info = google.get(
        'https://openidconnect.googleapis.com/v1/userinfo').json()
    session['user'] = user_info
    session['active_account'] = user_info.get('email')
    print(session['active_account'])
    # print(user_info)
    return redirect('/')

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')

@app.route('/')
def index():
    try:
        if 'user' in session and session['user']:
            user_account = session.get('user')
            full_name = user_account['name']
            email = user_account['email']
            
            # *---------------------------------------------------------------------
            # TODO: Add additional validation. Create user master in BQ instead
            # ! Cons: This will need a UI for user management
            # ? This only allows primer emails
            
            domain = email.split("@")[1]
            if not domain == "primergrp.com":
                print("Not valid primer email")
                return render_template('noaccess.html', email=email)
            else:
                print("Valid primer email")
            # *----------------------------------------------------------------------
            
            picture = user_account['picture']
            return render_template('index.html', full_name=full_name, email=email, picture=picture)
        else:
            return render_template('login.html')
    except Exception as e:
        print(f"Error: {e}")
    
    
@app.route('/dashboard')
def dashboard():
    if 'user' in session and session['user']:
        user_account = session.get('user')
        full_name = user_account['name']
        email = user_account['email']
        
        # *---------------------------------------------------------------------
        # TODO: Add additional validation. Create user master in BQ instead
        # ! Cons: This will need a UI for user management
        # ? This only allows primer emails
        
        domain = email.split("@")[1]
        if not domain == "primergrp.com":
            print("Not valid primer email")
            return render_template('noaccess.html', email=email)
        else:
            print("Valid primer email")
        # *----------------------------------------------------------------------
        print(get_master_data())
        picture = user_account['picture']
        return render_template('dashboard.html', full_name=full_name, email=email, picture=picture, master_data=get_master_data())
    else:
        return render_template('login.html')

# Upload with IR start <---------------------------------------------------------------------------------------------///////
@app.route('/api_upload_ir', methods=['POST'])
def api_upload_ir():
    try:
        file = request.files.get('ir_file')
        ir_type = request.form.get('ir_type')
        ir_description = request.form.get('ir_description')
        uploaded_by = session.get('active_account')

        if not file:
            return jsonify({'error': 'No file provided'}), 400

        file.stream.seek(0)
        response_msg = upload_file(file, ir_type, ir_description, uploaded_by)

        if response_msg['status'] == "success":
            file.stream.seek(0)
            send_to_doc_ai(file)
            return jsonify({"message":"File uploaded successfully.",
                            "status":"success"}), 200
        else:
            return jsonify({"message":"File was failed to upload.",
                            "status":"failed"}), 500

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


def upload_file(file, ir_type, ir_description, uploaded_by):
    try:
        url = f"{domain}/upload_ir"

        payload = {
            "ir_type": ir_type,
            "ir_description": ir_description,
            "uploaded_by": uploaded_by
        }

        files = {
            'file': (secure_filename(file.filename), file.stream, file.content_type)
        }

        headers = {
            'X-API-KEY': api_key
        }

        response = requests.post(
            url, files=files, headers=headers, data=payload)

        if response.status_code == 200:
            print("File upload success")
            return {"status":"success"}
        else:
            print("File upload failed")
            return {"status":"failed"}

    except Exception as e:
        return jsonify({"status": "failed"}), 500
# Upload with IR end <----------------------------------------------------------------------------------------------////////

# NEW API FOR BULK UPLOAD
@app.route('/api_upload_bulk', methods=['POST'])
def api_upload_bulk():
    try:
        url = f"{domain}/upload_bulk_to_gdrive"
        
        print("Starting bulk upload.")
        file_list = request.files.getlist('bulk_file')
        uploaded_by = session.get('active_account')
    
        if not file_list:
            return jsonify({'error': 'No files provided'}), 400
        
        payload = {
            "uploaded_by": uploaded_by
        }

        files = [
            ("bulk_file", (f.filename, f, f.mimetype))
            for f in file_list
        ]
        
        headers = {
            'X-API-KEY': api_key
        }

        response = requests.post(url, data=payload, files=files, headers=headers)
        
        print(f"API Response: {response.status_code} - {response.text}")
        
        if response.status_code == 200: 
            return {"status": "success"}
        else:
            return {"status": "failed", "details": response.text}
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


# Send to gemini api start <----------------------------------------------------------------------------------------////////
def send_to_doc_ai(file):
    try:
        print("Gemini processing has started")
        api_url = "https://us-west1-pgc-dma-dev-sandbox.cloudfunctions.net/cash-non-cash-gemini-test"

        file.stream.seek(0)

        files = {
            'file': (
                secure_filename(file.filename),
                file.stream,
                file.content_type
            )
        }
        
        response = requests.post(api_url, files=files)

        if response.status_code == 200:
            print(f"Status Code: {response.status_code}")

            doc_type = response.json().get('document_type')

            print(doc_type)

            if doc_type in ("CASH","NON-CASH"):
                processing_status = "success"
            else:
                processing_status = "failed"
                
            print(f"Gemini processing is {processing_status}")
            return {"status": processing_status}
        else:
            print(f"Gemini processing failed with status {response.status_code}")
            return {"status": "failed"}

    except Exception as e:
        print(f"Send to DOC AI Error: {e}")
        return {"status":"failed","message":f"{str(e)}"}
# Send to gemini api end <--------------------------------------------------------------------------------------------////////


def get_master_data():
    try:
        StartDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        url = f"{domain}/get_app_master_data"

        headers = {
            'X-API-KEY': api_key,
            'Content-Type': 'application/json'
        }

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(
                f"Error getting master data: {response.status_code} - {response.text}")
            return False

        app_master_list = response.json()

        if not app_master_list:
            print("No application master data found.")
            return False

        master_details = app_master_list

        return master_details

    except Exception as e:
        return False

if __name__ == '__main__':
    app.run(debug=True)
>>>>>>> ae8a73ac24edc2d688ee7ffce237359f35b53f15
