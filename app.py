from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
import uuid
from functions import save_file_info_to_bq, get_drive_service, is_valid_api_key, gemini_processing, BQ_CLIENT_READER, BQ_READER_CREDENTIALS, BQ_CLIENT_WRITER
from googleapiclient.http import MediaFileUpload
from google.cloud import bigquery

load_dotenv()

app = Flask(__name__)

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

@app.route('/get_app_master_data', methods=['POST'])
def get_app_master_data():
    try:
        print("Entering get_app_master_data endpoint.")
        
        api_key = request.headers.get('X-API-Key')
        if not api_key or not is_valid_api_key(api_key):
            return jsonify({"status": "error",
                            "message": "Unauthorized. Invalid API key."}), 401
        
        data = request.get_json()

        # user = request.args.get('user')

        print(f"User: {data}")

        user = data.get('user')

        if not user:
            return jsonify({"status": "error",
                            "message": "Missing required parameter: user"}), 400

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
                WHERE rn = 1
                and master.uploaded_by = @user
                order by cte.date_uploaded desc;


        """
        

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user", "STRING", user)
            ]
        )

        query_job = BQ_CLIENT_WRITER.query(query, job_config=job_config)
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