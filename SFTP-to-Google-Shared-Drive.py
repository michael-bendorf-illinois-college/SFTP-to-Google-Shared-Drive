import os
import glob
import zipfile
import csv
import logging
import mimetypes
import re
import shutil
import paramiko
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ------------------------------#
#       LOGGING SETUP           #
# ------------------------------#
# Create a new log file with a timestamp (format: log-yyyyMMddThhmmss.log)
log_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
LOG_FILENAME = f"log-{log_timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILENAME),
        logging.StreamHandler()
    ]
)

# ------------------------------#
#       CONFIGURATION           #
# ------------------------------#
# SFTP configuration
SFTP_HOST = "sftp.example.com"                     # SFTP server address
SFTP_PORT = 22                                     # SFTP port (default is 22)
SFTP_USERNAME = "your_sftp_username"               # SFTP username
SFTP_PASSWORD = "your_sftp_password"               # SFTP password (use with caution)
SFTP_REMOTE_PATH = "/remote/path"                  # Remote directory on the SFTP server
SFTP_FILENAME_MASK = r"datafile_\d{8}_\d{6}\.zip"  # Example pattern: datafile_YYYYMMDD_HHMMSS.zip
SFTP_DOWNLOAD_DIR = r"C:\path\to\sftp_download"    # Local temporary directory for SFTP download

# Local processing directories
# INPUT_DIRECTORY remains untouched in housekeeping (contains the script, service account JSON, logs, etc.)
INPUT_DIRECTORY = r"C:\path\where\this\lives"             # Local persistent directory where this script, service account json file and log files live
EXTRACTION_BASE_DIR = r"C:\path\to\your\temp_extraction"  # Local temporary directory for ZIP extraction
CONSOLIDATED_DIR = r"C:\path\to\consolidated"             # Local temporary directory for the master index CSV file

# Google Drive API configuration
# Note: The service account must have access to the Google Shared Drive folder.
SERVICE_ACCOUNT_FILE = os.path.join(INPUT_DIRECTORY, "your_service_account.json") # Create this in Google Cloud Console
SCOPES = ['https://www.googleapis.com/auth/drive']
DRIVE_FOLDER_ID = "your_drive_folder_id_here"       # Target folder ID on your Google Shared Drive

# ------------------------------#
#        SFTP FUNCTIONS         #
# ------------------------------#
def download_files_from_sftp(sftp_host, sftp_port, username, password, remote_path, local_dir, filename_mask):
    """
    Connects to the SFTP server via password-based authentication,
    finds all files matching the filename_mask in remote_path,
    downloads each to local_dir, and deletes the remote copies.
    Returns a list of downloaded local file paths.
    """
    downloaded_files = []
    try:
        logging.info(f"Connecting to SFTP server {sftp_host}...")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, port=sftp_port, username=username, password=password)
        sftp = ssh.open_sftp()
        logging.info(f"Connected. Listing files in remote directory {remote_path} ...")
        
        files = sftp.listdir(remote_path)
        matching_files = [f for f in files if re.fullmatch(filename_mask, f)]
        if not matching_files:
            logging.error("No files matching the given pattern were found on the SFTP server.")
            sftp.close()
            ssh.close()
            return downloaded_files
        
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        
        for filename in matching_files:
            remote_file_path = remote_path.rstrip("/") + "/" + filename
            local_file_path = os.path.join(local_dir, filename)
            logging.info(f"Downloading '{filename}' from {remote_file_path} to {local_file_path}...")
            try:
                sftp.get(remote_file_path, local_file_path)
                logging.info(f"Download of '{filename}' completed.")
                downloaded_files.append(local_file_path)
                try:
                    sftp.remove(remote_file_path)
                    logging.info(f"Deleted '{filename}' from remote server.")
                except Exception as delete_err:
                    logging.error(f"Error deleting remote file '{filename}': {delete_err}")
            except Exception as download_err:
                logging.error(f"Error downloading file '{filename}': {download_err}")

        sftp.close()
        ssh.close()
        return downloaded_files

    except Exception as e:
        logging.error(f"Error during SFTP download process: {e}")
        return downloaded_files

# ------------------------------#
#   GOOGLE DRIVE FUNCTIONS      #
# ------------------------------#
def init_drive_service(service_account_file, scopes):
    """
    Initializes the Google Drive API service using a service account.
    """
    credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
    service = build('drive', 'v3', credentials=credentials)
    logging.info("Google Drive service initialized.")
    return service

def upload_file_to_drive(local_file, drive_folder_id, drive_service):
    """
    Uploads a local file to the specified Google Drive folder.
    Returns the file ID if successful.
    """
    file_name = os.path.basename(local_file)
    file_metadata = {
        'name': file_name,
        'parents': [drive_folder_id]
    }
    mime_type, _ = mimetypes.guess_type(local_file)
    if mime_type is None:
        mime_type = 'application/octet-stream'
    media = MediaFileUpload(local_file, mimetype=mime_type)
    try:
        file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            supportsAllDrives=True,
            fields='id'
        ).execute()
        file_id = file.get('id')
        logging.info(f"Uploaded '{file_name}' to Google Drive with file ID: {file_id}")
        return file_id
    except Exception as e:
        logging.error(f"Error uploading '{file_name}' to Google Drive: {e}")
        return None

def upload_extracted_files(extraction_dir, drive_folder_id, drive_service):
    """
    Uploads all files in the extraction_dir to the Google Drive folder,
    skipping any file that is an index CSV (determined by its header containing "File name").
    """
    for file in os.listdir(extraction_dir):
        file_path = os.path.join(extraction_dir, file)
        if os.path.isfile(file_path):
            if file.lower().endswith('.csv'):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        first_line = f.readline()
                    if "File name" in first_line:
                        logging.info(f"Skipping index CSV file '{file}' for upload.")
                        continue
                except Exception as e:
                    logging.error(f"Error reading file '{file_path}': {e}")
            upload_file_to_drive(file_path, drive_folder_id, drive_service)
    logging.info(f"Finished uploading files from '{extraction_dir}'.")

# ------------------------------#
#     AGGREGATE INDEX CSV       #
# ------------------------------#
def aggregate_index_csv(master_csv_path, index_csv_paths):
    """
    Aggregates rows from each index CSV (with header containing "File name")
    into a single master CSV file using the first header encountered,
    then sorts the data rows (excluding the header) alphabetically by the columns:
    "Last", then "Preferred", then "File name".
    """
    master_header = None
    master_rows = []
    for idx_csv in index_csv_paths:
        try:
            with open(idx_csv, newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                header = next(reader)
                if master_header is None:
                    master_header = header
                    master_rows.append(header)
                elif header != master_header:
                    logging.warning(f"Header mismatch in {idx_csv}. Using master header.")
                for row in reader:
                    master_rows.append(row)
        except Exception as e:
            logging.error(f"Error reading index CSV '{idx_csv}': {e}")
    # Sort data rows (exclude header) by "Last", "Preferred", "File name"
    if len(master_rows) > 1:
        header = master_rows[0]
        try:
            last_idx = header.index("Last")
            preferred_idx = header.index("Preferred")
            file_name_idx = header.index("File name")
            data_rows = master_rows[1:]
            data_rows_sorted = sorted(
                data_rows,
                key=lambda r: (r[last_idx].lower(), r[preferred_idx].lower(), r[file_name_idx].lower())
            )
            master_rows = [header] + data_rows_sorted
        except Exception as e:
            logging.error(f"Error sorting master CSV rows: {e}")
    try:
        with open(master_csv_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            for row in master_rows:
                writer.writerow(row)
        logging.info(f"Aggregated and sorted master CSV created at '{master_csv_path}'.")
    except Exception as e:
        logging.error(f"Error writing master CSV '{master_csv_path}': {e}")

# ------------------------------#
#   ZIP ARCHIVE PROCESSING      #
# ------------------------------#
def process_zip_archive(zip_path, extract_dir):
    """
    Extracts the ZIP archive from zip_path into extract_dir, then locates the index CSV
    (a CSV whose header contains "File name") and renames additional files using the pattern:
        "Last, First - IC ID Number" (using "not admitted yet" if necessary).
    If a filename conflict occurs (i.e. the target name already exists), a sequence number
    is appended (e.g., " (1)", " (2)", etc.) until a unique filename is found.
    Returns the full path to the located index CSV.
    """
    index_csv_path = None
    try:
        logging.info(f"Processing ZIP archive: {zip_path}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        logging.info(f"Extraction completed to: {extract_dir}")

        index_csv_filename = None
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file in zip_ref.namelist():
                if file.lower().endswith('.csv'):
                    with zip_ref.open(file) as csvfile:
                        try:
                            header_line = csvfile.readline().decode('utf-8')
                        except Exception as e:
                            logging.error(f"Error reading header of '{file}': {e}")
                            continue
                        if "File name" in header_line:
                            index_csv_filename = file
                            break

        if not index_csv_filename:
            logging.error("Index CSV file with 'File name' header not found in the archive.")
        else:
            index_csv_path = os.path.join(extract_dir, index_csv_filename)
            logging.info(f"Found index CSV: {index_csv_path}")

            with open(index_csv_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    original_file = row.get("File name")
                    ic_id = row.get("IC ID Number")
                    first = row.get("Preferred")
                    last = row.get("Last")
                    if not (original_file and first and last):
                        logging.warning(f"Missing required data in row; skipping row: {row}")
                        continue
                    if not ic_id:
                        ic_id = "not admitted yet"
                    # Construct the new file name
                    base, ext = os.path.splitext(original_file)
                    new_filename = f"{last}, {first} - {ic_id}{ext}"
                    original_file_path = os.path.join(extract_dir, original_file)
                    candidate_path = os.path.join(extract_dir, new_filename)
                    counter = 1
                    # If the target file name already exists, append a sequence number
                    while os.path.exists(candidate_path):
                        candidate_filename = f"{last}, {first} - {ic_id} ({counter}){ext}"
                        candidate_path = os.path.join(extract_dir, candidate_filename)
                        counter += 1
                    new_file_path = candidate_path
                    if os.path.exists(original_file_path):
                        os.rename(original_file_path, new_file_path)
                        if counter > 1:
                            logging.info(f"Renamed '{original_file}' to '{os.path.basename(new_file_path)}' (sequence appended).")
                        else:
                            logging.info(f"Renamed '{original_file}' to '{os.path.basename(new_file_path)}'.")
                    else:
                        logging.error(f"File '{original_file}' not found in extracted contents.")
    except Exception as e:
        logging.error(f"Error processing archive '{zip_path}': {e}")
    return index_csv_path

def process_all_zip_archives(input_dir, extraction_base_dir, drive_folder_id, drive_service):
    """
    Processes all ZIP files in input_dir by:
      - Creating an extraction folder for each in extraction_base_dir.
      - Extracting and processing the ZIP to rename files.
      - Uploading non-index files from the extracted folder.
    Returns a list of all index CSV file paths found.
    """
    index_csv_list = []
    zip_files = glob.glob(os.path.join(input_dir, "*.zip"))
    if not zip_files:
        logging.error("No ZIP files found in the input directory.")
        return index_csv_list

    for zip_file in zip_files:
        zip_basename = os.path.splitext(os.path.basename(zip_file))[0]
        extraction_dir = os.path.join(extraction_base_dir, zip_basename)
        if not os.path.exists(extraction_dir):
            os.makedirs(extraction_dir)
        logging.info(f"Starting processing for: {zip_file}")
        idx_csv_path = process_zip_archive(zip_file, extraction_dir)
        if idx_csv_path:
            index_csv_list.append(idx_csv_path)
        upload_extracted_files(extraction_dir, drive_folder_id, drive_service)
        # Optionally, remove the extraction folder after successful upload:
        # shutil.rmtree(extraction_dir)
    return index_csv_list

# ------------------------------#
#       CLEANUP FUNCTIONS       #
# ------------------------------#
def cleanup_directory(directory):
    """
    Removes all files and subdirectories in the given directory.
    Log files (those starting with "log-" and ending with ".log") are preserved.
    """
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if filename.startswith("log-") and filename.endswith(".log"):
                logging.info(f"Preserving log file: {file_path}")
                continue
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                    logging.info(f"Deleted file: {file_path}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    logging.info(f"Deleted directory: {file_path}")
            except Exception as e:
                logging.error(f"Failed to delete {file_path}. Reason: {e}")

def cleanup_local_files():
    """
    Cleans up all local processing directories created during execution.
    This function completely deletes the SFTP_DOWNLOAD_DIR and EXTRACTION_BASE_DIR,
    and cleans out the CONSOLIDATED_DIR while leaving the INPUT_DIRECTORY untouched.
    """
    logging.info("Starting cleanup of local files and folders.")
    
    if os.path.exists(SFTP_DOWNLOAD_DIR):
        try:
            shutil.rmtree(SFTP_DOWNLOAD_DIR)
            logging.info(f"Deleted SFTP_DOWNLOAD_DIR: {SFTP_DOWNLOAD_DIR}")
        except Exception as e:
            logging.error(f"Failed to delete SFTP_DOWNLOAD_DIR {SFTP_DOWNLOAD_DIR}. Reason: {e}")
    
    if os.path.exists(EXTRACTION_BASE_DIR):
        try:
            shutil.rmtree(EXTRACTION_BASE_DIR)
            logging.info(f"Deleted EXTRACTION_BASE_DIR: {EXTRACTION_BASE_DIR}")
        except Exception as e:
            logging.error(f"Failed to delete EXTRACTION_BASE_DIR {EXTRACTION_BASE_DIR}. Reason: {e}")
    
    cleanup_directory(CONSOLIDATED_DIR)
    
    logging.info("Cleanup completed.")

# ------------------------------#
#             MAIN              #
# ------------------------------#
if __name__ == "__main__":
    # Step 1: Download all matching ZIP files from the SFTP server and delete them remotely.
    downloaded_files = download_files_from_sftp(
        SFTP_HOST, SFTP_PORT, SFTP_USERNAME, SFTP_PASSWORD,
        SFTP_REMOTE_PATH, SFTP_DOWNLOAD_DIR, SFTP_FILENAME_MASK
    )
    if not downloaded_files:
        logging.error("No files were downloaded from SFTP. Exiting.")
        exit(1)

    # Note: The downloaded ZIP files are processed directly from SFTP_DOWNLOAD_DIR.
    
    # Step 2: Initialize the Google Drive service.
    drive_service = init_drive_service(SERVICE_ACCOUNT_FILE, SCOPES)

    # Step 3: Process all ZIP archives in the SFTP_DOWNLOAD_DIR and upload files.
    index_csv_files = process_all_zip_archives(SFTP_DOWNLOAD_DIR, EXTRACTION_BASE_DIR, DRIVE_FOLDER_ID, drive_service)

    # Step 4: Aggregate all index CSV files into one master CSV file with a timestamp.
    if not os.path.exists(CONSOLIDATED_DIR):
        os.makedirs(CONSOLIDATED_DIR)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M")
    master_csv_filename = f"photos_uploaded-{timestamp}.csv"
    master_csv_path = os.path.join(CONSOLIDATED_DIR, master_csv_filename)
    aggregate_index_csv(master_csv_path, index_csv_files)

    # Step 5: Upload the master index CSV file to Google Drive.
    upload_file_to_drive(master_csv_path, DRIVE_FOLDER_ID, drive_service)

    logging.info("Process completed successfully.")

    # Step 6: Cleanup local processing directories (delete SFTP_DOWNLOAD_DIR and EXTRACTION_BASE_DIR completely,
    # and clean out CONSOLIDATED_DIR), leaving INPUT_DIRECTORY untouched.
    cleanup_local_files()
