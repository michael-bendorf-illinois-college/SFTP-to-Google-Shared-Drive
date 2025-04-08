# SFTP-to-Google-Shared-Drive
The script is designed to automate the ingestion, processing, and uploading of ZIP archives from an SFTP server into a Google Shared Drive. It also performs local housekeeping (cleanup) after processing.

Here’s a breakdown of what it does:

1. Logging Setup
Timestamped Log File:
At the very start, the script creates a new log file using the current date and time with the format log-yyyyMMddThhmmss.log. This ensures that each execution generates a fresh log for debugging and tracking purposes.

Logging Configuration:
The logging module is configured to output messages both to the created log file and to the console. This aids in real-time monitoring and later inspection of what happened during the run.

2. Configuration and Environment Setup
SFTP Credentials and Settings:
The script is configured with the SFTP server’s host, port, username, and password. It specifies a remote directory path and a regular expression (regex) mask (e.g., "datafile_\d{8}_\d{6}\.zip") to identify the ZIP files on the server. The files matching this mask are the ones to be processed.

Local Directories:
SFTP_DOWNLOAD_DIR: This is the local directory where the ZIP files downloaded from the SFTP server are stored temporarily.
EXTRACTION_BASE_DIR: This directory is used as a workspace for extracting the contents of each ZIP file into subfolders.
CONSOLIDATED_DIR: Here, the script stores the final aggregated “master index” CSV file.
INPUT_DIRECTORY: This directory holds static files, including the script itself, the service account JSON file, and log files.

Google Drive API Setup:
The script uses a service account for authentication with Google Drive. The service account JSON file is assumed to reside in the INPUT_DIRECTORY. The script is also configured with the necessary API scopes and the target Google Drive (or Shared Drive) folder ID where the files and master CSV will be uploaded.

3. SFTP File Download
Connecting to SFTP:
A function (download_files_from_sftp) establishes a connection to the SFTP server using Paramiko with the provided credentials.

Listing and Filtering Files:
Once connected, the script lists all files in the remote directory. It then uses the provided regex mask to filter the files to only those of interest (i.e., ZIP archives named according to a specific pattern).

Downloading and Deleting Remote Files:
For each matching file, the script downloads the file into the local SFTP_DOWNLOAD_DIR. After a successful download, it deletes the file from the SFTP server to ensure that they aren’t processed again in future runs.

4. Processing ZIP Archives
Extraction:
For each ZIP file, the script creates a corresponding subfolder within the EXTRACTION_BASE_DIR. The ZIP file is extracted entirely into its designated extraction folder.

Index CSV Identification:
Each ZIP file is expected to contain an index CSV file. The script scans through the names of files inside the ZIP and opens each CSV to read its header. It identifies the index CSV by detecting a header that contains the string "File name".

Renaming Additional Files Using CSV Data:
The index CSV contains several columns, including "File name", "IC ID Number", "Preferred", and "Last". For every row in the index CSV, the script constructs a new filename using the convention:
Last, First - IC ID Number.ext
If the "IC ID Number" column is empty, it defaults to the text "not admitted yet".
In case of a naming conflict (i.e., if a file with the new name already exists in the extraction directory), the script appends a sequence number (e.g., " (1)", " (2)", etc.) to the new filename to guarantee uniqueness. The actual renaming is performed on the extracted files in the extraction folder.

Uploading Non-CSV Files:
After processing the ZIP archive and renaming files, the function upload_extracted_files is called. It iterates over the extraction folder and uploads each file that is not an index CSV (it checks the CSV’s first line to determine if it’s the index file). Each file is uploaded using the Google Drive API.

Collecting Index CSV Paths:
The function returns the paths to all the index CSV files that were processed. These paths will later be used to aggregate the CSV data.

5. Aggregating the Index CSV Files
Aggregation of Data Rows:
The function aggregate_index_csv reads all individual index CSV files. It uses the header from the first CSV file encountered as the master header.

Sorting the Master Index:
After aggregation, the script sorts all the data rows (excluding the header) alphabetically using a three-level key:

Last – The surname.

Preferred – Typically the first name.

File name – The original file name.

Sorting is done in a case-insensitive manner. This sorted, aggregated CSV is then written to a new master file named with a timestamp in the format:
photos_uploaded-yyyyMMddThhmm.csv

6. Uploading to Google Drive
Uploading Extracted Files:
In parallel to processing individual ZIP files, the extracted non-index files are uploaded to the specified Google Drive folder using the service account credentials.

Uploading the Master Index CSV:
After the master CSV file is created and sorted, the script uploads it to the same Google Drive folder using the Google Drive API.

7. Cleanup Routine
Deleting Temporary Directories:
The cleanup routine is designed to remove all the local processing directories created during execution. Specifically, the entire SFTP_DOWNLOAD_DIR, EXTRACTION_BASE_DIR and CONSOLIDATED_DIR are deleted.

Important: The INPUT_DIRECTORY is intentionally left untouched because it contains static resources such as the script file, the service account JSON file, and log files.

Purpose of Cleanup:
This ensures that the environment is restored to its original state at the end of each run, preventing residual data from accumulating and keeping disk space clean.

8. End-to-End Flow Summary
Download Files:
The script connects to the SFTP server, filters files in the provided directory by name with a regex, downloads them into a temporary directory (SFTP_DOWNLOAD_DIR), and deletes them from the SFTP server.

Process ZIP Archives:
Each downloaded ZIP file is extracted in its own subfolder under EXTRACTION_BASE_DIR. The index CSV within each archive is identified, and its information is used to rename additional files. If duplicate names occur, a sequence number is appended to the file names.

Aggregate Index Data:
The data from all index CSV files is aggregated into one master CSV file. The rows are sorted alphabetically by “Last”, “Preferred”, and “File name” columns.

Upload to Google Drive:
The extracted non-index files and the master index CSV file are uploaded to a designated folder on Google Shared Drive using the Google Drive API and a service account.

Cleanup:
Temporary directories (SFTP_DOWNLOAD_DIR, EXTRACTION_BASE_DIR, and CONSOLIDATED_DIR) are completely removed, leaving the INPUT_DIRECTORY intact.

Final Notes
Error Handling and Logging:
Throughout the script, robust logging is used to track progress and errors for each step, making troubleshooting easier if something goes wrong during a run.

Modularity and Maintenance:
The script is divided into well-defined functions (for SFTP operations, Google Drive operations, processing ZIP files, aggregating CSV data, and cleanup) to make it modular and easier to maintain.

Scalability and Reusability:
Given its structure, this script can be extended or modified to work with other file types, additional processing logic, or different target destinations.
