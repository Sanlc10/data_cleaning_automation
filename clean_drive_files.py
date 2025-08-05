import logging
import os
import requests
import msal
import pandas as pd
from dotenv import load_dotenv
import atexit

# --- 1. Initial configuration ---
# Configure logging to reduce noise from production libraries.
logging.basicConfig(level=logging.INFO)
logging.getLogger("msal").setLevel(logging.WARNING)

# Load environment variables from a .env file.
load_dotenv()

# --- 2. DEFINING PATHS AND VARIABLES ---
# Gets the path to the directory where this script is located.
# This makes file paths work on any system (local, Render, etc.).
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_PATH = os.path.join(BASE_DIR, "downloads")
CACHE_FILE = os.path.join(BASE_DIR, "token_cache.bin")

# Environment variables for authentication.
CLIENT_ID = os.getenv("CLIENT_ID")
TENANT_ID = os.getenv("TENANT_ID")
USER_EMAIL = os.getenv("USER_EMAIL")

# --- 3. AUTHENTICATION WITH MSAL (Non-interactive flow for servers) ---
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["Files.ReadWrite", "User.Read"]

# Prepare the token cache to save the session between executions.
cache = msal.SerializableTokenCache()


def save_cache():
    if cache.has_state_changed:
        with open(CACHE_FILE, "w") as cache_file:
            cache_file.write(cache.serialize())


atexit.register(save_cache)  # Save the cache after the script ends.

# Load the cache if it already exists.
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as cache_file:
        cache.deserialize(cache_file.read())

# Create a request session to ensure non-interactive network behavior.
http_session = requests.Session()

# Initializes the MSAL client, passing it the cache and the request session.
app = msal.PublicClientApplication(
    CLIENT_ID,
    authority=AUTHORITY,
    token_cache=cache,
    http_client=http_session
)

# Attempts to silently obtain the access token using the cache.
result = None
accounts = app.get_accounts()
if accounts:
    print("✅ Account found in cache. Silently fetching token...")
    result = app.acquire_token_silent(scopes=SCOPE, account=accounts[0])
else:
    print("❌ ERROR: No account found in cache.")
    print("Make sure you have uploaded a 'token_cache.bin' file")
    exit()

if not result:
    print("❌ ERROR: Failed to retrieve access token. The refresh token may have expired.")
    print("SOLUTION: Run the script locally once to generate a new 'token_cache.bin' and update the Secret File in Render.")
    exit()

# Extract the token for use in API calls.
token = result["access_token"]
headers = {'Authorization': f'Bearer {token}'}
print("🔑 Authentication successful.")

# --- 4. MAIN LOGIC OF THE PROCESS ---
print("⚙️ Starting the file download and cleanup process...")

# Create the downloads folder if it doesn't exist.
if not os.path.exists(DOWNLOAD_PATH):
    os.makedirs(DOWNLOAD_PATH)

# Defines the information of the files to be processed.
files_to_process = [
    {
        "name": "file1.xlsx",
        "url": f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/root:/Smart_Ops_Lab_Vosyn/excel_1/file1.xlsx:/content"
    },
    {
        "name": "file2.xlsx",
        "url": f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/root:/Smart_Ops_Lab_Vosyn/excel_2/file2.xlsx:/content"
    },
    {
        "name": "file3.xlsx",
        "url": f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/root:/Smart_Ops_Lab_Vosyn/excel_3/file3.xlsx:/content"
    }
]

dataframes = []

# Loop to download each file and load it into a pandas DataFrame.
for file_info in files_to_process:
    file_name = file_info["name"]
    file_url = file_info["url"]
    print(f"📥 Descargando {file_name}...")

    response = requests.get(file_url, headers=headers)

    if response.status_code != 200:
        print(
            f"❌ Error downloading {file_name}: {response.status_code} - {response.text}")
        continue

    local_file_path = os.path.join(DOWNLOAD_PATH, file_name)
    with open(local_file_path, 'wb') as f:
        f.write(response.content)
    print(f"   -> Saved in {local_file_path}")

    # Reads the downloaded Excel file and adds it to the list of DataFrames.
    df = pd.read_excel(local_file_path)
    dataframes.append(df)

# Process files only if at least one has been downloaded.
if dataframes:
    print("📊 Merging downloaded files...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    print(f"   -> Combined DataFrame shape: {combined_df.shape}")

    print("🧼 Cleaning the data(droping duplicates and nulls)")
    cleaned_df = combined_df.drop_duplicates().dropna()
    print(f"   -> Clean DataFrame shape: {cleaned_df.shape}")

    # Save the clean file to your downloads folder.
    cleaned_file_name = "combined_cleaned.xlsx"
    cleaned_file_path = os.path.join(DOWNLOAD_PATH, cleaned_file_name)
    cleaned_df.to_excel(cleaned_file_path, index=False)
    print(f"💾 Clean file saved locally at: {cleaned_file_path}")

    # Upload the consolidated and cleaned file to OneDrive.
    print(f"📤 Uploading {cleaned_file_name} to OneDrive...")
    upload_url = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/root:/Smart_Ops_Lab_Vosyn/clean_excel/{cleaned_file_name}:/content"

    with open(cleaned_file_path, 'rb') as f:
        upload_response = requests.put(upload_url, headers=headers, data=f)

    if upload_response.status_code in [200, 201]:
        print("✅ Process completed successfully! File uploaded to OneDrive.")
    else:
        print(
            f"❌ Error uploading clean file: {upload_response.status_code} - {upload_response.text}")
else:
    print("⚠️ No files downloaded. There is no data to process.")
