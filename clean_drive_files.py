import logging
import os
import requests
import msal
import pandas as pd
from dotenv import load_dotenv
import atexit

# --- 1. CONFIGURACIÓN INICIAL Y DE ENTORNO ---
# Configura el logging para reducir el "ruido" de las librerías en producción.
logging.basicConfig(level=logging.INFO)
logging.getLogger("msal").setLevel(logging.WARNING)

# Carga las variables de entorno desde el archivo .env
load_dotenv()

# --- 2. DEFINICIÓN DE RUTAS Y VARIABLES ---
# Obtiene la ruta del directorio donde se encuentra este script.
# Esto hace que las rutas de los archivos funcionen en cualquier sistema (local, Render, etc.).
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_PATH = os.path.join(BASE_DIR, "downloads")
CACHE_FILE = os.path.join(BASE_DIR, "token_cache.bin")

# Carga las credenciales y configuraciones desde las variables de entorno.
CLIENT_ID = os.getenv("CLIENT_ID")
TENANT_ID = os.getenv("TENANT_ID")
USER_EMAIL = os.getenv("USER_EMAIL")  # Ej: "A830190951@my.uvm.edu.mx"

# --- 3. AUTENTICACIÓN CON MSAL (Flujo no interactivo para servidores) ---
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["Files.ReadWrite", "User.Read"]

# Prepara la caché de tokens para guardar la sesión entre ejecuciones.
cache = msal.SerializableTokenCache()


def save_cache():
    if cache.has_state_changed:
        with open(CACHE_FILE, "w") as cache_file:
            cache_file.write(cache.serialize())


atexit.register(save_cache)  # Guarda la caché al finalizar el script.

# Carga la caché si ya existe.
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as cache_file:
        cache.deserialize(cache_file.read())

# Crea una sesión de requests para asegurar un comportamiento de red no interactivo.
http_session = requests.Session()

# Inicializa el cliente de MSAL, pasándole la caché y la sesión de requests.
app = msal.PublicClientApplication(
    CLIENT_ID,
    authority=AUTHORITY,
    token_cache=cache,
    http_client=http_session
)

# Intenta obtener el token de acceso de forma silenciosa usando la caché.
result = None
accounts = app.get_accounts()
if accounts:
    print("✅ Cuenta encontrada en caché. Obteniendo token silenciosamente...")
    result = app.acquire_token_silent(scopes=SCOPE, account=accounts[0])
else:
    print("❌ ERROR: No se encontró ninguna cuenta en la caché.")
    print("Asegúrate de haber subido un archivo 'token_cache.bin' válido a Render.")
    exit()

if not result:
    print("❌ ERROR: No se pudo obtener token de acceso. El token de refresco pudo haber expirado.")
    print("SOLUCIÓN: Ejecuta el script localmente una vez para generar un nuevo 'token_cache.bin' y actualiza el Secret File en Render.")
    exit()

# Extrae el token para usarlo en las llamadas a la API.
token = result["access_token"]
headers = {'Authorization': f'Bearer {token}'}
print("🔑 Autenticación exitosa.")

# --- 4. LÓGICA PRINCIPAL DEL PROCESO ---
print("⚙️ Iniciando proceso de descarga y limpieza de archivos...")

# Crea la carpeta de descargas si no existe.
if not os.path.exists(DOWNLOAD_PATH):
    os.makedirs(DOWNLOAD_PATH)

# Define la información de los archivos a procesar.
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

# Bucle para descargar cada archivo y cargarlo en un DataFrame de pandas.
for file_info in files_to_process:
    file_name = file_info["name"]
    file_url = file_info["url"]
    print(f"📥 Descargando {file_name}...")

    response = requests.get(file_url, headers=headers)

    if response.status_code != 200:
        print(
            f"❌ Error al descargar {file_name}: {response.status_code} - {response.text}")
        continue

    local_file_path = os.path.join(DOWNLOAD_PATH, file_name)
    with open(local_file_path, 'wb') as f:
        f.write(response.content)
    print(f"   -> Guardado en {local_file_path}")

    # Lee el archivo Excel descargado y lo añade a la lista de DataFrames.
    df = pd.read_excel(local_file_path)
    dataframes.append(df)

# Procesa los archivos solo si se descargó al menos uno.
if dataframes:
    print("📊 Combinando archivos descargados...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    print(f"   -> Forma del DataFrame combinado: {combined_df.shape}")

    print("🧼 Limpiando datos (eliminando duplicados y nulos)...")
    cleaned_df = combined_df.drop_duplicates().dropna()
    print(f"   -> Forma del DataFrame limpio: {cleaned_df.shape}")

    # Guarda el archivo limpio en la carpeta de descargas.
    cleaned_file_name = "combined_cleaned.xlsx"
    cleaned_file_path = os.path.join(DOWNLOAD_PATH, cleaned_file_name)
    cleaned_df.to_excel(cleaned_file_path, index=False)
    print(f"💾 Archivo limpio guardado localmente en: {cleaned_file_path}")

    # Sube el archivo consolidado y limpio a OneDrive.
    print(f"📤 Subiendo {cleaned_file_name} a OneDrive...")
    upload_url = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/root:/Smart_Ops_Lab_Vosyn/clean_excel/{cleaned_file_name}:/content"

    with open(cleaned_file_path, 'rb') as f:
        upload_response = requests.put(upload_url, headers=headers, data=f)

    if upload_response.status_code in [200, 201]:
        print("✅ ¡Proceso completado exitosamente! Archivo subido a OneDrive.")
    else:
        print(
            f"❌ Error subiendo el archivo limpio: {upload_response.status_code} - {upload_response.text}")
else:
    print("⚠️ No se descargaron archivos. No hay datos para procesar.")
