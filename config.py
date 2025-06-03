# config.py
import os
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env file

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Ollama LLM Configuration
OLLAMA_BASE_URL = os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434')
OLLAMA_MODEL = os.getenv('OLLAMA_MODEL') # This will be checked for existence

# Logging Configuration
LOG_FILE = os.getenv('LOG_FILE', 'logs/sqlagent.log')

# Validate essential configurations
if not all([DB_USER, DB_PASSWORD, DB_NAME]):
    raise EnvironmentError("Missing essential database configuration (DB_USER, DB_PASSWORD, DB_NAME) in .env file.")

if not OLLAMA_MODEL: # Ensure an Ollama model is specified
    raise EnvironmentError("Missing OLLAMA_MODEL configuration in .env file. Please specify the Ollama model to use (e.g., mistral, codellama).")