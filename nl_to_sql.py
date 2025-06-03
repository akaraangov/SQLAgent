# nl_to_sql.py
import requests
import json
import config
from logger_setup import setup_logger

logger = setup_logger(config.LOG_FILE)

class NLToSQLConverter:
    def __init__(self, ollama_base_url=None, ollama_model=None):
        self.ollama_base_url = ollama_base_url or config.OLLAMA_BASE_URL
        self.ollama_model = ollama_model or config.OLLAMA_MODEL

        if not self.ollama_base_url or not self.ollama_model:
            msg = "Ollama base URL or model not configured. NL-to-SQL functionality will be disabled."
            logger.error(msg)
            # This should ideally be caught by config.py during startup,
            # but good to have a safeguard in the class itself.
            raise ValueError(msg)
        
        logger.info(f"NLToSQLConverter initialized to use Ollama: URL='{self.ollama_base_url}', Model='{self.ollama_model}'")
        self._test_ollama_connection()

    def _test_ollama_connection(self):
        """Tests basic connectivity to the Ollama API."""
        try:
            # A simple way to test is to hit the base URL or a tags endpoint
            response = requests.get(f"{self.ollama_base_url}/api/tags", timeout=5) # /api/tags lists local models
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            # Optionally, check if the configured model exists
            models_data = response.json()
            model_found = any(m.get('name','').startswith(self.ollama_model) for m in models_data.get('models',[]))
            if not model_found:
                logger.warning(f"Configured Ollama model '{self.ollama_model}' not found in local Ollama models. Ensure it's pulled. Available models: {[m.get('name') for m in models_data.get('models',[])]}")
                # Depending on strictness, you might raise an error here.
                # For now, we'll just log a warning and proceed.
            
            logger.info(f"Successfully connected to Ollama API at {self.ollama_base_url}.")

        except requests.exceptions.RequestException as e:
            error_msg = (f"Failed to connect to Ollama API at {self.ollama_base_url}. "
                         f"Ensure Ollama is running and the OLLAMA_BASE_URL is correct. Error: {e}")
            logger.error(error_msg)
            raise ConnectionError(error_msg) # This will halt app init if it happens in constructor


    def translate(self, nl_query: str, db_schema_str: str) -> str:
        """
        Translates natural language query to SQL using the configured Ollama model.
        """
        if not db_schema_str:
            logger.error("Database schema is empty. Cannot provide context to LLM.")
            raise ValueError("Database schema is required for NL-to-SQL conversion.")

        # Construct the prompt. This format generally works well.
        # You might need to adjust it based on the specific Ollama model's preferences.
        full_prompt = f"""
Given the following PostgreSQL database schema:
--- SCHEMA START ---
{db_schema_str}
--- SCHEMA END ---

Convert the following natural language query into a valid PostgreSQL SQL query.
Your response MUST be ONLY the SQL query itself, with no explanations, comments, or surrounding text.
Ensure the query is safe and only retrieves data (e.g., use SELECT statements).
Do not use any DML (INSERT, UPDATE, DELETE) or DDL (CREATE, ALTER, DROP) statements.

Natural Language Query: "{nl_query}"

SQL Query:
"""
        # Ollama API endpoint for generation
        ollama_api_url = f"{self.ollama_base_url}/api/generate"

        payload = {
            "model": self.ollama_model,
            "prompt": full_prompt,
            "stream": False,  # Get the full response at once
            "options": {      # Optional: model-specific parameters
                "temperature": 0.1, # Low temperature for more deterministic SQL
                "num_predict": 300, # Max tokens for the SQL query output, adjust as needed
                # "stop": ["\n\n", "---"] # Sequences that will stop generation
            }
        }

        try:
            logger.info(f"Sending NL query to Ollama model '{self.ollama_model}': '{nl_query}'")
            logger.debug(f"Full prompt being sent to Ollama:\n{full_prompt}")

            # Increased timeout as local LLMs can be slower
            response = requests.post(ollama_api_url, json=payload, timeout=90)
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

            response_data = response.json()
            logger.debug(f"Raw Ollama response object: {response_data}")

            if "response" not in response_data or not response_data["response"]:
                error_detail = response_data.get("error", "No 'response' field or empty response.")
                logger.error(f"Ollama response issue: {error_detail}")
                raise ValueError(f"LLM (Ollama) returned an empty or malformed response: {error_detail}")

            sql_query = response_data["response"].strip()
            
            # --- Start of robust cleanup for SQL query ---
            # Remove common LLM preamble/postamble and markdown
            # 1. Remove SQL markdown blocks
            if sql_query.startswith("```sql"):
                sql_query = sql_query[len("```sql"):].strip()
            if sql_query.endswith("```"):
                sql_query = sql_query[:-len("```")].strip()
            
            # 2. Remove leading/trailing quotes if the whole query is quoted
            if sql_query.startswith("'") and sql_query.endswith("'"):
                 sql_query = sql_query[1:-1]
            if sql_query.startswith('"') and sql_query.endswith('"'):
                 sql_query = sql_query[1:-1]

            # 3. If the query still contains "SELECT" (case-insensitive),
            #    try to extract from the first "SELECT" to the end,
            #    or up to known "explanation" markers.
            select_kw_upper = "SELECT "
            sql_upper = sql_query.upper()
            select_idx = sql_upper.find(select_kw_upper)

            if select_idx != -1:
                # Take from the actual case "SELECT " found
                actual_select_idx = sql_query.lower().find("select ")
                if actual_select_idx != -1:
                    sql_query = sql_query[actual_select_idx:]
                else: # Fallback if only uppercase was found (unlikely but safe)
                    sql_query = sql_query[select_idx:]


                # Remove common post-SQL chatter if any.
                # This is heuristic. The prompt should prevent this.
                explanation_markers = [
                    "this query will", "the query above", "explanation:", 
                    "here is the sql", "note:", "```" # A trailing backtick after already stripping sql block
                ]
                for marker in explanation_markers:
                    marker_idx = sql_query.lower().find(marker)
                    if marker_idx != -1:
                        sql_query = sql_query[:marker_idx].strip()
            
            # 4. Ensure it ends with a semicolon if it's a SELECT statement and doesn't have one
            if sql_query.upper().strip().startswith("SELECT") and not sql_query.strip().endswith(";"):
                sql_query = sql_query.strip() + ";"
            # --- End of robust cleanup ---

            logger.info(f"Ollama generated SQL (after cleanup): {sql_query}")
            if not sql_query or not sql_query.upper().strip().startswith("SELECT"):
                logger.warning(f"LLM (Ollama) returned an empty or non-SELECT SQL query after processing: '{sql_query}'. Original NL: '{nl_query}'")
                raise ValueError("LLM (Ollama) returned an empty or non-SELECT query after processing.")
            return sql_query
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout connecting to Ollama API at {ollama_api_url}. Model might be too slow or stuck.")
            raise ConnectionError(f"Timeout: Ollama API at {ollama_api_url} did not respond in time.")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error with Ollama API at {ollama_api_url}. Details: {e}")
            raise ConnectionError(f"Connection error: Could not connect to Ollama API at {ollama_api_url}. Ensure Ollama is running. Details: {e}")
        except requests.exceptions.HTTPError as e:
            error_body = e.response.text
            logger.error(f"HTTP error from Ollama API: {e.response.status_code} - {error_body}")
            raise ConnectionError(f"Ollama API HTTP error: {e.response.status_code}. Detail: {error_body[:200]}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON response from Ollama. Response: {response.text[:500]}... Error: {e}")
            raise ValueError(f"Ollama API returned non-JSON or malformed JSON. Error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during NL-to-SQL translation with Ollama: {e}", exc_info=True)
            raise

# Example usage for direct testing (python nl_to_sql.py)
if __name__ == '__main__':
    if not config.OLLAMA_MODEL:
        print("Skipping NL-to-SQL (Ollama) test: OLLAMA_MODEL not set in .env")
    else:
        print(f"Attempting to use Ollama model: {config.OLLAMA_MODEL} at {config.OLLAMA_BASE_URL}")
        try:
            converter = NLToSQLConverter()
            mock_schema = """
            Table employees has columns: id, name, department, salary, hire_date.
            Table departments has columns: id, name, location.
            """
            nl_queries = [
                "Show me all employees in the Engineering department",
                "List names of employees and their department locations for those hired after 2020",
                "What is the average salary in the Sales department?"
            ]
            for nl_q in nl_queries:
                print(f"\n--- Testing NL: {nl_q} ---")
                try:
                    sql = converter.translate(nl_q, mock_schema)
                    print(f"Generated SQL: {sql}")
                except Exception as e:
                    print(f"Error during translation: {e}")
        except Exception as e:
            print(f"Error initializing converter or during test: {e}")