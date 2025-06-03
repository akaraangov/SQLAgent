# app.py
import streamlit as st
import pandas as pd

# --- Early Imports (non-Streamlit affecting) ---
import config # Ensures config is loaded early
from logger_setup import setup_logger
from db_handler import get_db_schema_info, execute_query
from nl_to_sql import NLToSQLConverter
from sql_validator import SQLValidator

# --- Page Configuration (MUST BE THE FIRST STREAMLIT COMMAND) ---
st.set_page_config(page_title="Local SQL Agent", layout="wide")

# --- Global Logger ---
logger = setup_logger(config.LOG_FILE)

# --- Initialization Functions (modified to return errors, not call st.error directly) ---
@st.cache_resource # Cache resource for entire session
def initialize_app_resources():
    """Initializes all necessary resources and returns them, along with any error message."""
    schema_dict, schema_str, nl_converter, sql_validator_instance = None, None, None, None
    initialization_error = None

    try:
        logger.info("Attempting to load database schema...")
        schema_dict, schema_str = get_db_schema_info()
        if not schema_dict:
            initialization_error = "Failed to load database schema. Please check DB connection and logs."
            logger.error(f"Streamlit App: {initialization_error}")
            return schema_dict, schema_str, nl_converter, sql_validator_instance, initialization_error
        logger.info("Database schema loaded successfully.")

        logger.info("Attempting to initialize NLToSQLConverter...")
        nl_converter = NLToSQLConverter()
        logger.info("NLToSQLConverter initialized successfully.")

        logger.info("Attempting to initialize SQLValidator...")
        sql_validator_instance = SQLValidator(schema_dict)
        logger.info("SQLValidator initialized successfully.")

    except Exception as e:
        initialization_error = f"Critical error during app initialization: {e}"
        logger.error(f"Streamlit App: {initialization_error}", exc_info=True)
    
    return schema_dict, schema_str, nl_converter, sql_validator_instance, initialization_error

# --- Load Resources and Handle Initialization Errors ---
db_schema_dict, db_schema_str, nl_converter, sql_validator, app_init_error = initialize_app_resources()

if app_init_error:
    st.error(f"Application Initialization Failed: {app_init_error}")
    st.stop() # Stop further execution of the app if critical components failed

if not db_schema_dict or not nl_converter or not sql_validator:
    # This case should ideally be caught by app_init_error, but as a safeguard:
    st.error("One or more critical components (schema, NL converter, SQL validator) failed to initialize properly. The application cannot proceed.")
    logger.error("Streamlit App: Critical components are None after initialization attempt without specific error message.")
    st.stop()

# --- Streamlit UI ---
st.title("🗣️ Local SQL Agent 🤖")
st.caption("Convert natural language to SQL, validate, and execute against your PostgreSQL database.")

# Session state for holding query, SQL, results, errors
if 'nl_query' not in st.session_state:
    st.session_state.nl_query = ""
if 'generated_sql' not in st.session_state:
    st.session_state.generated_sql = ""
if 'query_results' not in st.session_state:
    st.session_state.query_results = None # Will be (columns, data) tuple
if 'error_message' not in st.session_state:
    st.session_state.error_message = ""

# --- Input Area ---
with st.form("nl_query_form"):
    nl_query_input = st.text_area("Enter your natural language query:",
                                  value=st.session_state.nl_query,
                                  height=100,
                                  key="nl_input_area")
    submit_button = st.form_submit_button("✨ Generate & Execute SQL")

if submit_button and nl_query_input:
    st.session_state.nl_query = nl_query_input
    st.session_state.generated_sql = ""
    st.session_state.query_results = None
    st.session_state.error_message = "" # Clear previous errors on new submission
    logger.info(f"UI: Received NL Query: '{st.session_state.nl_query}'")

    # 1. NL to SQL
    with st.spinner("Translating to SQL..."):
        try:
            st.session_state.generated_sql = nl_converter.translate(st.session_state.nl_query, db_schema_str)
            logger.info(f"UI: Generated SQL: '{st.session_state.generated_sql}'")
        except ValueError as ve: # Specific error from nl_to_sql for missing API key etc.
             st.session_state.error_message = f"Translation Error: {ve}"
             logger.error(f"UI: NL-to-SQL ValueError: {ve}")
        except ConnectionError as ce: # Specific error for OpenAI API issues
             st.session_state.error_message = f"Translation Service Error: {ce}"
             logger.error(f"UI: NL-to-SQL ConnectionError: {ce}")
        except Exception as e:
            st.session_state.error_message = "Unable to generate SQL. Please refine your question or check LLM configuration."
            logger.error(f"UI: NL-to-SQL failed: {e}", exc_info=True)

    # 2. Display Generated SQL (if successful) and Validate
    if st.session_state.generated_sql and not st.session_state.error_message:
        st.subheader("Generated SQL:")
        st.code(st.session_state.generated_sql, language="sql")

        # 3. Validate SQL
        with st.spinner("Validating SQL..."):
            try:
                is_valid, validation_msg = sql_validator.validate(st.session_state.generated_sql)
                if not is_valid:
                    st.session_state.error_message = f"Invalid SQL: {validation_msg}"
                    logger.warning(f"UI: SQL Validation Failed: {validation_msg} for SQL: {st.session_state.generated_sql}")
            except Exception as e:
                st.session_state.error_message = "Error during SQL validation. Please check the query."
                logger.error(f"UI: SQL Validation unexpected error: {e} for SQL: {st.session_state.generated_sql}", exc_info=True)

        # 4. Execute SQL (if valid)
        if not st.session_state.error_message: # If still no errors after validation
            with st.spinner("Executing SQL..."):
                try:
                    cols, data = execute_query(st.session_state.generated_sql)
                    st.session_state.query_results = (cols, data)
                    logger.info(f"UI: SQL Execution successful for: {st.session_state.generated_sql}")
                except Exception as e: # Catch SQLAlchemy errors or others from execute_query
                    st.session_state.error_message = f"SQL Execution Failed: {e}"
                    logger.error(f"UI: SQL Execution Failed: {e} for SQL: {st.session_state.generated_sql}", exc_info=True)
    
    # Force a rerun to update displays based on new session state
    # This needs to be conditional to avoid rerun loops if there was an input but processing failed early
    if nl_query_input: # Ensure there was an action to process
        #st.experimental_rerun()
        st.rerun()


# --- Display Area ---
if st.session_state.error_message:
    st.error(st.session_state.error_message)

# This condition helps display the SQL if it was generated but an error occurred afterwards (e.g., validation/execution)
if st.session_state.generated_sql and not st.session_state.query_results and st.session_state.error_message:
    st.subheader("Generated SQL (Execution/Validation Failed):")
    st.code(st.session_state.generated_sql, language="sql")


if st.session_state.query_results:
    st.subheader("Query Results:")
    cols, data = st.session_state.query_results
    if data:
        df = pd.DataFrame(data, columns=cols)
        st.dataframe(df)
    else:
        st.success("Query executed successfully, but no rows were returned.")

# --- Schema Display (Optional) ---
with st.expander("View Current Database Schema"):
    if db_schema_str: # Check if schema string was successfully loaded
        st.text(db_schema_str)
    else:
        st.warning("Database schema could not be displayed (it might not have been loaded successfully).")

logger.info("Streamlit app view updated/reloaded.")