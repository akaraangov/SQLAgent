# cli.py
import argparse
import sys
import pandas as pd

import config # Ensures config is loaded early
from logger_setup import setup_logger
from db_handler import get_db_schema_info, execute_query
from nl_to_sql import NLToSQLConverter
from sql_validator import SQLValidator

logger = setup_logger(config.LOG_FILE)

def main():
    parser = argparse.ArgumentParser(description="Local SQL Agent CLI")
    parser.add_argument("nl_query", type=str, help="Natural language query to process.")
    args = parser.parse_args()

    logger.info(f"CLI: Received NL Query: '{args.nl_query}'")

    try:
        # 1. Load Schema
        logger.info("CLI: Loading database schema...")
        db_schema_dict, db_schema_str = get_db_schema_info()
        if not db_schema_dict:
            print("Error: Could not load database schema. Check connection and logs.")
            logger.error("CLI: Failed to load database schema.")
            sys.exit(1)
        # print(f"DEBUG: Schema Loaded: {list(db_schema_dict.keys())}") # For debugging

    except Exception as e:
        print(f"Error initializing database connection or schema: {e}")
        logger.critical(f"CLI: Critical error during schema init: {e}", exc_info=True)
        sys.exit(1)

    # 2. Initialize components
    try:
        nl_converter = NLToSQLConverter()
        sql_validator = SQLValidator(db_schema_dict)
    except Exception as e:
        print(f"Error initializing core components: {e}")
        logger.critical(f"CLI: Failed to initialize NLConverter or SQLValidator: {e}", exc_info=True)
        sys.exit(1)

    generated_sql = ""
    try:
        # 3. NL to SQL
        print("Translating to SQL...")
        generated_sql = nl_converter.translate(args.nl_query, db_schema_str)
        logger.info(f"CLI: Generated SQL: '{generated_sql}'")
        print(f"\nGenerated SQL:\n{generated_sql}\n")

    except ValueError as ve:
         print(f"Translation Error: {ve}")
         logger.error(f"CLI: NL-to-SQL ValueError: {ve}")
         sys.exit(1)
    except ConnectionError as ce:
         print(f"Translation Service Error: {ce}")
         logger.error(f"CLI: NL-to-SQL ConnectionError: {ce}")
         sys.exit(1)
    except Exception as e:
        print("Error: Unable to generate SQL. Please refine your question or check LLM configuration.")
        logger.error(f"CLI: NL-to-SQL failed: {e}", exc_info=True)
        sys.exit(1)

    # 4. Validate SQL
    print("Validating SQL...")
    try:
        is_valid, validation_msg = sql_validator.validate(generated_sql)
        if not is_valid:
            print(f"Error: Invalid SQL. {validation_msg}")
            logger.warning(f"CLI: SQL Validation Failed: {validation_msg} for SQL: {generated_sql}")
            sys.exit(1)
        print("SQL Validated successfully.")
    except Exception as e:
        print("Error: Error during SQL validation.")
        logger.error(f"CLI: SQL Validation unexpected error: {e} for SQL: {generated_sql}", exc_info=True)
        sys.exit(1)

    # 5. Execute SQL
    print("\nExecuting SQL...")
    try:
        cols, data = execute_query(generated_sql)
        logger.info(f"CLI: SQL Execution successful for: {generated_sql}")
        
        if data:
            df = pd.DataFrame(data, columns=cols)
            print("\nQuery Results:")
            print(df.to_string(index=False)) # Pretty print for CLI
        else:
            print("\nQuery executed successfully, but no rows were returned.")
            
    except Exception as e:
        print(f"Error: SQL Execution Failed: {e}")
        logger.error(f"CLI: SQL Execution Failed: {e} for SQL: {generated_sql}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()