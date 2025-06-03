# db_handler.py
import sqlalchemy
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from logger_setup import setup_logger
import config

logger = setup_logger(config.LOG_FILE)

# Global cache for schema
_schema_cache = None
_engine_cache = None

def get_engine():
    """Creates and returns a SQLAlchemy engine, caching it."""
    global _engine_cache
    if _engine_cache is None:
        try:
            db_url = f"postgresql+psycopg2://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
            _engine_cache = create_engine(db_url)
            # Test connection
            with _engine_cache.connect() as connection:
                logger.info(f"Successfully connected to database: {config.DB_NAME} on {config.DB_HOST}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            _engine_cache = None # Reset cache on failure
            raise
    return _engine_cache

def get_db_schema_info():
    """
    Retrieves and caches the database schema information (tables and columns).
    Returns a dictionary and a string representation for LLM.
    Example dict: {'users': ['id', 'name', 'email'], 'products': ['sku', 'product_name', 'price']}
    Example string:
    "Table users has columns: id, name, email.
     Table products has columns: sku, product_name, price."
    """
    global _schema_cache
    if _schema_cache is not None:
        return _schema_cache

    engine = get_engine()
    if not engine:
        return {}, ""

    inspector = inspect(engine)
    schema_dict = {}
    schema_string_parts = []

    try:
        for table_name in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns(table_name)]
            schema_dict[table_name] = columns
            schema_string_parts.append(f"Table {table_name} has columns: {', '.join(columns)}.")
        
        _schema_cache = (schema_dict, "\n".join(schema_string_parts))
        logger.info(f"Database schema loaded and cached. Tables: {list(schema_dict.keys())}")
        return _schema_cache
    except Exception as e:
        logger.error(f"Error retrieving database schema: {e}")
        return {}, ""


def execute_query(sql_query: str, params: dict = None):
    """
    Executes a SQL query against the database.
    Uses parameterized queries if params are provided.
    """
    engine = get_engine()
    if not engine:
        raise ConnectionError("Database engine not initialized.")
        
    try:
        with engine.connect() as connection:
            # For SELECT queries, we expect results
            if sql_query.strip().upper().startswith("SELECT"):
                result = connection.execute(text(sql_query), params or {})
                column_names = list(result.keys())
                rows = result.fetchall()
                logger.info(f"Executed query successfully. Rows returned: {len(rows)}")
                return column_names, rows
            else:
                # For other DML/DDL (though we aim to prevent them via validation)
                # We might not always want to allow this, but for completeness:
                # transaction = connection.begin()
                # connection.execute(text(sql_query), params or {})
                # transaction.commit()
                # logger.info(f"Executed non-SELECT query successfully.")
                # return [], [] # Or some success message
                # For this agent, we will strictly validate to allow only SELECTs
                raise ValueError("Only SELECT queries are allowed for execution by this agent.")

    except sqlalchemy.exc.SQLAlchemyError as e:
        logger.error(f"SQL Execution Error: {e}. Query: {sql_query}")
        raise  # Re-raise the specific SQLAlchemy error
    except Exception as e:
        logger.error(f"Unexpected error during query execution: {e}. Query: {sql_query}")
        raise


if __name__ == '__main__':
    # Test functions
    try:
        print("Fetching schema...")
        schema_d, schema_s = get_db_schema_info()
        if schema_d:
            print("Schema Dictionary:", schema_d)
            print("\nSchema String for LLM:\n", schema_s)
        
            # Example: Replace with a valid table and query for your DB
            # test_table = list(schema_d.keys())[0] if schema_d else None
            # if test_table:
            #     print(f"\nExecuting test query on table '{test_table}'...")
            #     cols, data = execute_query(f"SELECT * FROM {test_table} LIMIT 2;")
            #     print("Columns:", cols)
            #     print("Data:", data)
            # else:
            #     print("No tables found to test query execution.")

    except Exception as e:
        print(f"An error occurred: {e}")