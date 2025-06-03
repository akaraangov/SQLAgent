# sql_validator.py
import sqlparse
from logger_setup import setup_logger
import config

logger = setup_logger(config.LOG_FILE)

class SQLValidator:
    def __init__(self, db_schema_dict: dict):
        """
        Initializes the validator with the database schema.
        db_schema_dict: {'table_name': ['col1', 'col2'], ...}
        """
        self.schema = db_schema_dict
        if not self.schema:
            logger.warning("SQLValidator initialized with an empty schema. Validation will be limited.")

    def is_readonly_query(self, sql_query: str) -> bool:
        """Checks if the query is a SELECT statement."""
        parsed = sqlparse.parse(sql_query)
        if not parsed:
            return False
        
        # Check first statement type
        stmt_type = parsed[0].get_type()
        if stmt_type != 'SELECT':
            logger.warning(f"Validation failed: Query is not a SELECT statement. Type: {stmt_type}")
            return False
        
        # Ensure no other dangerous statements are hidden (e.g., via comments or multiple statements)
        # A simple check for multiple statements separated by ';'
        # More robust would involve checking all tokens in all statements.
        if len(parsed) > 1:
            logger.warning(f"Validation failed: Multiple SQL statements detected.")
            return False # Disallow multiple statements for now

        return True

    def _extract_identifiers(self, tokens):
        """Helper to recursively extract table and column identifiers from parsed SQL."""
        tables = set()
        columns = set() # We won't validate columns as rigorously as tables for simplicity here
                        # A full column validation needs to understand context (which table a column belongs to)

        for token in tokens:
            if isinstance(token, sqlparse.sql.Identifier):
                name = token.get_real_name().lower() # Normalize to lower for case-insensitivity if DB is like that
                # This is a simplified check. `get_real_name` works well for simple cases.
                # A more robust approach would be to check token.ttype and ancestors.
                # If it's part of a FROM or JOIN clause, it's likely a table.
                # If it's part of a SELECT list or WHERE clause, it could be a column or table.alias.
                
                # Crude way to guess if it's a table:
                # Check if the token's parent is a `FROM` or `JOIN` clause, or if it's a simple identifier list.
                # This is still not perfect for complex queries with subqueries or CTEs.
                
                # For simplicity, we'll add all identifiers and check if they are known tables.
                # Column validation is harder without full parsing context.
                tables.add(name) 

            elif isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    name = identifier.get_real_name().lower()
                    tables.add(name) # Again, could be columns too

            elif token.is_group: # Recurse into groups like parentheses, functions, etc.
                t_sub, c_sub = self._extract_identifiers(token.tokens)
                tables.update(t_sub)
                columns.update(c_sub)
        return tables, columns


    def validate_schema_references(self, sql_query: str) -> (bool, str):
        """
        Validates if tables referenced in the SQL query exist in the schema.
        This is a simplified validation. True robust validation requires a full SQL parser
        that understands context (aliases, subqueries, CTEs).
        """
        if not self.schema:
            logger.warning("Schema not available for validation. Skipping schema reference check.")
            return True, "Schema not available for validation." # Pass if no schema to check against

        parsed = sqlparse.parse(sql_query)
        if not parsed:
            return False, "Invalid SQL syntax (parsing failed)."

        statement = parsed[0] # We assume only one statement from is_readonly_query check
        
        # Extract table names. This is tricky with aliases, subqueries, CTEs.
        # sqlparse helps, but full semantic understanding is complex.
        # We'll try a basic approach.
        
        extracted_tables = set()
        from_seen = False
        tokens = statement.tokens

        for token in tokens:
            if token.is_keyword and token.normalized == 'FROM':
                from_seen = True
                continue
            if token.is_keyword and token.normalized in ('JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL OUTER JOIN'):
                from_seen = True # Reset from_seen effectively for the next table identifier
                continue
            
            if from_seen:
                if isinstance(token, sqlparse.sql.Identifier):
                    # This identifier could be a table name, or an alias if an AS clause follows
                    # For simplicity, we take the first part of a multipart identifier (e.g., schema.table)
                    real_name = token.get_real_name()
                    if '.' in real_name: # Handle schema.table
                        real_name = real_name.split('.')[-1]
                    extracted_tables.add(real_name.lower()) # Normalize for comparison
                    from_seen = False # Assume next identifier after FROM/JOIN is the table
                                      # This doesn't handle complex JOIN conditions or subqueries in FROM
                elif isinstance(token, sqlparse.sql.IdentifierList): # For multiple tables in FROM
                    for ident in token.get_identifiers():
                        real_name = ident.get_real_name()
                        if '.' in real_name:
                            real_name = real_name.split('.')[-1]
                        extracted_tables.add(real_name.lower())
                    from_seen = False


        # This table extraction is very basic. For a production system, a more robust parser is needed.
        # Example: sqlglot library is excellent for this.
        # For now, we proceed with this simplification.
        
        schema_table_names = {name.lower() for name in self.schema.keys()}

        for table_name_from_query in extracted_tables:
            if table_name_from_query not in schema_table_names:
                # Before failing, check if it's an alias from a CTE (Common Table Expression)
                # This is advanced and sqlparse alone might not easily reveal CTE definitions
                # For now, we assume no complex CTEs that would mask real table names in this simple check
                logger.warning(f"Validation failed: Table '{table_name_from_query}' not found in schema. Known tables: {schema_table_names}")
                return False, f"Invalid SQL: Table '{table_name_from_query}' does not exist in the database schema."
        
        # Column validation is more complex as columns are context-dependent (belong to a table).
        # We'll skip rigorous column validation for this version to keep it manageable.
        # A basic check could be: are all mentioned column names present in *any* of the used tables?
        # This is prone to false positives/negatives.

        logger.info(f"Schema reference validation passed for tables: {extracted_tables}")
        return True, "SQL schema references seem valid."


    def validate(self, sql_query: str) -> (bool, str):
        """
        Performs all validation checks.
        Returns (True, "Validated") if valid, or (False, "Error message") if not.
        """
        if not sql_query.strip():
            return False, "SQL query is empty."

        # 1. Check if it's a read-only query (SELECT)
        if not self.is_readonly_query(sql_query):
            return False, "Invalid SQL: Only SELECT queries are permitted."

        # 2. Check for dangerous patterns (very basic, can be expanded)
        # Example: prevent execution of functions that might modify system state or filesystem
        # This is hard to do perfectly without a very specific denylist/allowlist.
        # For now, rely on the SELECT-only check.

        # 3. Validate table and column references against the schema
        #    This is the most complex part.
        valid_schema, schema_msg = self.validate_schema_references(sql_query)
        if not valid_schema:
            return False, schema_msg
        
        logger.info(f"SQL Query validated successfully: {sql_query}")
        return True, "SQL Validated"


# Example Usage:
if __name__ == '__main__':
    mock_db_schema = {
        'users': ['user_id', 'username', 'email', 'age'],
        'products': ['product_id', 'name', 'price'],
        'orders': ['order_id', 'user_id', 'product_id', 'quantity']
    }
    validator = SQLValidator(mock_db_schema)

    # Test cases
    queries_to_test = [
        ("SELECT * FROM users", True),
        ("SELECT username, email FROM users WHERE age > 30", True),
        ("SELECT u.username, p.name FROM users u JOIN orders o ON u.user_id = o.user_id JOIN products p ON o.product_id = p.product_id", True),
        ("SELECT name FROM products WHERE price < 100", True),
        ("DROP TABLE users;", False), # DDL
        ("UPDATE users SET age = 30 WHERE user_id = 1;", False), # DML
        ("SELECT * FROM non_existent_table;", False), # Non-existent table
        ("SELECT non_existent_column FROM users;", True), # Column check is basic/skipped in this impl
        ("SELECT * FROM users; SELECT * FROM products;", False), # Multiple statements
        ("   ", False), # Empty query
    ]

    for query, expected_valid in queries_to_test:
        is_valid, msg = validator.validate(query)
        print(f"Query: \"{query}\"")
        print(f"Expected: {'Valid' if expected_valid else 'Invalid'}, Got: {'Valid' if is_valid else 'Invalid'} - Message: {msg}")
        print("-" * 20)