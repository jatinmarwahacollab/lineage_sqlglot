import sqlglot
from sqlglot import expressions as exp
import re
import csv
import json
import logging
import snowflake.connector

# Configure logging for better debugging and visibility
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def extract_source_info_from_transformation(transformation):
    """
    Extract database, schema, table, and column information from a transformation string.
    """
    # This regex looks for patterns like "DATABASE.SCHEMA.TABLE".COLUMN or TABLE.COLUMN
    pattern = r'"?(\w+\.)?(\w+\.)?(\w+)"?\.(\w+)'
    match = re.search(pattern, transformation)

    if match:
        parts = match.groups()
        if len(parts) == 4:
            return {
                'database': parts[0].rstrip('.') if parts[0] else None,
                'schema': parts[1].rstrip('.') if parts[1] else None,
                'table': parts[2],
                'column': parts[3]
            }
        elif len(parts) == 3:
            return {
                'database': None,
                'schema': parts[0].rstrip('.') if parts[0] else None,
                'table': parts[1],
                'column': parts[2]
            }

    # If no match found, return all as None
    return {
        'database': None,
        'schema': None,
        'table': None,
        'column': None
    }

def trace_column_lineage(column_node, table, cte_definitions, table_alias_map, current_table, visited=None, depth=0, max_depth=10):
    if visited is None:
        visited = set()

    key = (table, column_node.sql())
    if key in visited:
        logging.warning(f"Already visited column '{column_node}' in table '{table}'. Skipping to prevent loops.")
        return {column_node.name}, table, column_node  # Avoid infinite loops

    if depth >= max_depth:
        logging.warning(f"Max recursion depth {max_depth} reached for column '{column_node}' in table '{table}'.")
        return {column_node.name}, table, column_node  # Return current state if max depth is reached

    # Add to visited
    visited.add(key)

    if table not in cte_definitions:
        # We've reached a base table; stop recursion
        return {column_node.name}, table, column_node

    cte_info = cte_definitions[table]
    column = column_node.name

    if column in cte_info:
        source_columns = cte_info[column]['source_columns']
        source_table = cte_info[column]['source_table']
        current_transformation = cte_info[column]['transformation']
    elif '*' in cte_info:
        source_columns = {column}
        source_table = cte_info['*']['source_table']
        current_transformation = column_node
    else:
        return {column_node.name}, table, column_node

    # If source_table is same as current table and we've already visited it, stop recursion
    if source_table == table and depth > 0:
        return source_columns, source_table, current_transformation

    def replace_columns(node, current_depth):
        if isinstance(node, exp.Column):
            src_col = node.name
            src_table = node.table or source_table
            column_with_table = exp.column(src_col, table=src_table)
            traced_columns, traced_table, traced_transformation = trace_column_lineage(
                column_with_table, src_table, cte_definitions, table_alias_map, current_table, visited.copy(),
                current_depth + 1, max_depth
            )
            return traced_transformation
        else:
            for arg_name, arg_value in node.args.items():
                if isinstance(arg_value, exp.Expression):
                    node.set(arg_name, replace_columns(arg_value, current_depth))
                elif isinstance(arg_value, list):
                    new_list = []
                    for item in arg_value:
                        if isinstance(item, exp.Expression):
                            new_item = replace_columns(item, current_depth)
                            new_list.append(new_item)
                        else:
                            new_list.append(item)
                    node.set(arg_name, new_list)
            return node

    # Apply replacement to the transformation with depth increment
    full_transformation = replace_columns(current_transformation, depth)

    final_columns = set()
    final_tables = set()
    for src_col in source_columns:
        src_col_node = exp.column(src_col, table=source_table)
        traced_columns, traced_table, traced_transformation = trace_column_lineage(
            src_col_node, source_table, cte_definitions, table_alias_map, current_table, visited.copy(),
            depth + 1, max_depth
        )
        final_columns.update(traced_columns)
        final_tables.add(traced_table)

    return final_columns, ', '.join(final_tables), full_transformation

def process_cte(cte, table_alias_map, cte_definitions):
    cte_name = cte.alias
    cte_columns = {}

    if isinstance(cte.this, exp.Select):
        from_clause = cte.this.args.get("from")
        source_table = "unknown"
        if from_clause:
            if isinstance(from_clause.this, exp.Table):
                source_table = from_clause.this.name
                if from_clause.this.db:
                    source_table = f"{from_clause.this.db}.{source_table}"
                if from_clause.this.catalog:
                    source_table = f"{from_clause.this.catalog}.{source_table}"
                # Map CTE name to itself
                table_alias_map[cte_name] = cte_name
            elif isinstance(from_clause.this, exp.Subquery):
                subquery_alias = from_clause.alias_or_name
                process_cte(from_clause, table_alias_map, cte_definitions)
                source_table = subquery_alias

        else:
            # If there's no FROM clause, source_table remains "unknown"
            table_alias_map[cte_name] = cte_name

        for select_expr in cte.this.expressions:
            if isinstance(select_expr, exp.Star):
                cte_columns['*'] = {
                    'source_columns': ['*'],
                    'transformation': select_expr,
                    'source_table': source_table
                }
            elif isinstance(select_expr, exp.Alias):
                alias_name = select_expr.alias
                source_columns, transformation, source_table_expr = extract_source_columns_and_transformation(
                    select_expr.this, table_alias_map, cte_definitions, current_table=source_table
                )
                cte_columns[alias_name] = {
                    'source_columns': source_columns,
                    'transformation': transformation,
                    'source_table': source_table_expr
                }
            elif isinstance(select_expr, exp.Column):
                column_name = select_expr.name
                # For direct columns from base table, set source_table directly
                cte_columns[column_name] = {
                    'source_columns': {column_name},
                    'transformation': select_expr,
                    'source_table': source_table
                }
            else:
                # For other expressions
                source_columns, transformation, source_table_expr = extract_source_columns_and_transformation(
                    select_expr, table_alias_map, cte_definitions, current_table=source_table
                )
                # We need to have an alias to store it in cte_columns
                alias_name = select_expr.alias_or_name
                if alias_name:
                    cte_columns[alias_name] = {
                        'source_columns': source_columns,
                        'transformation': transformation,
                        'source_table': source_table_expr
                    }

    cte_definitions[cte_name] = cte_columns

def extract_source_columns_and_transformation(node, table_alias_map, cte_definitions, current_table=None):
    if isinstance(node, exp.Column):
        source_column = node.name
        table_name = node.table if node.table else current_table
        if table_name is None:
            table_name = "unknown"
        real_table_name = table_alias_map.get(table_name, table_name)
        node.set("table", table_name)

        # Trace lineage to find the original source column and table
        source_columns, source_table, transformation = trace_column_lineage(
            node, real_table_name, cte_definitions, table_alias_map, current_table
        )
        return source_columns, transformation, source_table

    elif isinstance(node, exp.Identifier):
        return {node.name}, node, current_table or "unknown"

    elif isinstance(node, exp.Boolean):
        return set(), node, "constant"

    elif isinstance(node, exp.Literal):
        return set(), node, "constant"

    elif isinstance(node, exp.Paren):
        source_columns, transformation, source_table = extract_source_columns_and_transformation(
            node.this, table_alias_map, cte_definitions, current_table=current_table
        )
        return source_columns, exp.Paren(this=transformation), source_table

    elif isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Add, exp.Sub, exp.Mul, exp.Div)):
        # Handle binary operations recursively for both sides
        left_columns, left_transformation, left_table = extract_source_columns_and_transformation(
            node.left, table_alias_map, cte_definitions, current_table=current_table
        )
        right_columns, right_transformation, right_table = extract_source_columns_and_transformation(
            node.right, table_alias_map, cte_definitions, current_table=current_table
        )
        operator_node = type(node)(this=left_transformation, expression=right_transformation)
        source_tables = set(filter(lambda x: x not in ["unknown", "constant"], [left_table, right_table]))
        source_table = ', '.join(source_tables) if source_tables else current_table or "unknown"
        source_columns = left_columns.union(right_columns)
        return source_columns, operator_node, source_table

    elif isinstance(node, (exp.Count, exp.Sum, exp.Min, exp.Max, exp.Avg)):
        # Recursively handle the inner expression of the function
        source_columns = set()
        source_tables = set()
        expressions = []

        # Recursively extract source columns and transformations
        for arg in [node.this] + node.expressions:
            if arg:
                arg_columns, arg_transformation, arg_table = extract_source_columns_and_transformation(
                    arg, table_alias_map, cte_definitions, current_table=current_table
                )
                source_columns.update(arg_columns)
                source_tables.update(arg_table.split(', '))
                expressions.append(arg_transformation)

        # Create the function node with updated transformations
        func_node = type(node)(this=expressions[0] if expressions else None)

        # Set the distinct flag if applicable
        if node.args.get("distinct"):
            func_node.set("distinct", True)

        # Remove 'unknown' and 'constant' from source_tables
        source_tables.discard('unknown')
        source_tables.discard('constant')

        # Set the final source table
        source_table = ', '.join(source_tables) if source_tables else current_table or "unknown"

        return source_columns, func_node, source_table

    elif isinstance(node, exp.Case):
        # Recursively handle the case expression
        source_columns = set()
        source_tables = set()
        new_ifs = []
        for when_clause in node.args.get("ifs", []):
            cond_columns, cond_transformation, cond_table = extract_source_columns_and_transformation(
                when_clause.this, table_alias_map, cte_definitions, current_table=current_table
            )
            true_columns, true_transformation, true_table = extract_source_columns_and_transformation(
                when_clause.args['true'], table_alias_map, cte_definitions, current_table=current_table
            )
            source_columns.update(cond_columns)
            source_columns.update(true_columns)
            source_tables.update(cond_table.split(', '))
            source_tables.update(true_table.split(', '))
            new_when_clause = exp.When(this=cond_transformation, expression=true_transformation)
            new_ifs.append(new_when_clause)

        else_expr = node.args.get('default')
        if else_expr:
            else_columns, else_transformation, else_table = extract_source_columns_and_transformation(
                else_expr, table_alias_map, cte_definitions, current_table=current_table
            )
            source_columns.update(else_columns)
            source_tables.update(else_table.split(', '))
            new_default = else_transformation
        else:
            new_default = None

        case_node = exp.Case(ifs=new_ifs, default=new_default)
        source_tables.discard('unknown')
        source_tables.discard('constant')
        source_table = ', '.join(source_tables) if source_tables else current_table or "unknown"
        return source_columns, case_node, source_table

    elif isinstance(node, exp.Cast):
        source_columns, inner_transformation, source_table = extract_source_columns_and_transformation(
            node.this, table_alias_map, cte_definitions, current_table=current_table
        )
        cast_type = node.args.get('to')
        cast_node = exp.Cast(this=inner_transformation, to=cast_type)
        return source_columns, cast_node, source_table

    elif isinstance(node, exp.Coalesce):
        source_columns = set()
        source_tables = set()
        expressions = []
        for arg in [node.this] + node.expressions:
            if arg:
                arg_columns, arg_transformation, arg_table = extract_source_columns_and_transformation(
                    arg, table_alias_map, cte_definitions, current_table=current_table
                )
                source_columns.update(arg_columns)
                source_tables.update(arg_table.split(', '))
                expressions.append(arg_transformation)

        coalesce_node = exp.Coalesce(expressions=expressions)
        source_tables.discard('unknown')
        source_tables.discard('constant')
        source_table = ', '.join(source_tables) if source_tables else current_table or "unknown"
        return source_columns, coalesce_node, source_table

    elif isinstance(node, exp.TimestampTrunc):
        source_columns = set()
        source_tables = set()

        # Handle the timestamp to be truncated (`this` argument)
        if node.this:
            arg_columns, arg_transformation, arg_table = extract_source_columns_and_transformation(
                node.this, table_alias_map, cte_definitions, current_table=current_table
            )
            source_columns.update(arg_columns)
            source_tables.update(arg_table.split(', '))

        # The truncation unit (`unit` argument) does not need tracing, it is a constant
        trunc_unit = node.args.get("unit")

        # Handle the optional time zone argument (`zone`)
        trunc_zone = None
        if "zone" in node.args and node.args["zone"]:
            trunc_zone = node.args["zone"]

        # Create the new `TimestampTrunc` node
        timestamp_trunc_node = exp.TimestampTrunc(
            this=arg_transformation,
            unit=trunc_unit,
            zone=trunc_zone
        )

        # Remove 'unknown' or 'constant' entries from the source tables
        source_tables.discard("unknown")
        source_tables.discard("constant")

        # Set the final source table from the collected tables
        source_table = ', '.join(source_tables) if source_tables else current_table or "unknown"
        return source_columns, timestamp_trunc_node, source_table

    else:
        return set(), node, current_table or "unknown"

def process_with_and_select(ast):
    table_alias_map = {}
    cte_definitions = {}
    final_columns = []

    # Process CTEs
    with_clause = ast.args.get("with")
    if with_clause:
        for cte in with_clause.expressions:
            process_cte(cte, table_alias_map, cte_definitions)

    # Process main SELECT
    if isinstance(ast, exp.Select):
        from_clause = ast.args.get("from")
        if from_clause and isinstance(from_clause.this, exp.Table):
            main_table = from_clause.this.name
            table_alias_map[main_table] = main_table
        elif from_clause and isinstance(from_clause.this, exp.Subquery):
            subquery_alias = from_clause.alias_or_name
            process_cte(from_clause, table_alias_map, cte_definitions)
            main_table = subquery_alias
        else:
            main_table = "unknown"

        current_table = main_table

        for select_expr in ast.expressions:
            if isinstance(select_expr, exp.Star):
                if main_table in cte_definitions:
                    for col, info in cte_definitions[main_table].items():
                        try:
                            source_columns, source_table, transformation = trace_column_lineage(
                                exp.to_column(col), main_table, cte_definitions, table_alias_map, current_table)
                            transformation_sql = transformation.sql(dialect='snowflake') if transformation else ''
                            actual_column_name = ', '.join(source_columns)
                            source_info = extract_source_info(source_table)
                            final_columns.append({
                                "Final Column": col,
                                "Source Database": source_info['database'],
                                "Source Schema": source_info['schema'],
                                "Source Table": source_info['table'],
                                "Source Columns": actual_column_name,
                                "Transformation": transformation_sql
                            })
                        except Exception as e:
                            logging.error(f"Error tracing column '{col}': {e}")
                            final_columns.append({
                                "Final Column": col,
                                "Source Database": "Unknown",
                                "Source Schema": "Unknown",
                                "Source Table": "Unknown",
                                "Source Columns": "Unknown",
                                "Transformation": "Error tracing column"
                            })
                else:
                    final_columns.append({
                        "Final Column": "*",
                        "Source Database": "Unknown",
                        "Source Schema": "Unknown",
                        "Source Table": main_table,
                        "Source Columns": "Select all columns",
                        "Transformation": "Select all columns"
                    })
            elif isinstance(select_expr, exp.Alias):
                alias_name = select_expr.alias
                try:
                    source_columns, transformation, source_table_expr = extract_source_columns_and_transformation(
                        select_expr.this, table_alias_map, cte_definitions, current_table=current_table
                    )
                    transformation_sql = transformation.sql(dialect='snowflake') if transformation else ''
                    actual_column_name = ', '.join(source_columns)
                    source_info = extract_source_info(source_table_expr)
                    final_columns.append({
                        "Final Column": alias_name,
                        "Source Database": source_info['database'],
                        "Source Schema": source_info['schema'],
                        "Source Table": source_info['table'],
                        "Source Columns": actual_column_name,
                        "Transformation": transformation_sql
                    })
                except Exception as e:
                    logging.error(f"Error tracing alias '{alias_name}': {e}")
                    final_columns.append({
                        "Final Column": alias_name,
                        "Source Database": "Unknown",
                        "Source Schema": "Unknown",
                        "Source Table": "Unknown",
                        "Source Columns": "Unknown",
                        "Transformation": "Error tracing alias"
                    })
            elif isinstance(select_expr, exp.Alias) or isinstance(select_expr, exp.Column):
                try:
                    source_columns, transformation, source_table_expr = extract_source_columns_and_transformation(
                        select_expr.this if isinstance(select_expr, exp.Alias) else select_expr,
                        table_alias_map,
                        cte_definitions,
                        current_table=current_table
                    )
                    transformation_sql = transformation.sql(dialect='snowflake') if transformation else ''
                    actual_column_name = ', '.join(source_columns)

                    # Extract source info from the transformation string
                    source_info = extract_source_info_from_transformation(transformation_sql)

                    final_columns.append({
                        "Final Column": select_expr.alias if isinstance(select_expr, exp.Alias) else select_expr.name,
                        "Source Database": source_info['database'] or "Unknown",
                        "Source Schema": source_info['schema'] or "Unknown",
                        "Source Table": source_info['table'] or source_table_expr,
                        "Source Columns": source_info['column'] or actual_column_name,
                        "Transformation": transformation_sql
                    })
                except Exception as e:
                    logging.error(f"Error tracing expression: {e}")
                    final_columns.append({
                        "Final Column": select_expr.alias if isinstance(select_expr, exp.Alias) else select_expr.name,
                        "Source Database": "Unknown",
                        "Source Schema": "Unknown",
                        "Source Table": "Unknown",
                        "Source Columns": "Unknown",
                        "Transformation": "Error tracing expression"
                    })

    return final_columns

def display_lineage(final_columns):
    """
    Display the lineage details in a structured format.
    """
    if not final_columns:
        logging.info("No lineage data to display.")
        return

    print("\nFinal columns and their source information:")
    for col in final_columns:
        print(f"Final Column: {col.get('Final Column', 'Unknown')}")
        print(f"Source Database: {col.get('Source Database', 'Unknown')}")
        print(f"Source Schema: {col.get('Source Schema', 'Unknown')}")
        print(f"Source Table: {col.get('Source Table', 'Unknown')}")
        print(f"Source Columns: {col.get('Source Columns', 'Unknown')}")
        print(f"Transformation: {col.get('Transformation', 'Unknown')}\n")

def main():
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('user'),
        password=os.getenv('password'),
        account=os.getenv('account'),
        warehouse=os.getenv('warehouse'),
        database=os.getenv('database'),
        schema=os.getenv('schema'),
        role=os.getenv('role')
    )

    cursor = conn.cursor()

    # Fetch distinct DATABASE, SCHEMA, TABLE_NAME, REFERENCE, EXPANDED_SQL
    cursor.execute("""
        SELECT DISTINCT DATABASE, SCHEMA, TABLE_NAME, REFERENCE, EXPANDED_SQL
        FROM JAFFLE_LINEAGE.LINEAGE_DATA.TABLE_SCHEMA_REF
        WHERE EXPANDED_SQL IS NOT NULL
    """)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    # Truncate table
    truncate_sql = """
        TRUNCATE TABLE JAFFLE_LINEAGE.LINEAGE_DATA.COLUMN_LINEAGE_SQLGLOT
    """
    try:
        cursor.execute(truncate_sql)
    except Exception as e:
        logging.error(f"Error truncating records into COLUMN_LINEAGE_SQLGLOT: {e}")

    for row in rows:
        input_record = dict(zip(columns, row))
        sql_query = input_record['EXPANDED_SQL']

        print("\nProcessing SQL Query:\n")
        try:
            ast = sqlglot.parse_one(sql_query, read='snowflake')
            logging.info("SQL parsed successfully.\n")
        except Exception as e:
            logging.error(f"Error parsing SQL: {e}")
            continue  # Skip this SQL query and move to the next

        final_columns = process_with_and_select(ast)

        # Map and insert the results into COLUMN_LINEAGE_SQLGLOT
        for col in final_columns:
            output_record = {
                'DATABASE_NAME': input_record['DATABASE'],
                'SCHEMA_NAME': input_record['SCHEMA'],
                'TABLE_NAME': input_record['TABLE_NAME'],
                'REFERENCE': input_record['REFERENCE'],
                'EXPANDED_SQL': sql_query,
                'FINAL_COLUMN': col.get('Final Column', 'Unknown'),
                'SOURCE_TABLE': col.get('Source Table', 'Unknown'),
                'SOURCE_DATABASE': col.get('Source Database', 'Unknown'),
                'SOURCE_SCHEMA': col.get('Source Schema', 'Unknown'),
                'SOURCE_COLUMNS': col.get('Source Columns', 'Unknown'),
                'TRANSFORMATION': col.get('Transformation', 'Unknown')
            }
            

            # Prepare INSERT statement
            insert_sql = """
                INSERT INTO JAFFLE_LINEAGE.LINEAGE_DATA.COLUMN_LINEAGE_SQLGLOT
                (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, REFERENCE, EXPANDED_SQL, FINAL_COLUMN, SOURCE_TABLE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_COLUMNS, TRANSFORMATION)
                VALUES (%(DATABASE_NAME)s, %(SCHEMA_NAME)s, %(TABLE_NAME)s, %(REFERENCE)s, %(EXPANDED_SQL)s, %(FINAL_COLUMN)s, %(SOURCE_TABLE)s, %(SOURCE_DATABASE)s, %(SOURCE_SCHEMA)s, %(SOURCE_COLUMNS)s, %(TRANSFORMATION)s)
            """
            try:
                cursor.execute(insert_sql, output_record)
            except Exception as e:
                logging.error(f"Error inserting record into COLUMN_LINEAGE_SQLGLOT: {e}")
                logging.error(f"Record data: {json.dumps(output_record)}")

    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
