import great_expectations as gx
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource, check_if_datasource_name_exists

context_new = gx.get_context()

"""
Creating a datasource for snowflake data using yaml file

Define all the credentials from the specific snowflake account

Save the datasource

"""


def snowflake_conn():
    datasource_name = "my_datasource_new"

    host = "ab12092.ap-southeast-1"  # The account name (include region -- ex 'ABCD.us-east-1')
    username = "SupriyaNeela"
    database = "assignment_db"  # The database name
    schema_name = "my_schema"  # The schema name
    warehouse = "assignment_wh"  # The warehouse name
    role = "admin"  # The role name
    table_name = "employees"  # A table that you would like to add initially as a Data Asset
    password = "Supriya@08"

    example_yaml = f"""
	name: {datasource_name}
	class_name: Datasource
	execution_engine:
	  class_name: SqlAlchemyExecutionEngine
	  credentials:
	    host: {host}
	    username: {username}
	    database: {database}
	    query:
	      schema: {schema_name}
	      warehouse: {warehouse}
	      role: {role}
	    password: {password}
	    drivername: snowflake
	data_connectors:
	  default_runtime_data_connector_name:
	    class_name: RuntimeDataConnector
	    batch_identifiers:
	      - default_identifier_name
	  default_inferred_data_connector_name:
	    class_name: InferredAssetSqlDataConnector
	    include_schema_name: True
	    introspection_directives:
	      schema_name: {schema_name}
	  default_configured_data_connector_name:
	    class_name: ConfiguredAssetSqlDataConnector
	    assets:
	      {table_name}:
	        class_name: Asset
	        schema_name: {schema_name}
	"""
    print(example_yaml)

    sanitize_yaml_and_save_datasource(context_new, example_yaml, overwrite_existing=False)
    context_new.list_datasources()

# print(snowflake_conn())
print(context_new.list_datasources())