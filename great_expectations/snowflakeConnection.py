from DataConnection import DataConnection

"""
Details required for creating batch request : 
datasource_name - datasource name created using the config parameters for specific data
data_connector_name - connector name used to create the datasource
data_asset_name - table or path name of the data

"""


# batch request for snowflake data
class SnowflakeData(DataConnection):
    # @snowflakeData - creating batch request for snowflake data connection
    @staticmethod
    def get_connection():
        # Creating batch request for snowflake connection
        batch_request1 = {
            "datasource_name": "my_datasource_new",
            "data_connector_name": "default_inferred_data_connector_name",
            "data_asset_name": "my_schema.employees",
            "limit": 1000,
        }
        return batch_request1
