# import great_expectations as ge
# from great_expectations.datasource import PandasDatasource
#
# # Define the path to your CSV file
# csv_file_path = "/Users/saisupriya/Desktop/OBE/gx_tutorials/data/yellow_tripdata_sample_2019-01.csv"
#
# # Create a data context
# context = ge.data_context.DataContext()
#
# # Define the data source
# context.add_datasource(
#     "my_csv_datasource",
#     class_name="PandasDatasource",
#     batch_kwargs={
#         "path": csv_file_path,
#         "header": True,  # Set to True if the CSV file has a header row
#         "delimiter": ","  # Set the delimiter character used in the CSV file
#     }
# )
#
# print(context.list_datasources())

import great_expectations as ge
import pandas as pd
from great_expectations.datasource import Datasource
from great_expectations.core.batch import BatchRequest
from great_expectations.dataset import PandasDataset

# Create a Pandas DataFrame
df = pd.read_csv("/Users/saisupriya/Desktop/OBE/gx_tutorials/data/yellow_tripdata_sample_2019-01.csv")

# Convert the DataFrame to a PandasDataset
dataset = PandasDataset(df)

# Create a Datasource object
datasource = Datasource(name="my_csv_datasource")

# Create a BatchRequest
batch_request = BatchRequest(
    datasource_name="my_csv_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="/Users/saisupriya/Desktop/OBE/gx_tutorials/data/yellow_tripdata_sample_2019-01.csv",
)

# Create a Batch from the BatchRequest
batch = datasource.get_batch_from_batch_request(dataset=dataset, batch_request=batch_request)





