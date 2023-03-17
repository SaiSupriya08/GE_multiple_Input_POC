import datetime

import pandas as pd

import great_expectations as gx
import great_expectations.jupyter_ux
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError

import snowflakeConnection
import csvConnection

from sys import argv

source_name = argv[1]
print("Source: " + source_name)

# Creating the data context
context = gx.get_context(
    context_root_dir="/Users/saisupriya/Desktop/OBE/gx_tutorials/great_expectations"
)

# Setting the expectation_suite
expectation_suite_name = "expectation_test"

# batch = snowflakeConnection.SnowflakeData()

# Calling batch request
if source_name == "snowflake":
    batch = snowflakeConnection.SnowflakeData()
elif source_name == "csv":
    batch = csvConnection.CsvData()
else:
    print("Source not found")

batch_request = batch.get_connection()
print(batch_request)

# batch2 = csvConnection.CsvData()
# batch_request2 = batch2.get_connection()
# print(batch_request2)

# Instantiating the validator
validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name
)

print(validator.head(n_rows=5, fetch_all=False))

# Creating the expectations
validator.expect_table_column_count_to_equal(13)
print(validator.expect_table_column_count_to_equal(13))

validator.expect_table_row_count_to_be_between(min_value=1, max_value=1000)

validator.expect_table_columns_to_match_ordered_list(column_list=["employee_id", "first_name", "last_name", "email",
                                                                  "phone_number", "hire_date", "job_id", "salary",
                                                                  "manager_id", "department_id", "elt_ts", "elt_by",
                                                                  "file_name"])

validator.expect_compound_columns_to_be_unique(column_list=["employee_id", "email", "phone_number"])

validator.expect_column_values_to_be_between(column="elt_ts", min_value="2023-02-05 04:21:14.674",
                                             max_value="2023-02-06 04:21:14.674")
print(validator.expect_column_values_to_be_between(column="elt_ts", min_value="2023-02-05 04:21:14.674",
                                                   max_value="2023-02-06 04:21:14.674"))

# from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
# profiler = UserConfigurableProfiler(profile_dataset=validator)


# suite = profiler.build_suite()


print(validator.get_expectation_suite(discard_failed_expectations=False))
validator.save_expectation_suite(discard_failed_expectations=False)

# checkpoint_config = {
#    "class_name": "SimpleCheckpoint",
#    "validations": [
#        {
#            "batch_request": batch_request,
#            "expectation_suite_name": expectation_suite_name
#        }
#    ]
# }
# checkpoint = SimpleCheckpoint(
#    f"{validator.active_batch_definition.data_asset_name}_{expectation_suite_name}",
#    context,
#    **checkpoint_config
# )
# checkpoint_result = checkpoint.run()
#
# context.build_data_docs()

# validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
# context.open_data_docs(resource_identifier=validation_result_identifier)
