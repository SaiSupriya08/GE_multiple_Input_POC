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


class Data:
    def __init__(self, source_name):
        self.source_name = source_name

    def get_batch_request(self):
        if self.source_name == "snowflake":
            batch = snowflakeConnection.SnowflakeData()
        elif self.source_name == "csv":
            batch = csvConnection.CsvData()
        else:
            print("Source not found")
        return batch.get_connection()


class Validator:
    def __init__(self, batch_request, expectation_suite_name):
        self.validator = context.get_validator(batch_request=BatchRequest(**batch_request),
                                               expectation_suite_name=expectation_suite_name)

    def validate(self):
        self.validator.expect_table_column_count_to_equal(13)
        self.validator.expect_table_row_count_to_be_between(min_value=1, max_value=1000)
        self.validator.expect_table_columns_to_match_ordered_list(column_list=["employee_id", "first_name",
                                                                               "last_name", "email", "phone_number",
                                                                               "hire_date", "job_id", "salary",
                                                                               "manager_id", "department_id",
                                                                               "elt_ts", "elt_by", "file_name"])
        self.validator.expect_compound_columns_to_be_unique(column_list=["employee_id", "email", "phone_number"])
        self.validator.expect_column_values_to_be_between(column="elt_ts", min_value="2023-02-05 04:21:14.674",
                                                          max_value="2023-02-06 04:21:14.674")

    def get_expectation_suite(self):
        return self.validator.get_expectation_suite(discard_failed_expectations=False)

    def save_expectation_suite(self):
        self.validator.save_expectation_suite(discard_failed_expectations=False)


if __name__ == '__main__':
    source_name = argv[1]
    print("Source: " + source_name)

    # Creating the data context
    context = gx.get_context(context_root_dir="/Users/saisupriya/Desktop/OBE/gx_tutorials/great_expectations")

    # Setting the expectation_suite
    expectation_suite_name = "expectation_test"

    data = Data(source_name=source_name)
    batch_request = data.get_batch_request()

    validator = Validator(batch_request=batch_request, expectation_suite_name=expectation_suite_name)
    validator.validate()

    print(validator.get_expectation_suite())
    validator.save_expectation_suite()