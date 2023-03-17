import unittest
from unittest.mock import patch
from ExpectationsLibrary import Validator


class TestValidator(unittest.TestCase):

    def setUp(self):
        self.validator_snowflake = Validator(source_name="snowflake", expectation_suite_name="expectation_test")
        self.validator_csv = Validator(source_name="csv", expectation_suite_name="expectation_test")
        self.validator_invalid = Validator(source_name="invalid", expectation_suite_name="expectation_test")

    def test_connect_snowflake(self):
        self.validator_snowflake.connect()
        self.assertIsNotNone(self.validator_snowflake.batch_request)

    def test_connect_csv(self):
        self.validator_csv.connect()
        self.assertIsNotNone(self.validator_csv.batch_request)

    def test_connect_invalid(self):
        with self.assertRaises(ValueError):
            self.validator_invalid.connect()

    @patch("great_expectations.data_context.DataContext.get_validator")
    def test_create_validator(self, mock_get_validator):
        self.validator_snowflake.batch_request = {"datasource": "snowflake", "table": "employees"}
        self.validator_snowflake.create_validator()
        mock_get_validator.assert_called_with(
            batch_request={"datasource": "snowflake", "table": "employees"},
            expectation_suite_name="expectation_test"
        )
        self.assertIsNotNone(self.validator_snowflake.validator)

    def test_create_validator_no_batch_request(self):
        with self.assertRaises(ValueError):
            self.validator_snowflake.create_validator()

    def test_set_expectations(self):
        self.validator_snowflake.batch_request = {"datasource": "snowflake", "table": "employees"}
        self.validator_snowflake.create_validator()
        self.validator_snowflake.set_expectations()
        # Add additional assertions here

    def test_set_expectations_no_validator(self):
        with self.assertRaises(ValueError):
            self.validator_snowflake.set_expectations()

    @patch("great_expectations.validator.validator.Validator.save_expectation_suite")
    def test_save_expectation_suite(self, mock_save_expectation_suite):
        self.validator_snowflake.batch_request = {"datasource": "snowflake", "table": "employees"}
        self.validator_snowflake.create_validator()
        self.validator_snowflake.set_expectations()
        self.validator_snowflake.save_expectation_suite()
        mock_save_expectation_suite.assert_called_with(discard_failed_expectations=False)

    def test_save_expectation_suite_no_validator(self):
        with self.assertRaises(ValueError):
            self.validator_snowflake.save_expectation_suite()


if __name__ == "__main__":
    unittest.main()

# import unittest
# import snowflakeConnection
# import csvConnection
#
#
# class TestCount(unittest.TestCase):
#     def test_connection_Snowflake(self):
#         print(snowflakeConnection.SnowflakeData.get_connection())
#         self.assertIsNotNone(snowflakeConnection.SnowflakeData.get_connection())
#
#     def test_connection_CSV(self):
#         print(csvConnection.CsvData.get_connection())
#         self.assertIsNotNone(csvConnection.CsvData.get_connection())
#
#
# if __name__ == '__main__':
#     unittest.main()
