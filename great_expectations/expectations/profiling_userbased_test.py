import great_expectations as gx

context = gx.get_context(
    context_root_dir='path/to/my/context/root/directory/great_expectations'
)

expectation_suite_name = "expectation_test"

batch_request = {
    "datasource_name": "my_datasource_new",
    "data_connector_name": "default_inferred_data_connector_name",
    "data_asset_name": "my_schema.employees",
    "limit": 1000,
}

from great_expectations.core.batch import BatchRequest

validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name
)

validator.head(n_rows=5, fetch_all=False)

from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
profiler = UserConfigurableProfiler(profile_dataset=validator)


suite = profiler.build_suite()


