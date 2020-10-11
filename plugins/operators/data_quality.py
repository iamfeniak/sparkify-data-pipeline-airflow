from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 connection_id='',
                 data_sources=None,
                 test_case_statements=None,
                 test_case_expected_outcomes=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.data_sources = data_sources
        self.test_case_statements = test_case_statements
        self.test_case_expected_outcomes = test_case_expected_outcomes

    def execute(self, context):
        redshift_hook = PostgresHook(self.connection_id)
        for data_source in self.data_sources:
            self.log.info("Running data quality checks on table {}".format(data_source))
            for test_case, expected_result in zip(self.test_case_statements, self.test_case_expected_outcomes):
                sql_query = test_case.format(data_source)
                actual_result = redshift_hook.get_records(sql_query)
                if (len(actual_result) < 1) or (actual_result[0][0] < expected_result):
                    self.log.error("Test case {} failed on table {}".format(sql_query, data_source))
                    self.log.error("Expected result was: {} . Actual result is: {} .".format(
                        expected_result, actual_result))
                    raise ValueError("Test case {} failed on table {}".format(test_case, data_source))
            self.log.info("Successfully finished running data quality checks on table {}".format(data_source))
