from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 fact_table,
                 sql_statement,
                 connection_id="redshift",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.fact_table = fact_table
        self.sql_statement = sql_statement

    def execute(self, context):
        redshift = PostgresHook(self.connection_id)
        self.log.info("Appending records to fact table {}".format(self.fact_table))
        redshift.run("INSERT INTO {} ({})".format(self.fact_table, self.sql_statement))
