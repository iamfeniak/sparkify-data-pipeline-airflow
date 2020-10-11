from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 dimension_table,
                 sql_statement,
                 mode="truncate-insert",
                 connection_id="redshift",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.dimension_table = dimension_table
        self.sql_statement = sql_statement
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(self.connection_id)
        if self.mode == "truncate-insert":
            self.log.info("Truncating dimension table {}".format(self.dimension_table))
            redshift.run("TRUNCATE {}".format(self.dimension_table))
            self.log.info("Inserting records into dimension table {}".format(self.dimension_table))
            redshift.run("INSERT INTO {} ({})".format(self.dimension_table, self.sql_statement))
        else:
            self.log.info("Appending records to dimension table {}".format(self.dimension_table))
            redshift.run("INSERT INTO {} ({})".format(self.dimension_table, self.sql_statement))

