from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 insert_sql_stmt="",
                 table_name="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.insert_sql_stmt = insert_sql_stmt
        self.table_name = table_name
        self.truncate = truncate

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        if self.truncate:
            self.log.info(f"Truncate dimension table {self.table_name}")
            postgres.run(f"TRUNCATE TABLE {self.table_name};")
            
        self.log.info(f"Load data to dimension table {self.table_name}")
        postgres.run(f"INSERT INTO {self.table_name} {self.insert_sql_stmt};")