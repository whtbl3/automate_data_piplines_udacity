from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 sql_insert_stmt="",
                 table_name="", 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_insert_stmt = sql_insert_stmt
        self.table_name = table_name
        
    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info(f"Load data to fact table {self.table_name}")
        postgres.run(f"INSERT INTO {self.table_name} {self.sql_insert_stmt}")