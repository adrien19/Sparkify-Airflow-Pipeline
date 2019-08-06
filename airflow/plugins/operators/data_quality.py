from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Defining operators params (with defaults)
                 tables=[],
                 redshift_conn_id='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Mapping params to passed in values
        self.table = tables
        self.redshift_conn_id = redshift_conn_id
        self.load_dimension_sql = load_dimension_s

    def execute(self, context):
        self.log.info('Starting data quality check for {} in Redshift').format(self.table)
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for table in self.tables:

            records = redshift.get_records("SELECT COUNT(*) FROM {}").format(table)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. {} returned no results").format(table)
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError("Data quality check failed. {} contained 0 rows").format(table)
            logging.info("Data quality on table {} check passed with {} records").format(table, records[0][0])
