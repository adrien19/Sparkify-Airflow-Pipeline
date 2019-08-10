from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    dimensions_table_insert = ("""
        INSERT INTO {destination_table} (
            {fields}
            )
            {load_dimension}
    """)

    @apply_defaults
    def __init__(self,
                 # Defining operators params (with defaults)
                 table='',
                 fields='',
                 redshift_conn_id='',
                 load_dimension='',
                 append_data = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Mapping params to passed in values
        self.table = table
        self.fields = fields
        self.redshift_conn_id = redshift_conn_id
        self.load_dimension = load_dimension
        self.append_data = append_data

    def execute(self, context):
        self.log.info(f'Loading {self.table} dimensions in redshift')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)



        if self.append_data == False:
            self.log.info('clearing data from Redshift table for new data')
            redshift.run("DELETE FROM {}".format(self.table))

        dimensions_table_insert = LoadDimensionOperator.dimensions_table_insert.format(
            destination_table = self.table,
            fields = self.fields,
            load_dimension = self.load_dimension
        )

        redshift.run(dimensions_table_insert)
