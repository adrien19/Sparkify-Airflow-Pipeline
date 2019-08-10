from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    facts_table_insert = ("""
        INSERT INTO {destination_facts_table} (
            {fields})
            {load_facts_sql}
    """)

    @apply_defaults
    def __init__(self,
                 # Defining operators params (with defaults)
                 table = '',
                 fields = '',
                 redshift_conn_id = '',
                 load_facts_sql = '',
                 append_data = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Mapping passed in params
        self.table = table
        self.fields = fields
        self.redshift_conn_id = redshift_conn_id
        self.load_facts_sql = load_facts_sql
        self.append_data = append_data

    def execute(self, context):
        self.log.info('Loading facts data into redshift facts_table')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if self.append_data == False:
            self.log.info('clearing data from Redshift table for new data')
            redshift.run("DELETE FROM {}".format(self.table))

        facts_table_insert = LoadFactOperator.facts_table_insert.format(
            destination_facts_table = self.table,
            fields = self.fields,
            load_facts_sql = self.load_facts_sql
        )

        redshift.run(facts_table_insert)
