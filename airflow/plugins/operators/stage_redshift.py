from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_from_s3_sql = """
        COPY {}
        FROM {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Defining operator params (with defaults)
                 table='',
                 redshift_conn_id='',
                 aws_credentials_id='',
                 s3_key='',
                 s3_bucket='',
                 delimiter='',
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Mapping params to passed in values
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.delimiter=delimiter
        self.ignore_headers= ignore_headers

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info('Making connection to redshift')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('clearing data from Redshift table for new data')
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('Starting to copy data from s3 to redshift')
        rendered_data_source_key = self.s3_key.format(**context)
        data_source_s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_data_source_key)

        copy_from_s3_sql = StageToRedshiftOperator.copy_from_s3_sql.format(
            self.table,
            data_source_s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter
        )

        redshift.run(copy_from_s3_sql)
