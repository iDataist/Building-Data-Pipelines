from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id= '',
                 aws_credentials_id = '', 
                 table_name ='',
                 s3_bucket='',
                 s3_key='',
                 delimiter=',',
                 headers = 1, 
                 quote_char = '',
                 file_type = '',            
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.headers = headers
        self.quote_char = quote_char
        self.file_type = file_type
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Emptying stage table %s' % self.table_name)
        redshift.run('DELETE FROM %s' % self.table_name)
        
        s3_path = 's3://%s/%s' % (self.s3_bucket, self.s3_key)    
        copy_statement = """
                    COPY %s
                    FROM '%s'
                    access_key_id '%s'
                    secret_access_key '%s'
                    """ % (self.table_name, s3_path, credentials.access_key, credentials.secret_key)
        if self.table_name == 'staging_events': 
            file_statement = "json's3://udacity-dend/log_json_path.json';"
        if self.table_name == 'staging_songs':
            file_statement = "json'auto';"
        
        full_copy_statement = '%s %s' % (copy_statement, file_statement)
        self.log.info('Copying data from S3 to Redshift')
        redshift.run(full_copy_statement)
        self.log.info('Staging completed')
