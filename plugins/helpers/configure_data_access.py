class ConfigureDataAccess():
    REGION = 'us-west-2'
    S3_BUCKET = 'udacity-dend'
    S3_LOG_KEY = 'log_data'
    DATA_FORMAT_EVENT= f"JSON 's3://{S3_BUCKET}/log_json_path.json'"
    DATA_FORMAT_SONG= "JSON 'auto'"
    S3_SONG_KEY = 'song_data'
    AWS_CREDENTIALS_ID = 'aws_credentials'
    REDSHIFT_CONN_ID = 'redshift'