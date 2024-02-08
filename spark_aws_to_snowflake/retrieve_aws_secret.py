#pip install boto3
#pip install aws-secretsmanager-caching

import botocore 
import botocore.session 
#from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 

client = botocore.session.get_session().create_client('secretsmanager')

secret=client.get_secret_value(SecretId="snowflake/capstone/config")

print(secret)