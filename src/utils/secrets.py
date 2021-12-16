
import boto3
import json
import utils.env as env

def load_secrets():
  secret_name = env.get_required('SECRET_NAME')
  region_name = env.get_required('AWS_REGION')

  client = boto3.client(
      service_name='secretsmanager',
      region_name=region_name
  )

  get_secret_value_response = client.get_secret_value(
      SecretId=secret_name
  )

  secrets = json.loads(get_secret_value_response['SecretString'])

  for key in secrets:
      env.set_env(key, secrets[key])
