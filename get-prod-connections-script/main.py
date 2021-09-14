'''
this is a credential-less POC and will not work until the secret variables on lines 8-9 are replaced with
proper secrets.

The purpose of this script is to allow those doing local development to pull connection strings from a production
environment into a locally running Astro environment
'''

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from cryptography.fernet import Fernet
from sqlalchemy import create_engine
from secrets import aws_sandbox_api_host, aws_sandbox_api_key, aws_sandbox_db_host, aws_sandbox_db_name, \
    aws_sandbox_db_username, aws_sandbox_db_password

class ConnectionTools:
    def __init__(self):
        self.host = aws_sandbox_api_host
        self.api_key = aws_sandbox_api_key
        self.db_host = aws_sandbox_db_host
        self.db_name = aws_sandbox_db_name
        self.db_username = aws_sandbox_db_username
        self.db_password = aws_sandbox_db_password

    def _upload_connections(self):
        engine = create_engine(f'postgresql://postgres:postgres@localhost/postgres')
        connection = engine.connect()
        df = self._export_connections()
        df.to_sql('connection', connection, if_exists='replace', index=False)

    def _export_connections(self):
        engine = create_engine(f'postgresql://{self.db_username}:{self.db_password}@{self.db_host}/{self.db_name}')
        connection = engine.connect()
        cursor = connection.execute('select * from airflow.connection')

        dictrows = [dict(row) for row in cursor]

        for r in dictrows: r['password'] = self._decrypt_password(r['password']) #decrypt password using production fernet
        for r in dictrows: r['password'] = self._encrypt_password(r['password']) #encrypt password using local fernet

        df = pd.DataFrame.from_records(dictrows)

        return df

    def _encrypt_password(self, password):
        key = Fernet(self._get_local_fernet_key())
        return (key.encrypt(bytes(password, encoding='utf8'))).decode('utf-8')

    def _decrypt_password(self, password):
        key = Fernet(self._get_external_fernet_key())
        return (key.decrypt(bytes(password, encoding='utf8'))).decode('utf-8')

    def _get_external_fernet_key(self):
        url = self.host + f'/airflow/api/v1/config'
        headers = {
            "cache-control": "no-cache",
            "content-type": "application/json",
            "accept": "application/json",
            "authorization": self.api_key
        }
        response = requests.get(url=url, headers=headers).json()
        for item in response['sections']:
            if item['name'] == 'core':
                options = item['options']
                for o in options:
                    if o['key'] == 'fernet_key':
                        try:
                            fernet_key = o['value']
                        except:
                            raise ValueError("fernet key doesn't exist")
        return fernet_key

    '''CONFIG NEEDS TO BE EXPOSED USING: AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True in .env'''
    def _get_local_fernet_key(self):
        url = 'http://localhost:8080/api/v1/config'
        headers = {
            "cache-control": "no-cache",
            "content-type": "application/json",
            "accept": "application/json"
        }
        response = requests.get(url=url, headers=headers, auth=HTTPBasicAuth('admin', 'admin')).json()
        for item in response['sections']:
            if item['name'] == 'core':
                options = item['options']
                for o in options:
                    if o['key'] == 'fernet_key':
                        try:
                            fernet_key = o['value']
                        except:
                            raise ValueError("fernet key doesn't exist")
        return fernet_key

if __name__ == '__main__':
    ConnectionTools()._upload_connections()

