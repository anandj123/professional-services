# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START gae_python37_bigquery]
import concurrent.futures

import flask
from google.cloud import bigquery
import re, time, datetime, configparser

config = configparser.ConfigParser()
config.read('config.ini')
app = flask.Flask(__name__)
bigquery_client = bigquery.Client(project=config['BQ_SETUP']['PROJECT_ID'])


@app.route("/")
def main():
    print(config['BQ_SETUP']['PROJECT_ID'])
    bq_create_dataset()
    bq_create_table()
    return flask.render_template_string('output : <br> {{ what|safe }}', what=query_auditlog())

def bq_create_dataset():
    dataset_ref = bigquery_client.dataset(config['BQ_SETUP']['DATASET_ID'])

    try:
        bigquery_client.get_dataset(dataset_ref)
    except:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))

def bq_create_table():
    dataset_ref = bigquery_client.dataset(config['BQ_SETUP']['DATASET_ID'])

    # Prepares a reference to the table
    table_ref = dataset_ref.table(config['BQ_SETUP']['TABLE_ID'])

    try:
        bigquery_client.get_table(table_ref)
    except:
        schema = [
            bigquery.SchemaField('job_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('hash', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('timestamp', 'TIMESTAMP', mode='REQUIRED'),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))
def get_hash(query):
    tokens = re.findall(r"[\w']+", query)
    otokens = []
    
    for token in tokens:
        if (token.startswith("'") |
            token.startswith('"')):
            continue
        else:
            otokens.append(token)
        #print(token + " : " + out)
    #print("Input : " + ' '.join(tokens))
    #print("Output : " + ' '.join(otokens))
    #rint('************************************')
    return hash(' '.join(otokens))
def query_auditlog():
    returnvalue = ''
    query_job = bigquery_client.query("""
SELECT 
      a.timestamp, 
      protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatus.state , 
      protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobName.jobId,  
      protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobConfiguration.query.query 
FROM `{}.{}.cloudaudit_googleapis_com_data_access_*` a
LEFT outer join `{}.{}.{}` b
ON protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobName.jobId = b.job_id 
WHERE protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatus.state = 'DONE' 
AND b.timestamp IS NULL
ORDER by a.timestamp
        """.format(
            config['BQ_SETUP']['PROJECT_ID'],
            config['BQ_SETUP']['DATASET_ID'],
            config['BQ_SETUP']['PROJECT_ID'],
            config['BQ_SETUP']['DATASET_ID'],
            config['BQ_SETUP']['TABLE_ID']))

    results = query_job.result()  # Waits for job to complete.
    rows_list = []
    for row in results:
        returnvalue += "timestamp : {} ------ jobId :{}    <br/> ".format(row.timestamp, row.jobId)
        print("timestamp : {} ------ jobId :{} ".format(row.timestamp, row.jobId))
        get_hash(row.query)
        insert_row = (
            row.jobId, 
            get_hash(row.query), 
            row.timestamp)
        rows_list.append(insert_row)
    if (len(rows_list) > 0):
        # Prepares a reference to the dataset
        dataset_ref = bigquery_client.dataset(config['BQ_SETUP']['DATASET_ID'])
        table_ref = dataset_ref.table(config['BQ_SETUP']['TABLE_ID'])
        table = bigquery_client.get_table(table_ref)  # API call
        errors = bigquery_client.insert_rows(table, rows_list)  # API request
        print(errors)
        assert errors == []
    return returnvalue
if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)



    # Todo
    #   AppEngine
    #   Scheduler for App Engine
    #   Deployment (Terraform)
    #   Deduplication of jobId
    # Done
    #   Tokenize query
    #   Config file
    #   Best effort Dedupe
    #
    # [END gae_python37_bigquery]