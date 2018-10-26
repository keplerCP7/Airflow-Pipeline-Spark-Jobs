from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3, json, pprint, requests, textwrap, time, logging, requests
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'bigdata-insights',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 10),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

dag = DAG('pyspark-pipeline', concurrency=3, schedule_interval=None, default_args=default_args)

def generate_perfil_input(**kwargs):
        ti = kwargs['ti']
        host = 'http://ec2-xx-xx-x-xxxx.compute-1.amazonaws.com:8998'
        data = {"file": "s3://hdata-belcorp/pyspark-files/Data_Perfil_Input.py", "name": "Test LivyREST", "driverMemory":"3G", "args":["PA", "201801"]}
        headers = {'Content-Type': 'application/json'}
        r = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
        id = r.json()['id']
        track_statement_progress(host, id)

def launch_perfil_training(**kwargs):
        ti = kwargs['ti']
        host = 'http://ec2-xx-xx-x-xxx.compute-1.amazonaws.com:8998'
        data = {"file": "s3://hdata-belcorp/pyspark-files/Perfiles_Training.py", "name": "Test LivyREST", "driverMemory":"12G", "args":["PA", "201801"]}
        headers = {'Content-Type': 'application/json'}
        r = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
        id = r.json()['id']
        track_statement_progress(host, id)


def track_statement_progress(host, id):
        statement_status = ''
        final_statement_status = ''
        # Poll the status of the submitted scala code
        while (statement_status != 'success' and statement_status != 'dead'):
        # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
                statement_url = host + '/batches/' + str(id)
                statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
                statement_status = statement_response.json()['state']
                logging.info('Statement status: ' + statement_status)

                #logging the logs
                lines = requests.get(statement_url + '/log', headers={'Content-Type': 'application/json'}).json()['log']
                for line in lines:
                        logging.info(line)
                time.sleep(10)
        if statement_status == 'success':
                #curl -X DELETE localhost:8998/batches/53
                requests.delete(statement_url, headers={'Content-Type': 'application/json'})
                final_statement_status = 'success'
                return
        if final_statement_status == 'dead':
                logging.info('Statement exception: ' + lines.json()['log'])
                for trace in statement_response.json()['output']['traceback']:
                        logging.info(trace)
                raise ValueError('Final Statement Status: ' + final_statement_status)
        logging.info('Final Statement Status: ' + final_statement_status)


generatePerfilInput = PythonOperator(
    task_id='generatePerfilInput',
    python_callable=generate_perfil_input,
    dag=dag)

launchPerfilTraining = PythonOperator(
    task_id='launchPerfilTraining',
    python_callable=launch_perfil_training,
    dag=dag)


generatePerfilInput.set_downstream(launchPerfilTraining)

#generatePerfilInput >> launchPerfilTraining
