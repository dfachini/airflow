# [START import_module]
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
# [END import_module]

# # [START MongoDB Connector]
# client = pymongo.MongoClient('mongodb://root:VQLnZB1QIp%@mongodb.airflow.svc.cluster.local:27017')
# db = client.mongo
# # [END MongoDB Connector]

# [START Query MongoDB Data Collection Produtos]
def query_mongo_collection():
    import pymongo
    import json
    import pandas as pd
    from pandas.io.json import json_normalize

# [START MongoDB Connector]
    try:
        client = pymongo.MongoClient('mongodb://root:VQLnZB1QIp@mongodb.airflow.svc.cluster.local:27017/mongo')
        db = client['mongo']
        print("Conectado com Sucesso")
    except Exception as com_erro:
        print(com_erro)
# [END MongoDB Connector]

    for x in db["produtos"].find():
        df = pd.json_normalize(x)
    print(df.head())
# [END Query MongoDB Data Collection Produtos]

# [START Extract MongoDB Data]
def extract_mongo():
    import pymongo
    import json
    import pandas as pd
    from pandas.io.json import json_normalize

    df.to_csv('/tmp/mongo.csv')
    print("Extração Finalizada.")
    return 'Extract mongoDB completed.'
# [END Extract MongoDB Data]

# [START default_args]
default_args = {
    'owner': 'david fachini',
    'depends_on_past': False,
    'email': ['david.fachini@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}
# [END default_args]

# [START instantiate_dag]
with DAG(
    dag_id='mongodb_dag',
    default_args=default_args,
    description='Query and Export MongoDB Data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    catchup=False,
    tags=['mongodb', 'query', 'extraction'],
) as dag:
# [END instantiate_dag]

# [START basic_task]
    query_mongo_task = PythonVirtualenvOperator(
        task_id='query_mongo_data',
        python_callable=query_mongo_collection,
        requirements=["pymongo"],
    )

    extract_mongo_task = PythonVirtualenvOperator(
        task_id='extract_mongodb_to_csv',
        python_callable=extract_mongo,
        requirements=["pymongo"],
    )

    list_csv_file_task = BashOperator(
        task_id='cat_csv_to_file',
        bash_command='cat /tmp/mongo.csv',
    )
# [END basic_task]

query_mongo_task >> extract_mongo_task >> list_csv_file_task
