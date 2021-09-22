# [START import_module]
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
# [END import_module]

# [START Query MongoDB Data Collection Produtos]
# def query_mongo_collection():
    import pymongo
    import json
    import pandas as pd
    from pandas.io.json import json_normalize

# [START MongoDB Connector]
    # client = pymongo.MongoClient('mongodb://root:VQLnZB1QIp@mongodb.airflow.svc.cluster.local:27017')
    from airflow.hooks.base_hook import BaseHook
    conn = BaseHook.get_connection('iron_analytics_db')
    # db = conn.
    print(conn)
# [END MongoDB Connector]


