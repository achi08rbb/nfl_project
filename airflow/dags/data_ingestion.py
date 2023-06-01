import os
import logging
import requests
import pandas as pd
import re
from pathlib import Path
import threading, queue
import pyarrow
import argparse
from bs4 import BeautifulSoup
import time

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from datetime import datetime,timedelta

import os.path
import sys

# Add code directory to python path to access the extraction_nfl module
sys.path.append(os.path.join(os.path.dirname('/opt/airflow/code/')))

# Import functions from the module                
from extraction_nfl import get_teams, get_teams_stats, get_athlete_ids, get_athletes_stats, get_athletes, webscrape_defense_stats, get_leaders

#===================================================================================
# CHANGE YOUR PROJECT VARIABLES HERE
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "nfl-project-de")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "nfl-data-lake_nfl-project-de")
REGION= os.environ.get("GCP_REGION", "asia-east1")

#===================================================================================
# CHANGE DESIRED PARAMS HERE
year=2022
season_type=2
#===================================================================================
teamIds=[
    '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9',
    '10',
    '11',
    '12',
    '13',
    '14',
    '15',
    '16',
    '17',
    '18',
    '19',
    '20',
    '21',
    '22',
    '23',
    '24',
    '25',
    '26',
    '27',
    '28',
    '29',
    '30',
    '33',
    '34'
]  

#===========


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 2,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
@dag(
    dag_id="nfl_extraction_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['nfl-project-de'],
)

def nfl_extract_load_GCS():
    # Extracting
    @task
    def task_teams():
        teams=get_teams(teamIds, year).to_json()
        return teams
    
    @task
    def task_teams_stats():
        teams_stats=get_teams_stats(teamIds, year, season_type).to_json()
        return teams_stats
    
    @task
    def task_athlete_ids():
        athlete_ids=get_athlete_ids()
        return athlete_ids

    @task
    def task_athlete_stats(athlete_ids: list):
        athletes_stats=get_athletes_stats(athlete_ids, year, season_type).to_json()
        return athletes_stats
    
    @task
    def task_athletes(athlete_ids: list):
        athletes=get_athletes(athlete_ids, year).to_json()
        return athletes
    
    @task
    def task_defense_stats():
        teams_defense_stats = webscrape_defense_stats(year, season_type).to_json()
        return teams_defense_stats

    @task
    def task_leaders():
        leaders=get_leaders(year,season_type).to_json()
        return leaders

    # Loading
    @task
    def load_to_gcs(json_list: list, filenames: list):
        
        print(f"Loading for nfl data to parquet started")
        
        path=f'gs://{BUCKET}'
        for json, filename in zip(json_list, filenames):
            df=pd.read_json(json, dtype='string')
            df.to_parquet(f'{path}/nfl_parquets/{filename}/{year}/{season_type}/{filename}_parquet', engine='pyarrow')

        print(f"Loading for nfl data to parquet ended")

    @task
    def upload_positions_to_gcs(bash_command):
        operator=BashOperator(task_id="upload_positions_to_gcs", bash_command=bash_command)
        operator.execute(context={})

    teams=task_teams()
    teams_stats=task_teams_stats()
    athlete_ids=task_athlete_ids()
    athletes=task_athletes(athlete_ids)
    athletes_stats=task_athlete_stats(athlete_ids)
    leaders=task_leaders()
    teams_defense_stats=task_defense_stats()

    json_list=[
    teams,
    teams_stats,
    athletes,
    athletes_stats,
    leaders,
    teams_defense_stats
    ]

    filenames=[
    'teams',
    'teams_stats',
    'athletes',
    'athletes_stats',
    'leaders',
    'teams_defense_stats'
    ]
    
    load_to_gcs(json_list, filenames)
    bash_command=f"gsutil cp /opt/airflow/csv/* gs://{BUCKET}/nfl_csv/"
    upload_positions_to_gcs(bash_command)

nfl_extract_load_GCS()

default_args2 = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 2,
    "depends_on_past": False,
    "wait_for_downstream": True,
}

@dag(
    dag_id="nfl_transformation_dag",
    schedule_interval="@once",
    default_args=default_args2,
    catchup=False,
    max_active_runs=1,
    tags=['nfl-project-de'],
    
)

def nfl_transform_load_BQ():
    
    @task
    def task_script_to_GCS():
        operator=BashOperator(
            task_id="pyspark_script_to_gcs",
            bash_command=f"gsutil -m cp -r /opt/airflow/code/ gs://{BUCKET}/"
            )
        operator.execute(context={})

    @task
    def task_pyspark():
        operator=BashOperator(
            task_id="pyspark_job_to_dataproc",
            bash_command=f"gcloud dataproc jobs submit pyspark \
                --cluster=nfl-spark-cluster \
                --region={REGION} \
                --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar\
                gs://{BUCKET}/code/transform_pyspark.py \
                -- \
                    --year={year} \
                    --season_type={season_type} "
            )
        operator.execute(context={})
    
    task_script_to_GCS() >> task_pyspark()

nfl_transform_load_BQ()