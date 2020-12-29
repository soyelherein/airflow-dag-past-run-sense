"""
   Copyright [2020] [soyel.alam@ucdconnect.ie]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

from random import choice
from airflow import DAG
from airflow import models
from airflow.macros import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'soyelherein',
    'depends_on_past': True,
    'start_date': datetime(2020,12,29,2,0,0)
}


def load_final_table_status(**kwargs):
    x = choice([0,1])
    if x:
      raise ValueError("Loading to the final table failed")
    return x


with DAG('dag_past_run_sense',
         schedule_interval='*/2 * * * *',
         default_args=default_args,
         max_active_runs=1) as dag:

    read_incr_data = BashOperator(
        task_id='read_incr_data',
        wait_for_downstream=True,
        bash_command='date'
    )

    prepare_scd2 = BashOperator(
        task_id='prepare_scd2',
        bash_command='date'
    )

    load_final_table = PythonOperator(
        task_id="load_final_table",
        python_callable=load_final_table_status
    )

    read_incr_data >> prepare_scd2 >> load_final_table
    read_incr_data >> load_final_table


