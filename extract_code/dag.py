from __future__ import annotations

import logging

import pendulum

from airflow import DAG
from airflow.decorators import task

from main import main

log = logging.getLogger(__name__)

with DAG(
    dag_id='blablacar_usecase_dag',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:

    @task(task_id="finish_api_extraction")
    def c(ds=None, **kwargs):
        print(kwargs)
        return 'dummy return'

    run_this_c = c()

    @task(task_id="main_processing")
    def b(**kwargs):
        logging.info('main processing debut')
        logging.info('execution_date : {}'.format(kwargs['ds']))
        logging.info('dag name : {}'.format(kwargs['dag']))
        main()
        logging.info('main processing end')

    run_this_b = b()


    @task(task_id="start_api_extraction")
    def a(ds=None, **kwargs):
        print(kwargs)
        return 'dummy return'

    run_this_a = a()

    run_this_a >> run_this_b >> run_this_c
