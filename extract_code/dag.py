from airflow.operators.python import PythonOperator
from airflow.models import DAG
from datetime import datetime
from time import sleep
from main import main

execution_date = datetime.today()

args = {
    'owner': 'airflow',
    'start_date': execution_date,
}

dag = DAG(
    dag_id='blablacar_usecase_dag',
    default_args=args,
    catchup=False,
    schedule=None,
    tags=['dummy_run'])


def dummy_start_task(ds, **kwargs):
    """
    dummy task that will be used on the dag as first task of python operator callable
    :param ds:
    :param kwargs:
    :return:
    """
    print(kwargs)
    print(ds)
    return sleep(2)


def dummy_end_task(ds, **kwargs):
    """
    dummy task that will be used on the dag as last task of python operator callable
    :param ds:
    :param kwargs:
    :return:
    """
    print(kwargs)
    print(ds)
    return sleep(2)


def extraction_main(ds, **kwargs):
    """
    extarction task that will be used on the dag as main python operator callable for data processing in the usecase
    :param ds:
    :param kwargs:
    :return:
    """
    print(kwargs)
    print(ds)
    return main()


python_operator_t1 = PythonOperator(
    task_id='debut_task',
    python_callable=dummy_start_task,
    dag=dag)

python_operator_t3 = PythonOperator(
    task_id='end_task',
    python_callable=dummy_end_task,
    dag=dag)

python_operator_t2 = PythonOperator(
    task_id='api_call_processing_db_upsert_task',
    python_callable=extraction_main,
    dag=dag)
