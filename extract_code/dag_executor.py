from datetime import datetime
from dag import python_operator_t1, python_operator_t2, python_operator_t3


def main():
    # op1 >> op2 >> op3
    python_operator_t1.execute(context={'ds': datetime.utcnow(), 'task': 'debut_task'})
    python_operator_t2.execute(context={'ds': datetime.utcnow(), 'task': 'api_call_processing_db_upsert_task'})
    python_operator_t3.execute(context={'ds': datetime.utcnow(), 'task': 'end_task'})


if __name__ == '__main__':
    main()
