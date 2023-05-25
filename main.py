import pandas as pd
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

import model
import download

default_args = {
    'owner': 'Gerard & Pol',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email': ['gcastrca25@alumnes.ub.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # remind the CRON fashion is: minute hour day month WEEKDAY
    'schedule_interval': '0 20 * * MON',
    # then, means every day and month
    # whenever the day is MONDAY at 20:00h (8pm).
}

local_path: str = os.getcwd()


def task_1():
    download.train_files()


def task_2() -> None:
    _train_1 = pd.read_csv(os.path.join(local_path, 'train1.csv'))
    _train_2 = pd.read_csv(os.path.join(local_path, 'train2.csv'))
    train: pd.DataFrame = pd.concat([_train_1, _train_2])

    model.serialize(train)


def task_3() -> None:
    download.prediction_file()


def task_4() -> None:
    clf = model.load()
    _pred_df = pd.read_csv(os.path.join(local_path, 'predict.csv'))
    prediction_df = pd.DataFrame({"Prediction": clf.predict(_pred_df)})
    prediction_df.to_csv(os.path.join(local_path, 'prediction.csv'))

    # we finally print the predictions
    print(prediction_df)


dag = DAG(
    "ml_batch_mode", catchup=False,
    description="Deployment of simple batch mode ML model.",
    default_args=default_args)

t1 = PythonOperator(task_id="Task 1: download training .csv",
                    python_callable=task_1, dag=dag)
t2 = PythonOperator(task_id="Task 2: serialize model",
                    python_callable=task_2, dag=dag)
t3 = PythonOperator(task_id="Task 3: download prediction .csv",
                    python_callable=task_3, dag=dag)
t4 = PythonOperator(task_id="Task 4: load model and print predictions",
                    python_callable=task_4, dag=dag)

# First, the training file must be downloaded (t1);
# and then the training can start (t2).
# If the training is not completed, the inference cannot start (t4).
# But the download of the prediction file (t3) can be
# done in parallel to the model's training.
t1 >> t2 >> t4
t3 >> t4
