"""
Implementation of the simple batch mode ML DAG.

Bear in mind this module must be placed at ./dags to be found by airflow.
Besides, to set up Airflow variables we can export them using the GUI.
"""

import os
from datetime import datetime, timedelta
import pandas as pd
import requests
import pickle

from sklearn.linear_model import LogisticRegression
from sklearn.utils import shuffle

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

RANDOM_STATE: int = 123

default_args = {
    'owner': 'airflow',  # we use the default Airflow user (with same passwd!)
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Variable.set('localpath', '.')
# Variable.set('s3', 'https://ub-2021.s3-eu-west-1.amazonaws.com/data')
local_path: str = Variable.get("localpath")
s3: str = Variable.get("s3")


# ##########################################################################
# DOWNLOAD RELATED AUXILIARY FUNCTIONS
# ##########################################################################

def _from_url_to_csv(file_url: str, file_name: str) -> None:
    pd.read_csv(file_url).to_csv(os.path.join(local_path, f'{file_name}.csv'))


def download_train_files() -> None:
    # we have to do it this way because train_url is a .txt url
    # (containing 2 .csv urls)
    for i, _train_url in enumerate(requests.get(
            f"{s3}/data.txt").text.split("\n"), start=1):
        _from_url_to_csv(_train_url, f'train{i}')


def download_prediction_file() -> None:
    _from_url_to_csv(f"{s3}/predict.csv", 'predict')


# ##########################################################################
# MODEL RELATED AUXILIARY FUNCTIONS
# ##########################################################################


def model_serialization(train: pd.DataFrame) -> None:
    features: pd.DataFrame = train.drop(["Species"], axis=1)
    labels: pd.Series = train["Species"]

    features, labels = shuffle(features, labels, random_state=RANDOM_STATE)

    clf = LogisticRegression(random_state=RANDOM_STATE)
    clf.fit(features, labels)

    with open('model.pkl', 'wb') as serialized_model:
        pickle.dump(clf, serialized_model)


def model_load() -> LogisticRegression:
    with open('model.pkl', 'rb') as serialized_model:
        return pickle.load(serialized_model)


# ##########################################################################
# DAG RELATED FUNCTIONS
# ##########################################################################

def task_1():
    download_train_files()


def task_2() -> None:
    _train_1 = pd.read_csv(os.path.join(local_path, 'train1.csv'))
    _train_2 = pd.read_csv(os.path.join(local_path, 'train2.csv'))
    train: pd.DataFrame = pd.concat([_train_1, _train_2])

    model_serialization(train)


def task_3() -> None:
    download_prediction_file()


def task_4() -> None:
    clf = model_load()
    _pred_df = pd.read_csv(os.path.join(local_path, 'predict.csv'))
    prediction_df = pd.DataFrame({"Prediction": clf.predict(_pred_df)})
    prediction_df.to_csv(os.path.join(local_path, 'prediction.csv'))

    # we finally print the predictions
    print(prediction_df)


# DAG definition

dag = DAG(
    "ml_batch_mode", catchup=False,
    description="Deployment of simple batch mode ML model.",
    default_args=default_args,
    # remind the CRON fashion is: minute hour day month weekday
    schedule_interval='0 20 * * 1',
    # then, means every day and month
    # whenever the day is 1/7 (Monday) at 20:00h (8pm).
    )

t1 = PythonOperator(task_id="Task_1_download_training_csv",
                    python_callable=task_1, dag=dag)
t2 = PythonOperator(task_id="Task_2_serialize_model",
                    python_callable=task_2, dag=dag)
t3 = PythonOperator(task_id="Task_3_download_prediction_csv",
                    python_callable=task_3, dag=dag)
t4 = PythonOperator(task_id="Task_4_load_model_and_print_predictions",
                    python_callable=task_4, dag=dag)

# First, the training file must be downloaded (t1);
# and then the training can start (t2).
# If the training is not completed, the inference cannot start (t4).
# But the download of the prediction file (t3) can be
# done in parallel to the model's training.
t1 >> t2 >> t4
t3 >> t4
