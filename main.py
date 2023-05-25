"""
Implementation of the simple batch mode ML DAG.

Bear in mind this module must be placed at ./dags to be found by airflow.
Besides, to set up Airflow variables we can export them using Environment
Variables:

```bash
export AIRFLOW_VAR_VARIABLES='{"local_path":".","train_url":"https://ub-2021.s3-eu-west-1.amazonaws.com/data/data.txt","predict_url":"https://ub-2021.s3-eu-west-1.amazonaws.com/data/predict.csv"}'
```

This way, the variable 'variables' will be found in Airflow using:

```python
from airflow.models import Variable
airflow_json = Variable.get("variables", deserialize_json=True)
print(airflow_json["train_url"])
```

"""

import os
from datetime import datetime, timedelta
import pandas as pd
from requests import get
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
    # remind the CRON fashion is: minute hour day month WEEKDAY
    'schedule_interval': '0 20 * * MON',
    # then, means every day and month
    # whenever the day is MONDAY at 20:00h (8pm).
}

airflow_json = Variable.get("variables", deserialize_json=True)
local_path: str = airflow_json["local_path"]
train_url: str = airflow_json["train_url"]
predict_url: str = airflow_json["predict_url"]


# ##########################################################################
# DOWNLOAD RELATED AUXILIARY FUNCTIONS
# ##########################################################################

def _from_url_to_csv(file_url: str, file_name: str) -> None:
    pd.read_csv(file_url).to_csv(os.path.join(local_path, f'{file_name}.csv'))


def download_train_files() -> None:
    # we have to do it this way because train_url is a .txt url
    # (containing 2 .csv urls)
    for i, _train_url in enumerate(get(train_url).text.split("\n")):
        _from_url_to_csv(_train_url, f'train{i+1}')


def download_prediction_file() -> None:
    _from_url_to_csv(predict_url, 'predict')


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
