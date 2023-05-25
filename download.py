import os
from requests import get
from airflow.models import Variable
from pandas import read_csv

local_path: str = os.getcwd()
data_url: str = 'https://ub-2021.s3-eu-west-1.amazonaws.com/data/data.txt'
predict_url: str = 'https://ub-2021.s3-eu-west-1.amazonaws.com/data/predict.csv'


def _from_url_to_csv(file_url: str, file_name: str) -> None:
    read_csv(file_url).to_csv(os.path.join(local_path, f'{file_name}.csv'))


def train_files() -> None:
    for i, train_url in enumerate(get(data_url).text.split("\n")):
        _from_url_to_csv(train_url, f'train{i+1}')


def prediction_file() -> None:
    _from_url_to_csv(predict_url, 'predict')
