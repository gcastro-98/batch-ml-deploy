# Batch mode ML model deployment
Deployment of simple batch mode ML model in production.
In particular, a pipeline is implemented using airflow to train a ML model 
based on data in a s3 bucket and, then we print the prediction. Regarding the:
### **ML model**
- We will use a simple regression model (serialized using the 
`training.ipynb`)
### **Data**
We will use the _Iris_ dataset, divided as:
  - **Training** data: each line of the [data.txt](https://ub-2021.s3-eu-west-1.amazonaws.com/data/data.txt) contains a data file link
  - **Prediction** data: available [here](https://ub-2021.s3-eu-west-1.amazonaws.com/data/predict.csv)

## Launching Airflow

We need to open Airflow using the corresponding `docker-compose.yaml`.
If you don't have it, download it via:
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.1/docker-compose.yaml'
```

Before executing Airflow, ensure you have 
[set it up](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user) 
correctly. 

#### Linux users

In particular, some Linux users may have `root` permission problems when 
executing Airflow, which can be avoided doing: 
```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

#### First time running it

We need to create the **first user account** with:
```
docker compose up airflow-init
```

#### Finally...

After all the former, we can already launch Airflow with
```bash
docker compose up
```

## Launching the scripts

Bear in mind this module must be placed at `./dags` to be found by airflow
(after we ran it by `docker compose up`).
Besides, to set up Airflow variables we can export 
them using Environment Variables:
```bash
export AIRFLOW_VAR_VARIABLES='{"local_path":".","train_url":"https://ub-2021.s3-eu-west-1.amazonaws.com/data/data.txt","predict_url":"https://ub-2021.s3-eu-west-1.amazonaws.com/data/predict.csv"}'
```

This way, the variable 'variables' will be found in Airflow using:
```python
from airflow.models import Variable
airflow_json = Variable.get("variables", deserialize_json=True)
print(airflow_json["train_url"])
```