# Batch mode ML model deployment
We deploy simple batch mode ML model in production.
In particular, a pipeline is implemented using airflow to train a ML model 
based on data in a s3 bucket and, then we print the prediction. Regarding the:
### **ML model**
- We will use a simple regression model (serialized using the 
`training.ipynb`)
### **Data**
We will used the _Iris_ dataset, divided as:
  - **Training** data: each line of the [data.txt](https://ub-2021.s3-eu-west-1.amazonaws.com/data/data.txt) contains a data file link
  - **Prediction** data: available [here](https://ub-2021.s3-eu-west-1.amazonaws.com/data/predict.csv)

### Dedicated environment

A proper conda environment should be set-up to execute the scripts. The environment.yml file already gathers the needed packages; thus, it is as simple as executing this:
```bash
conda env create -f environment.yml
```

Finally, if it is desired to keep the conda environment as Jupyter notebook's kernel, we can type:
```bash
conda activate mlbatch
python -m ipykernel install --user --name=mlbatch
```
