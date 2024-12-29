import pandas as pd
import numpy as np
from sklearn.utils import resample
from sklearn.preprocessing import LabelEncoder

def model(dbt, session):
    # dbt configuration
    dbt.config( materialized="table",
                packages    =["pandas","numpy","scikit-learn","joblib==1.2.0"],
                alias='feature_encoded')

    base_data     = dbt.ref("3fct_fraud_ml_preprocessing_gb").to_pandas()
    # codify cathegoric variables with One-Hot Encoding
    base_data_encoded = pd.get_dummies(base_data, columns=['PAYMENTTYPE'])

    return base_data_encoded
