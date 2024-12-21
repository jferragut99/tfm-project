import pandas as pd
import numpy as np
from sklearn.utils import resample
from sklearn.preprocessing import LabelEncoder

def model(dbt, session):
    # dbt configuration
    dbt.config( materialized="table",
                packages    =["pandas","numpy","scikit-learn","joblib==1.2.0"])

    base_data     = dbt.ref("3fct_fraud_ml_preprocessing_gb").to_pandas()
    # label encoding : codify cathegoric variables
    le = LabelEncoder()
    base_data['PAYMENTTYPE_ENCODED'] = le.fit_transform(base_data['PAYMENTTYPE'])
    base_data['CHANNEL_ENCODED'] = le.fit_transform(base_data['CHANNEL'])
    base_data = base_data[['PAYMENTTYPE_ENCODED','PAYMENTTYPE','CHANNEL_ENCODED','CHANNEL']].drop_duplicates()

    return base_data