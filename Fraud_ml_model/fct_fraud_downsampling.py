import pandas as pd
import numpy as np
from sklearn.utils import resample
from sklearn.preprocessing import LabelEncoder

def model(dbt, session):
    # dbt configuration
    dbt.config( materialized="table",
                packages    =["pandas","numpy","scikit-learn","joblib==1.2.0"])

    base_data     = dbt.ref("3fct_fraud_ml_preprocessing_gb").to_pandas()
    feature_encoded     = dbt.ref("feature_encoded").to_pandas()
    # label encoding : codify cathegoric variables
    base_data1 = base_data.merge(feature_encoded[['PAYMENTTYPE', 'PAYMENTTYPE_ENCODED']].drop_duplicates(), how='left', on='PAYMENTTYPE')
    base_data2 = base_data1.merge(feature_encoded[['CHANNEL', 'CHANNEL_ENCODED']].drop_duplicates(), how='left', on='CHANNEL')
    base_data3 = base_data2.drop(columns=['PAYMENTTYPE','CHANNEL'])

    base_data3   =   base_data3.sort_values(by=['IS_FRAUD', 'BOOKINGREFERENCE','BOOKING_TIMESTAMP'])

    target          =   'IS_FRAUD'
    #### -------- Downsampling --------

    true_fraud    =   base_data3[base_data3[target] == True]
    false_fraud    =   base_data3[base_data3[target] == False]

    false_fraud_downsampled = resample(false_fraud,replace=False,n_samples=4*len(true_fraud),random_state=42)

    base_data_downsampled = pd.concat([true_fraud,false_fraud_downsampled])

    # mix data 
    base_data_downsampled = base_data_downsampled.sample(frac=1, random_state=42)

    return base_data_downsampled