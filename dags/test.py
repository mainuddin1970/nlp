from datetime import datetime
import pandas as pd
from scipy.stats import yeojohnson
import sklearn.preprocessing as preproc
from sklearn.cluster import KMeans

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt


def kmean_cluster(file='/opt/airflow/data/rfm_analysis.csv'):
    '''
    doing k-mean clustering

    Parameters
    ----------
    file : str, optional
        _description_, by default '/opt/airflow/data/rfm_analysis.csv'

    Returns
    -------
    _type_
        _description_
    '''
    rfm = pd.read_csv(file)

    df = pd.DataFrame()
    df["CustomerID"] = rfm.index
    for col in ['recency', 'frequency', 'monetary']:
        value, _ = yeojohnson(rfm[col])
        df[col] = value
    features = ['recency', 'frequency', 'monetary']
    # Standardization - note that by definition, some outputs will be negative
    df[features] = preproc.StandardScaler().fit_transform(df[features])
    # Initialize KMeans
    kmeans = KMeans(n_clusters=4, random_state=1)

    # Fit k-means clustering on the normalized data set
    kmeans.fit(df[features])
    # Extract cluster labels
    rfm['cluster_labels'] = kmeans.labels_
    rfm.to_csv("/opt/airflow/data/customer_segmentation.csv", index=False)
    return True


def segmentation():
    '''
    This function doing customer segmentation

    Returns
    -------
    _type_
        _description_
    '''
    data = pd.read_excel("/opt/airflow/data/SmallOnlineRetail.xlsx")
    print(data.shape)
    # remove data with no CustomerID
    data = data[pd.notnull(data['CustomerID'])]
    # only uk data
    uk_data = data[data.Country == 'United Kingdom']
    uk_data = uk_data[(uk_data['Quantity'] > 0)]
    uk_data = uk_data[(uk_data['UnitPrice'] > 0)]
    uk_data = uk_data[['CustomerID', 'InvoiceDate', 'InvoiceNo', 'Quantity', 'UnitPrice']]
    # total price
    uk_data['TotalPrice'] = uk_data['Quantity'] * uk_data['UnitPrice']
    PRESENT = dt.datetime(2011,12,10)
    uk_data['InvoiceDate'] = pd.to_datetime(uk_data['InvoiceDate'])
    # rfm analysis
    rfm = uk_data.groupby('CustomerID').agg({'InvoiceDate': lambda date: (PRESENT - date.max()).days,
                                            'InvoiceNo': 'count',
                                            'TotalPrice': 'sum'})
    rfm = rfm.rename(columns={'InvoiceDate': 'recency',
                            'InvoiceNo': 'frequency',
                            'TotalPrice': 'monetary'})
    print(rfm.head())
    # convert to int
    rfm['recency'] = rfm['recency'].astype(int)
    rfm['frequency'] = rfm['frequency'].astype(int)
    rfm['monetary'] = rfm['monetary'].astype(int)
    # binning
    rfm['r_quartile'] = pd.qcut(rfm['recency'], 4, labels=['1','2','3','4'])
    rfm['f_quartile'] = pd.qcut(rfm['frequency'], 4, labels=['4','3','2','1'])
    rfm['m_quartile'] = pd.qcut(rfm['monetary'], 4, labels=['4','3','2','1'])
    # rfm score
    rfm['RFM_Score'] = rfm.r_quartile.astype(str)+ rfm.f_quartile.astype(str) + rfm.m_quartile.astype(str)
    rfm['RFM_Score_num'] = rfm.r_quartile.astype(int)+ rfm.f_quartile.astype(int) + rfm.m_quartile.astype(int)
    # Creating custom segments

    # Define rfm_level function
    def rfm_level(df):
        if df['RFM_Score_num'] >= 10:
            return 'Low'
        elif ((df['RFM_Score_num'] >= 6) and (df['RFM_Score_num'] < 10)):
            return 'Middle'
        else:
            return 'Top'

    # Create a new variable RFM_Level
    rfm['RFM_Level'] = rfm.apply(rfm_level, axis=1)

    print(rfm.head())
    rfm.to_csv("/opt/airflow/data/rfm_analysis.csv", index=False)
    return True


local_workflow = DAG(
    "CustomerSegmentation",
    schedule_interval="50 08 * * *",
    start_date=datetime(2023, 6, 16)
)

TASK_ONE = 'load_analysis'
TASK_TWO = 'kmean_cluster'

with local_workflow:
    segmentation = PythonOperator(
        task_id=TASK_ONE,
        python_callable=segmentation
    )

    kmean_cluster = PythonOperator(
        task_id=TASK_TWO,
        python_callable=kmean_cluster
    )

    segmentation >> kmean_cluster
