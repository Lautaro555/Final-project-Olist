from datetime import timedelta
import datetime as dt
import boto3
import pandas as pd 
import numpy as np 
import os 
from airflow import DAG 
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from sklearn.preprocessing import LabelEncoder
dag_path = os.getcwd() 

def data_cleaning():
    s3 = boto3.Session(
        aws_access_key_id='',
        aws_secret_access_key='',)
    s3_resource = s3.resource('s3')
    
    
    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_closed_deals_dataset.csv")
    Closed_deals = pd.read_csv(csv_obj.get()['Body'])
    Closed_deals.drop_duplicates(inplace=True)
    Closed_deals.dropna(inplace=True)

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_customers_dataset.csv")
    Customers = pd.read_csv(csv_obj.get()['Body'])
    Customers.drop_duplicates(inplace=True)
    Customers.dropna(inplace=True)

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_geolocation_dataset.csv")
    Geolocation = pd.read_csv(csv_obj.get()['Body'])
    Geolocation.drop_duplicates(inplace=True)
    Geolocation.dropna(inplace=True)

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_marketing_qualified_leads_dataset.csv")
    Marketing = pd.read_csv(csv_obj.get()['Body'])
    Marketing.drop_duplicates(inplace=True)
    Marketing.dropna(inplace=True)

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_order_items_dataset.csv")
    Order_items = pd.read_csv(csv_obj.get()['Body'])
    Order_items.drop_duplicates(inplace=True)
    Order_items.dropna(inplace=True)

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_order_payments_dataset.csv")
    Order_payments = pd.read_csv(csv_obj.get()['Body'])
    Order_payments.drop_duplicates(inplace=True)
    Order_payments.dropna(inplace=True)

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_order_reviews_dataset.csv")
    Order_reviews = pd.read_csv(csv_obj.get()['Body'])
    Order_reviews.drop_duplicates(inplace=True)
    Order_reviews.dropna(inplace=True)

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_sellers_dataset.csv")
    Sellers = pd.read_csv(csv_obj.get()['Body'])
    Sellers.drop_duplicates(inplace=True)
    Sellers.dropna(inplace=True)

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_products_dataset.csv")
    Products = pd.read_csv(csv_obj.get()['Body'])
    Products.drop_duplicates(inplace=True)
    Products.dropna(inplace=True)

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_orders_dataset.csv")
    Orders = pd.read_csv(csv_obj.get()['Body'])
    Orders.drop_duplicates(inplace=True)
    Orders.dropna(inplace=True)
    Orders['Tiempo_entrega'] = pd.to_datetime(Orders["order_approved_at"]) - pd.to_datetime(Orders['order_delivered_customer_date'])
    Orders['Tiempo_entrega'] = Orders['Tiempo_entrega'].apply(lambda x: x.days + (x.seconds // 86400)) 


    datasets_combinados=Orders.merge(Order_reviews,on="order_id")
    datasets_combinados=datasets_combinados.merge(Order_payments,on="order_id")
    datasets_combinados=datasets_combinados.merge(Order_items,on="order_id")
    datasets_combinados=datasets_combinados.merge(Sellers,on="seller_id")
    datasets_combinados=datasets_combinados.merge(Products,on="product_id")

    datasets_combinados["Month"] = pd.DatetimeIndex(datasets_combinados["order_approved_at"]).month
    datasets_combinados["Year"] = pd.DatetimeIndex(datasets_combinados["order_approved_at"]).year
    datasets_combinados['avg_price_month'] = datasets_combinados.groupby(['seller_id','Month',"Year"])['price'].transform('mean')

    columnas = [0,1,9,19,20,21]
    datasets_combinados.columns
    le = LabelEncoder()
    for col in columnas:
     datasets_combinados[datasets_combinados.columns[col]] = le.fit_transform(datasets_combinados[datasets_combinados.columns[col]])

    Valoracion = pd.DataFrame(columns=['seller_id'])
    Valoracion.seller_id = datasets_combinados.seller_id.unique()

    Valoracion = Valoracion.merge(datasets_combinados[['seller_id', 'seller_state']].groupby(['seller_id']).max(), on= 'seller_id')
    Valoracion = Valoracion.merge(datasets_combinados[['seller_id', 'seller_city']].groupby(['seller_id']).max(), on= 'seller_id')
    Valoracion = Valoracion.merge(datasets_combinados[['seller_id', 'product_id']].groupby(['seller_id']).count(), on= 'seller_id')
    Valoracion = Valoracion.merge(datasets_combinados[['seller_id', 'product_category_name']].groupby(['seller_id']).nunique(), on= 'seller_id')
    Valoracion = Valoracion.merge(datasets_combinados[['seller_id', 'Tiempo_entrega']].groupby(['seller_id']).mean().round(2), on= 'seller_id')
    Valoracion = Valoracion.merge(datasets_combinados[['seller_id', 'review_score']].groupby(['seller_id']).mean().round(2), on= 'seller_id')
    Valoracion = Valoracion.merge(datasets_combinados[['seller_id', 'order_id']].groupby(['seller_id']).count(), on= 'seller_id')
    Valoracion = Valoracion.merge(datasets_combinados[['seller_id', 'price']].groupby(['seller_id']).sum(), on= 'seller_id')
    Valoracion = Valoracion.merge(datasets_combinados[['seller_id', 'avg_price_month']].groupby("seller_id").mean().reset_index().round(2), on= 'seller_id')

    Valoracion.drop_duplicates(inplace=True)

    Valoracion.rename({'product_id':'distinct_prod', 'Tiempo_entrega':'delivery_avg', 'product_category_name':'distinct_categories',\
                'review_score':'review_avg', 'order_id':'total_orders', 'price':'total_income'}, axis=1, inplace=True)
    
    #Completar con la informacion de la base de datos de RDS una vez la creemos
    engine = create_engine(f"postgresql://{rds_config['user']}:{rds_config['password']}@{rds_config['host']}:{rds_config['port']}/{rds_config['dbname']}")
    Valoracion.to_sql("tabla_valoracion", engine, if_exists='replace')

    print("Data succesfully loaded to database")
    
data_cleaning_dag = DAG(
    dag_id='Data_cleaning',
    start_date=dt.datetime.today(),
    schedule_interval=timedelta(days=1),
    catchup=False)
    
clean_data = PythonOperator(
task_id="data_cleaning",
python_callable=data_cleaning,
dag=data_cleaning_dag)

clean_data
