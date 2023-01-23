from datetime import timedelta
import datetime as dt
import boto3
import pandas as pd  
import os 
from airflow import DAG 
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from sklearn.preprocessing import LabelEncoder
dag_path = os.getcwd() 

def data_cleaning():
    engine = create_engine("")

    dataframe_list = []
    names_list = []
    dict={}

    #Conection to amazon S3
    s3 = boto3.Session(
        aws_access_key_id='',
        aws_secret_access_key='',)
    s3_resource = s3.resource('s3')

    #Creation of the audit table
    if not engine.has_table("audit"):
        engine.execute('''CREATE TABLE audit (
        timestamp TIMESTAMP DEFAULT NOW(),
        table_name VARCHAR(50),
        last_modified_dataset VARCHAR(50),
        detail VARCHAR(50)
        )''')

    #Load of all datasets from S3, removing duplicated and empty rows

    #First the dataset last modified date is check to see if there is a new modification
    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_closed_deals_dataset.csv")
    last_modified_dataset = csv_obj.last_modified.strftime('%Y-%m-%d %H:%M:%S')

    #If the table already exist and the last modified date is diferent to the one in the audit table in the database the new dataset is upload
    if engine.has_table("Closed_deals") == True:
        sql_query = f"SELECT * FROM audit WHERE table_name='Closed_deals' AND last_modified_dataset='{last_modified_dataset}'"
        df_query = pd.read_sql(sql_query, con=engine)
        if df_query.empty == True:
            print("a")
            Closed_deals = pd.read_csv(csv_obj.get()['Body'])
            Closed_deals.drop_duplicates(inplace=True)
            #The column dtype is change to prevent a future error when comparing with the table in the database
            Closed_deals['has_gtin'] = Closed_deals['has_gtin'].astype('float64')
            Closed_deals['has_company'] = Closed_deals['has_company'].astype('float64')
            
            #The last modified date is saved for later inserting into audit table
            dict["Closed_deals"]=last_modified_dataset      
            
            #The table name and the dataframe is add to a list that will be use to upload the data to the database
            dataframe_list.append(Closed_deals)
            names_list.append("Closed_deals")
    #if the table doesn´t exist then the dataset is loaded
    else:
        print("b")
        Closed_deals = pd.read_csv(csv_obj.get()['Body'])
        Closed_deals.drop_duplicates(inplace=True)

        Closed_deals['has_gtin'] = Closed_deals['has_gtin'].astype('float64')
        Closed_deals['has_company'] = Closed_deals['has_company'].astype('float64')
        
        dataframe_list.append(Closed_deals)
        names_list.append("Closed_deals")
        dict["Closed_deals"]=last_modified_dataset  
        
        
    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_customers_dataset.csv")
    last_modified_dataset = csv_obj.last_modified.strftime('%Y-%m-%d %H:%M:%S')
    if engine.has_table("Customers") == True:
        sql_query = f"SELECT * FROM audit WHERE table_name='Customers' AND last_modified_dataset='{last_modified_dataset}'"
        df_query = pd.read_sql(sql_query, con=engine)
        if df_query.empty == True:
            Customers = pd.read_csv(csv_obj.get()['Body'])
            Customers.drop_duplicates(inplace=True)
            dataframe_list.append(Customers)
            names_list.append("Customers")
            dict["Customers"]=last_modified_dataset  
    else:
        Customers = pd.read_csv(csv_obj.get()['Body'])
        Customers.drop_duplicates(inplace=True)
        dataframe_list.append(Customers)
        names_list.append("Customers")
        dict["Customers"]=last_modified_dataset  
        

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_geolocation_dataset.csv")
    last_modified_dataset = csv_obj.last_modified.strftime('%Y-%m-%d %H:%M:%S')
    if engine.has_table("Geolocation") == True:
        sql_query = f"SELECT * FROM audit WHERE table_name='Geolocation' AND last_modified_dataset='{last_modified_dataset}'"
        df_query = pd.read_sql(sql_query, con=engine)
        if df_query.empty == True:
            Geolocation = pd.read_csv(csv_obj.get()['Body'])
            Geolocation.drop_duplicates(inplace=True)
            dataframe_list.append(Geolocation)
            names_list.append("Geolocation")
            dict["Geolocation"]=last_modified_dataset  
    else:
        Geolocation = pd.read_csv(csv_obj.get()['Body'])
        Geolocation.drop_duplicates(inplace=True)
        dataframe_list.append(Geolocation)
        names_list.append("Geolocation")
        dict["Geolocation"]=last_modified_dataset  


    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_marketing_qualified_leads_dataset.csv")
    last_modified_dataset = csv_obj.last_modified.strftime('%Y-%m-%d %H:%M:%S')
    if engine.has_table("Marketing") == True:
        sql_query = f"SELECT * FROM audit WHERE table_name='Marketing' AND last_modified_dataset='{last_modified_dataset}'"
        df_query = pd.read_sql(sql_query, con=engine)
        if df_query.empty == True:
            Marketing = pd.read_csv(csv_obj.get()['Body'])
            Marketing.drop_duplicates(inplace=True)
            dataframe_list.append(Marketing)
            names_list.append("Marketing")
            dict["Marketing"]=last_modified_dataset  
    else:
        Marketing = pd.read_csv(csv_obj.get()['Body'])
        Marketing.drop_duplicates(inplace=True)
        dataframe_list.append(Marketing)
        names_list.append("Marketing")
        dict["Marketing"]=last_modified_dataset  

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_order_items_dataset.csv")
    last_modified_dataset = csv_obj.last_modified.strftime('%Y-%m-%d %H:%M:%S')
    if engine.has_table("Order_items") == True:
        sql_query = f"SELECT * FROM audit WHERE table_name='Order_items' AND last_modified_dataset='{last_modified_dataset}'"
        df_query = pd.read_sql(sql_query, con=engine)
        if df_query.empty == True:
            Order_items = pd.read_csv(csv_obj.get()['Body'])
            Order_items.drop_duplicates(inplace=True)
            dataframe_list.append(Order_items)
            names_list.append("Order_items")
            dict["Order_items"]=last_modified_dataset  
        else:
            Order_items = pd.read_sql("select * from Order_items", con=engine) 
    else:
        Order_items = pd.read_csv(csv_obj.get()['Body'])
        Order_items.drop_duplicates(inplace=True)
        dataframe_list.append(Order_items)
        names_list.append("Order_items")
        dict["Order_items"]=last_modified_dataset  

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_order_payments_dataset.csv")
    last_modified_dataset = csv_obj.last_modified.strftime('%Y-%m-%d %H:%M:%S')
    if engine.has_table("Order_payments") == True:
        sql_query = f"SELECT * FROM audit WHERE table_name='Order_payments' AND last_modified_dataset='{last_modified_dataset}'"
        df_query = pd.read_sql(sql_query, con=engine)
        if df_query.empty == True:
            Order_payments = pd.read_csv(csv_obj.get()['Body'])
            Order_payments.drop_duplicates(inplace=True)
            dataframe_list.append(Order_payments)
            names_list.append("Order_payments")
            dict["Order_payments"]=last_modified_dataset  
        else:
            Order_payments = pd.read_sql("select * from Order_payments", con=engine)  
    else:
        Order_payments = pd.read_csv(csv_obj.get()['Body'])
        Order_payments.drop_duplicates(inplace=True)
        dataframe_list.append(Order_payments)
        names_list.append("Order_payments")
        dict["Order_payments"]=last_modified_dataset  

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_order_reviews_dataset.csv")
    last_modified_dataset = csv_obj.last_modified.strftime('%Y-%m-%d %H:%M:%S')
    if engine.has_table("Order_reviews") == True:
        sql_query = f"SELECT * FROM audit WHERE table_name='Order_reviews' AND last_modified_dataset='{last_modified_dataset}'"
        df_query = pd.read_sql(sql_query, con=engine)
        if df_query.empty == True:
            Order_reviews = pd.read_csv(csv_obj.get()['Body'])
            Order_reviews.drop_duplicates(inplace=True)
            dataframe_list.append(Order_reviews)
            names_list.append("Order_reviews")
            dict["Order_reviews"]=last_modified_dataset  
        else:
            Order_reviews = pd.read_sql("select * from Order_reviews", con=engine)
    else:
        Order_reviews = pd.read_csv(csv_obj.get()['Body'])
        Order_reviews.drop_duplicates(inplace=True)
        dataframe_list.append(Order_reviews)
        names_list.append("Order_reviews")
        dict["Order_reviews"]=last_modified_dataset  
            
            
    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_sellers_dataset.csv")
    last_modified_dataset = csv_obj.last_modified.strftime('%Y-%m-%d %H:%M:%S')
    if engine.has_table("Sellers") == True:
        sql_query = f"SELECT * FROM audit WHERE table_name='Sellers' AND last_modified_dataset='{last_modified_dataset}'"
        df_query = pd.read_sql(sql_query, con=engine)
        if df_query.empty == True:
            Sellers = pd.read_csv(csv_obj.get()['Body'])
            Sellers.drop_duplicates(inplace=True)
            dataframe_list.append(Sellers)
            names_list.append("Sellers")
            dict["Sellers"]=last_modified_dataset  
        else:
            Sellers = pd.read_sql("select * from Sellers", con=engine)         
    else:
        Sellers = pd.read_csv(csv_obj.get()['Body'])
        Sellers.drop_duplicates(inplace=True)
        dataframe_list.append(Sellers)
        names_list.append("Sellers")
        dict["Sellers"]=last_modified_dataset  
        
        
    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_products_dataset.csv")
    last_modified_dataset = csv_obj.last_modified.strftime('%Y-%m-%d %H:%M:%S')
    if engine.has_table("Products") == True:
        sql_query = f"SELECT * FROM audit WHERE table_name='Products' AND last_modified_dataset='{last_modified_dataset}'"
        df_query = pd.read_sql(sql_query, con=engine)
        if df_query.empty == True:
            Products = pd.read_csv(csv_obj.get()['Body'])
            Products.drop_duplicates(inplace=True)
            dataframe_list.append(Products)
            names_list.append("Products")
            dict["Products"]=last_modified_dataset  
        else:
            Products = pd.read_sql("select * from Products", con=engine)
    else:
        Products = pd.read_csv(csv_obj.get()['Body'])
        Products.drop_duplicates(inplace=True)
        dataframe_list.append(Products)
        names_list.append("Products")
        dict["Products"]=last_modified_dataset  

    csv_obj = s3_resource.Object("p-raw-datasets", "Datasets_original/olist_orders_dataset.csv")
    last_modified_dataset = csv_obj.last_modified.strftime('%Y-%m-%d %H:%M:%S')
    if engine.has_table("Orders") == True:
        sql_query = f"SELECT * FROM audit WHERE table_name='Orders' AND last_modified_dataset='{last_modified_dataset}'"
        df_query = pd.read_sql(sql_query, con=engine)
        if df_query.empty == True:
            Orders = pd.read_csv(csv_obj.get()['Body'])
            Orders.drop_duplicates(inplace=True)
            dataframe_list.append(Orders)
            names_list.append("Orders")
            dict["Orders"]=last_modified_dataset  
        else:
            Orders = pd.read_sql("select * from Orders", con=engine) 
    else:
        Orders = pd.read_csv(csv_obj.get()['Body'])
        Orders.drop_duplicates(inplace=True)
        dataframe_list.append(Orders)
        names_list.append("Orders")
        dict["Orders"]=last_modified_dataset  
            
    if len(names_list)>0:
        if any([n in names_list for n in ["Order_items", "Order_payments", "Order_reviews", "Sellers", "Products", "Orders"]]):
            #In the dataframe Orders a new column is added and calculated
            Orders['Tiempo_entrega'] = pd.to_datetime(Orders["order_approved_at"]) - pd.to_datetime(Orders['order_delivered_customer_date'])
            Orders['Tiempo_entrega'] = Orders['Tiempo_entrega'].apply(lambda x: x.days + (x.seconds // 86400)) 
            
            #Combination of the dataframes to create a new table
            datasets_combinados=Orders.merge(Order_reviews,on="order_id")
            datasets_combinados=datasets_combinados.merge(Order_payments,on="order_id")
            datasets_combinados=datasets_combinados.merge(Order_items,on="order_id")
            datasets_combinados=datasets_combinados.merge(Sellers,on="seller_id")
            datasets_combinados=datasets_combinados.merge(Products,on="product_id")

            #Adding column avg_price_month
            datasets_combinados["Month"] = pd.DatetimeIndex(datasets_combinados["order_approved_at"]).month
            datasets_combinados["Year"] = pd.DatetimeIndex(datasets_combinados["order_approved_at"]).year
            datasets_combinados['avg_income_month'] = datasets_combinados.groupby(['seller_id','Month',"Year"])['price'].transform('mean')

            #labelencoder for id columns
            columnas = [0,1,9,19,20,21]
            datasets_combinados.columns
            le = LabelEncoder()
            for col in columnas:
                datasets_combinados[datasets_combinados.columns[col]] = le.fit_transform(datasets_combinados[datasets_combinados.columns[col]])

            Valoration = pd.DataFrame(columns=['seller_id'])
            Valoration.seller_id = datasets_combinados.seller_id.unique()

            #Merge of all necesary columns by seller_id column.
            Valoration = Valoration.merge(datasets_combinados[['seller_id', 'seller_state']].groupby(['seller_id']).max(), on= 'seller_id')
            Valoration = Valoration.merge(datasets_combinados[['seller_id', 'seller_city']].groupby(['seller_id']).max(), on= 'seller_id')
            Valoration = Valoration.merge(datasets_combinados[['seller_id', 'product_id']].groupby(['seller_id']).count(), on= 'seller_id')
            Valoration = Valoration.merge(datasets_combinados[['seller_id', 'product_category_name']].groupby(['seller_id']).nunique(), on= 'seller_id')
            Valoration = Valoration.merge(datasets_combinados[['seller_id', 'Tiempo_entrega']].groupby(['seller_id']).mean().round(2), on= 'seller_id')
            Valoration = Valoration.merge(datasets_combinados[['seller_id', 'review_score']].groupby(['seller_id']).mean().round(2), on= 'seller_id')
            Valoration = Valoration.merge(datasets_combinados[['seller_id', 'order_id']].groupby(['seller_id']).count(), on= 'seller_id')
            Valoration = Valoration.merge(datasets_combinados[['seller_id', 'price']].groupby(['seller_id']).sum(), on= 'seller_id')
            Valoration = Valoration.merge(datasets_combinados[['seller_id', 'avg_income_month']].groupby("seller_id").mean().reset_index().round(2), on= 'seller_id')

            Valoration.drop_duplicates(inplace=True)

            #Rename of columns
            Valoration.rename({'product_id':'distinct_prod', 'Tiempo_entrega':'delivery_avg', 'product_category_name':'distinct_categories',\
                        'review_score':'review_avg', 'order_id':'total_orders', 'price':'total_income'}, axis=1, inplace=True)

            dataframe_list.append(Valoration)
            names_list.append("Valoration")
            dict["Valoration"]=dt.datetime.now().replace(microsecond=0)

        #Upload only the new rows of each dataset to the database and only of the datasets that have changes
        for n,i in enumerate(dataframe_list):
            if engine.has_table(names_list[n]) == True:
                old_data = pd.read_sql(f"select * from {names_list[n]}", con=engine)

                data_merged = pd.merge(i, old_data, how='left', indicator=True)
                data_appended = data_merged[data_merged['_merge'] == 'left_only']

                data_appended.to_sql(names_list[n], con=engine, if_exists='append', index=False)
                timestamp= dt.datetime.now()
                engine.execute('''INSERT INTO audit (timestamp, table_name, last_modified_dataset, detail) 
                                VALUES (%s,%s,%s,"Adding new data to table")''', (timestamp, names_list[n],dict[names_list[n]]))
            else:
                i.to_sql(names_list[n], con=engine, index=False)
                timestamp= dt.datetime.now()
                engine.execute('''INSERT INTO audit (timestamp, table_name, last_modified_dataset, detail) 
                                VALUES (%s,%s,%s,"Creation of table and data load")''', (timestamp, names_list[n],dict[names_list[n]]))
            
        print("Data succesfully loaded to database")
    else:
        print("No changes detected in the datasets")
    
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
