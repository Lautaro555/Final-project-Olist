import tkinter as tk
import keyring
import pandas as pd
from tkinter import filedialog
import boto3

#Conection to amazon S3
s3 = boto3.Session(
    aws_access_key_id=keyring.get_password("aws", "access key"),
    aws_secret_access_key=keyring.get_password("aws", "secret key"),)
s3_resource = s3.resource('s3')

def select_file():
    filepath = filedialog.askopenfilename()
    print("Selected:", filepath)
    update_s3(filepath)

def update_s3(filepath):
    table_name = table_var.get()
    # leer las columnas del archivo elegido por el usuario
    df_new = pd.read_csv(filepath)
    new_cols = df_new.columns
    # leer las columnas del archivo existente en el bucket
    obj = s3_resource.Object('p-raw-datasets', f'Datasets_original/{table_name}.csv')
    df_existing = pd.read_csv(obj.get()['Body'])
    existing_cols = df_existing.columns
    # comparar las columnas
    if new_cols.equals(existing_cols):
        s3_resource.meta.client.upload_file(filepath, 'p-raw-datasets', f'Datasets_original/{table_name}.csv')
        print("File uploaded successfully")
    else:
        print("The columns of the selected file don't match with the columns of the existing file in the bucket.")

root = tk.Tk()
root.geometry("400x300")
root.title("Select the CSV you want to update")

table_var = tk.StringVar(value="olist_closed_deals_dataset")
table_dropdown = tk.OptionMenu(root, table_var, "olist_closed_deals_dataset", "olist_customers_dataset", "olist_geolocation_dataset", "olist_marketing_qualified_leads_dataset", "olist_order_items_dataset", "olist_order_payments_dataset", "olist_order_reviews_dataset", "olist_orders_dataset","olist_products_dataset","olist_sellers_dataset")
table_dropdown.pack()

file_button = tk.Button(root, text="Select path to the CSV you want to upload", command=select_file)
file_button.pack()

root.mainloop()
