from fastapi import FastAPI
import pandas as pd
from sqlalchemy import create_engine
import keyring

app = FastAPI(title= "API to consult your score as seller in Olist platform",
    description= "Scores are based on several range of sellers with similarities",   
)

engine = create_engine(keyring.get_password("aws", "database"))
connection = engine.connect()
connection = connection.execution_options(database='olist')

@app.get("/")
def read_root():
    return "API is running correctly. Use /docs"


@app.get("/seller/{seller_id}")
async def read_item(seller_id: str):
    query = (f'''SELECT total_income_kpi, review_avg_kpi, delivery_avg_kpi, performance_kpi
            FROM Evaluation WHERE seller_id = "{seller_id}"''')
    Result = pd.read_sql_query(query, con=engine)
    if Result.shape[0] < 1: return 'Seller not found on Evaluation table. Check id and try again'
    return f'total_income_kpi: {Result.total_income_kpi[0].round(2)}, review_avg_kpi: {Result.review_avg_kpi[0].round(2)}, delivery_avg_kpi: {Result.delivery_avg_kpi[0].round(2)}, performance_kpi: {Result.performance_kpi[0].round(2)}'