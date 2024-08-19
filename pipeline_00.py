import os
import gdown
import duckdb as db
import pandas as pd 
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
from duckdb import DuckDBPyRelation
import psycopg2

# Loading variable set on .env file
load_dotenv() 


#------------------------------
# GET FILES FROM GOOGLE DRIVE
#------------------------------
def download_files(url_files, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    gdown.download_folder(url_files, output=local_dir, quiet=False, use_cookies=False)


#------------------------------
# LIST FILES ON THE FOLDER
#------------------------------
def list_files_csv(dir):
    csv_files = []

    all_files = os.listdir(dir)
    for file in all_files:
        if file.endswith("csv"): # Getting the csv files 
            path = os.path.join(dir, file)
            csv_files.append(path)
    return csv_files
    

#------------------------------
# READ CSV FILES AND RETURN DF DuckDB
#------------------------------
def read_csv(file_path):
    df_duckb = db.read_csv(file_path).df()
    return df_duckb


#------------------------------
# Transforming
#------------------------------
def transform(df):
    # Creating new column
    df_new = db.sql("""
                    select
                        concat(year(sell_date), '-', month(sell_date)) as sell_month,
                        sum(order_value) as total_value
                    from 
                        df
                    group by 
                        concat(year(sell_date), '-', month(sell_date))
                    """).df()
    return df_new


#------------------------------
# Verifying existing data 
#------------------------------
def verify_data(query, df_new, key):
    conn = psycopg2.connect(
        dbname='dbduck',
        user=os.getenv("pguser"),
        password=os.getenv("pgpass"),
        host=os.getenv("pghost"),
        port=os.getenv("pgport")
    )

    # select the table    
    df_original = pd.DataFrame(pd.read_sql_query(query, conn))

    # closing the connection
    conn.close()


    #Returning only data that is not on database yet
    df_verified = db.sql(f"""
                        select dn.*
                        from df_new as dn
                        left join df_original as dfo on dn."{key}" = dfo."{key}"
                        where dfo."{key}" is null;
    """).df()

    return df_verified


#------------------------------
# Saving data on POSTGRESQL
#------------------------------
def save_on_psql(df_db, table):
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    df_db.to_sql(table, con=engine, if_exists="append", index=False)



#------------------------------
# Creating a main to execute
#------------------------------
if __name__ == "__main__":

    url_files = "https://drive.google.com/drive/u/0/folders/1sHWsb1_Dnu8xTzOznWOqFGBZCs5aLNA9"
    local_dir = "./pasta_down"

    # Download the files from drive
    download_files(url_files, local_dir)
    

    # Read which files in the google drive exists
    files = list_files_csv(local_dir)     
    for file in files: # Creating a loop to read every file 

        df_csv = read_csv(file)        

        # Do transformation with this data
        df_transform = transform(df_csv)  

        # Verify if data exist on both orders and calculated
        df_csv_v = verify_data("select distinct order_id from orders", df_csv, "order_id")
        df_transform_v = verify_data("select distinct sell_month from orders_calc", df_transform, "sell_month")


        # Load on PostgreSQL
        save_on_psql(df_csv, "orders") 
        save_on_psql(df_transform_v, "orders_calc") 
