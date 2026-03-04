import mysql.connector
from config import *

tb=TABLE_NAME
def make_connection():

    conn=mysql.connector.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        database=DB
        
        
    )
    return conn

def batch_insert( insert_query: str, values: list[tuple], batch_size: int = BATCH_SIZE):
    con=make_connection()
    cursor=con.cursor()
    total_records = len(values)
    batch_count = 0
    failed_batches = []

    for start in range(0, total_records, batch_size):
        end = min(start + batch_size, total_records) # to avoid array out of bound index error
        batch = values[start:end]

        try:
            cursor.executemany(insert_query, batch)
            con.commit()
            batch_count += 1
            print(f"Inserted batch {batch_count} : {start} to {end}")

        except Exception as e:
            print(f"Batch failed ({start} → {end})")
            print("Error:", e)
            failed_batches.append(batch)

    cursor.close()
    con.close()    

    return batch_count, failed_batches

def create_table(tb):
    try:
        connection=make_connection()    
        cursor=connection.cursor()
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS {tb}(
            id int auto_increment primary key,
            res_id varchar(50),
            res_name varchar(200),
            res_website varchar(200),
            restaurant_contact varchar(200),
            full_address varchar(200),
            region varchar(20),
            city varchar(20),
            pincode int,
            state varchar(20),
            licence_no BIGINT,
            open_time varchar(20),
            close_time varchar(20),
            Cuisions TEXT   ,
            menu_items TEXT
            )

            ''')
        connection.commit()
                    
        print(f"table {tb} created !!")
        
    except Exception as  e:
     print(e)    
        
    finally:    

        cursor.close()
        connection.close() 


def insert(tb:str,data:dict):
    try:
        failed_batches = []
        columns=",".join(data.keys())
        
        vals=",".join(["%s"] * len(data))

        q=f"INSERT INTO {tb}({columns})VALUES({vals})"
        print(data.values())
        batch_count, failed_batches=batch_insert(q,[tuple(data.values())])    #make sure passing json cause sql cant convert list,dict to str
        return batch_count
        
    except Exception as e:
       print("Error:",e)
       print("Failed Batch:",failed_batches) 