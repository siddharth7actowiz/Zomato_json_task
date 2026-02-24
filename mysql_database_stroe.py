import json
import mysql.connector
input_file="D:\\Siddharth\\json\\ZOMATO_2026_02_24.json"

def load_validated_json(json_file):
    with open(json_file, "r") as file:
        data = json.load(file)
    return data

json_data = load_validated_json(input_file)
print(json_data)


#Connection for Database

con = mysql.connector.connect(
    user="root",
    password='actowiz',
    host="localhost",
    port=3306,
    database="zomato"  # your existing database
)

cursor = con.cursor()

create_table_sql="""
    CREATE TABLE IF NOT EXISTS ZOMATODATA(
    restaurant_id INT ,
    restaurant_name VARCHAR(50),
    restaurant_contact JSON,
    fssai_licence_number bigint,
    address_info JSON,
    cuisines JSON,
    timings JSON,
    menu_categories JSON
    )

"""
cursor.execute(create_table_sql)
# Insert query
insert_sql = """
INSERT INTO ZOMATODATA(
    restaurant_id,
    restaurant_name,
    restaurant_contact,
    fssai_licence_number,
    address_info,
    cuisines,
    timings,
    menu_categories
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


restaurant_id = json_data.get("restaurant_id")
restaurant_name = json_data.get("restaurant_name")
restaurant_contact = json_data.get("restaurant_contact", [])
fssai_number = json_data.get("fssai_licence_number")  # just take as-is
address_info = json_data.get("address_info", {})
cuisines = json_data.get("cuisines", [])
timings = json_data.get("timings", {})
menu_categories = json_data.get("menu_categories", [])

row_values = (
restaurant_id,
restaurant_name,
json.dumps(restaurant_contact),
fssai_number,
json.dumps(address_info),
json.dumps(cuisines),
json.dumps(timings),
json.dumps(menu_categories)
)

cursor.execute(insert_sql, row_values)
print(cursor.rowcount, "row inserted.")

# Commit and close
con.commit()
cursor.close()
con.close()




'''

import json
import mysql.connector

input_file = "D:\\Siddharth\\json\\ZOMATO_2026_02_24.json"

def load_validated_json(json_file):
    with open(json_file, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data

json_data = load_validated_json(input_file)
print(json_data)

# Database connection
con = mysql.connector.connect(
    user="root",
    password='actowiz',
    host="localhost",
    port=3306,
    database="zomato"
)

cursor = con.cursor()

# Create table
create_table_sql = """
CREATE TABLE IF NOT EXISTS ZOMATODATA(
    restaurant_id INT PRIMARY KEY,
    restaurant_name VARCHAR(50),
    restaurant_contact JSON,
    fssai_licence_number BIGINT,
    address_info JSON,
    cuisines JSON,
    timings JSON,
    menu_categories JSON
)
"""
cursor.execute(create_table_sql)

# Insert query
insert_sql = """
INSERT INTO ZOMATODATA(
    restaurant_id,
    restaurant_name,
    restaurant_contact,
    fssai_licence_number,
    address_info,
    cuisines,
    timings,
    menu_categories
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Loop over JSON and insert
for key, val in json_data.items():
    restaurant_id = val.get("restaurant_id")
    restaurant_name = val.get("restaurant_name")
    restaurant_contact = val.get("restaurant_contact", {})
    fssai_number = val.get("fssai_licence_number")  # just take as-is
    address_info = val.get("address_info", {})
    cuisines = val.get("cuisines", [])
    timings = val.get("timings", {})
    menu_categories = val.get("menu_categories", [])

    row_values = (
        restaurant_id,
        restaurant_name,
        json.dumps(restaurant_contact),
        fssai_number,
        json.dumps(address_info),
        json.dumps(cuisines),
        json.dumps(timings),
        json.dumps(menu_categories)
    )

    cursor.execute(insert_sql, row_values)
    print(cursor.rowcount, "row inserted.")

# Commit and close
con.commit()
cursor.close()
con.close()



'''