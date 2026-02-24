import json
import mysql.connector

input_file = "D:\\Siddharth\\json\\ZOMATO_2026_02_24.json"

with open(input_file, "r", encoding="utf-8") as file:
    data = json.load(file)

restaurant_data = data["restaurant_data"]
cuisine_data = data["cuisine_data"]

con = mysql.connector.connect(
    user="root",
    password="actowiz",
    host="localhost",
    port=3306,
    database="zomato"
)

cursor = con.cursor()



cursor.execute("""
CREATE TABLE IF NOT EXISTS restaurant (
    restaurant_id BIGINT PRIMARY KEY,
    restaurant_name VARCHAR(255),
    restaurant_contact VARCHAR(50),
    full_address TEXT,
    region VARCHAR(100),
    city VARCHAR(100),
    pincode VARCHAR(20),
    state VARCHAR(100),
    fssai_licence_number BIGINT,
    open_time VARCHAR(20),
    close_time VARCHAR(20)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS cuisines (
    id INT AUTO_INCREMENT PRIMARY KEY,
    restaurant_id BIGINT,
    cuisine_name VARCHAR(150),
    cuisine_url TEXT,
    FOREIGN KEY (restaurant_id) REFERENCES restaurant(restaurant_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS menu_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    restaurant_id BIGINT,
    category_name VARCHAR(255),
    item_id VARCHAR(100),
    item_name VARCHAR(255),
    item_description TEXT,
    item_slugs TEXT,
    is_veg BOOLEAN,
    FOREIGN KEY (restaurant_id) REFERENCES restaurant(restaurant_id)
)
""")


insert_restaurant = """
INSERT INTO restaurant VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE restaurant_name=VALUES(restaurant_name)
"""

cursor.execute(insert_restaurant, (
    restaurant_data.get("restaurant_id"),
    restaurant_data.get("restaurant_name"),
    restaurant_data.get("restaurant_contact"),
    restaurant_data.get("full_address"),
    restaurant_data.get("region"),
    restaurant_data.get("city"),
    restaurant_data.get("pincode"),
    restaurant_data.get("state"),
    restaurant_data.get("fssai_licence_number"),
    restaurant_data.get("open_time"),
    restaurant_data.get("close_time"),
))



total_cuisines = cuisine_data.get("total_cuisines", 0)

for i in range(1, total_cuisines + 1):
    cursor.execute("""
        INSERT INTO cuisines (restaurant_id, cuisine_name, cuisine_url)
        VALUES (%s,%s,%s)
    """, (
        cuisine_data.get(f"cuisine_{i}_restaurant_id"),
        cuisine_data.get(f"cuisine_{i}_name"),
        cuisine_data.get(f"cuisine_{i}_url")
    ))




total_items = restaurant_data.get("total_items", 0)

for i in range(1, total_items + 1):
    cursor.execute("""
        INSERT INTO menu_items (
            restaurant_id,
            category_name,
            item_id,
            item_name,
            item_description,
            item_slugs,
            is_veg
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        restaurant_data.get("restaurant_id"),
        restaurant_data.get(f"item_{i}_category"),
        restaurant_data.get(f"item_{i}_id"),
        restaurant_data.get(f"item_{i}_name"),
        restaurant_data.get(f"item_{i}_description"),
        restaurant_data.get(f"item_{i}_slugs"),
        restaurant_data.get(f"item_{i}_is_veg"),
    ))



con.commit()


cursor.close()
con.close()