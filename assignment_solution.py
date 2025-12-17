import sqlite3

DB_USERS = "users.db"
DB_PRODUCTS = "products.db"
DB_ORDERS = "orders.db"

def setup_databases():
    print("--- Database Setup Started ---")

    
    with sqlite3.connect(DB_USERS) as conn:
        conn.execute("DROP TABLE IF EXISTS users") # Purana table delete karega taaki fresh start ho
        conn.execute('''CREATE TABLE users 
                        (id INTEGER PRIMARY KEY, name TEXT, email TEXT)''')
    print(f"Success: {DB_USERS} created with fields (id, name, email)")

    
    with sqlite3.connect(DB_PRODUCTS) as conn:
        conn.execute("DROP TABLE IF EXISTS products")
        conn.execute('''CREATE TABLE products 
                        (id INTEGER PRIMARY KEY, name TEXT, price REAL)''')
    print(f"Success: {DB_PRODUCTS} created with fields (id, name, price)")
    
    
    with sqlite3.connect(DB_ORDERS) as conn:
        conn.execute("DROP TABLE IF EXISTS orders")
        conn.execute('''CREATE TABLE orders 
                        (id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, quantity INTEGER)''')
    print(f"Success: {DB_ORDERS} created with fields (id, user_id, product_id, quantity)")

if __name__ == "__main__":
    setup_databases()