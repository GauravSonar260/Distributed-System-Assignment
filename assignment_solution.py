import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

# --- 1. Configuration ---
DB_USERS = "users.db"
DB_PRODUCTS = "products.db"
DB_ORDERS = "orders.db"

# --- 2. Database Setup ---
def setup_databases():
    # Setup Users DB
    with sqlite3.connect(DB_USERS) as conn:
        conn.execute("DROP TABLE IF EXISTS users")
        conn.execute('''CREATE TABLE users 
                        (id INTEGER PRIMARY KEY, name TEXT, email TEXT)''')

    # Setup Products DB
    with sqlite3.connect(DB_PRODUCTS) as conn:
        conn.execute("DROP TABLE IF EXISTS products")
        conn.execute('''CREATE TABLE products 
                        (id INTEGER PRIMARY KEY, name TEXT, price REAL)''')

    # Setup Orders DB
    with sqlite3.connect(DB_ORDERS) as conn:
        conn.execute("DROP TABLE IF EXISTS orders")
        conn.execute('''CREATE TABLE orders 
                        (id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, quantity INTEGER)''')
    print("--- Database Setup Complete ---")

# --- 3. Validation Logic ---
def validate_user(data):
    if not data.get('name') or not data.get('email'):
        return False, "Missing name or email"
    return True, "Valid"

def validate_product(data):
    try:
        if float(data.get('price', 0)) < 0:
            return False, "Price cannot be negative"
    except ValueError:
        return False, "Invalid price format"
    return True, "Valid"

def validate_order(data):
    try:
        if int(data.get('quantity', 0)) <= 0:
            return False, "Quantity must be positive"
    except ValueError:
        return False, "Invalid quantity format"
    return True, "Valid"

# --- 4. Insertion Logic (Thread Workers) ---
def insert_user(data):
    
    is_valid, msg = validate_user(data)
    if not is_valid:
        return f"[User ID {data['id']}] Failed: {msg}"
    
    try:
        
        with sqlite3.connect(DB_USERS, timeout=10) as conn:
            conn.execute("INSERT INTO users (id, name, email) VALUES (?, ?, ?)", 
                           (data['id'], data['name'], data['email']))
            conn.commit()
        return f"[User ID {data['id']}] Success: Inserted {data['name']}"
    except sqlite3.IntegrityError:
        return f"[User ID {data['id']}] Failed: ID already exists"
    except Exception as e:
        return f"[User ID {data['id']}] Error: {str(e)}"

def insert_product(data):
    is_valid, msg = validate_product(data)
    if not is_valid:
        return f"[Product ID {data['id']}] Failed: {msg}"

    try:
        with sqlite3.connect(DB_PRODUCTS, timeout=10) as conn:
            conn.execute("INSERT INTO products (id, name, price) VALUES (?, ?, ?)", 
                           (data['id'], data['name'], data['price']))
            conn.commit()
        return f"[Product ID {data['id']}] Success: Inserted {data['name']}"
    except sqlite3.IntegrityError:
        return f"[Product ID {data['id']}] Failed: ID already exists"

def insert_order(data):
    is_valid, msg = validate_order(data)
    if not is_valid:
        return f"[Order ID {data['id']}] Failed: {msg}"

    try:
        with sqlite3.connect(DB_ORDERS, timeout=10) as conn:
            conn.execute("INSERT INTO orders (id, user_id, product_id, quantity) VALUES (?, ?, ?, ?)", 
                           (data['id'], data['user_id'], data['product_id'], data['quantity']))
            conn.commit()
        return f"[Order ID {data['id']}] Success: Order for User {data['user_id']}"
    except sqlite3.IntegrityError:
        return f"[Order ID {data['id']}] Failed: ID already exists"

# -- 5. Data Preparation (From  PDF) ---
users_data = [
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"},
    {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
    {"id": 4, "name": "David", "email": "david@example.com"},
    {"id": 5, "name": "Eve", "email": "eve@example.com"},
    {"id": 6, "name": "Frank", "email": "frank@example.com"},
    {"id": 7, "name": "Grace", "email": "grace@example.com"},
    {"id": 8, "name": "Alice", "email": "alice@example.com"}, 
    {"id": 9, "name": "Henry", "email": "henry@example.com"},
    {"id": 10, "name": "", "email": "jane@example.com"} # Invalid: Name missing
]

products_data = [
    {"id": 1, "name": "Laptop", "price": 1000.00},
    {"id": 2, "name": "Smartphone", "price": 700.00},
    {"id": 3, "name": "Headphones", "price": 150.00},
    {"id": 4, "name": "Monitor", "price": 300.00},
    {"id": 5, "name": "Keyboard", "price": 50.00},
    {"id": 6, "name": "Mouse", "price": 30.00},
    {"id": 7, "name": "Laptop", "price": 1000.00},
    {"id": 8, "name": "Smartwatch", "price": 250.00},
    {"id": 9, "name": "Gaming Chair", "price": 500.00},
    {"id": 10, "name": "Earbuds", "price": -50.00} # Invalid: Negative Price
]

orders_data = [
    {"id": 1, "user_id": 1, "product_id": 1, "quantity": 2},
    {"id": 2, "user_id": 2, "product_id": 2, "quantity": 1},
    {"id": 3, "user_id": 3, "product_id": 3, "quantity": 5},
    {"id": 4, "user_id": 4, "product_id": 1, "quantity": 1},
    {"id": 5, "user_id": 5, "product_id": 3, "quantity": 3},
    {"id": 6, "user_id": 6, "product_id": 4, "quantity": 4},
    {"id": 7, "user_id": 7, "product_id": 2, "quantity": 2},
    {"id": 8, "user_id": 8, "product_id": 0, "quantity": 0}, # Invalid: Qty 0
    {"id": 9, "user_id": 9, "product_id": 1, "quantity": -1}, # Invalid: Qty -1
    {"id": 10, "user_id": 11, "product_id": 2, "quantity": 2}
]

# --- 6. Main Execution ---
if __name__ == "__main__":
    setup_databases()
    print("--- Starting Concurrent Insertions (10 Workers) ---")
    
    results = []
    
    
    with ThreadPoolExecutor(max_workers=10) as executor:
    
        user_futures = [executor.submit(insert_user, u) for u in users_data]
        product_futures = [executor.submit(insert_product, p) for p in products_data]
        order_futures = [executor.submit(insert_order, o) for o in orders_data]
        
    
        for f in user_futures + product_futures + order_futures:
            results.append(f.result())


    print("\n--- Final Results ---")
    for r in results:
        print(r)