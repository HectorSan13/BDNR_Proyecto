# ============================
#     POPULATE UNIFICADO
# ============================

from uuid import uuid4
from datetime import datetime, timedelta
import random
import string
import pydgraph
from faker import Faker


from connect import conectar_mongodb, conectar_cassandra

# ==========================================================
#               CONFIGURACIÓN GLOBAL
# ==========================================================

NUM_USERS = 50
NUM_PRODUCTS = 120
NUM_SESSIONS = 200

# IDs UNIVERSALES (COMPARTIDOS ENTRE AMBAS BASES)
USERS = [uuid4() for _ in range(NUM_USERS)]
PRODUCTS = [uuid4() for _ in range(NUM_PRODUCTS)]
CATEGORIES = [uuid4() for _ in range(10)]

# ==========================================================
#                FUNCIONES GENERALES
# ==========================================================

def random_timestamp(days_back=30):
    now = datetime.now()
    delta = timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return now - delta

def random_word(n=6):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(n))

def random_search_terms():
    return " ".join(random_word(random.randint(4, 10)) for _ in range(random.randint(2, 5)))

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

# ==========================================================
#                MONGO DB POPULATION
# ==========================================================

def populate_mongodb():
    db = conectar_mongodb()
    fake = Faker("es_MX")

    # Crear índices
    db.products.create_index("price")
    db.products.create_index("brand")
    db.products.create_index("seller_name")
    db.products.create_index("created_at")
    db.products.create_index("category")
    db.products.create_index("tags")
    db.products.create_index("stock")

    db.users.create_index("email", unique=True)
    db.users.create_index("name", unique=True)
    db.carts.create_index("name", unique=True)

    print("Índices creados correctamente.")

    # Populate Products
    categorias = ["Electrónica", "Accesorios", "Hogar", "Gaming", "Deporte"]
    tags_pool = ["oferta", "nuevo", "inalámbrico", "gaming", "premium", "eco"]

    productos = []
    for i in range(100):
        productos.append({
            "product_id": f"P{i+1:03}",
            "name": fake.word().capitalize() + " " + random.choice(["Pro", "Max", "Lite", "Plus"]),
            "price": round(random.uniform(100, 5000), 2),
            "category": random.choice(categorias),
            "tags": random.sample(tags_pool, k=random.randint(1, 3)),
            "brand": fake.company(),
            "seller_id": f"V{i%10+1:03}",
            "seller_name": fake.company(),
            "created_at": fake.date_time_this_year(),
            "stock": random.randint(0, 50)
        })
    db.products.insert_many(productos)

    # Populate Users
    usuarios = []
    for i in range(10):
        wishlist_items = []
        for _ in range(random.randint(1, 3)):
            prod = random.choice(productos)
            wishlist_items.append({
                "product_id": prod["product_id"],
                "name": prod["name"],
                "added_at": fake.date_time_this_year()
            })

        usuarios.append({
            "name": fake.name(),
            "email": fake.email(),
            "wishlist": wishlist_items
        })
    db.users.insert_many(usuarios)

    # Populate Carts 
    carritos = []
    for u in usuarios:
        items = []
        for _ in range(random.randint(1, 4)):
            prod = random.choice(productos)
            items.append({
                "product_id": prod["product_id"],
                "name": prod["name"],
                "quantity": random.randint(1, 3),
                "price": prod["price"]
            })
        total = sum(item["quantity"] * item["price"] for item in items)
        carritos.append({
            "name": u["name"],
            "items": items,
            "total": total
        })
    db.carts.insert_many(carritos)

    print("Populate con 100 productos, 10 usuarios y 10 carritos realizado correctamente.")


# ==========================================================
#                CASSANDRA POPULATION
# ==========================================================

def populate_cassandra():
    print("\n=== Poblando Cassandra ===")

    session = conectar_cassandra(keyspace="mi_ecommerce")
    if session is None:
        print(" No se pudo conectar a Cassandra. Abortando población.")
        return

    # -------------------- Crear Tablas --------------------
    TABLES = {
        "product_views": """
            CREATE TABLE IF NOT EXISTS product_views (
                user_id UUID,
                product_id UUID,
                timestamp TIMESTAMP,
                PRIMARY KEY (user_id, timestamp)
            );
        """,
        "search_history": """
            CREATE TABLE IF NOT EXISTS search_history (
                user_id UUID,
                search_terms TEXT,
                timestamp TIMESTAMP,
                PRIMARY KEY (user_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC);
        """,
        "purchases": """
            CREATE TABLE IF NOT EXISTS purchases (
                user_id UUID,
                product_id UUID,
                purchase_date TIMESTAMP,
                price DECIMAL,
                PRIMARY KEY (user_id, purchase_date)
            );
        """,
        "product_duration": """
            CREATE TABLE IF NOT EXISTS product_duration (
                user_id UUID,
                product_id UUID,
                duration_seconds INT,
                timestamp TIMESTAMP,
                PRIMARY KEY (user_id, timestamp)
            );
        """,
        "recommendation_clicks": """
            CREATE TABLE IF NOT EXISTS recommendation_clicks (
                user_id UUID,
                recommended_product_id UUID,
                click_timestamp TIMESTAMP,
                PRIMARY KEY (user_id, click_timestamp)
            );
        """,
        "cart_products": """
            CREATE TABLE IF NOT EXISTS cart_products (
                user_id UUID,
                product_id UUID,
                add_to_cart_time TIMESTAMP,
                quantity INT,
                PRIMARY KEY (user_id, add_to_cart_time)
            );
        """,
        "abandoned_cart": """
            CREATE TABLE IF NOT EXISTS abandoned_cart (
                user_id UUID,
                product_id UUID,
                abandon_time TIMESTAMP,
                cart_value DECIMAL,
                PRIMARY KEY (user_id, abandon_time)
            );
        """,
        "filter_interactions": """
            CREATE TABLE IF NOT EXISTS filter_interactions (
                user_id UUID,
                filter_type TEXT,
                filter_value TEXT,
                timestamp TIMESTAMP,
                PRIMARY KEY (user_id, timestamp)
            );
        """,
        "navigation_sessions": """
            CREATE TABLE IF NOT EXISTS navigation_sessions (
                session_id UUID,
                user_id UUID,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                device_type TEXT,
                PRIMARY KEY (session_id)
            );
        """,
        "unpurchased_views": """
            CREATE TABLE IF NOT EXISTS unpurchased_views (
                user_id UUID,
                product_id UUID,
                view_time TIMESTAMP,
                purchase_flag BOOLEAN,
                PRIMARY KEY (user_id, view_time)
            );
        """,
        "recommendations_shown": """
            CREATE TABLE IF NOT EXISTS recommendations_shown (
                user_id UUID,
                recommended_product_id UUID,
                display_time TIMESTAMP,
                algorithm_version TEXT,
                PRIMARY KEY (user_id, display_time)
            );
        """,
        "wishlist": """
            CREATE TABLE IF NOT EXISTS wishlist (
                user_id UUID,
                product_id UUID,
                wishlist_add_time TIMESTAMP,
                PRIMARY KEY (user_id, wishlist_add_time)
            );
        """,
        "product_comparisons": """
            CREATE TABLE IF NOT EXISTS product_comparisons (
                user_id UUID,
                product_ids_compared LIST<UUID>,
                compare_time TIMESTAMP,
                PRIMARY KEY (user_id, compare_time)
            );
        """,
        "category_metrics": """
            CREATE TABLE IF NOT EXISTS category_metrics (
                user_id UUID,
                category_id UUID,
                week_start TIMESTAMP,
                views INT,
                clicks INT,
                purchases INT,
                PRIMARY KEY ((user_id, category_id), week_start)
            );
        """
    }

    for name, query in TABLES.items():
        session.execute(query)
        print(f"✔ Tabla creada: {name}")

    # -------------------- PREPARED STATEMENTS --------------------
    stmt_product_views = session.prepare("""
        INSERT INTO product_views (user_id, product_id, timestamp)
        VALUES (?, ?, ?)
    """)
    stmt_search_history = session.prepare("""
        INSERT INTO search_history (user_id, search_terms, timestamp)
        VALUES (?, ?, ?)
    """)
    stmt_purchases = session.prepare("""
        INSERT INTO purchases (user_id, product_id, purchase_date, price)
        VALUES (?, ?, ?, ?)
    """)
    stmt_product_duration = session.prepare("""
        INSERT INTO product_duration (user_id, product_id, duration_seconds, timestamp)
        VALUES (?, ?, ?, ?)
    """)
    stmt_recommendation_clicks = session.prepare("""
        INSERT INTO recommendation_clicks (user_id, recommended_product_id, click_timestamp)
        VALUES (?, ?, ?)
    """)
    stmt_cart_products = session.prepare("""
        INSERT INTO cart_products (user_id, product_id, add_to_cart_time, quantity)
        VALUES (?, ?, ?, ?)
    """)
    stmt_abandoned_cart = session.prepare("""
        INSERT INTO abandoned_cart (user_id, product_id, abandon_time, cart_value)
        VALUES (?, ?, ?, ?)
    """)
    stmt_filter_interactions = session.prepare("""
        INSERT INTO filter_interactions (user_id, filter_type, filter_value, timestamp)
        VALUES (?, ?, ?, ?)
    """)
    stmt_navigation_sessions = session.prepare("""
        INSERT INTO navigation_sessions (session_id, user_id, start_time, end_time, device_type)
        VALUES (?, ?, ?, ?, ?)
    """)
    stmt_unpurchased_views = session.prepare("""
        INSERT INTO unpurchased_views (user_id, product_id, view_time, purchase_flag)
        VALUES (?, ?, ?, ?)
    """)
    stmt_recommendations_shown = session.prepare("""
        INSERT INTO recommendations_shown (user_id, recommended_product_id, display_time, algorithm_version)
        VALUES (?, ?, ?, ?)
    """)
    stmt_wishlist = session.prepare("""
        INSERT INTO wishlist (user_id, product_id, wishlist_add_time)
        VALUES (?, ?, ?)
    """)
    stmt_product_comparisons = session.prepare("""
        INSERT INTO product_comparisons (user_id, product_ids_compared, compare_time)
        VALUES (?, ?, ?)
    """)
    stmt_category_metrics = session.prepare("""
        INSERT INTO category_metrics (user_id, category_id, week_start, views, clicks, purchases)
        VALUES (?, ?, ?, ?, ?, ?)
    """)

    # -------------------- INSERTAR DATOS --------------------
    print("Insertando datos en Cassandra...")

    for user in USERS:
        for _ in range(5):
            session.execute(stmt_product_views, (
                user,
                random.choice(PRODUCTS),
                random_timestamp()
            ))
        for _ in range(10):
            session.execute(stmt_search_history, (
                user,
                random_search_terms(),
                random_timestamp()
            ))
        for _ in range(10):
            session.execute(stmt_purchases, (
                user,
                random.choice(PRODUCTS),
                random_timestamp(),
                random.randint(100, 2000)
            ))
        for _ in range(10):
            session.execute(stmt_product_duration, (
                user,
                random.choice(PRODUCTS),
                random.randint(10, 500),
                random_timestamp()
            ))
        for _ in range(10):
            session.execute(stmt_recommendation_clicks, (
                user,
                random.choice(PRODUCTS),
                random_timestamp()
            ))
        for _ in range(10):
            session.execute(stmt_cart_products, (
                user,
                random.choice(PRODUCTS),
                random_timestamp(),
                random.randint(1, 5)
            ))
        for _ in range(10):
            session.execute(stmt_abandoned_cart, (
                user,
                random.choice(PRODUCTS),
                random_timestamp(),
                random.randint(200, 2000)
            ))
        for _ in range(10):
            session.execute(stmt_filter_interactions, (
                user,
                random.choice(["precio", "marca", "categoría"]),
                random_word(5),
                random_timestamp()
            ))
        for _ in range(10):
            session.execute(stmt_navigation_sessions, (
                uuid4(),
                user,
                random_timestamp(),
                random_timestamp(),
                random.choice(["mobile", "desktop", "tablet"])
            ))
        for _ in range(10):
            session.execute(stmt_unpurchased_views, (
                user,
                random.choice(PRODUCTS),
                random_timestamp(),
                False
            ))
        for _ in range(10):
            session.execute(stmt_recommendations_shown, (
            user,
            random.choice(PRODUCTS),
            random_timestamp(),
            f"v{random.randint(1,5)}"
            ))

        for _ in range(10):
            session.execute(stmt_wishlist, (
                user,
                random.choice(PRODUCTS),
                random_timestamp()
            ))

        for _ in range(10):
            session.execute(stmt_product_comparisons, (
                user,
                [random.choice(PRODUCTS) for _ in range(2)],
                random_timestamp()
            ))

        for _ in range(10):
            session.execute(stmt_category_metrics, (
                user,
                random.choice(CATEGORIES),
                random_timestamp(),
                random.randint(1, 20),  # views
                random.randint(1, 10),  # clicks
                random.randint(0, 5)    # purchases
            ))
    print("=== Cassandra poblada correctamente ===\n")

import csv
import pydgraph

def connect_dgraph():
    client_stub = pydgraph.DgraphClientStub('localhost:9080')
    client = pydgraph.DgraphClient(client_stub)
    return client

def load_csv(filename):
    with open(filename, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)

def create_users(client, users):
    mutations = []
    for u in users:
        mutations.append({
            "uid": f"_:{u['user_id']}",
            "dgraph.type": "User",
            "name": u['name'],
            "email": u['email'],
            "joined_at": u['joined_at']
        })
    txn = client.txn()
    try:
        txn.mutate(set_obj=mutations)
        txn.commit()
        print(f"{len(users)} users inserted.")
    finally:
        txn.discard()

def create_products(client, products):
    mutations = []
    for p in products:
        mutations.append({
            "uid": f"_:{p['product_id']}",
            "dgraph.type": "Product",
            "name": p['name'],
            "category": p['category'],
            "price": float(p['price'])
        })
    txn = client.txn()
    try:
        txn.mutate(set_obj=mutations)
        txn.commit()
        print(f"{len(products)} products inserted.")
    finally:
        txn.discard()

def create_reviews(client, reviews):
    mutations = []
    for r in reviews:
        mutations.append({
            "uid": f"_:{r['review_id']}",
            "dgraph.type": "Review",
            "rating": float(r['rating']),
            "comment": r['comment'],
            "review_created_at": r['created_at'],
            "reviewed_by": {"uid": f"_:{r['reviewed_by_uid']}"},
            "of_product": {"uid": f"_:{r['of_product_uid']}"}
        })
    txn = client.txn()
    try:
        txn.mutate(set_obj=mutations)
        txn.commit()
        print(f"{len(reviews)} reviews inserted.")
    finally:
        txn.discard()

def create_interactions(client, interactions):
    mutations = []
    for i in interactions:
        mutations.append({
            "uid": f"_:{i['interaction_id']}",
            "dgraph.type": "Interaction",
            "interaction_type": i['interaction_type'],
            "timestamp": i['timestamp'],
            "duration": float(i['duration']),
            "by_user": {"uid": f"_:{i['by_user_uid']}"},
            "with_product": {"uid": f"_:{i['with_product_uid']}"}
        })
    txn = client.txn()
    try:
        txn.mutate(set_obj=mutations)
        txn.commit()
        print(f"{len(interactions)} interactions inserted.")
    finally:
        txn.discard()

def create_carts(client, carts):
    mutations = []
    for c in carts:
        contains_list = [{"uid": f"_:{pid.strip()}"} for pid in c['contains_product_ids'].split(";")]
        mutations.append({
            "uid": f"_:{c['cart_id']}",
            "dgraph.type": "Cart",
            "cart_created_at": c['created_at'],
            "contains": contains_list,
            "owner": {"uid": f"_:{c['user_uid']}"}  # relación con el usuario
        })
    txn = client.txn()
    try:
        txn.mutate(set_obj=mutations)
        txn.commit()
        print(f"{len(carts)} carts inserted.")
    finally:
        txn.discard()


# ==========================================================
#                EJECUCIÓN PRINCIPAL
# ==========================================================

if __name__ == "__main__":
    populate_mongodb()
    populate_cassandra()
    client = connect_dgraph()

    users = load_csv("data/users.csv")
    products = load_csv("data/products.csv")
    reviews = load_csv("data/reviews.csv")
    interactions = load_csv("data/interactions.csv")
    carts = load_csv("data/carts.csv")

    create_users(client, users)
    create_products(client, products)
    create_reviews(client, reviews)
    create_interactions(client, interactions)
    create_carts(client, carts)



