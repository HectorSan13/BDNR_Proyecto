from connect import conectar_mongodb
from datetime import datetime
from faker import Faker
import random

def inicializar_colecciones():
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


    
    # Populate Users (~10)
    usuarios = []
    for i in range(10):
        wishlist_items = []
        for _ in range(random.randint(1, 3)):
            prod = random.choice(productos)  # producto real
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

    # Populate Carts (~10)
    carritos = []
    for u in usuarios:
        items = []
        for _ in range(random.randint(1, 4)):
            prod = random.choice(productos)  # producto real
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

if __name__ == "__main__":
    inicializar_colecciones()