# 1. Consultar carrito
def consultar_carrito(db, nombre: str):
    pipeline = [
        {"$match": {"name": nombre}},
        {"$lookup": {
            "from": "carts",
            "localField": "name",
            "foreignField": "name",
            "as": "carrito"
        }},
        {"$unwind": "$carrito"},
        {"$unwind": "$carrito.items"},
        {"$lookup": {
            "from": "products",
            "localField": "carrito.items.product_id",
            "foreignField": "product_id",
            "as": "producto"
        }},
        {"$unwind": "$producto"},
        {"$project": {
            "_id": 0,
            "usuario": "$name",
            "producto": "$producto.name",
            "precio": "$producto.price",
            "cantidad": "$carrito.items.quantity",
            "subtotal": {"$multiply": ["$producto.price", "$carrito.items.quantity"]}
        }}
    ]
    return list(db.users.aggregate(pipeline))


# 2. Consultar productos por rango de precio
def productos_por_precio(db, minimo: int, maximo: int):
    pipeline = [
        {"$match": {"price": {"$gte": minimo, "$lte": maximo}}},
        {"$project": {"_id": 0, "name": 1, "price": 1, "category": 1, "brand": 1}}
    ]
    return list(db.products.aggregate(pipeline))


# 3. Consultar producto específico por rango de precio
def producto_especifico_precio(db, nombre: str, minimo: int, maximo: int):
    pipeline = [
        {"$match": {
            "name": {"$regex": nombre, "$options": "i"},
            "price": {"$gte": minimo, "$lte": maximo}
        }},
        {"$project": {"_id": 0, "name": 1, "price": 1, "category": 1, "brand": 1, "seller_name": 1}}
    ]
    return list(db.products.aggregate(pipeline))


# 4. Consultar nuevos lanzamientos por vendedor
def nuevos_lanzamientos(db, vendedor: str):
    pipeline = [
        {"$match": {"seller_name": {"$regex": vendedor, "$options": "i"}}},
        {"$sort": {"created_at": -1}},
        {"$project": {"_id": 0, "name": 1, "price": 1, "created_at": 1, "category": 1, "brand": 1}}
    ]
    return list(db.products.aggregate(pipeline))


# 5. Consultar productos por categoría
def productos_por_categoria(db, categoria: str):
    pipeline = [
        {"$match": {"category": categoria}},
        {"$project": {"_id": 0, "name": 1, "price": 1, "tags": 1, "brand": 1, "seller_name": 1}}
    ]
    return list(db.products.aggregate(pipeline))


# 6. Consultar productos disponibles (stock > 0) por nombre
def productos_disponibles(db, nombre: str):
    pipeline = [
        {"$match": {
            "name": {"$regex": nombre, "$options": "i"},
            "stock": {"$gt": 0}
        }},
        {"$project": {"_id": 0, "name": 1, "price": 1, "stock": 1, "category": 1, "brand": 1, "seller_name": 1}}
    ]
    return list(db.products.aggregate(pipeline))


# 7. Consultar wishlist de usuario
def wishlist_usuario(db, nombre: str):
    pipeline = [
        {"$match": {"name": nombre}},
        {"$unwind": "$wishlist"},
        {"$lookup": {
            "from": "products",
            "localField": "wishlist.product_id",
            "foreignField": "product_id",
            "as": "producto"
        }},
        {"$unwind": "$producto"},
        {"$project": {
            "_id": 0,
            "usuario": "$name",
            "producto": "$producto.name",
            "precio": "$producto.price",
            "categoria": "$producto.category",
            "marca": "$producto.brand",
            "vendedor": "$producto.seller_name",
            "fecha_agregado": "$wishlist.added_at"
        }}
    ]
    return list(db.users.aggregate(pipeline))


# 8. Coincidencias entre carrito y wishlist de un usuario
def coincidencias_carrito_wishlist(db, nombre: str):
    pipeline = [
        {"$match": {"name": nombre}},
        {"$lookup": {
            "from": "carts",
            "localField": "name",
            "foreignField": "name",
            "as": "cart_info"
        }},
        {"$unwind": "$cart_info"},
        {"$project": {
            "wishlist": "$wishlist",
            "cart_items": "$cart_info.items"
        }},
        {"$project": {
            "common_products": {
                "$setIntersection": [
                    {"$map": {"input": "$wishlist", "as": "w", "in": "$$w.product_id"}},
                    {"$map": {"input": "$cart_items", "as": "c", "in": "$$c.product_id"}}
                ]
            }
        }},
        {"$lookup": {
            "from": "products",
            "localField": "common_products",
            "foreignField": "product_id",
            "as": "product_info"
        }},
        {"$unwind": "$product_info"},
        {"$project": {
            "_id": 0,  
            "product_id": "$product_info.product_id",
            "name": "$product_info.name",
            "price": "$product_info.price"
        }}
    ]
    return list(db.users.aggregate(pipeline))

# 9. Consultar productos por vendedor y categoría
def productos_vendedor_categoria(db, vendedor: str, categoria: str):
    pipeline = [
        {"$match": {
            "seller_name": {"$regex": vendedor, "$options": "i"},
            "category": categoria
        }},
        {"$project": {"_id": 0, "name": 1, "price": 1, "brand": 1}}
    ]
    return list(db.products.aggregate(pipeline))


# 10. Productos más frecuentes en carritos
def productos_mas_en_carritos(db):
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {
            "_id": "$items.product_id",
            "total_en_carritos": {"$sum": "$items.quantity"}
        }},
        {"$lookup": {
            "from": "products",
            "localField": "_id",
            "foreignField": "product_id",
            "as": "producto"
        }},
        {"$unwind": "$producto"},
        {"$project": {
            "_id": 0,
            "producto": "$producto.name",
            "total_en_carritos": 1
        }},
        {"$sort": {"total_en_carritos": -1}}
    ]
    return list(db.carts.aggregate(pipeline))