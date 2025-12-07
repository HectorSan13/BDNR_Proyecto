
import datetime
import json


import pydgraph

def set_schema(client):
    schema = """
    # ----------- Tipos de Nodos -----------
    type User {
        name
        email
        joined_at
        reviewed
        purchased
        interacted
        has_cart
    }
    
    type Product {
        name
        category
        price
        reviews
        purchased_by
        purchased_with
        interactions
    }
    
    type Review {
        rating
        comment
        review_created_at
        reviewed_by
        of_product
    }
    
    type Interaction {
        interaction_type
        timestamp
        duration
        by_user
        with_product
    }
    
    type Cart {
        cart_created_at
        contains
    }
    
    # ----------- Predicados con Índice -----------
    # ---------- User ----------
    name: string @index(term) .
    email: string @index(exact) .
    joined_at: datetime .
    
    reviewed: [uid] .        
    purchased: [uid] .    
    interacted: [uid] .      
    has_cart: [uid] .          
    
    
    # ---------- Product ----------
    category: string @index(exact) .
    price: float .
    
    reviews: [uid] .         
    purchased_with: [uid] .  
    interactions: [uid] .    
    
    
    # ---------- Review ----------
    rating: float @index(float) .
    comment: string @index(fulltext) .
    review_created_at: datetime @index(hour) .
    
    reviewed_by: [uid] .                
    
    # ---------- Interaction ----------
    interaction_type: string @index(exact) .
    timestamp: datetime @index(hour) .
    duration: float .
    
    by_user: [uid] .           # Interaction → User
    with_product: [uid] .      # Interaction → Product
    
    
    # ---------- Cart ----------
    cart_created_at: datetime @index(day) .
    contains: [uid] .        # Cart → Product
    """
    return client.alter(pydgraph.Operation(schema=schema))
    
    
# QUERIES

# 1. Obtener reseñas de un producto
def get_reviews(client, product_id):
    query = """
    query getReviews($pid: string) {
      reviews(func: type(Review)) @filter(eq(of_product.product_id, $pid)) {
        rating
        comment
        review_created_at
        of_product {
          name
        }
        reviewed_by {
          name
          email
        }
      }
    }
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query, {"$pid": product_id})
        return json.loads(res.json)
    finally:
        txn.discard()



# 2. Registro de interacciones (view, click, purchase)
def get_user_interactions(client, user_uid):
    query = """
    query userInteractions($uid: uid) {
      user(func: uid($uid)) {
        name
        interacted(orderdesc: timestamp, first: 20) {
          interaction_type
          timestamp
          duration
          with_product {
            name
          }
        }
      }
    }
    """
    variables = {"uid": user_uid}
    res = client.txn(read_only=True).query(query, variables)
    return json.loads(res.json)

# 3. Recomendación basada en historial de compras
def get_purchase_recommendations(client, user_uid):
    query = """
    query recommendations($uid: string) {
      user(func: uid($uid)) {
        purchased {
          name
          purchased_with {
            name
            price
          }
        }
      }
    }
    """
    variables = {"uid": user_uid}
    res = client.txn(read_only=True).query(query, variables)
    return json.loads(res.json)

# 4. Recomendación basada en productos comprados juntos (co-purchase)
def get_copurchased_products(client, product_uid):
    query = """
    query copurchase($pid: string) {
      product(func: uid($pid)) {
        name
        purchased_with {
          name
          price
        }
      }
    }
    """
    variables = {"pid": product_uid}
    res = client.txn(read_only=True).query(query, variables)
    return json.loads(res.json)

# 5. Productos Populares
# Más comprados
import json

import json

def get_popular_products(client):
    query = """
    {
      allProducts(func: type(Product)) {
        uid
        name
        purchased_count: count(purchased_by)
      }
    }
    """
    # Ejecutar consulta en Dgraph
    res = client.txn(read_only=True).query(query)
    data = json.loads(res.json)

    # Extraer lista de productos
    products = data.get("allProducts", [])

    # Ordenar en Python por purchased_count descendente
    products_sorted = sorted(
        products,
        key=lambda x: x.get("purchased_count", 0),
        reverse=True
    )

    # Devolver top 10
    return products_sorted[:10]


# Más vistos
def get_most_viewed_products(client):
    query = """
    {
      mostViewed(func: type(Product), orderdesc: count(interactions), first: 10) {
        name
        view_count: count(interactions)
      }
    }
    """
    res = client.txn(read_only=True).query(query)
    return json.loads(res.json)

# 6. Recomendación por usuarios similares
def get_similar_users(client, user_uid):
    query = """
    query similarUsers($uid: string) {
      var(func: uid($uid)) {
        purchasedProducts as purchased
      }

      similar(func: type(User)) @filter(uid_in(purchased, uid(purchasedProducts))) {
        name
        purchased {
          name
        }
      }
    }
    """
    variables = {"uid": user_uid}
    res = client.txn(read_only=True).query(query, variables)
    return json.loads(res.json)

# 7. Recomendación por productos con reseñas rating > 4
def get_top_rated_products(client):
    query = """
    {
      topRated(func: type(Product)) @cascade {
        name
        reviews @filter(gt(rating, 4)) {
          rating
        }
      }
    }
    """
    res = client.txn(read_only=True).query(query)
    return json.loads(res.json)

# 8. Análisis de comportamiento (vistas, duración, abandono)
def get_product_views(client, product_uid):
    query = """
    query productViews($pid: string) {
      product(func: uid($pid)) {
        name
        interactions @filter(eq(interaction_type, "view")) {
          timestamp
          duration
          by_user {
            name
          }
        }
      }
    }
    """
    variables = {"pid": product_uid}
    res = client.txn(read_only=True).query(query, variables)
    return res.json

# 9. Recomendación por tendencia
def get_trending_products(client):
    query = """
    {
      trending(func: type(Product)) 
        @filter(gt(count(~purchased), 20)) 
        @cascade {
          name
          popularity: count(~purchased)
          good_reviews: count(reviews @filter(gt(rating, 4)))
        }
    }
    """
    res = client.txn(read_only=True).query(query)
    return res.json

# 10. Recomendación por abandono de carrito
def get_abandoned_cart_recommendations(client, user_uid):
    query = """
    query abandoned($uid: string) {
      user(func: uid($uid)) {
        name
        has_cart {
          contains {
            name
            price
            purchased_with {
              name
            }
          }
        }
      }
    }
    """
    variables = {"uid": user_uid}
    res = client.txn(read_only=True).query(query, variables)
    return res.json
    
def debug_reviews(client):
    import json
    q = """
    {
      allReviews(func: type(Review)) {
        uid
        rating
        comment
        of_product {
          uid
          name
        }
        reviewed_by {
          uid
          name
        }
      }
    }
    """
    res = client.txn(read_only=True).query(q)
    print(json.dumps(json.loads(res.json), indent=2))
    
def debug_uids(client):
    import json
    q = """
    {
      allUsers(func: type(User)) {
        uid
        user_id
        name
        email
      }
      allProducts(func: type(Product)) {
        uid
        product_id
        name
      }
      allReviews(func: type(Review)) {
        uid
        review_id
        rating
        of_product {
          uid
          name
        }
        reviewed_by {
          uid
          name
        }
      }
      allInteractions(func: type(Interaction)) {
        uid
        interaction_id
        interaction_type
        by_user {
          uid
          name
        }
        with_product {
          uid
          name
        }
      }
      allCarts(func: type(Cart)) {
        uid
        cart_id
        cart_created_at
        contains {
          uid
          name
        }
      }
    }
    """
    res = client.txn(read_only=True).query(q)
    print(json.dumps(json.loads(res.json), indent=2))