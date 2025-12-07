from cassandra.query import SimpleStatement
from uuid import UUID
from datetime import datetime
import uuid
from connect import conectar_mongodb

def productView(session):
    # Conectar a Mongo
    mongo = conectar_mongodb(db_name="mi_ecommerce")

    # Pedir al usuario
    user_id = input("Ingresa el UUID del usuario: ")
    fecha_inicio = input("Ingresa fecha inicio (YYYY-MM-DD): ")
    fecha_fin = input("Ingresa fecha fin (YYYY-MM-DD): ")

    # Convertir tipos
    user_id = uuid.UUID(user_id)
    fecha_inicio = datetime.strptime(fecha_inicio, "%Y-%m-%d")
    fecha_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")

    # Ejecutar query en Cassandra
    query = """
        SELECT product_id, timestamp
        FROM product_views
        WHERE user_id = %s
        AND timestamp >= %s
        AND timestamp <= %s
        ALLOW FILTERING;
    """
    rows = session.execute(query, (user_id, fecha_inicio, fecha_fin))

    print("\n=== Historial de vistas de productos ===")
    print(f"{'Producto':36} | {'Nombre':20} | {'Precio':7} | {'Fecha y hora'}")
    print("-"*90)

    for row in rows:
        pid = str(row.product_id)
        fecha = row.timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # Buscar en Mongo
        producto = mongo.products.find_one({"product_id": pid}, {"name": 1, "price": 1, "_id": 0})

        if producto:
            nombre = producto.get("name", "Desconocido")
            precio = producto.get("price", "N/A")
        else:
            nombre = "No encontrado"
            precio = "N/A"

        print(f"{pid} | {nombre:<20} | ${precio:<6} | {fecha}")
    

def searchHistory(session, user_id):
    print("\n Consultando historial de búsqueda...")

    try:
        user_id = UUID(user_id)  

        query = """
        SELECT search_terms, timestamp
        FROM search_history
        WHERE user_id = ?
        ORDER BY timestamp DESC;
        """

        stmt = session.prepare(query)
        rows = session.execute(stmt, [user_id])

        print("\n Historial de búsqueda:")
        for row in rows:
            print(f"- {row.search_terms} | {row.timestamp}")

    except Exception as e:
        print(f" Error ejecutando la consulta: {e}")

        query = """
        SELECT search_terms, timestamp 
        FROM search_history 
        WHERE user_id = %s
        ORDER BY timestamp DESC;
        """

        rows = session.execute(query, (user_id,))

        print(f"\n Historial de búsqueda del usuario {user_id}:\n")

        for row in rows:
            print(f"- {row.timestamp} → '{row.search_terms}'")

        print("\nConsulta finalizada.\n")

    except Exception as e:
        print(f" Error ejecutando la consulta: {e}")

def getPurchases(session, user_id):
    print("\n Consultando historial de compras...")

    try:
        # Convertir string a UUID real
        user_id = UUID(user_id)

        query = """
        SELECT product_id, purchase_date, price
        FROM purchases
        WHERE user_id = ?
        ORDER BY purchase_date ASC;
        """
 
        stmt = session.prepare(query)
        rows = session.execute(stmt, [user_id])

        print("\n🛒 Historial de Compras:")
        found = False
        for row in rows:
            found = True
            print(f"- Producto: {row.product_id} | Fecha: {row.purchase_date} | Precio: ${row.price}")

        if not found:
            print(" El usuario no tiene compras registradas.")

    except Exception as e:
        print(f" Error ejecutando la consulta: {e}")

def getProductDuration(session, user_id):
    print("\n Consultando tiempo de permanencia en productos...")

    try:
        # Convertir string a UUID válido
        user_id = UUID(user_id)

        query = """
        SELECT product_id, duration_seconds, timestamp
        FROM product_duration
        WHERE user_id = ?
        ORDER BY timestamp ASC;
        """

        stmt = session.prepare(query)
        rows = session.execute(stmt, [user_id])

        print("\n Tiempo de permanencia por producto:")
        found = False
        for row in rows:
            found = True
            print(f"- Producto: {row.product_id} | Duración: {row.duration_seconds}s | Fecha: {row.timestamp}")

        if not found:
            print(" El usuario no tiene datos de permanencia registrados.")

    except Exception as e:
        print(f"Error ejecutando la consulta: {e}")

def getRecommendationClicks(session, user_id):
    print("\n Consultando clics en recomendaciones...")

    try:
        # Convertir string a UUID
        user_id = UUID(user_id)

        query = """
        SELECT recommended_product_id, click_timestamp
        FROM recommendation_clicks
        WHERE user_id = ?
        ORDER BY click_timestamp ASC;
        """

        stmt = session.prepare(query)
        rows = session.execute(stmt, [user_id])

        print("\n Clics en productos recomendados:")

        found = False
        for row in rows:
            found = True
            print(f"- Producto recomendado: {row.recommended_product_id} | Fecha del clic: {row.click_timestamp}")

        if not found:
            print(" El usuario no tiene clics registrados en recomendaciones.")

    except Exception as e:
        print(f" Error ejecutando la consulta: {e}")

from uuid import UUID

def getCartProducts(session, user_id):
    print("\n Consultando productos agregados al carrito...")

    try:
        # Convertir el UUID a tipo válido
        user_id = UUID(user_id)

        query = """
        SELECT product_id, add_to_cart_time, quantity
        FROM cart_products
        WHERE user_id = ?
        ORDER BY add_to_cart_time ASC;
        """

        stmt = session.prepare(query)
        rows = session.execute(stmt, [user_id])

        print("\n📦 Historial de productos agregados al carrito:")

        found = False
        for row in rows:
            found = True
            print(f"- Producto: {row.product_id} | Cantidad: {row.quantity} | Fecha: {row.add_to_cart_time}")

        if not found:
            print(" El usuario no ha agregado productos al carrito.")

    except Exception as e:
        print(f" Error ejecutando la consulta: {e}")

def getAbandonedCart(session, user_id):
    print("\n Consultando carritos abandonados...")

    try:
        # Convertir la cadena a UUID válido
        user_id = UUID(user_id)

        query = """
        SELECT product_id, abandon_time, cart_value
        FROM abandoned_cart
        WHERE user_id = ?
        ORDER BY abandon_time ASC;
        """

        stmt = session.prepare(query)
        rows = session.execute(stmt, [user_id])

        print("\n❗ Historial de carritos abandonados:")

        found = False
        for row in rows:
            found = True
            print(f"- Producto: {row.product_id} | Valor del carrito: {row.cart_value} | Fecha: {row.abandon_time}")

        if not found:
            print(" El usuario no ha abandonado ningún carrito.")

    except Exception as e:
        print(f" Error ejecutando la consulta: {e}")

def getFilterInteractions(session, user_id):
    print("\n Consultando interacciones con filtros...")

    try:
        # Convertir cadena → UUID
        user_id = UUID(user_id)

        query = """
        SELECT filter_type, filter_value, timestamp
        FROM filter_interactions
        WHERE user_id = ?
        ORDER BY timestamp ASC;
        """

        stmt = session.prepare(query)
        rows = session.execute(stmt, [user_id])

        print("\n📌 Historial de filtros aplicados:")

        found = False
        for row in rows:
            found = True
            print(f"- [{row.timestamp}] Tipo: {row.filter_type} | Valor: {row.filter_value}")

        if not found:
            print(" El usuario no ha aplicado filtros.")

    except Exception as e:
        print(f" Error ejecutando la consulta: {e}")


def getNavigationSessions(session, user_id):
    print("\n Consultando sesiones de navegación del usuario...")

    try:
        # Convertir a UUID
        user_id = UUID(user_id)

        query = """
        SELECT session_id, start_time, end_time, device_type
        FROM navigation_sessions
        WHERE user_id = ?
        ALLOW FILTERING;
        """

        stmt = session.prepare(query)
        rows = session.execute(stmt, [user_id])

        print("\n Sesiones encontradas:")

        found = False
        for row in rows:
            found = True
            print(f"\n Sesión: {row.session_id}")
            print(f"    Inicio: {row.start_time}")
            print(f"    Fin: {row.end_time}")
            print(f"    Dispositivo: {row.device_type}")

        if not found:
            print(" Este usuario no tiene sesiones registradas.")

    except Exception as e:
        print(f" Error ejecutando consulta: {e}")


def getUnpurchasedViews(session, user_id, fecha_inicio=None, fecha_fin=None):
    """
    Consulta los productos que un usuario vio, indicando si fueron comprados.
    """
    print("\n Consultando productos vistos pero no comprados...")

    try:
        user_id = UUID(user_id)

        query = """
        SELECT product_id, view_time, purchase_flag
        FROM unpurchased_views
        WHERE user_id = ?
        """
        
        params = [user_id]

        # Filtrado por rango de fechas
        if fecha_inicio and fecha_fin:
            query += " AND view_time >= ? AND view_time <= ?"
            params.extend([fecha_inicio, fecha_fin])

        query += " ALLOW FILTERING;"

        stmt = session.prepare(query)
        rows = session.execute(stmt, params)

        found = False
        for row in rows:
            found = True
            status = " Comprado" if row.purchase_flag else " No comprado"
            print(f"\n Producto: {row.product_id}")
            print(f"    Vista: {row.view_time}")
            print(f"    Estado: {status}")

        if not found:
            print(" No se encontraron vistas registradas para este usuario.")

    except Exception as e:
        print(f" Error ejecutando consulta: {e}")

from uuid import UUID
from datetime import datetime

def getRecommendationsShown(session, user_id, fecha_inicio=None, fecha_fin=None):
    """
    Consulta las recomendaciones mostradas a un usuario.
    """
    print("\n Consultando recomendaciones mostradas...")

    try:
        user_id = UUID(user_id)

        query = """
        SELECT recommended_product_id, display_time, algorithm_version
        FROM recommendations_shown
        WHERE user_id = ?
        """
        
        params = [user_id]

        # Filtrado por rango de fechas
        if fecha_inicio and fecha_fin:
            query += " AND display_time >= ? AND display_time <= ?"
            params.extend([fecha_inicio, fecha_fin])

        query += " ALLOW FILTERING;"

        stmt = session.prepare(query)
        rows = session.execute(stmt, params)

        found = False
        for row in rows:
            found = True
            print(f"\n Producto recomendado: {row.recommended_product_id}")
            print(f"   Mostrado: {row.display_time}")
            print(f"   Algoritmo: {row.algorithm_version}")

        if not found:
            print(" No se encontraron recomendaciones registradas para este usuario.")

    except Exception as e:
        print(f"Error ejecutando consulta: {e}")

def getWishlist(session, user_id, fecha_inicio=None, fecha_fin=None):
    """
    Consulta la wishlist de un usuario en Cassandra.
    """
    print("\n Consultando wishlist del usuario...")

    try:
        user_id = UUID(user_id)

        query = """
        SELECT product_id, wishlist_add_time
        FROM wishlist
        WHERE user_id = ?
        """

        params = [user_id]

        # Filtrado por rango de fechas
        if fecha_inicio and fecha_fin:
            query += " AND wishlist_add_time >= ? AND wishlist_add_time <= ?"
            params.extend([fecha_inicio, fecha_fin])

        query += " ALLOW FILTERING;"

        stmt = session.prepare(query)
        rows = session.execute(stmt, params)

        found = False
        for row in rows:
            found = True
            print(f"\n Producto en wishlist: {row.product_id}")
            print(f"   Agregado: {row.wishlist_add_time}")

        if not found:
            print(" No se encontraron productos en la wishlist de este usuario.")

    except Exception as e:
        print(f" Error ejecutando consulta: {e}")



def getProductComparisons(session, user_id, fecha_inicio=None, fecha_fin=None):
    """
    Consulta los eventos de comparaciones de productos de un usuario en Cassandra.
    """
    print("\n Consultando comparaciones de productos del usuario...")

    try:
        user_id = UUID(user_id)

        query = """
        SELECT product_ids_compared, compare_time
        FROM product_comparisons
        WHERE user_id = ?
        """

        params = [user_id]

        # Filtrado por rango de fechas
        if fecha_inicio and fecha_fin:
            query += " AND compare_time >= ? AND compare_time <= ?"
            params.extend([fecha_inicio, fecha_fin])

        query += " ALLOW FILTERING;"

        stmt = session.prepare(query)
        rows = session.execute(stmt, params)

        found = False
        for row in rows:
            found = True
            print(f"\n Comparación realizada el: {row.compare_time}")
            print("   Productos comparados:")
            for pid in row.product_ids_compared:
                print(f"   - {pid}")

        if not found:
            print(" No se encontraron comparaciones para este usuario.")

    except Exception as e:
        print(f" Error ejecutando consulta: {e}")


def getCategoryMetrics(session, user_id, category_id, fecha_inicio=None, fecha_fin=None):
    """
    Consulta métricas por categoría de un usuario en Cassandra.
    """
    print("\n Consultando métricas de categoría...")

    try:
        user_id = UUID(user_id)
        category_id = UUID(category_id)

        query = """
        SELECT week_start, views, clicks, purchases
        FROM category_metrics
        WHERE user_id = ? AND category_id = ?
        """
        params = [user_id, category_id]

        # Filtrado por rango de fechas
        if fecha_inicio and fecha_fin:
            query += " AND week_start >= ? AND week_start <= ?"
            params.extend([fecha_inicio, fecha_fin])

        query += " ALLOW FILTERING;"

        stmt = session.prepare(query)
        rows = session.execute(stmt, params)

        found = False
        for row in rows:
            found = True
            print(f"\n📌 Semana que inicia el: {row.week_start}")
            print(f"   Views: {row.views}, Clicks: {row.clicks}, Purchases: {row.purchases}")

        if not found:
            print(" No se encontraron métricas para este usuario y categoría.")

    except Exception as e:
        print(f" Error ejecutando consulta: {e}")

def getRecommendationFeedback(session, user_id, fecha_inicio=None, fecha_fin=None):
    """
    Consulta el feedback de recomendaciones de un usuario en Cassandra.
    """
    print("\n Consultando feedback de recomendaciones...")

    try:
        user_id = UUID(user_id)

        query = """
        SELECT feedback_time, product_id, feedback_score
        FROM recommendation_feedback
        WHERE user_id = ?
        """
        params = [user_id]

        # Filtrado por rango de fechas
        if fecha_inicio and fecha_fin:
            query += " AND feedback_time >= ? AND feedback_time <= ?"
            params.extend([fecha_inicio, fecha_fin])

        query += " ALLOW FILTERING;"

        stmt = session.prepare(query)
        rows = session.execute(stmt, params)

        found = False
        for row in rows:
            found = True
            print(f"\n Feedback registrado el: {row.feedback_time}")
            print(f"   Producto: {row.product_id}")
            print(f"   Score: {row.feedback_score}")

        if not found:
            print(" No se encontraron feedbacks para este usuario.")

    except Exception as e:
        print(f" Error ejecutando consulta: {e}")















