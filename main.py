from pupulate import populate_cassandra
from pupulate import populate_mongodb
from Cassandra import cQuerys
from connect import conectar_cassandra
import datetime
from Mongo import mongo_queries
from connect import conectar_mongodb
from connect import conectar_dgraph
from connect import conectar_mongodb, conectar_cassandra
import pydgraph
import sys

 
from Dgraph.model import *
from pupulate import *



client = conectar_dgraph()
if client is None:
    print("No se pudo conectar a Dgraph")
    
def connect_dgraph_local():
    client_stub = pydgraph.DgraphClientStub('localhost:9080')
    return pydgraph.DgraphClient(client_stub)

def main_menu():
    db = conectar_mongodb()
    while True:
         print("\n===== MENÚ PRINCIPAL =====")
         print("1. Usuarios")
         print("2. Productos")
         print("3. Carritos")
         print("4. Inicializar base de datos (populate)")
         print("5. DROP ALL ")
         print("0. Salir")
         choice = input("Selecciona una opción: ")

         if choice == "1":
            menu_usuarios(db)
         elif choice == "2":
            menu_productos(db)
         elif choice == "3":
            menu_carritos(db)
         elif choice == "4":
            poblar_todo()
         elif choice == "5":
            drop_all_databases()
         elif choice == "0":
            print("Saliendo del sistema...")
            break
         else:
            print(" Opción inválida. Intenta de nuevo.")




def poblar_todo():
    print("\n===============================")
    print("  Poblando bases de datos")
    print("===============================\n")

    populate_mongodb()     # MongoDB
    populate_cassandra()   # Cassandra
    # populate_dgraph()    # Pendiente

    print("Bases pobladas \n")

def menu_usuarios(db):
    db = conectar_mongodb()
    session = conectar_cassandra(keyspace="mi_ecommerce")
    if session is None :
        print("conectalo bien")
    while True:
        print("\n--- MENÚ USUARIOS ---")
        print("1. Consultar wishlist de usuario")
        print("2. Coincidencias entre carrito y wishlist")
        print("3. Registro de vistas por producto")
        print("4. Historial de busqueda")
        print("5. Registro de compras")
        print("6. Tiempo de permanencia")
        print("7. Clics en recomendaciones")
        print("8. interacciones con filtros")
        print("9. sesiones de navegacion")
        print("10.productos vistos no comprados")
        print("11.recomendaciones mostradas")
        print("12.Comparacion de productos")
        print("13.metricas por categoria")
        print("14.feedback sobre recomendaciones")
        print("0. Regresar")
        opcion = input("Selecciona opción: ")

        if opcion == "1":
            nombre = input("Nombre del usuario: ")
            resultado = mongo_queries.wishlist_usuario(db, nombre)
            print(resultado)
        elif opcion == "2":
            nombre = input("Nombre del usuario: ")
            resultado = mongo_queries.coincidencias_carrito_wishlist(db, nombre)
            print(resultado)
        elif opcion == "3":
             cQuerys.productView(session)
        elif opcion == "4":
            user_id = input("Ingrese el id del usuario: ")
            cQuerys.searchHistory(session, user_id)
            
        elif opcion == "5":
            user_id = input("ingresa el id del usario: ")
            cQuerys.getPurchases(session,user_id)
        elif opcion == "6":
            user_id = input("Ingrese el UUID del usuario: ")
            cQuerys.getProductDuration(session, user_id)
        elif opcion == "7":
             user_id = input("Ingrese el UUID del usuario: ")
             cQuerys.getRecommendationClicks(session,user_id)
        elif opcion== "8":
            user_id = input("Ingrese el UUID del usuario: ")
            cQuerys.getFilterInteractions(session, user_id)
        elif opcion == "9":
            user_id = input("Ingrese el UUID del usuario: ")
            cQuerys.getNavigationSessions(session, user_id)
        elif opcion == "10":
            user_id = input("Ingrese el UUID del usuario: ")
            
            fecha_inicio_str = input("Fecha inicio (YYYY-MM-DD) o ENTER para omitir: ")
            fecha_fin_str = input("Fecha fin (YYYY-MM-DD) o ENTER para omitir: ")

            fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d") if fecha_inicio_str else None
            fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d") if fecha_fin_str else None

            cQuerys.getUnpurchasedViews(session, user_id, fecha_inicio, fecha_fin)  
        elif opcion == "11":
            user_id = input("Ingrese el UUID del usuario: ")
            # Opcional: pedir rango de fechas
            fecha_inicio_str = input("Fecha inicio (YYYY-MM-DD) o ENTER para omitir: ")
            fecha_fin_str = input("Fecha fin (YYYY-MM-DD) o ENTER para omitir: ")

            fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d") if fecha_inicio_str else None
            fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d") if fecha_fin_str else None

            cQuerys.getRecommendationsShown(session, user_id, fecha_inicio, fecha_fin)
        elif opcion == "12":
             user_id = input("Ingrese el UUID del usuario: ")
            # Opcional: pedir rango de fechas
             fecha_inicio_str = input("Fecha inicio (YYYY-MM-DD) o ENTER para omitir: ")
             fecha_fin_str = input("Fecha fin (YYYY-MM-DD) o ENTER para omitir: ")
             fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d") if fecha_inicio_str else None
             fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d") if fecha_fin_str else None

             cQuerys.getProductComparisons(session, user_id, fecha_inicio, fecha_fin)
        
        elif opcion == "13":
            user_id = input("Ingrese el UUID del usuario: ")
            category_id = input("Ingrese el UUID de la categoría: ")

            fecha_inicio_str = input("Fecha inicio (YYYY-MM-DD) o ENTER para omitir: ")
            fecha_fin_str = input("Fecha fin (YYYY-MM-DD) o ENTER para omitir: ")

            fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d") if fecha_inicio_str else None
            fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d") if fecha_fin_str else None

            cQuerys.getCategoryMetrics(session, user_id, category_id, fecha_inicio, fecha_fin)
        elif opcion == "14":  # Feedback sobre recomendaciones
            user_id = input("Ingrese el UUID del usuario: ")

            fecha_inicio_str = input("Fecha inicio (YYYY-MM-DD) o ENTER para omitir: ")
            fecha_fin_str = input("Fecha fin (YYYY-MM-DD) o ENTER para omitir: ")

            fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d") if fecha_inicio_str else None
            fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d") if fecha_fin_str else None

            cQuerys.getRecommendationFeedback(session, user_id, fecha_inicio, fecha_fin)
        elif opcion == "0":
            break
        else:
            print("Opción inválida, intenta de nuevo.")


def menu_productos(db):
    db = conectar_mongodb()
    session = conectar_cassandra(keyspace="mi_ecommerce")
    client = conectar_dgraph()
    while True:
        print("\n--- MENÚ PRODUCTOS ---")
        print("1. Consultar productos por rango de precio")
        print("2. Consultar producto específico por rango de precio")
        print("3. Nuevos lanzamientos por vendedor")
        print("4. Consultar productos por categoría")
        print("5. Consultar productos disponibles por nombre")
        print("6. Consultar productos por vendedor y categoría")
        print("7.  reviews")
        print("8. productos comprados juntos")
        print("9. productos mas populares")
        print("0. Regresar")
        opcion = input("Selecciona opción: ")

        if opcion == "1":
            minimo = int(input("Precio mínimo: "))
            maximo = int(input("Precio máximo: "))
            resultado = mongo_queries.productos_por_precio(db, minimo, maximo)
            print(resultado)
        elif opcion == "2":
            nombre = input("Nombre del producto: ")
            minimo = int(input("Precio mínimo: "))
            maximo = int(input("Precio máximo: "))
            resultado = mongo_queries.producto_especifico_precio(db, nombre, minimo, maximo)
            print(resultado)
        elif opcion == "3":
            vendedor = input("Nombre del vendedor: ")
            resultado = mongo_queries.nuevos_lanzamientos(db, vendedor)
            print(resultado)
        elif opcion == "4":
            categoria = input("Categoría: ")
            resultado = mongo_queries.productos_por_categoria(db, categoria)
            print(resultado)
        elif opcion == "5":
            nombre = input("Nombre parcial del producto: ")
            resultado = mongo_queries.productos_disponibles(db, nombre)
            print(resultado)
        elif opcion == "6":
            vendedor = input("Nombre del vendedor: ")
            categoria = input("Categoría: ")
            resultado = mongo_queries.productos_vendedor_categoria(db, vendedor, categoria)
            print(resultado)
        elif opcion == "7":
            product_id = input("ID del producto: ")
            print(get_reviews(client, product_id))
        elif opcion == "8":
            res = get_copurchased_products(client, "some-product-id")
            print(json.dumps(res, indent=2, ensure_ascii=False))

        elif opcion == "9":
             print("Productos más populares:")
             res1 = get_popular_products(client)
        
        elif opcion == "10":
            print(get_most_viewed_products(client))   
        elif opcion == "11":
            print(get_top_rated_products(client))
        elif opcion == "12":
            product_uid = input("UID del producto: ")
            print(get_product_views(client, product_uid))
        elif opcion == "13":
            print(get_trending_products(client)) 
        elif opcion == "0":
            break
        else:
            print("Opción inválida, intenta de nuevo.")
            
def menu_carritos(db):
    db = conectar_mongodb()
    session = conectar_cassandra(keyspace="mi_ecommerce")
    if session is None :
        print("conectalo bien")
    while True:
        print("\n--- MENÚ CARRITOS ---")
        print("1. Consultar carrito de usuario")
        print("2. Productos más frecuentes en carritos")
        print("3. Abandono de carrito")
        print("0. Regresar")
        opcion = input("Selecciona opción: ")

        if opcion == "1":
            nombre = input("Nombre del usuario: ")
            resultado = mongo_queries.consultar_carrito(db, nombre)
            print(resultado)
        elif opcion == "2":
            resultado = mongo_queries.productos_mas_en_carritos(db)
            print(resultado)
        elif opcion == "7":
             user_id = input("Ingrese el UUID del usuario: ")
             cQuerys.getAbandonedCart(session, user_id)
        elif opcion == "0":
            break
        else:
            print("Opción inválida, intenta de nuevo.")



def drop_mongodb():
    print("Conectando a MongoDB...")
    db = conectar_mongodb(db_name="mi_ecommerce")
    if db is not None:  # <- cambio aquí
        confirm = input("  Esto eliminará todas las colecciones de MongoDB. ¿Continuar? (s/n): ")
        if confirm.lower() == 's':
            db.products.drop()
            db.users.drop()
            db.carts.drop()
            print(" MongoDB eliminado completamente.")
        else:
            print("MongoDB: operación cancelada.")
    else:
        print(" No se pudo conectar a MongoDB.")


def drop_cassandra():
    print("Conectando a Cassandra...")
    session = conectar_cassandra(keyspace="mi_ecommerce")
    if session:
        confirm = input("  Esto eliminará todas las tablas de Cassandra. ¿Continuar? (s/n): ")
        if confirm.lower() == 's':
            tablas = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='mi_ecommerce';")
            for t in tablas:
                session.execute(f"DROP TABLE IF EXISTS {t.table_name};")
            print(" Cassandra eliminado completamente.")
        else:
            print("Cassandra: operación cancelada.")
    else:
        print(" No se pudo conectar a Cassandra.")

def drop_dgraph():
    print("Conectando a Dgraph...")
    try:
        client_stub = pydgraph.DgraphClientStub('localhost:9080')
        client = pydgraph.DgraphClient(client_stub)
        confirm = input("  Esto eliminará todo en Dgraph. ¿Continuar? (s/n): ")
        if confirm.lower() == 's':
            op = pydgraph.Operation(drop_all=True)
            client.alter(op)
            print(" Dgraph eliminado completamente.")
        else:
            print("Dgraph: operación cancelada.")
    except Exception as e:
        print(f" Error conectando a Dgraph: {e}")
    finally:
        client_stub.close()

def drop_all_databases():
    print("\n=== DROP ALL DATABASES ===")
    drop_mongodb()
    drop_cassandra()
    drop_dgraph()
    print("=== Operación completa ===\n")


# ======================================================
#             EJECUCIÓN DEL MENÚ
# ======================================================
if __name__ == "__main__":
    main_menu()
