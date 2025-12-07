# connect.py
from pymongo import MongoClient
from cassandra.cluster import Cluster
import time
import pydgraph

# =========================
#       MONGODB
# =========================
def conectar_mongodb(uri="mongodb://localhost:27017", db_name="mi_ecommerce", retries=5, delay=3):
    """
    Conecta a MongoDB con reintentos automáticos.
    """
    for i in range(retries):
        try:
            cliente = MongoClient(uri, serverSelectionTimeoutMS=3000)
            cliente.server_info()  # Lanza excepción si no puede conectarse
            db = cliente[db_name]
            
            return db
        except Exception as e:
            print(f"Intento {i+1}/{retries}: Error MongoDB: {e}")
            time.sleep(delay)
    print(" No se pudo conectar a MongoDB después de varios intentos.")
    return None

# =========================
#       CASSANDRA
# =========================
def conectar_cassandra(hosts=["127.0.0.1"], keyspace="bdnr_cassandra", port=9042, retries=5, delay=5):
    """
    Conecta a Cassandra, crea el keyspace si no existe y devuelve la sesión.
    """
    for i in range(retries):
        try:
            cluster = Cluster(contact_points=hosts, port=port)
            session = cluster.connect()

            # Crear keyspace si no existe
            session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            
            # Seleccionar keyspace
            session.set_keyspace(keyspace)
           
            return session
        except Exception as e:
            print(f"Intento {i+1}/{retries}: Error Cassandra: {e}")
            time.sleep(delay)
    print(" No se pudo conectar a Cassandra después de varios intentos.")
    return None



def conectar_dgraph(host="localhost:9080"):
    """
    Conecta a Dgraph y devuelve el cliente.
    """
    try:
        client_stub = pydgraph.DgraphClientStub(host)
        client = pydgraph.DgraphClient(client_stub)
        print(" Dgraph conectado correctamente")
        return client
    except Exception as e:
        print(f" Error al conectar a Dgraph: {e}")
        return None

# =========================
#       EJEMPLO DE USO
# =========================
if __name__ == "__main__":
    mongo_db = conectar_mongodb()
    cassandra_session = conectar_cassandra()

"""def conectar_dgraph(host="localhost:9080"):
    try:
        client_stub = pydgraph.DgraphClientStub(host)
        client = pydgraph.DgraphClient(client_stub)
        print("dgraph arriba")
        return client
    except Exception as e:
        print(f"Error Dgraph: {e}")
        return None

if __name__ == "__main__":
    mongo_db = conectar_mongodb()
    cassandra_session = conectar_cassandra()
    dgraph_client = conectar_dgraph()
"""


