import os
import json
from kafka import KafkaConsumer
import snowflake.connector
from dotenv import load_dotenv

# 1. Cargar variables de entorno
load_dotenv()

# 2. Configuración de Conexión a Snowflake
def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv('SN_USER'),
        password=os.getenv('SN_PASSWORD'),
        account=os.getenv('SN_ACCOUNT'),
        warehouse=os.getenv('SN_WAREHOUSE'),
        database=os.getenv('SN_DATABASE'),
        schema=os.getenv('SN_SCHEMA')
    )

# 3. Configuración de Kafka
consumer = KafkaConsumer(
    'facturacion_baz_hot',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def ingest_to_snowflake():
    conn = get_snowflake_conn()
    cur = conn.cursor()
    print("Conexión exitosa. Esperando facturas de BAZ para ingesta...")

    try:
        for message in consumer:
            factura_json = message.value
            uuid_factura = factura_json['Comprobante']['_UUID'] #
            
            # Insertar el JSON crudo en la tabla Bronze 
            sql = "INSERT INTO STG_FACTURACION_RAW (RAW_JSON, SOURCE_FILE) SELECT PARSE_JSON(%s), %s"
            cur.execute(sql, (json.dumps(factura_json), f"stream_{uuid_factura}"))
            
            print(f" [✓] Factura {uuid_factura} enviada a Snowflake Bronze Layer.")
            
    except Exception as e:
        print(f"Error en la ingesta: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    ingest_to_snowflake()