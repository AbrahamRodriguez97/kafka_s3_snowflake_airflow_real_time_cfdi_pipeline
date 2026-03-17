import json
from kafka import KafkaConsumer
import os

# Configuración del Consumidor
consumer = KafkaConsumer(
    'facturacion_baz_hot',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='grupo-procesamiento-baz',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Simulamos la ruta de "Landing Zone" (S3) - prueba local con carpeta
LANDING_ZONE = "../data/bronze_landing/"
os.makedirs(LANDING_ZONE, exist_ok=True)

def save_to_bronze(data):
    """Guarda el JSON crudo en la zona de aterrizaje (Simulación de S3)"""
    uuid_factura = data['Comprobante']['_UUID']
    file_path = f"{LANDING_ZONE}invoice_{uuid_factura}.json"
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    
    print(f" [OK] Factura {uuid_factura} aterrizada en Capa Bronze (S3).")

if __name__ == "__main__":
    print("Consumidor activo. Esperando facturas de BAZ...")
    try:
        for message in consumer:
            factura = message.value
            save_to_bronze(factura)
    except KeyboardInterrupt:
        print("Consumidor detenido.")