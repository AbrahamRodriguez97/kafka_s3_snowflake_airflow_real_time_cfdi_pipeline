import os
import json
import boto3
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# 1. Configuración de Cliente AWS S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# 2. Configuración de Kafka
consumer = KafkaConsumer(
    'facturacion_baz_hot',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def stream_to_s3():
    print(f"Consumidor activo. Enviando facturas de BAZ a S3: {BUCKET_NAME}...")
    try:
        for message in consumer:
            factura = message.value
            uuid_factura = factura['Comprobante']['_UUID'] #
            file_name = f"bronze/invoice_{uuid_factura}.json"
            
            # Subir directamente el JSON a S3
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(factura)
            )
            print(f" [✓] Factura {uuid_factura} aterrizada en S3.")
            
    except Exception as e:
        print(f"Error en streaming a S3: {e}")

if __name__ == "__main__":
    stream_to_s3()