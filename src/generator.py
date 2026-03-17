import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

# Configuración del Productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'facturacion_baz_hot'

def generate_cfdi_payload():
    """Genera un CFDI 4.0 basado en los requerimientos de BAZ """
    invoice_id = str(uuid.uuid4())
    tipo = random.choice(["I", "E"]) # Ingreso o Egreso
    
    # Estructura extraída de documentos técnicos (modificados por privacidad)
    payload = {
        "Comprobante": {
            "Emisor": {
                "_Nombre": "NUEVA ELEKTRA DEL MILENIO", #
                "_Rfc": "ECE9610253TA",
                "_RegimenFiscal": "623"
            },
            "Receptor": {
                "_Nombre": "BANCO AZTECA SA", #
                "_Rfc": "BAI0205236Y8",
                "_UsoCFDI": "G02"
            },
            "_UUID": invoice_id,
            "_Fecha": datetime.now().isoformat(),
            "_TipoDeComprobante": tipo,
            "_Version": "4.0",
            "_Total": round(random.uniform(100.0, 50000.0), 2),
            "Conceptos": {
                "Concepto": {
                    "_Descripcion": "Servicios de Consultoría de Datos",
                    "_Importe": "1000.00",
                    "_ValorUnitario": "1000.00"
                }
            }
        }
    }
    return payload

def start_streaming():
    print(f"Iniciando migración en tiempo real al tópico: {TOPIC_NAME}...")
    try:
        while True:
            data = generate_cfdi_payload()
            producer.send(TOPIC_NAME, data)
            print(f" [✓] CFDI enviado (Total: ${data['Comprobante']['_Total']})")
            time.sleep(2) # Simula flujo constante de facturas
    except KeyboardInterrupt:
        print("\nTransmisión detenida por el usuario.")

if __name__ == "__main__":
    start_streaming()