from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# 1. Definición de argumentos básicos
default_args = {
    'owner': 'Abraham_DataEng',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'baz_daily_invoice_processing',
    default_args=default_args,
    description='Automatización robusta S3 -> Bronze -> Silver para facturas BAZ',
    schedule_interval='@daily', 
    catchup=False
) as dag:

    # Tarea 1: Carga Masiva (S3 a Bronze)
    task_load_bronze = SnowflakeOperator(
        task_id='load_s3_to_bronze',
        snowflake_conn_id='snowflake_baz_conn', 
        sql="""
            COPY INTO BAZ_BILLING_DB.BRONZE.STG_FACTURACION_RAW (RAW_JSON, SOURCE_FILE)
            FROM (SELECT $1, metadata$filename FROM @BAZ_BILLING_DB.BRONZE.S3_BAZ_STAGE)
            FILE_FORMAT = (FORMAT_NAME = BAZ_BILLING_DB.BRONZE.JSON_FORMAT)
            ON_ERROR = 'CONTINUE';
        """
    )

    # Tarea 2: Transformación a Capa Silver (Encabezados)
    task_transform_silver_headers = SnowflakeOperator(
        task_id='transform_silver_headers',
        snowflake_conn_id='snowflake_baz_conn',
        sql="""
            MERGE INTO BAZ_BILLING_DB.SILVER.FACTURAS_ENCABEZADO AS target
            USING (
                SELECT 
                    RAW_JSON:Comprobante:_UUID::STRING AS ID,
                    RAW_JSON:Comprobante:_Version::STRING AS VER,
                    RAW_JSON:Comprobante:_Fecha::TIMESTAMP_NTZ AS FEC,
                    RAW_JSON:Comprobante:_TipoDeComprobante::STRING AS TIP,
                    RAW_JSON:Comprobante:_Total::NUMERIC(18,2) AS TOT,
                    RAW_JSON:Comprobante:Emisor:_Nombre::STRING AS E_NOM,
                    RAW_JSON:Comprobante:Emisor:_Rfc::STRING AS E_RFC,
                    RAW_JSON:Comprobante:Receptor:_Nombre::STRING AS R_NOM,
                    RAW_JSON:Comprobante:Receptor:_Rfc::STRING AS R_RFC
                FROM BAZ_BILLING_DB.BRONZE.STG_FACTURACION_RAW
            ) AS source
            ON target.ID_FACTURA = source.ID
            WHEN NOT MATCHED THEN
                INSERT (ID_FACTURA, VERSION_CFDI, FECHA_EMISION, TIPO_COMPROBANTE, TOTAL_FACTURA, EMISOR_NOMBRE, EMISOR_RFC, RECEPTOR_NOMBRE, RECEPTOR_RFC, FECHA_PROCESAMIENTO)
                VALUES (source.ID, source.VER, source.FEC, source.TIP, source.TOT, source.E_NOM, source.E_RFC, source.R_NOM, source.R_RFC, CURRENT_TIMESTAMP());
        """
    )

    # Definición del flujo
    task_load_bronze >> task_transform_silver_headers