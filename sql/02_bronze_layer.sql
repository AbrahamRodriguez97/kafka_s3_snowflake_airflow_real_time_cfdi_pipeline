-- 1. Cargar todos los archivos de S3 a la tabla Bronze de un solo golpe
COPY INTO BAZ_BILLING_DB.BRONZE.STG_FACTURACION_RAW (RAW_JSON, SOURCE_FILE)
FROM (
    SELECT 
        $1,                -- El contenido completo del JSON
        metadata$filename  -- El nombre del archivo en S3
    FROM @BAZ_BILLING_DB.BRONZE.S3_BAZ_STAGE
)
FILE_FORMAT = (FORMAT_NAME = BAZ_BILLING_DB.BRONZE.JSON_FORMAT)
ON_ERROR = 'CONTINUE';

-- Verificar ahora la carga
SELECT * FROM BAZ_BILLING_DB.BRONZE.STG_FACTURACION_RAW LIMIT 10;