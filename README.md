# Real-Time CFDI 4.0 Data Pipeline: S3 to Snowflake via Airflow

<img width="975" height="385" alt="image" src="https://github.com/user-attachments/assets/73d1ebb7-2c2a-4dab-a2cf-8c60cfce968a" />



## Introducción
**Nombre del Cliente:**  Grupo Salinas

**Rol Desempeñado en el Proyecto:**  Consultor

**Fecha de Inicio:** diciembre 2022

**Fecha de Fin:**   agosto 2023

**Descripción del Proyecto:**  Implementación de la solución para facturación electrónica

**Principales Responsabilidades:**  
* Levantamiento de requerimientos para la instalación de Vertica 
* Instalación de Vertica con una consola de administración. 
* Asesoría en el uso del producto. 
* Optimización de consultas para mejorar el performance.

**Detalle de proyecto:**

El proyecto original consistió en la implementación de una solución de facturación electrónica para Grupo Salinas (BAZ). El reto principal era que el sistema de base de datos anterior no estaba optimizado para procesar datos "en caliente" (tiempo real). 

Mi intervención técnica permitió:

1.	Simular y validar la carga de archivos JSON mediante flex tables.
2.	Migrar masivamente históricos y establecer una migración en tiempo real de facturas hacia ambientes productivos.
3.	Optimizar el performance mediante el ajuste de consultas y estructuras de almacenamiento.

**Adaptación:**

Recrearemos esta solución migrando los archivos JSON de facturas (CFDI 4.0) hacia un Data Warehouse moderno (Snowflake), aplicando una Arquitectura Medallion (Bronze, Silver, Gold) para garantizar la calidad y velocidad que las empresas y proyectos exigen hoy.

## Resumen del proyecto
Este proyecto simula un ecosistema de datos de alto volumen para el procesamiento de facturación electrónica, inspirado en los retos operativos de grandes retailers como Grupo Salinas (BAZ). El pipeline automatiza la ingesta, validación y transformación de datos financieros, pasando de eventos en tiempo real hacia una arquitectura de almacenamiento Medallion en la nube.

Como Ingeniero de Datos, mi objetivo fue resolver la fragmentación de datos y asegurar la integridad financiera mediante procesos idempotentes y escalables.

## Arquitectura técnica

El flujo de datos sigue el estándar de la industria para procesamiento de datos moderno:

* **Ingesta & Streaming:** Un generador simula ventas en tiempo real, enviando eventos JSON a Apache Kafka.

<img width="1058" height="199" alt="image" src="https://github.com/user-attachments/assets/2ada0ac7-5d60-4fea-acab-92c2ad76c3b6" />


* **Staging (Bronze):** Los datos se depositan en un bucket de Amazon S3 para su almacenamiento persistente de bajo costo.

<img width="1033" height="768" alt="image" src="https://github.com/user-attachments/assets/5dc7cb36-5734-4bbc-910c-5f3c89be634c" />


* **Data Warehousing (Snowflake):**  
    Capa Bronze: Almacenamiento de datos crudos (Semi-estructurados) mediante COPY INTO.

<img width="1064" height="395" alt="image" src="https://github.com/user-attachments/assets/3e6b8ccc-a770-46f8-aa41-bd0773d43e25" />


Capa Silver: Limpieza, tipado y estructuración de datos mediante lógica de MERGE (Upsert).

<img width="1052" height="124" alt="image" src="https://github.com/user-attachments/assets/6dbf45ff-56c6-4e6d-991e-dae2277c40ea" />


* **Orquestación:** Apache Airflow (Dockerizado) gestiona las dependencias, reintentos y el flujo de trabajo programado.

<img width="1304" height="252" alt="image" src="https://github.com/user-attachments/assets/8ae2eb5c-19e0-4374-ad58-c922817aef94" />


## Retos de ingeniería y soluciones

**1. Garantizando la Idempotencia (El problema del duplicado)**

Durante el desarrollo, nos enfrentamos a errores de integridad por llaves duplicadas (UUIDs). En un entorno financiero, cargar dos veces la misma factura es un error crítico.

**Solución:** Implementé una lógica de Upsert utilizando la sentencia ```MERGE``` en Snowflake. Esto asegura que si el pipeline se ejecuta múltiples veces, solo se inserten registros nuevos o se actualicen los existentes, haciendo que el pipeline sea idempotente.

**2. Troubleshooting de infraestructura cloud**

La conexión entre Airflow (en contenedores) y Snowflake presentó retos de resolución de nombres y compatibilidad de librerías.

**Solución:** Resolví conflictos de dependencias críticas de Python (```pyarrow```) dentro de la imagen de Docker y optimicé la configuración de red y autenticación global de Snowflake, permitiendo una conectividad robusta y segura.

## Stack tecnológico

* **Lenguajes:** Python (Scripts de ingesta), SQL (Snowflake Dialect).
* **Cloud:** AWS (S3), Snowflake (Data Warehouse).
* **Data Tools:** Apache Kafka, Apache Airflow..
* **Infraestructura:** Docker & Docker Compose.

## Cómo ejecutar este laboratorio

1.	Clonar el repositorio.
2.	Configurar credenciales en ```.env``` (AWS y Snowflake).
3.	Ejecutar ```docker-compose up -d```.
4. Acceder a ```localhost:8080``` y activar el DAG ```baz_pipeline_dag```.
