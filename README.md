Proyecto ETL con Apache Beam

Este proyecto implementa un pipeline ETL que procesa datos de órdenes en formato CSV y genera salidas estructuradas en formato JSONL.

El pipeline se ejecuta en entorno local utilizando Apache Beam con DirectRunner.

Tecnologías utilizadas
Python 3
Apache Beam
Estructura del proyecto
.
├── scripts/
│   └── etl.py
├── data/
│   ├── orders.csv
│   └── output/
├── requirements.txt
└── README.md
Instalación

Crear entorno virtual:

python -m venv venv
venv\Scripts\activate

Instalar dependencias:

pip install -r requirements.txt
Ejecución

Configurar variables de entorno:

set LOCAL_INPUT=data/orders.csv
set LOCAL_OUTPUT=data/output

Ejecutar el pipeline:

python scripts\etl.py
Resultados

El pipeline genera archivos en:

data/output/

Archivos generados:

orders_staging-00000-of-00001.jsonl
orders_summary-00000-of-00001.jsonl
Notas
El pipeline está configurado para ejecución local
No se utilizan servicios en la nube
No se requiere Docker ni Airflow para su ejecución
Los archivos de salida son generados automáticamente
Objetivo

Este proyecto demuestra la implementación de un pipeline ETL funcional utilizando Apache Beam en un entorno local, enfocado en procesamiento de datos batch.
