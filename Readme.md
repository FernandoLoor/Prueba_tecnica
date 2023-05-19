# Prueba técnica

Repositorio con el diseño de la herramienta de visualización de datos transaccionales generados por un bot.


## Descripción

En el repositorio se incluye una notebook de Python utilizada para análisis exploratorio del dataset provisto, una implementación de un pipeline pensado para ejecutar localmente un servicio de ingesta, almacenamiento, procesamiento y visualización de los datos a través de gráficos interactivos, y una propuesta de escalabilidad para un pipeline en un entorno stream.

## Para generar el reporte de ejemplo

Los datasets resultantes del proceso de ETL se almacenan en la carpeta "AggregatedDatasets", desde donde son cargados por la herramienta de visualización, la cuál puede correrse descargando el repositorio localmente e instalando plotly y dash. 
- En la carpeta "src", abrir una terminal y usar:

>pip install plotly==5.14.1

>pip install dash

>python Report.py

- En la línea de comandos Dash publica la dirección localhost con el puerto respectivo, acceder a la misma con "Ctrl + Enter" y el tablero se abrirá en el navegador por defecto (testeado en Mozilla).

## Documentación

En el archivo docs/Solutions.pdf se encuentran descripciones de las soluciones a cada punto del examen.

## Requerimientos

1.  Seaborn
2.  Pandas
3.  Plotly + Dash
4.  MySQL server
5.  MYSQL Connector for Python
6.  Flask
7.  PyArrow
