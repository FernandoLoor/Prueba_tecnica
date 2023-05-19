# Prueba técnica

Repositorio con el diseño de la herramienta de visualización de datos transaccionales generados por un bot.


## Descripción

En el repositorio se incluye una notebook de Python utilizada para análisis exploratorio del dataset provisto, una implementación de un pipeline pensado para ejecutar localmente un servicio de ingesta, almacenamiento, procesamiento y visualización de los datos a través de gráficos interactivos, y una propuesta de escalabilidad para un pipeline en un entorno stream.

## Para generar el reporte de ejemplo

Los datasets resultantes del proceso de ETL se almacenan en la carpeta "AggregatedDatasets", desde donde son cargados por la herramienta de visualización, la cuál puede correrse descargando el repositorio localmente e instalando plotly y dash. 
- En la carpeta “components”, abrir una terminal y usar:
>pip install plotly==5.14.1
>pip install dash
>python Report.py
- En la línea de comandos Dash publica la dirección localhost con el puerto respectivo, acceder a la misma con "Ctrl + Enter" y el tablero se abrirá en el navegador por defecto (testeado en Mozilla).

## Documentación
En el archivo Descripción.pdf se encuentran comentarios de los puntos 1 y 2 de la consigna. El punto 2 explica con un poco más de detalle la estructura del repositorio y la función de cada script.

En el archivo “Propuesta_High_End.pdf” se describe el diseño de la solución para el punto 3 de la consigna.


## Requerimientos

1.  SEABORN.    
2.  PANDAS.
3.  Plotly + DASH.
4.  MySQL server.
5.  MYSQL CONNECTOR for Python.
6.  FLASK.
7.  PYARROW.
