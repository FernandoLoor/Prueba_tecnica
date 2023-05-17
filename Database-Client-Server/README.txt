Códigos DB:

- server.py: 
recibe las trades del cliente/generador una a una y las inserta en las tablas luego de validarlas.

- cliente.py:
Envía una a una, trades al servidor.

- clienteBatch.py: 
Se especifica la dirección del server. Abre el archivo ".orc" y envía una a una las líneas del archivo para que el server las inserte en la BD.

- clienteRandom.py: 
Genera trades aleatorias en un bucle, que el server inserta en la base de datos mySQL.



ETL.py: usa connectDB() para cargar las tablas que le pidamos luego de hacer un filtrado.
	Para traer los datos usa “get_trades_daily”, que trae un join de las tablas de “trades” y “bots”
	Se le pueden poner las fechas inicial y final como parámetros de entrada, que por defecto sean ayer y hoy.


