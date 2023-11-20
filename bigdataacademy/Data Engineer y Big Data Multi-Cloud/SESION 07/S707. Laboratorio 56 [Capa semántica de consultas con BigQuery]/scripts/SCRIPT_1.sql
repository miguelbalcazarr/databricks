-- Eliminamos la tabla si existe
DROP TABLE IF EXISTS bronze.persona;

-- Creamos la tabla
CREATE EXTERNAL TABLE bronze.persona(
	ID STRING,
	NOMBRE STRING,
	TELEFONO STRING,
	CORREO STRING,
	FECHA_INGRESO STRING,
	EDAD STRING,
	SALARIO STRING,
	ID_EMPRESA STRING
) OPTIONS (
	uris=['gs://storagebdaXXX/bronze/persona/*'],
	format=csv,
	field_delimiter='|',
	skip_leading_rows=1,
	encoding='ISO-8859-1'
);

-- Consultamos
SELECT * FROM bronze.persona;