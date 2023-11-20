-- Eliminamos la tabla si existe
DROP TABLE IF EXISTS gold.persona;

-- Creamos la tabla
CREATE EXTERNAL TABLE gold.persona(
	ID STRING,
	NOMBRE STRING,
	TELEFONO STRING,
	CORREO STRING,
	FECHA_INGRESO STRING,
	EDAD INT,
	SALARIO DECIMAL,
	ID_EMPRESA STRING
) OPTIONS (
  uris=['gs://storagebdaXXX/gold/persona/*.parquet'],
  format=parquet
);

-- Si consultamos la tabla encontraremos el error "*.parquet: matched no files"
-- Esto se debe a que la tabla está vacía
SELECT * FROM gold.persona;