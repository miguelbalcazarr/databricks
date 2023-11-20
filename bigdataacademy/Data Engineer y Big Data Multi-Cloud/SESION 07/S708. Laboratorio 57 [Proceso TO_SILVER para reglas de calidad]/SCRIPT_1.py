def transform(record, emitter, context):
  if (
    (record["ID_PERSONA"] != None) &
    (record["ID_EMPRESA"] != None) &
    (record["MONTO"] != None) &
    (record["FECHA"] != None) &
    (record["MONTO"] > 0)
  ):
    emitter.emit(record)
