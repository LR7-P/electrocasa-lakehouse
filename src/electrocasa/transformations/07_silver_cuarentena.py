"""SILVER — cuarentena consultable.

Una fila por (registro, regla violada) con: fuente, regla, condición, política,
id del registro, lote, archivo de origen, fecha de ingesta, fecha de proceso y el
registro completo en JSON (sin dni/salario/email de empleados).

- politica = 'drop' -> el registro NO está en Silver (rechazado).
- politica = 'warn' -> el registro SÍ está en Silver, la violación queda registrada.

Se recalcula desde Bronze en cada corrida: corregir una regla o un mapeo y volver a
correr el pipeline reprocesa los registros automáticamente.
"""
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from utils.calidad import rechazos
from utils.reglas import REGLAS

spark = SparkSession.getActiveSession()


@dp.materialized_view(
    name="silver.cuarentena",
    comment="Registros que violan una regla de calidad: fuente, regla, lote, fecha y registro original.",
    table_properties={"quality": "silver"},
)
def silver_cuarentena():
    partes = [rechazos(spark.read.table(f"{fuente}_preparada"), fuente) for fuente in REGLAS]
    return reduce(DataFrame.unionByName, partes)
