"""SILVER — reseñas (JSON Lines anidado).

- tags: arreglo de texto (nulo -> arreglo vacío).
- respuestas: arreglo de structs (autor, texto, fecha); autor faltante -> 'sin_autor'.
- metadata: llega como texto JSON en Bronze y se convierte a VARIANT aquí.
- Deduplicación por resena_id (las reseñas del lote 1 se repiten en el lote 2).
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from utils.calidad import con_expectativas
from utils.normalizacion import fecha, id_limpio, texto_limpio, timestamp_sql

spark = SparkSession.getActiveSession()


@dp.temporary_view(name="resenas_preparada")
def resenas_preparada():
    b = spark.read.table("bronze.resenas")
    calificacion = F.expr(
        "CASE WHEN try_cast(trim(calificacion) AS DECIMAL(5,2)) = "
        "floor(try_cast(trim(calificacion) AS DECIMAL(5,2))) "
        "THEN CAST(try_cast(trim(calificacion) AS DECIMAL(5,2)) AS INT) END"
    )
    respuestas = F.expr(
        "transform(respuestas, r -> named_struct("
        "'autor', coalesce(nullif(trim(r.autor), ''), 'sin_autor'), "
        "'texto', nullif(trim(r.texto), ''), "
        f"'fecha', {timestamp_sql('r.fecha')}))"
    )
    df = b.select(
        id_limpio("resena_id").alias("resena_id"),
        id_limpio("producto_id").alias("producto_id"),
        id_limpio("cliente_id").alias("cliente_id"),
        calificacion.alias("calificacion"),
        texto_limpio("calificacion").alias("calificacion_original"),
        texto_limpio("comentario").alias("comentario"),
        F.coalesce(F.col("tags"), F.expr("CAST(array() AS ARRAY<STRING>)")).alias("tags"),
        respuestas.alias("respuestas"),
        fecha("fecha_resena").alias("fecha_resena"),          # dos formatos -> DATE
        F.col("metadata").alias("metadata_json"),
        "_source_file", "_lote", "_ingest_ts",
    )
    w = Window.partitionBy("resena_id").orderBy(F.col("_ingest_ts").asc(), F.col("_source_file").asc())
    rn = F.when(F.col("resena_id").isNull(), F.lit(1)).otherwise(F.row_number().over(w))
    df = df.withColumn("_rn", rn).filter("_rn = 1").drop("_rn")

    catalogo = (
        spark.read.table("silver.catalogo_actual").select("producto_id").withColumn("_en_cat", F.lit(True))
    )
    return (
        df.join(catalogo, "producto_id", "left")
        .withColumn("producto_en_catalogo", F.coalesce(F.col("_en_cat"), F.lit(False)))
        .drop("_en_cat")
    )


@dp.materialized_view(
    name="silver.resenas",
    comment="Reseñas limpias y deduplicadas. metadata como VARIANT (estructura distinta por origen).",
    table_properties={"quality": "silver"},
)
@con_expectativas("resenas")
def silver_resenas():
    return spark.read.table("resenas_preparada").withColumn(
        "metadata", F.expr("try_parse_json(metadata_json)")
    )
