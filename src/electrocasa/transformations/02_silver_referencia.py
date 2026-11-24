"""SILVER — tablas de referencia: sucursales y catálogo de productos.

bronze.sucursales y bronze.catalogo las carga COPY INTO (fuera del pipeline); aquí se
leen en batch desde Materialized Views.
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from utils.calidad import con_expectativas
from utils.normalizacion import CATEGORIAS, decimal, fecha, id_limpio, mapear, texto_limpio

spark = SparkSession.getActiveSession()


# ----------------------------------------------------------------- Sucursales
@dp.temporary_view(name="sucursales_preparada")
def sucursales_preparada():
    b = spark.read.table("bronze.sucursales")
    df = b.select(
        id_limpio("sucursal_id").alias("sucursal_id"),
        texto_limpio("nombre_sucursal").alias("nombre_sucursal"),
        F.initcap(F.lower(texto_limpio("distrito"))).alias("distrito"),
        F.initcap(F.lower(texto_limpio("ciudad"))).alias("ciudad"),   # casing de ciudad unificado
        F.initcap(F.lower(texto_limpio("region"))).alias("region"),
        fecha("fecha_apertura").alias("fecha_apertura"),
        "_source_file", "_lote", "_ingest_ts",
    )
    w = Window.partitionBy("sucursal_id").orderBy(F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


@dp.materialized_view(
    name="silver.sucursales",
    comment="Sucursales estandarizadas (ciudad con casing unificado). Una fila por sucursal.",
    table_properties={"quality": "silver"},
)
@con_expectativas("sucursales")
def silver_sucursales():
    return spark.read.table("sucursales_preparada")


# ------------------------------------------------------------------- Catálogo
@dp.temporary_view(name="catalogo_preparada")
def catalogo_preparada():
    b = spark.read.table("bronze.catalogo")
    df = b.select(
        id_limpio("producto_id").alias("producto_id"),
        texto_limpio("nombre_producto").alias("nombre_producto"),
        mapear("categoria", CATEGORIAS, "otra").alias("categoria"),
        texto_limpio("categoria").alias("categoria_original"),
        F.initcap(F.lower(texto_limpio("marca"))).alias("marca"),
        decimal("precio_lista").alias("precio_lista"),           # "S/ 1,299.90" -> 1299.90
        texto_limpio("precio_lista").alias("precio_lista_original"),
        F.lower(texto_limpio("activo")).isin("true", "1", "si", "sí", "yes").alias("activo"),
        F.col("_source_file").alias("snapshot_archivo"),
        "_source_file", "_lote", "_ingest_ts",
    )
    # Orden de los snapshots = momento en que se cargó cada archivo (marzo en el lote 1, agosto en el lote 2)
    df = df.withColumn(
        "snapshot_ts", F.min("_ingest_ts").over(Window.partitionBy("snapshot_archivo"))
    )
    # Productos repetidos dentro de un mismo snapshot (marzo): queda uno solo,
    # priorizando el precio válido y, entre válidos, el mayor.
    w = Window.partitionBy("producto_id", "snapshot_archivo").orderBy(
        F.expr("coalesce(precio_lista > 0, false)").desc(),
        F.col("precio_lista").desc_nulls_last(),
        F.col("nombre_producto").asc_nulls_last(),
    )
    rn = F.when(F.col("producto_id").isNull(), F.lit(1)).otherwise(F.row_number().over(w))
    return df.withColumn("_rn", rn).filter("_rn = 1").drop("_rn")


@dp.materialized_view(
    name="silver.catalogo",
    comment="Catálogo por snapshot (producto_id + snapshot_archivo). Descontinuados se conservan con activo = false.",
    table_properties={"quality": "silver"},
)
@con_expectativas("catalogo")
def silver_catalogo():
    return spark.read.table("catalogo_preparada")


@dp.materialized_view(
    name="silver.catalogo_actual",
    comment="Última versión de cada producto según el último snapshot en que aparece.",
    table_properties={"quality": "silver"},
)
def silver_catalogo_actual():
    c = spark.read.table("silver.catalogo")
    w = Window.partitionBy("producto_id").orderBy(F.col("snapshot_ts").desc())
    return (
        c.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select(
            "producto_id", "nombre_producto", "categoria", "marca", "precio_lista",
            "activo", "snapshot_archivo", "snapshot_ts",
        )
    )
