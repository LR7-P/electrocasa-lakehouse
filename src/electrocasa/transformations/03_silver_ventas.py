"""SILVER — ventas y devoluciones.

Materialized Views sobre las Streaming Tables de Bronze: se recalculan con todos los
lotes, así la deduplicación de reingestas (la misma venta en dos lotes) es exacta.
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from utils.calidad import con_expectativas
from utils.normalizacion import (
    CANALES, METODOS_PAGO, decimal, entero, fecha, id_limpio, mapear, texto_limpio,
)

spark = SparkSession.getActiveSession()


def deduplicar(df, llave: str):
    """Una fila por llave (la primera que llegó). Las filas con llave nula no se
    colapsan entre sí: llegan a la expectativa y terminan en cuarentena."""
    w = Window.partitionBy(llave).orderBy(F.col("_ingest_ts").asc(), F.col("_source_file").asc())
    rn = F.when(F.col(llave).isNull(), F.lit(1)).otherwise(F.row_number().over(w))
    return df.withColumn("_rn", rn).filter("_rn = 1").drop("_rn")


def productos_catalogo():
    return (
        spark.read.table("silver.catalogo_actual")
        .select("producto_id")
        .withColumn("_en_catalogo", F.lit(True))
    )


# --------------------------------------------------------------------- Ventas
@dp.temporary_view(name="ventas_preparada")
def ventas_preparada():
    b = spark.read.table("bronze.ventas")
    df = b.select(
        id_limpio("venta_id").alias("venta_id"),
        id_limpio("sucursal_id").alias("sucursal_id"),
        id_limpio("producto_id").alias("producto_id"),
        id_limpio("vendedor_id").alias("vendedor_id"),
        entero("cantidad").alias("cantidad"),
        decimal("monto_total").alias("monto_total"),               # "1.234,50" -> 1234.50
        texto_limpio("monto_total").alias("monto_total_original"),
        mapear("metodo_pago", METODOS_PAGO, "otro").alias("metodo_pago"),
        texto_limpio("metodo_pago").alias("metodo_pago_original"),
        fecha("fecha_venta").alias("fecha_venta"),                  # 3 formatos -> DATE
        mapear("canal", CANALES, "desconocido").alias("canal"),
        "_source_file", "_lote", "_ingest_ts",
    )
    df = deduplicar(df, "venta_id")
    return (
        df.join(productos_catalogo(), "producto_id", "left")
        .withColumn("producto_en_catalogo", F.coalesce(F.col("_en_catalogo"), F.lit(False)))
        .drop("_en_catalogo")
    )


@dp.materialized_view(
    name="silver.ventas",
    comment="Ventas limpias y deduplicadas por venta_id (tienda física y ecommerce).",
    table_properties={"quality": "silver"},
)
@con_expectativas("ventas")
def silver_ventas():
    return spark.read.table("ventas_preparada")


# --------------------------------------------------------------- Devoluciones
@dp.temporary_view(name="devoluciones_preparada")
def devoluciones_preparada():
    """Vista con el join a ventas: la expectativa reembolso <= monto de la venta se
    evalúa fila a fila sobre esta vista."""
    b = spark.read.table("bronze.devoluciones")
    df = b.select(
        id_limpio("devolucion_id").alias("devolucion_id"),
        id_limpio("venta_id").alias("venta_id"),
        id_limpio("sucursal_id").alias("sucursal_id_origen"),
        id_limpio("producto_id").alias("producto_id_origen"),
        texto_limpio("motivo").alias("motivo"),
        decimal("monto_reembolso").alias("monto_reembolso"),
        texto_limpio("monto_reembolso").alias("monto_reembolso_original"),
        fecha("fecha_devolucion").alias("fecha_devolucion"),
        "_source_file", "_lote", "_ingest_ts",
    )
    df = deduplicar(df, "devolucion_id")

    ventas = spark.read.table("silver.ventas").select(
        "venta_id",
        F.col("monto_total").alias("monto_venta"),
        F.col("sucursal_id").alias("sucursal_venta"),
        F.col("producto_id").alias("producto_venta"),
        F.lit(True).alias("_venta_ok"),
    )
    df = (
        df.join(ventas, "venta_id", "left")
        .withColumn("venta_existe", F.coalesce(F.col("_venta_ok"), F.lit(False)))
        # Si la devolución no trae sucursal o producto, se toman de la venta de origen
        .withColumn("sucursal_id", F.coalesce("sucursal_id_origen", "sucursal_venta"))
        .withColumn("producto_id", F.coalesce("producto_id_origen", "producto_venta"))
        .drop("_venta_ok", "sucursal_id_origen", "producto_id_origen", "sucursal_venta", "producto_venta")
    )
    return (
        df.join(productos_catalogo(), "producto_id", "left")
        .withColumn("producto_en_catalogo", F.coalesce(F.col("_en_catalogo"), F.lit(False)))
        .drop("_en_catalogo")
    )


@dp.materialized_view(
    name="silver.devoluciones",
    comment="Devoluciones limpias, deduplicadas y validadas contra la venta de origen.",
    table_properties={"quality": "silver"},
)
@con_expectativas("devoluciones")
def silver_devoluciones():
    return spark.read.table("devoluciones_preparada")
