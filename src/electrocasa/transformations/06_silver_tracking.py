"""SILVER — tracking de envíos.

Bronze de esta fuente = tabla destino del conector query-based de Lakeflow Connect
(bronze.tracking_envios, append only). Aquí se estandariza estado y courier y se
eliminan los mensajes reprocesados: el mismo cambio de estado del mismo envío
registrado dos veces con distinto evento_id. Se conserva el primero (menor fecha_modificacion).
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from utils.calidad import con_expectativas
from utils.normalizacion import COURIERS, ESTADOS_ENTREGA, id_limpio, mapear, texto_limpio

spark = SparkSession.getActiveSession()
TRACKING_TABLA = spark.conf.get("tracking_tabla")


@dp.temporary_view(name="tracking_preparada")
def tracking_preparada():
    b = spark.read.table(TRACKING_TABLA)
    df = b.select(
        F.col("evento_id").cast("bigint").alias("evento_id"),
        id_limpio("tracking_id").alias("tracking_id"),
        id_limpio("venta_id").alias("venta_id"),
        # Conjunto cerrado de couriers; uno nuevo no mapeado conserva su nombre con casing unificado
        F.coalesce(
            mapear("courier", COURIERS, None),
            F.initcap(F.lower(F.regexp_replace(texto_limpio("courier"), r"\s+", " "))),
        ).alias("courier"),
        texto_limpio("courier").alias("courier_original"),
        mapear("estado_entrega", ESTADOS_ENTREGA, "desconocido").alias("estado_entrega"),
        texto_limpio("estado_entrega").alias("estado_entrega_original"),
        id_limpio("sucursal_origen").alias("sucursal_origen"),
        F.col("fecha_actualizacion").cast("date").alias("fecha_actualizacion"),
        F.col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
        F.lit("azure_sql:dbo.TrackingEnvios").alias("_source_file"),
        F.lit("conector_lakeflow").alias("_lote"),
        F.col("fecha_modificacion").cast("timestamp").alias("_ingest_ts"),
    )
    # Mensajes reprocesados: mismo envío + mismo estado (+ misma fecha de negocio)
    w = Window.partitionBy("tracking_id", "estado_entrega", "fecha_actualizacion").orderBy(
        F.col("fecha_modificacion").asc(), F.col("evento_id").asc()
    )
    rn = F.when(F.col("tracking_id").isNull(), F.lit(1)).otherwise(F.row_number().over(w))
    df = df.withColumn("_rn", rn).filter("_rn = 1").drop("_rn")

    ventas = spark.read.table("silver.ventas").select("venta_id").withColumn("_venta_ok", F.lit(True))
    return (
        df.join(ventas, "venta_id", "left")
        .withColumn("venta_existe", F.coalesce(F.col("_venta_ok"), F.lit(False)))
        .drop("_venta_ok")
    )


@dp.materialized_view(
    name="silver.tracking",
    comment="Eventos de tracking con estado y courier estandarizados y sin mensajes reprocesados.",
    table_properties={"quality": "silver"},
)
@con_expectativas("tracking")
def silver_tracking():
    return spark.read.table("tracking_preparada")
