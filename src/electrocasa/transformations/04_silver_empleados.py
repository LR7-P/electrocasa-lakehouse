"""SILVER — Empleados: eventos limpios + historia SCD Type 2.

Historización (SCD2 construido sobre la tabla de eventos, sin AUTO CDC):
- Cada evento válido genera una versión del empleado.
- Orden de los eventos: fecha_evento (cuándo ocurrió) y, para desempatar dos eventos
  del mismo día, fecha_registro (cuándo lo cargó RR.HH.). Nunca current_timestamp().
- La historia es una Materialized View que se recalcula con todos los eventos: un
  evento de junio que llega en el lote 3 queda ubicado en su lugar automáticamente.
- vigente_desde inclusivo, vigente_hasta exclusivo (fecha del evento siguiente).
- Los atributos que un evento no trae (p. ej. el salario en una transferencia)
  se arrastran de la versión anterior.
- estado: ACTIVO desde un ALTA, CESADO desde una BAJA/cese; SIN_ALTA si no hubo alta.

Masking: dni y salario llevan MASK en el schema de las tablas del pipeline.
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from utils.calidad import con_expectativas
from utils.config import SALARIO_MAX, SALARIO_MIN
from utils.normalizacion import TIPOS_EVENTO, decimal, fecha, id_limpio, mapear, marca_tiempo, texto_limpio

spark = SparkSession.getActiveSession()
CATALOGO = spark.conf.get("catalogo")
MASK_DNI = f"{CATALOGO}.silver.mask_dni"
MASK_SALARIO = f"{CATALOGO}.silver.mask_salario"


@dp.temporary_view(name="empleados_preparada")
def empleados_preparada():
    b = spark.read.table("bronze.empleados_eventos")
    df = b.select(
        id_limpio("id_empleado").alias("id_empleado"),
        F.initcap(F.lower(texto_limpio("nombre"))).alias("nombre"),
        F.expr("nullif(regexp_replace(trim(dni), '[^0-9A-Za-z]', ''), '')").alias("dni"),
        F.lower(texto_limpio("email")).alias("email"),
        decimal("salario").alias("salario"),
        id_limpio("sucursal_id").alias("sucursal_id"),
        texto_limpio("cargo").alias("cargo"),
        mapear("tipo_evento", TIPOS_EVENTO, "DESCONOCIDO").alias("tipo_evento"),
        texto_limpio("tipo_evento").alias("tipo_evento_original"),
        fecha("fecha_evento").alias("fecha_evento"),
        marca_tiempo("fecha_registro").alias("fecha_registro"),
        "_source_file", "_lote", "_ingest_ts",
    )
    # Reingestas del mismo evento: queda el primero que llegó
    llave_evento = ["id_empleado", "tipo_evento", "fecha_evento", "fecha_registro", "sucursal_id", "salario"]
    w = Window.partitionBy(*llave_evento).orderBy(F.col("_ingest_ts").asc(), F.col("_source_file").asc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    # Mismo dni en dos empleados distintos (error de digitación): se marca, no se descarta
    w_dni = Window.partitionBy("dni")
    df = df.withColumn(
        "dni_duplicado",
        F.when(F.col("dni").isNull(), F.lit(False)).otherwise(
            F.size(F.collect_set("id_empleado").over(w_dni)) > 1
        ),
    )
    df = df.withColumn(
        "salario_en_rango",
        F.col("salario").isNull() | F.col("salario").between(SALARIO_MIN, SALARIO_MAX),
    )
    return df


SCHEMA_EVENTOS = f"""
    id_empleado STRING,
    nombre STRING,
    dni STRING MASK {MASK_DNI},
    email STRING,
    salario DECIMAL(12,2) MASK {MASK_SALARIO},
    sucursal_id STRING,
    cargo STRING,
    tipo_evento STRING,
    tipo_evento_original STRING,
    fecha_evento DATE,
    fecha_registro TIMESTAMP,
    dni_duplicado BOOLEAN,
    salario_en_rango BOOLEAN,
    _source_file STRING,
    _lote STRING,
    _ingest_ts TIMESTAMP
"""


@dp.materialized_view(
    name="silver.empleados_eventos",
    comment="Eventos de RR.HH. limpios (tipo de evento estandarizado, fechas a DATE/TIMESTAMP).",
    schema=SCHEMA_EVENTOS,
    table_properties={"quality": "silver"},
)
@con_expectativas("empleados")
def silver_empleados_eventos():
    return spark.read.table("empleados_preparada").select(
        "id_empleado", "nombre", "dni", "email", "salario", "sucursal_id", "cargo",
        "tipo_evento", "tipo_evento_original", "fecha_evento", "fecha_registro",
        "dni_duplicado", "salario_en_rango", "_source_file", "_lote", "_ingest_ts",
    )


SCHEMA_HISTORIA = f"""
    id_empleado STRING,
    version INT,
    nombre STRING,
    dni STRING MASK {MASK_DNI},
    email STRING,
    salario DECIMAL(12,2) MASK {MASK_SALARIO},
    sucursal_id STRING,
    cargo STRING,
    estado STRING,
    tipo_evento STRING,
    fecha_evento DATE,
    fecha_registro TIMESTAMP,
    vigente_desde DATE,
    vigente_hasta DATE,
    es_version_actual BOOLEAN
"""


@dp.materialized_view(
    name="silver.empleados_historia",
    comment=(
        "SCD Type 2 de empleados: una versión por evento con su intervalo de vigencia "
        "[vigente_desde, vigente_hasta). Ordenado por fecha_evento y fecha_registro."
    ),
    schema=SCHEMA_HISTORIA,
    table_properties={"quality": "silver"},
)
def silver_empleados_historia():
    e = spark.read.table("silver.empleados_eventos")
    orden = [F.col("fecha_evento").asc(), F.col("fecha_registro").asc_nulls_first(), F.col("_ingest_ts").asc()]
    w = Window.partitionBy("id_empleado").orderBy(*orden)
    w_acum = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    estado_evento = (
        F.when(F.col("tipo_evento") == "ALTA", F.lit("ACTIVO"))
        .when(F.col("tipo_evento") == "BAJA", F.lit("CESADO"))
    )
    siguiente = F.lead("fecha_evento").over(w)
    return e.select(
        "id_empleado",
        F.row_number().over(w).cast("int").alias("version"),
        F.last("nombre", ignorenulls=True).over(w_acum).alias("nombre"),
        F.last("dni", ignorenulls=True).over(w_acum).alias("dni"),
        F.last("email", ignorenulls=True).over(w_acum).alias("email"),
        F.last("salario", ignorenulls=True).over(w_acum).cast("decimal(12,2)").alias("salario"),
        F.last("sucursal_id", ignorenulls=True).over(w_acum).alias("sucursal_id"),
        F.last("cargo", ignorenulls=True).over(w_acum).alias("cargo"),
        F.coalesce(F.last(estado_evento, ignorenulls=True).over(w_acum), F.lit("SIN_ALTA")).alias("estado"),
        "tipo_evento",
        "fecha_evento",
        "fecha_registro",
        F.col("fecha_evento").alias("vigente_desde"),
        siguiente.alias("vigente_hasta"),
        siguiente.isNull().alias("es_version_actual"),
    )
