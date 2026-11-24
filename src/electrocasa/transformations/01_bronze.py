"""BRONZE — ingesta raw con Auto Loader, dirigida por metadata (config/fuentes.json).

- Streaming Tables: los archivos llegan por lotes y solo se agregan (append).
- Schema explícito (sin inferSchema). Todo como texto: la limpieza es de Silver.
- Columnas técnicas de auditoría: archivo de origen, momento de ingesta y lote.
- El pipeline administra el checkpoint y la ubicación del schema (no se configuran a mano).
- Sin particionar por fecha de negocio.

Las fuentes con método copy_into (catálogo y sucursales) se cargan fuera del pipeline,
en notebooks/01_copy_into.ipynb, y Tracking llega con el conector de Lakeflow Connect.
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.config import fuentes, ruta_landing

spark = SparkSession.getActiveSession()
CATALOGO = spark.conf.get("catalogo")

LOTE = F.expr(
    "coalesce(nullif(regexp_extract(_metadata.file_name, '(?i)lote[_-]?([0-9]+)', 1), ''), "
    "_metadata.file_name)"
)


def crear_tabla_bronze(fuente: dict) -> None:
    @dp.table(
        name=fuente["tabla_bronze"],
        comment=f"Bronze {fuente['nombre']}: {fuente['comentario']}",
        table_properties={"quality": "bronze", "fuente": fuente["nombre"]},
    )
    def _bronze():
        lector = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fuente["formato"])
        )
        for clave, valor in fuente["opciones"].items():
            lector = lector.option(clave, valor)
        return (
            lector.schema(fuente["schema"])
            .load(ruta_landing(CATALOGO, fuente["ruta"]))
            .select(
                "*",
                F.col("_metadata.file_name").alias("_source_file"),
                F.col("_metadata.file_path").alias("_source_path"),
                F.col("_metadata.file_modification_time").alias("_file_modification_ts"),
                LOTE.alias("_lote"),
                F.current_timestamp().alias("_ingest_ts"),
            )
        )


for _fuente in fuentes(metodo="auto_loader"):
    crear_tabla_bronze(_fuente)
