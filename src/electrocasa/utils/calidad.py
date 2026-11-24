"""Aplicación de expectativas y construcción de la cuarentena."""
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.reglas import EXCLUIR_DE_CUARENTENA, LLAVES, REGLAS, condicion_segura, reglas_por_politica


def con_expectativas(fuente: str):
    """Decorador: aplica las reglas de la fuente con su política (fail / drop / warn)."""

    def decorador(funcion):
        fallar = reglas_por_politica(fuente, "fail")
        descartar = reglas_por_politica(fuente, "drop")
        advertir = reglas_por_politica(fuente, "warn")
        if advertir:
            funcion = dp.expect_all(advertir)(funcion)
        if descartar:
            funcion = dp.expect_all_or_drop(descartar)(funcion)
        if fallar:
            funcion = dp.expect_all_or_fail(fallar)(funcion)
        return funcion

    return decorador


def rechazos(df: DataFrame, fuente: str) -> DataFrame:
    """Una fila por (registro, regla violada), con fuente, regla, lote y fecha de proceso.

    Se recalcula completa en cada corrida a partir de Bronze: si corriges una regla o
    un mapeo, el registro se reprocesa solo en la siguiente actualización del pipeline.
    """
    excluir = set(EXCLUIR_DE_CUARENTENA.get(fuente, []))
    columnas_registro = [
        c for c, tipo in df.dtypes if c not in excluir and not tipo.startswith("variant")
    ]
    partes = []
    for regla in REGLAS[fuente]:
        if regla["politica"] == "fail":
            continue
        partes.append(
            df.filter(F.expr(f"NOT {condicion_segura(regla['condicion'])}")).select(
                F.lit(fuente).alias("fuente"),
                F.lit(regla["nombre"]).alias("regla"),
                F.lit(regla["condicion"]).alias("condicion"),
                F.lit(regla["politica"]).alias("politica"),
                F.col(LLAVES[fuente]).cast("string").alias("id_registro"),
                F.col("_lote").cast("string").alias("lote"),
                F.col("_source_file").cast("string").alias("archivo_origen"),
                F.col("_ingest_ts").cast("timestamp").alias("fecha_ingesta"),
                F.current_timestamp().alias("fecha_proceso"),
                F.to_json(F.struct(*columnas_registro)).alias("registro"),
            )
        )
    return reduce(DataFrame.unionByName, partes)
