"""GOLD — tablas que responden las seis preguntas de negocio (marzo a octubre de 2026).

Todas se calculan sobre Silver (datos limpios y deduplicados; los rechazados no cuentan).
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from utils.config import PERIODO_FIN, PERIODO_INICIO

spark = SparkSession.getActiveSession()
CATALOGO = spark.conf.get("catalogo")

INI = f"DATE'{PERIODO_INICIO}'"
MES_INI = int(PERIODO_INICIO[5:7])
MES_FIN = int(PERIODO_FIN[5:7])
FIN = f"DATE'{PERIODO_FIN}'"
PROPS = {"quality": "gold"}


# ------------------------------------------------------------------ Pregunta 1
@dp.materialized_view(
    name="gold.ventas_netas_sucursal_mes",
    comment="P1: ventas, devoluciones, ventas netas, cantidad de ventas y ticket promedio por sucursal y mes.",
    table_properties=PROPS,
)
def ventas_netas_sucursal_mes():
    return spark.sql(f"""
        WITH v AS (
            SELECT sucursal_id,
                   CAST(date_trunc('MONTH', fecha_venta) AS DATE) AS mes,
                   SUM(monto_total)          AS ventas,
                   COUNT(DISTINCT venta_id)  AS cantidad_ventas
            FROM silver.ventas
            WHERE fecha_venta BETWEEN {INI} AND {FIN}
            GROUP BY ALL
        ),
        d AS (
            SELECT sucursal_id,
                   CAST(date_trunc('MONTH', fecha_devolucion) AS DATE) AS mes,
                   SUM(monto_reembolso) AS devoluciones
            FROM silver.devoluciones
            WHERE fecha_devolucion BETWEEN {INI} AND {FIN}
            GROUP BY ALL
        )
        SELECT coalesce(v.sucursal_id, d.sucursal_id)            AS sucursal_id,
               s.nombre_sucursal,
               s.ciudad,
               coalesce(v.mes, d.mes)                            AS mes,
               CAST(coalesce(v.ventas, 0) AS DECIMAL(14,2))      AS ventas,
               CAST(coalesce(d.devoluciones, 0) AS DECIMAL(14,2)) AS devoluciones,
               CAST(coalesce(v.ventas, 0) - coalesce(d.devoluciones, 0) AS DECIMAL(14,2)) AS ventas_netas,
               coalesce(v.cantidad_ventas, 0)                    AS cantidad_ventas,
               CAST(try_divide(v.ventas, v.cantidad_ventas) AS DECIMAL(14,2)) AS ticket_promedio
        FROM v
        FULL OUTER JOIN d ON v.sucursal_id = d.sucursal_id AND v.mes = d.mes
        LEFT JOIN silver.sucursales s ON s.sucursal_id = coalesce(v.sucursal_id, d.sucursal_id)
    """)


# ------------------------------------------------------------------ Pregunta 2
@dp.materialized_view(
    name="gold.top_productos_unidades",
    comment="P2a: top 10 de productos más vendidos por unidades (marzo-octubre 2026).",
    table_properties=PROPS,
)
def top_productos_unidades():
    return spark.sql(f"""
        WITH u AS (
            SELECT producto_id,
                   SUM(cantidad)            AS unidades,
                   COUNT(DISTINCT venta_id) AS cantidad_ventas
            FROM silver.ventas
            WHERE fecha_venta BETWEEN {INI} AND {FIN}
            GROUP BY producto_id
        ),
        r AS (
            SELECT u.*, ROW_NUMBER() OVER (ORDER BY unidades DESC, producto_id) AS ranking
            FROM u
        )
        SELECT r.ranking, r.producto_id, c.nombre_producto, c.categoria, c.marca,
               r.unidades, r.cantidad_ventas
        FROM r LEFT JOIN silver.catalogo_actual c USING (producto_id)
        WHERE r.ranking <= 10
    """)


@dp.materialized_view(
    name="gold.top_productos_tasa_devolucion",
    comment="P2b: top 10 de productos con mayor tasa de devolución (devoluciones / ventas del producto).",
    table_properties=PROPS,
)
def top_productos_tasa_devolucion():
    return spark.sql(f"""
        WITH v AS (
            SELECT producto_id, COUNT(DISTINCT venta_id) AS cantidad_ventas
            FROM silver.ventas
            WHERE fecha_venta BETWEEN {INI} AND {FIN}
            GROUP BY producto_id
        ),
        d AS (
            SELECT producto_id, COUNT(DISTINCT devolucion_id) AS cantidad_devoluciones
            FROM silver.devoluciones
            WHERE fecha_devolucion BETWEEN {INI} AND {FIN}
            GROUP BY producto_id
        ),
        t AS (
            SELECT v.producto_id, v.cantidad_ventas,
                   coalesce(d.cantidad_devoluciones, 0) AS cantidad_devoluciones,
                   CAST(coalesce(d.cantidad_devoluciones, 0) / v.cantidad_ventas AS DECIMAL(8,4)) AS tasa_devolucion
            FROM v LEFT JOIN d USING (producto_id)
            WHERE v.cantidad_ventas > 0
        ),
        r AS (
            SELECT t.*, ROW_NUMBER() OVER (
                       ORDER BY tasa_devolucion DESC, cantidad_devoluciones DESC, producto_id) AS ranking
            FROM t
        )
        SELECT r.ranking, r.producto_id, c.nombre_producto, c.categoria,
               r.cantidad_ventas, r.cantidad_devoluciones, r.tasa_devolucion
        FROM r LEFT JOIN silver.catalogo_actual c USING (producto_id)
        WHERE r.ranking <= 10
    """)


# ------------------------------------------------------------------ Pregunta 3
@dp.materialized_view(
    name="gold.dotacion_sucursal_mes",
    comment="P3: empleados activos por sucursal al último día de cada mes (SCD2 de empleados).",
    table_properties=PROPS,
)
def dotacion_sucursal_mes():
    return spark.sql(f"""
        WITH cortes AS (
            SELECT last_day(make_date(2026, CAST(id AS INT), 1)) AS fecha_corte
            FROM range({MES_INI}, {MES_FIN} + 1)
        ),
        activos AS (
            SELECT c.fecha_corte, h.sucursal_id, COUNT(DISTINCT h.id_empleado) AS empleados_activos
            FROM cortes c
            JOIN silver.empleados_historia h
              ON h.estado = 'ACTIVO'
             AND h.vigente_desde <= c.fecha_corte
             AND (h.vigente_hasta IS NULL OR h.vigente_hasta > c.fecha_corte)
            GROUP BY ALL
        )
        SELECT s.sucursal_id, s.nombre_sucursal, s.ciudad, c.fecha_corte,
               CAST(date_trunc('MONTH', c.fecha_corte) AS DATE) AS mes,
               coalesce(a.empleados_activos, 0) AS empleados_activos
        FROM silver.sucursales s
        CROSS JOIN cortes c
        LEFT JOIN activos a ON a.sucursal_id = s.sucursal_id AND a.fecha_corte = c.fecha_corte
    """)


# ------------------------------------------------------------------ Pregunta 4
@dp.materialized_view(
    name="gold.ventas_vendedor_sucursal_mes",
    comment="P4: ventas de tienda física por vendedor, sucursal del vendedor en la fecha de la venta (SCD2) y mes.",
    table_properties=PROPS,
)
def ventas_vendedor_sucursal_mes():
    return spark.sql(f"""
        SELECT v.vendedor_id,
               coalesce(h.sucursal_id, 'SIN_ASIGNAR')         AS sucursal_vendedor,
               s.nombre_sucursal,
               CAST(date_trunc('MONTH', v.fecha_venta) AS DATE) AS mes,
               CAST(SUM(v.monto_total) AS DECIMAL(14,2))      AS monto_ventas,
               COUNT(DISTINCT v.venta_id)                     AS cantidad_ventas
        FROM silver.ventas v
        LEFT JOIN silver.empleados_historia h
               ON h.id_empleado = v.vendedor_id
              AND h.estado = 'ACTIVO'
              AND h.vigente_desde <= v.fecha_venta
              AND (h.vigente_hasta IS NULL OR h.vigente_hasta > v.fecha_venta)
        LEFT JOIN silver.sucursales s ON s.sucursal_id = h.sucursal_id
        WHERE v.canal = 'tienda_fisica'
          AND v.fecha_venta BETWEEN {INI} AND {FIN}
        GROUP BY ALL
    """)


# ------------------------------------------------------------------ Pregunta 5
@dp.materialized_view(
    name="gold.resenas_negativas_categoria_mes",
    comment="P5: % de reseñas negativas (1 o 2) por categoría (último snapshot) y mes de la reseña.",
    table_properties=PROPS,
)
def resenas_negativas_categoria_mes():
    # silver.resenas ya excluye calificaciones inválidas; el INNER JOIN excluye productos
    # que no existen en el catálogo (no entran ni en numerador ni en denominador).
    return spark.sql(f"""
        SELECT c.categoria,
               CAST(date_trunc('MONTH', r.fecha_resena) AS DATE) AS mes,
               COUNT(*) AS total_resenas,
               SUM(CASE WHEN r.calificacion <= 2 THEN 1 ELSE 0 END) AS resenas_negativas,
               CAST(100.0 * SUM(CASE WHEN r.calificacion <= 2 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(6,2))
                   AS pct_negativas
        FROM silver.resenas r
        JOIN silver.catalogo_actual c ON c.producto_id = r.producto_id
        WHERE r.fecha_resena BETWEEN {INI} AND {FIN}
        GROUP BY ALL
    """)


# ------------------------------------------------------------------ Pregunta 6
@dp.materialized_view(
    name="gold.despachos_courier",
    comment="P6: horas promedio pendiente->entregado y % de envíos devueltos por courier.",
    table_properties=PROPS,
)
def despachos_courier():
    return spark.sql("""
        WITH envio AS (
            SELECT tracking_id,
                   min_by(courier, fecha_modificacion) AS courier,
                   MIN(CASE WHEN estado_entrega = 'pendiente' THEN fecha_modificacion END) AS primer_pendiente,
                   MIN(CASE WHEN estado_entrega = 'entregado' THEN fecha_modificacion END) AS primer_entregado,
                   MAX(CASE WHEN estado_entrega = 'devuelto' THEN 1 ELSE 0 END) = 1     AS fue_devuelto
            FROM silver.tracking
            GROUP BY tracking_id
        )
        SELECT courier,
               COUNT(*) AS envios,
               COUNT(primer_entregado) AS envios_entregados,
               CAST(AVG(CASE WHEN primer_entregado IS NOT NULL AND primer_pendiente IS NOT NULL
                              AND primer_entregado >= primer_pendiente
                             THEN (unix_timestamp(primer_entregado) - unix_timestamp(primer_pendiente)) / 3600.0
                        END) AS DECIMAL(10,2)) AS horas_promedio_entrega,
               SUM(CASE WHEN fue_devuelto THEN 1 ELSE 0 END) AS envios_devueltos,
               SUM(CASE WHEN fue_devuelto OR primer_entregado IS NOT NULL THEN 1 ELSE 0 END) AS envios_finalizados,
               CAST(100.0 * try_divide(
                        SUM(CASE WHEN fue_devuelto THEN 1 ELSE 0 END),
                        SUM(CASE WHEN fue_devuelto OR primer_entregado IS NOT NULL THEN 1 ELSE 0 END)
                    ) AS DECIMAL(6,2)) AS pct_devueltos
        FROM envio
        GROUP BY courier
    """)


# ------------------------------------------------- Extra: VARIANT en reseñas
# Revisa las llaves reales con:  SELECT schema_of_variant_agg(metadata) FROM silver.resenas
@dp.materialized_view(
    name="gold.resenas_negativas_por_origen",
    comment="Extra VARIANT: % de reseñas negativas por origen (app/web/kiosko) leído de metadata.",
    table_properties=PROPS,
)
def resenas_negativas_por_origen():
    return spark.sql(f"""
        SELECT coalesce(metadata:origen::string, metadata:source::string,
                        metadata:canal::string, metadata:plataforma::string, 'desconocido') AS origen,
               coalesce(metadata:version_app::string, metadata:app_version::string,
                        metadata:version::string) AS version_app,
               COUNT(*) AS total_resenas,
               SUM(CASE WHEN calificacion <= 2 THEN 1 ELSE 0 END) AS resenas_negativas,
               CAST(100.0 * SUM(CASE WHEN calificacion <= 2 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(6,2))
                   AS pct_negativas
        FROM silver.resenas
        WHERE fecha_resena BETWEEN {INI} AND {FIN}
        GROUP BY ALL
    """)


# ------------------------------------------- Vista de empleados (masking)
@dp.materialized_view(
    name="gold.empleados_vista",
    comment="Versión actual de cada empleado. dni y salario enmascarados salvo para electrocasa_ingenieria.",
    schema=f"""
        id_empleado STRING,
        nombre STRING,
        dni STRING MASK {CATALOGO}.silver.mask_dni,
        salario DECIMAL(12,2) MASK {CATALOGO}.silver.mask_salario,
        sucursal_id STRING,
        nombre_sucursal STRING,
        cargo STRING,
        estado STRING,
        vigente_desde DATE
    """,
    table_properties=PROPS,
)
def empleados_vista():
    return spark.sql("""
        SELECT h.id_empleado, h.nombre, h.dni, h.salario, h.sucursal_id,
               s.nombre_sucursal, h.cargo, h.estado, h.vigente_desde
        FROM silver.empleados_historia h
        LEFT JOIN silver.sucursales s ON s.sucursal_id = h.sucursal_id
        WHERE h.es_version_actual
    """)
