"""Funciones de normalización reutilizables en Silver.

Todas devuelven expresiones SQL (texto) o Columns, para poder usarlas tanto en
DataFrames como dentro de funciones de orden superior (transform, etc.).
"""
from pyspark.sql import Column
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Conjuntos cerrados (mapeo por expresión regular sobre el texto normalizado:
# minúsculas, sin tildes, solo letras/números separados por un espacio).
# Las reglas se evalúan en orden: la primera que coincide gana.
# Ajusta los patrones si tu exploración en Bronze encuentra variantes nuevas:
#   SELECT metodo_pago, count(*) FROM bronze.ventas GROUP BY 1 ORDER BY 2 DESC
# ---------------------------------------------------------------------------
METODOS_PAGO = [
    (r"yape", "yape"),
    (r"plin", "plin"),
    (r"(efect|efv|cash|contado)", "efectivo"),
    (r"(transf|^trf$|deposit|banca)", "transferencia"),
    (r"(tarj|^tc$|^td$|credit|debit|visa|master|amex|pos)", "tarjeta"),
]

CATEGORIAS = [
    (r"(blanc|refri|lavad|secad|congel)", "linea_blanca"),
    (r"(cocin|horno|microond|campana|licuad)", "cocina"),
    (r"(clima|aire|ventil|calef|termo)", "climatizacion"),
    (r"(entret|tv|televi|audio|sonido|video|consol|gam)", "entretenimiento"),
    (r"(electron|tecno|comput|laptop|celular|smart)", "electronica"),
]

TIPOS_EVENTO = [
    (r"(salar|sueld|remun|aument)", "CAMBIO_SALARIO"),
    (r"(transf|trasl|reasig|cambio de suc|cambio suc|movim)", "TRANSFERENCIA"),
    (r"(baja|cese|cesad|renun|despid|desvinc|retiro|termin|salida)", "BAJA"),
    (r"(alta|ingres|contrat|nuevo|incorpor|hire)", "ALTA"),
]

ESTADOS_ENTREGA = [
    (r"(devuel|devol|retorn)", "devuelto"),
    (r"(entreg|recib)", "entregado"),
    (r"(transit|camino|ruta|reparto|despach|enviad)", "en_transito"),
    (r"(pend|registr|cread|prepar)", "pendiente"),
]

# Couriers: "Olva" y "Olva Courier" son el mismo courier (descubierto en la exploración)
COURIERS = [
    (r"olva", "Olva Courier"),
    (r"shalom", "Shalom"),
    (r"servientrega", "Servientrega"),
]

CANALES = [
    (r"(ecom|e com|online|web|digital|app)", "ecommerce"),
    (r"(tienda|fisic|presencial|store|local)", "tienda_fisica"),
]


def _texto_normalizado_sql(col_sql: str) -> str:
    """minúsculas, sin tildes, separadores colapsados a un espacio."""
    return (
        "trim(regexp_replace(lower(translate(trim(CAST({c} AS STRING)), "
        "'ÁÉÍÓÚÜÑáéíóúüñ', 'AEIOUUNaeiouun')), '[^a-z0-9]+', ' '))"
    ).format(c=col_sql)


def mapear(col_sql: str, reglas: list[tuple[str, str]], por_defecto: str | None) -> Column:
    """Mapea las variantes de escritura a un conjunto cerrado de valores.
    por_defecto=None deja NULL cuando ningún patrón coincide."""
    n = _texto_normalizado_sql(col_sql)
    casos = " ".join(f"WHEN {n} RLIKE '{patron}' THEN '{valor}'" for patron, valor in reglas)
    otro = "NULL" if por_defecto is None else f"'{por_defecto}'"
    return F.expr(
        f"CASE WHEN nullif(trim(CAST({col_sql} AS STRING)), '') IS NULL THEN NULL "
        f"{casos} ELSE {otro} END"
    )


def id_limpio(col_sql: str) -> Column:
    """Identificadores en mayúsculas, sin espacios; vacío -> NULL."""
    return F.expr(f"nullif(upper(trim(CAST({col_sql} AS STRING))), '')")


def texto_limpio(col_sql: str) -> Column:
    return F.expr(f"nullif(trim(CAST({col_sql} AS STRING)), '')")


def fecha_sql(col_sql: str) -> str:
    """Fecha en cualquiera de los formatos de origen -> DATE.
    try_to_timestamp devuelve NULL (en vez de fallar con ANSI) si el formato no coincide."""
    s = f"trim(CAST({col_sql} AS STRING))"
    formatos = ["yyyy-MM-dd", "dd/MM/yyyy", "d/M/yyyy",
                "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm:ss"]
    intentos = ", ".join(f"to_date(try_to_timestamp({s}, '{f}'))" for f in formatos)
    return f"coalesce({intentos}, to_date(try_to_timestamp({s})))"


def fecha(col_sql: str) -> Column:
    return F.expr(fecha_sql(col_sql))


def timestamp_sql(col_sql: str) -> str:
    s = f"trim(CAST({col_sql} AS STRING))"
    return (
        f"coalesce(try_to_timestamp({s}), try_to_timestamp({s}, 'dd/MM/yyyy HH:mm:ss'), "
        f"try_to_timestamp({s}, 'dd/MM/yyyy HH:mm'), CAST({fecha_sql(col_sql)} AS TIMESTAMP))"
    )


def marca_tiempo(col_sql: str) -> Column:
    return F.expr(timestamp_sql(col_sql))


def decimal_sql(col_sql: str, tipo: str = "DECIMAL(12,2)") -> str:
    """Montos y precios en texto -> decimal.
    - '1.234,50' / '150,90'  (coma decimal, punto de miles)
    - 'S/ 1,299.90' / '1299.90' (punto decimal, coma de miles, con moneda)
    Valores no convertibles -> NULL (luego los atrapa una expectativa)."""
    s = f"regexp_replace(trim(CAST({col_sql} AS STRING)), '[^0-9,.-]', '')"
    coma_decimal = (
        f"({s} RLIKE '^-?[0-9]{{1,3}}([.][0-9]{{3}})+(,[0-9]+)?$' "
        f"OR {s} RLIKE '^-?[0-9]+,[0-9]{{1,2}}$')"
    )
    return (
        f"CASE WHEN {coma_decimal} "
        f"THEN try_cast(replace(replace({s}, '.', ''), ',', '.') AS {tipo}) "
        f"ELSE try_cast(replace({s}, ',', '') AS {tipo}) END"
    )


def decimal(col_sql: str, tipo: str = "DECIMAL(12,2)") -> Column:
    return F.expr(decimal_sql(col_sql, tipo))


def entero(col_sql: str) -> Column:
    return F.expr(f"try_cast(try_cast(trim(CAST({col_sql} AS STRING)) AS DECIMAL(12,2)) AS INT)")
