"""Reglas de calidad por fuente: única fuente de verdad para el pipeline y los contratos.

politica:
  - fail : el pipeline se detiene (dato de referencia sin el cual nada se puede unir)
  - drop : la fila no entra a Silver y queda en silver.cuarentena
  - warn : la fila entra a Silver, la violación se registra en las métricas del
           pipeline y también en silver.cuarentena (politica = 'warn') para trazabilidad
"""

REGLAS: dict[str, list[dict]] = {
    "sucursales": [
        {"nombre": "sucursal_id_presente", "condicion": "sucursal_id IS NOT NULL", "politica": "fail"},
        {"nombre": "ciudad_presente", "condicion": "ciudad IS NOT NULL", "politica": "warn"},
    ],
    "catalogo": [
        {"nombre": "producto_id_presente", "condicion": "producto_id IS NOT NULL", "politica": "drop"},
        {"nombre": "precio_valido", "condicion": "precio_lista IS NOT NULL AND precio_lista > 0", "politica": "warn"},
        {"nombre": "categoria_valida", "condicion": "categoria IS NOT NULL AND categoria <> 'otra'", "politica": "warn"},
        {"nombre": "marca_presente", "condicion": "marca IS NOT NULL", "politica": "warn"},
    ],
    "ventas": [
        {"nombre": "venta_id_presente", "condicion": "venta_id IS NOT NULL", "politica": "drop"},
        {"nombre": "fecha_venta_valida", "condicion": "fecha_venta IS NOT NULL", "politica": "drop"},
        {"nombre": "sucursal_presente", "condicion": "sucursal_id IS NOT NULL", "politica": "drop"},
        {"nombre": "monto_positivo", "condicion": "monto_total IS NOT NULL AND monto_total > 0", "politica": "drop"},
        {"nombre": "cantidad_positiva", "condicion": "cantidad IS NOT NULL AND cantidad > 0", "politica": "drop"},
        {"nombre": "canal_valido", "condicion": "canal IN ('tienda_fisica', 'ecommerce')", "politica": "drop"},
        {"nombre": "producto_en_catalogo", "condicion": "producto_en_catalogo", "politica": "warn"},
        {"nombre": "metodo_pago_conocido", "condicion": "metodo_pago IS NOT NULL AND metodo_pago <> 'otro'", "politica": "warn"},
        {"nombre": "vendedor_en_tienda_fisica", "condicion": "canal <> 'tienda_fisica' OR vendedor_id IS NOT NULL", "politica": "warn"},
    ],
    "devoluciones": [
        {"nombre": "devolucion_id_presente", "condicion": "devolucion_id IS NOT NULL", "politica": "drop"},
        {"nombre": "venta_existe", "condicion": "venta_existe", "politica": "drop"},
        {"nombre": "reembolso_valido",
         "condicion": "monto_reembolso IS NOT NULL AND monto_reembolso >= 0 AND monto_reembolso <= monto_venta",
         "politica": "drop"},
        {"nombre": "fecha_devolucion_valida", "condicion": "fecha_devolucion IS NOT NULL", "politica": "drop"},
        {"nombre": "sucursal_presente", "condicion": "sucursal_id IS NOT NULL", "politica": "drop"},
        {"nombre": "motivo_presente", "condicion": "motivo IS NOT NULL", "politica": "warn"},
        {"nombre": "producto_en_catalogo", "condicion": "producto_en_catalogo", "politica": "warn"},
    ],
    "empleados": [
        {"nombre": "id_empleado_presente", "condicion": "id_empleado IS NOT NULL", "politica": "drop"},
        {"nombre": "fecha_evento_presente", "condicion": "fecha_evento IS NOT NULL", "politica": "drop"},
        {"nombre": "tipo_evento_valido",
         "condicion": "tipo_evento IN ('ALTA', 'TRANSFERENCIA', 'CAMBIO_SALARIO', 'BAJA')", "politica": "drop"},
        {"nombre": "salario_en_rango", "condicion": "salario_en_rango", "politica": "warn"},
        {"nombre": "dni_unico", "condicion": "NOT dni_duplicado", "politica": "warn"},
        {"nombre": "dni_presente", "condicion": "dni IS NOT NULL", "politica": "warn"},
    ],
    "resenas": [
        {"nombre": "resena_id_presente", "condicion": "resena_id IS NOT NULL", "politica": "drop"},
        {"nombre": "calificacion_valida", "condicion": "calificacion IS NOT NULL AND calificacion BETWEEN 1 AND 5", "politica": "drop"},
        {"nombre": "fecha_resena_valida", "condicion": "fecha_resena IS NOT NULL", "politica": "drop"},
        {"nombre": "producto_en_catalogo", "condicion": "producto_en_catalogo", "politica": "warn"},
        {"nombre": "comentario_presente", "condicion": "comentario IS NOT NULL", "politica": "warn"},
    ],
    "tracking": [
        {"nombre": "tracking_id_presente", "condicion": "tracking_id IS NOT NULL", "politica": "drop"},
        {"nombre": "estado_valido",
         "condicion": "estado_entrega IN ('pendiente', 'en_transito', 'entregado', 'devuelto')", "politica": "drop"},
        {"nombre": "fecha_modificacion_presente", "condicion": "fecha_modificacion IS NOT NULL", "politica": "drop"},
        {"nombre": "venta_existe", "condicion": "venta_existe", "politica": "warn"},
        {"nombre": "fecha_actualizacion_presente", "condicion": "fecha_actualizacion IS NOT NULL", "politica": "warn"},
    ],
}

# Columna que identifica cada registro en la cuarentena
LLAVES = {
    "sucursales": "sucursal_id",
    "catalogo": "producto_id",
    "ventas": "venta_id",
    "devoluciones": "devolucion_id",
    "empleados": "id_empleado",
    "resenas": "resena_id",
    "tracking": "evento_id",
}

# Columnas sensibles que no se copian al registro JSON de la cuarentena
EXCLUIR_DE_CUARENTENA = {
    "empleados": ["dni", "salario", "email"],
}


def condicion_segura(condicion: str) -> str:
    """Una condición que evalúa NULL cuenta como violación."""
    return f"coalesce(({condicion}), false)"


def reglas_por_politica(fuente: str, politica: str) -> dict[str, str]:
    return {
        r["nombre"]: condicion_segura(r["condicion"])
        for r in REGLAS[fuente]
        if r["politica"] == politica
    }
