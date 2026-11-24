"""Carga de la configuración de fuentes (ingesta por metadata).

El pipeline lee config/fuentes.json al iniciar: sumar una fuente de archivos nueva
solo requiere agregar una entrada en ese archivo, sin tocar el código de Bronze.
"""
import json
from pathlib import Path

_RUTA_CONFIG = Path(__file__).resolve().parent.parent / "config" / "fuentes.json"

# Período de análisis de las preguntas de negocio
PERIODO_INICIO = "2026-03-01"
PERIODO_FIN = "2026-10-31"

# Rango aceptado de salario mensual (S/). Fuera de él se marca como advertencia.
SALARIO_MIN = 500
SALARIO_MAX = 30000


def cargar_config() -> dict:
    with open(_RUTA_CONFIG, encoding="utf-8") as f:
        return json.load(f)


def fuentes(metodo: str | None = None) -> list[dict]:
    """Devuelve las fuentes configuradas, opcionalmente filtradas por método de ingesta."""
    todas = cargar_config()["fuentes"]
    return [f for f in todas if metodo is None or f["metodo"] == metodo]


def ruta_landing(catalogo: str, subcarpeta: str) -> str:
    base = cargar_config()["landing"].format(catalogo=catalogo)
    return f"{base}/{subcarpeta}/"
