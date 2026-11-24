# ElectroCasa Lakehouse — Proyecto Integrador

Plataforma de datos sobre **Azure Databricks / Databricks Free Edition** para ElectroCasa, una cadena de 40 sucursales de electrodomésticos en Perú. Integra seis fuentes heterogéneas (ventas, catálogo, RR.HH., reseñas, devoluciones y tracking de envíos) y la tabla de referencia de sucursales en un modelo **Medallion** (Bronze → Silver → Gold) construido con **Lakeflow Spark Declarative Pipelines**, orquestado con **Lakeflow Jobs**, gobernado con **Unity Catalog** y desplegado con un **Declarative Automation Bundle**.

> Los datos, sucursales y personas son ficticios.

---

## 1. El caso

Hoy no hay una fuente única de verdad: cada sucursal exporta sus ventas por separado, RR.HH. registra eventos en su propio sistema, las reseñas se leen a mano y el tracking vive en la base del courier. La capa Gold responde seis preguntas de negocio (marzo a octubre de 2026):

| # | Pregunta | Tabla Gold |
|---|---|---|
| 1 | Ventas netas, cantidad de ventas y ticket promedio por sucursal y mes | `gold.ventas_netas_sucursal_mes` |
| 2 | Top 10 por unidades y top 10 por tasa de devolución | `gold.top_productos_unidades`, `gold.top_productos_tasa_devolucion` |
| 3 | Dotación activa por sucursal al cierre de cada mes | `gold.dotacion_sucursal_mes` |
| 4 | Ventas de tienda física por vendedor, sucursal del vendedor **en la fecha de la venta** y mes | `gold.ventas_vendedor_sucursal_mes` |
| 5 | % de reseñas negativas por categoría y mes | `gold.resenas_negativas_categoria_mes` |
| 6 | Horas promedio de entrega y % de devueltos por courier | `gold.despachos_courier` |
| extra | Reseñas negativas por origen (VARIANT) | `gold.resenas_negativas_por_origen` |
| extra | Versión actual de empleados con masking | `gold.empleados_vista` |

---

## 2. Arquitectura

```mermaid
flowchart LR
  subgraph Fuentes
    A1[CSV ventas]:::f
    A2[CSV devoluciones]:::f
    A3[CSV empleados]:::f
    A4[JSONL reseñas]:::f
    A5[JSON catálogo]:::f
    A6[CSV sucursales]:::f
    A7[(Azure SQL<br/>dbo.TrackingEnvios)]:::f
  end
  subgraph Landing["/Volumes/&lt;cat&gt;/default/vol_landing"]
    L[subcarpeta por fuente]
  end
  A1 & A2 & A3 & A4 & A5 & A6 --> L
  subgraph Bronze
    B1[ventas / devoluciones /<br/>empleados_eventos / resenas<br/>Auto Loader - Streaming Tables]
    B2[catalogo / sucursales<br/>COPY INTO - Delta]
    B3[tracking_envios<br/>Lakeflow Connect query-based]
  end
  L --> B1
  L --> B2
  A7 --> B3
  subgraph Silver
    S1[ventas, devoluciones, resenas,<br/>catalogo, catalogo_actual, sucursales,<br/>tracking, empleados_eventos]
    S2[empleados_historia<br/>SCD Type 2]
    Q[cuarentena]
  end
  B1 & B2 & B3 --> S1
  S1 --> S2
  S1 -. rechazos .-> Q
  subgraph Gold
    G[6 preguntas de negocio<br/>+ extras]
  end
  S1 & S2 --> G
  G --> C[Analistas / Auditoría]
  classDef f fill:#eef,stroke:#88a
```

**Orden del job:**

```
copy_into_bronze (notebook) ─┐
                             ├─► pipeline_principal (pipeline) ─► validacion (notebook)
ingesta_tracking (pipeline) ─┘
```

La ingesta de Tracking y el pipeline principal van **en secuencia** (en Free Edition solo puede haber una pipeline activa por tipo).

### Estructura del repositorio

```
electrocasa-lakehouse/
├── databricks.yml                       # bundle: variables y targets dev / prod
├── resources/
│   ├── electrocasa_pipeline.yml         # pipeline Lakeflow SDP (serverless)
│   └── electrocasa_job.yml              # job: tareas, dependencias, trigger, reintentos y alertas
├── src/electrocasa/
│   ├── config/fuentes.json              # ingesta por metadata (rutas, formato, schema, opciones)
│   ├── transformations/
│   │   ├── 01_bronze.py                 # Auto Loader dirigido por fuentes.json
│   │   ├── 02_silver_referencia.py      # sucursales, catálogo, catálogo actual
│   │   ├── 03_silver_ventas.py          # ventas y devoluciones
│   │   ├── 04_silver_empleados.py       # eventos + SCD2 (con MASK)
│   │   ├── 05_silver_resenas.py         # reseñas (VARIANT)
│   │   ├── 06_silver_tracking.py        # tracking sin reprocesados
│   │   ├── 07_silver_cuarentena.py      # rechazos consultables
│   │   └── 08_gold.py                   # preguntas de negocio
│   └── utils/
│       ├── config.py                    # carga de fuentes.json, período, rangos
│       ├── normalizacion.py             # fechas, decimales, mapeos a conjuntos cerrados
│       ├── reglas.py                    # reglas de calidad (fuente de verdad de pipeline y contratos)
│       └── calidad.py                   # expectativas por política + constructor de cuarentena
├── contratos/                           # un contrato de datos por fuente
├── notebooks/
│   ├── 00_setup.ipynb                   # catálogo, schemas, volume, máscaras, GRANT / REVOKE
│   ├── 01_copy_into.ipynb               # COPY INTO idempotente (catálogo, sucursales)
│   ├── 02_validacion.ipynb              # conteos, rechazos y falla si Gold está vacío
│   └── 03_monitoreo_y_evidencias.ipynb  # event log, system tables, masking, SCD2, VARIANT
└── img/                                 # capturas de evidencia
```

---

## 3. Ingesta

### 3.1 Método por fuente

| Fuente | Método | Tabla Bronze | Justificación |
|---|---|---|---|
| Ventas | **Auto Loader** | `bronze.ventas` (ST) | Lotes incrementales que solo agregan filas; Auto Loader recuerda qué archivos procesó y escala si el volumen crece. |
| Devoluciones | **Auto Loader** | `bronze.devoluciones` (ST) | Mismo patrón append incremental que ventas. |
| Empleados | **Auto Loader** | `bronze.empleados_eventos` (ST) | Son eventos (append puro), con llegadas tardías en lotes posteriores. |
| Reseñas | **Auto Loader** | `bronze.resenas` (ST) | JSON Lines anidado por lotes; schema explícito con arreglos y structs. |
| Catálogo | **COPY INTO** | `bronze.catalogo` (Delta) | Snapshot completo poco frecuente (marzo, agosto): una carga por archivo, idempotente, sin necesidad de un stream. |
| Sucursales | **COPY INTO** | `bronze.sucursales` (Delta) | Referencia de un solo archivo; COPY INTO no lo vuelve a cargar si se reejecuta. |
| Tracking | **Lakeflow Connect** (query-based SQL Server) | `bronze.tracking_envios` | Obligatorio. Cursor `fecha_modificacion`, PK `evento_id`. |

- **Schema explícito** en todos los CSV (todo como `STRING` en Bronze: los montos llegan con coma decimal y las fechas en varios formatos; la conversión es de Silver). No se usa `inferSchema`.
- **Columnas de auditoría** en Bronze: `_source_file` (`_metadata.file_name`), `_source_path`, `_file_modification_ts`, `_lote` e `_ingest_ts`.
- **Sin lógica de negocio en Bronze** y **sin particionar por fecha de negocio**.
- Auto Loader corre **dentro del pipeline**: el checkpoint y la ubicación del schema los administra el pipeline (no se configuran rutas a mano).
- **COPY INTO va fuera del pipeline** (tarea `copy_into_bronze` del job) y el pipeline lee esas tablas en batch desde Materialized Views.
- Catálogo con `multiLine` + `primitivesAsString` (evita `DELTA_FAILED_TO_MERGE_FIELDS` porque `activo` llega como booleano).

### 3.2 Tracking (Lakeflow Connect)

1. Connection de Unity Catalog tipo **SQL Server** (`Catalog > External data > Connections > Create connection`). La contraseña vive **solo** en la Connection.
2. Catálogo foráneo `electrocasa_sqlserver` (solo para explorar la tabla).
3. Ingesta query-based: tabla `dbo.TrackingEnvios`, **cursor `fecha_modificacion`**, **PK `evento_id`** (no `tracking_id`, que se repite por diseño), destino `electrocasa_dev.bronze.tracking_envios`, columnas seleccionadas: `evento_id, tracking_id, venta_id, courier, estado_entrega, sucursal_origen, fecha_actualizacion, fecha_modificacion`.
4. **Modo de historia: append only.** Cada fila es un evento inmutable; no hay nada que actualizar. Append conserva todos los eventos (incluidos los reprocesados) tal como llegaron, y la deduplicación se hace en Silver, donde es visible y auditable.

**Evidencia de las dos corridas** (`DESCRIBE HISTORY electrocasa_dev.bronze.tracking_envios`):

| Corrida | Operación | Filas |
|---|---|---|
| 1 (carga inicial) | `<completar>` | `<completar: 15.139>` |
| 2 (sin cambios) | sin versión nueva | 0 |

![Primera corrida del conector](img/tracking_corrida1.png)
![Segunda corrida del conector](img/tracking_corrida2.png)
![DESCRIBE HISTORY](img/tracking_history.png)

---

## 4. Silver: limpieza, estandarización, deduplicación y calidad

Silver se implementa con **Materialized Views** sobre Bronze. Al recalcularse con todos los lotes, la deduplicación de reingestas es exacta y los eventos tardíos de Empleados quedan en su lugar sin lógica extra.

| Normalización | Cómo |
|---|---|
| Fechas en varios formatos → `DATE`/`TIMESTAMP` | `coalesce` de `try_to_timestamp` con cada formato (no falla con ANSI) |
| `1.234,50` y `S/ 1,299.90` → `DECIMAL(12,2)` | detección de coma decimal vs. coma de miles (`utils/normalizacion.py`) |
| Métodos de pago, categorías, tipos de evento, estados, canal | mapeo por expresión regular a conjuntos cerrados |
| Courier | casing unificado (`initcap`) |
| `ciudad` en sucursales | `initcap(lower(trim))` |
| Reingestas | ventas por `venta_id`, devoluciones por `devolucion_id`, reseñas por `resena_id` (queda la primera que llegó) |
| Productos repetidos en marzo | uno por producto y snapshot (precio válido y mayor) |
| Reprocesados de tracking | mismo `tracking_id` + estado + `fecha_actualizacion` → queda el de menor `fecha_modificacion` |

### 4.1 Expectativas de calidad y políticas

Las reglas viven en `src/electrocasa/utils/reglas.py`; el pipeline (`dp.expect_all`, `expect_all_or_drop`, `expect_all_or_fail`) y los contratos se generan desde ahí, así **el contrato coincide con el código**.

| Fuente | Regla | Política | Por qué |
|---|---|---|---|
| Sucursales | `sucursal_id` no nulo | **fail** | Es la dimensión que une ventas, devoluciones, dotación y vendedores; sin ella todo Gold sería incorrecto. Es un archivo de referencia chico: mejor detener y corregir. |
| Ventas | `venta_id`, `fecha_venta`, `sucursal_id` presentes; `monto_total > 0`; `cantidad > 0`; canal válido | **drop** | Sin ellos la venta no se puede asignar a sucursal/mes ni sumar (P1, P2, P4). Se descarta la fila, no todo el lote. |
| Ventas | producto en catálogo, método de pago conocido, vendedor en tienda física | warn | La venta ocurrió: excluirla distorsionaría las ventas netas. |
| Catálogo | `producto_id` presente | drop | Sin llave no hay producto. |
| Catálogo | `precio_lista` numérico > 0, categoría válida, marca presente | warn | Ninguna pregunta usa el precio; descartar el producto haría que sus reseñas se excluyan como "producto inexistente". |
| Empleados | `id_empleado` y `fecha_evento` no nulos; `tipo_evento` válido | **drop** | Sin ellos no se puede ubicar el evento en la historia. Un `NOT NULL` en el schema no lo impide: lo frena la expectativa. |
| Empleados | salario en rango (500–30 000), dni único, dni presente | warn | No afectan dotación ni ventas por vendedor. |
| Reseñas | `calificacion` entera entre 1 y 5; `resena_id` y `fecha_resena` presentes | **drop** | P5 se calcula sobre la calificación. |
| Reseñas | producto en catálogo, comentario presente | warn | La reseña es válida; P5 excluye los productos inexistentes con un `INNER JOIN`. |
| Devoluciones | venta existe; `0 <= monto_reembolso <= monto de la venta`; fecha y sucursal presentes | **drop** | La regla se evalúa sobre la vista con el join a ventas. Un reembolso inválido restaría ventas netas que no existen. |
| Tracking | estado dentro de {pendiente, en_transito, entregado, devuelto}; `tracking_id` y `fecha_modificacion` presentes | **drop** | P6 se basa en el estado y el tiempo entre estados. |
| Tracking | venta existe, `fecha_actualizacion` presente | warn | El tiempo de entrega del courier sigue siendo válido. |

### 4.2 Cuarentena consultable (`silver.cuarentena`)

Una fila por registro y regla violada, con **fuente, regla, condición, política, id del registro, lote, archivo de origen, fecha de ingesta, fecha de proceso y el registro completo en JSON** (sin `dni`/`salario`/`email`). `politica = 'drop'` son los rechazados; `politica = 'warn'` quedan en Silver pero registrados. La tabla se recalcula desde Bronze en cada corrida, así que **corregir un mapeo o una regla reprocesa automáticamente** los registros.

```sql
SELECT fuente, regla, politica, count(*) FROM electrocasa_dev.silver.cuarentena GROUP BY ALL;
```

### 4.3 Historización de Empleados (SCD Type 2)

Implementado **armando los intervalos sobre la tabla de eventos** (`silver.empleados_historia`, Materialized View), en lugar de `AUTO CDC`:

- Cada evento válido genera una versión. Orden: **`fecha_evento`** (cuándo ocurrió) y **`fecha_registro`** (cuándo lo cargó RR.HH.) para desempatar dos eventos del mismo día. Nunca `current_timestamp()`.
- `vigente_desde = fecha_evento`, `vigente_hasta = fecha del evento siguiente` (exclusivo), `NULL` = versión actual.
- Los atributos que un evento no trae (p. ej. el salario en una transferencia) se arrastran de la versión anterior (`last(..., ignorenulls)`).
- `estado`: `ACTIVO` desde un ALTA, `CESADO` desde una BAJA/cese, `SIN_ALTA` si no hay alta.
- **Eventos tardíos:** como la vista se recalcula con todos los eventos, un evento de junio que llega en el lote 3 se ubica en su lugar y reabre/cierra los intervalos correctos.
- **Por qué no `AUTO CDC`:** con eventos tardíos y empates en el mismo día, la historia completa recalculada es determinista y fácil de auditar; el orden sale igual de los datos de la fuente.

Uso en Gold: un empleado está activo en la fecha *d* si `estado = 'ACTIVO' AND vigente_desde <= d AND (vigente_hasta IS NULL OR vigente_hasta > d)`. Una baja en *d* lo deja inactivo ese día. En P4, si no hay versión activa en la fecha de la venta → `SIN_ASIGNAR`.

### 4.4 Decisiones

| Decisión | Elección | Justificación |
|---|---|---|
| Ventas con producto inexistente | Se conservan (warn, `producto_en_catalogo = false`) | La venta ocurrió; excluirla subestima P1. |
| Reseñas con producto inexistente | Se conservan en Silver (warn) y P5 las excluye | Lo pide la pregunta; Silver no pierde información. |
| Devoluciones con venta inexistente | Se descartan (drop → cuarentena) | No se puede validar el reembolso contra la venta ni asegurar a qué venta resta. |
| Tracking con venta inexistente | Se conserva (warn) | El tiempo del courier es válido igual. |
| Mismo `dni` en dos empleados | Se conservan ambos, marcados `dni_duplicado` (warn) | La llave es `id_empleado`; el dni no participa en ninguna pregunta. |
| Salarios fuera de rango | Se conservan, marcados `salario_en_rango = false` (warn) | No afectan las preguntas; corregirlos requiere a RR.HH. |
| Máscara de `dni` | Últimos 3 dígitos visibles (`*****678`) | Permite verificación parcial sin exponer el documento. |
| Máscara de `salario` | `NULL` | La función debe devolver `DECIMAL(12,2)`; no admite `***`. |
| Conector de Tracking | Append only | Eventos inmutables; la dedup se hace en Silver. |
| Productos descontinuados | Se conservan con `activo = false` | Ventas y reseñas históricas los referencian. |
| Auto Loader vs COPY INTO | Auto Loader: ventas, devoluciones, empleados, reseñas. COPY INTO: catálogo, sucursales | Ver 3.1. |

---

## 5. Gold

Las siete tablas Gold se calculan sobre Silver, filtrando el período 2026-03-01 a 2026-10-31:

- **P1:** ventas por `fecha_venta` y devoluciones por `fecha_devolucion` se agregan **por separado** y se unen con `FULL OUTER JOIN` (un mes con devoluciones y sin ventas también aparece). Ticket = ventas sin restar devoluciones / `COUNT(DISTINCT venta_id)`.
- **P2:** unidades = `SUM(cantidad)`; tasa = `COUNT(DISTINCT devolucion_id)` / `COUNT(DISTINCT venta_id)` del producto.
- **P3:** calendario de cierres de mes × historia SCD2; incluye sucursales con 0 empleados.
- **P4:** solo `tienda_fisica`, join por rango de vigencia; sin versión vigente → `SIN_ASIGNAR`.
- **P5:** categoría del **último snapshot** (`silver.catalogo_actual`); `INNER JOIN` excluye productos inexistentes; Silver ya excluye calificaciones inválidas.
- **P6:** primer `pendiente` y primer `entregado` por envío (`fecha_modificacion`); % devueltos = envíos con `devuelto` / envíos con estado final.

---

## 6. Orquestación (Lakeflow Jobs)

`resources/electrocasa_job.yml`:

| Tarea | Tipo | Depende de | Reintentos |
|---|---|---|---|
| `copy_into_bronze` | notebook | — | 2 (1 min) |
| `ingesta_tracking` | pipeline (conector) | — | 2 (2 min) |
| `pipeline_principal` | pipeline | ambas anteriores | 1 |
| `validacion` | notebook | `pipeline_principal` | 0 (si falla es que hay un problema real de datos) |

- **Trigger: llegada de archivo** sobre `/Volumes/<catalogo>/default/vol_landing/`. Los lotes llegan sin calendario fijo; correr por horario gastaría cuota sin datos nuevos. `min_time_between_triggers_seconds: 300` y `wait_after_last_change_seconds: 120` agrupan los archivos de un mismo lote en una corrida. En `prod` el trigger queda pausado (`presets.trigger_pause_status: PAUSED`) para no consumir la cuota de Free Edition.
- **Alertas:** correo en fallo y en éxito (`email_alertas`).
- `max_concurrent_runs: 1`: nunca dos pipelines a la vez.

### Compute

| Recurso | Compute | Justificación |
|---|---|---|
| Pipeline principal | Serverless, modo triggered (no continuo) | Corre una vez por lote, datos chicos (decenas de miles de filas); no tiene sentido un cluster encendido. Continuo en serverless está prohibido. |
| Conector Tracking | Serverless (lo administra Lakeflow Connect) | Ingesta incremental corta por cursor. |
| Notebooks del job (COPY INTO, validación) | Serverless jobs compute | Tareas cortas; arranque rápido; sin clusters all-purpose en el job productivo. |
| Consultas ad hoc / evidencias | SQL warehouse 2X-Small o serverless notebook | Interactivo y de bajo consumo. |

---

## 7. Gobierno y seguridad (Unity Catalog)

- Catálogos `electrocasa_dev` y `electrocasa_prod`; schemas `bronze`, `silver`, `gold`; volume `default.vol_landing`. Todas las tablas son **managed**.
- Grupos creados en **Settings > Identity and access > Groups** (no por API/SDK, que los crea como grupos locales y el `GRANT` falla con `PRINCIPAL_DOES_NOT_EXIST`).

| Grupo | Permisos (`notebooks/00_setup.ipynb`) |
|---|---|
| `electrocasa_ingenieria` | `USE CATALOG`; `USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MATERIALIZED VIEW, CREATE FUNCTION, EXECUTE` en bronze/silver/gold; `READ/WRITE VOLUME` en landing |
| `electrocasa_analistas` | `USE CATALOG`; `USE SCHEMA, SELECT` en gold |
| `electrocasa_auditoria` | `USE CATALOG`; `USE SCHEMA, SELECT` en gold; system tables (ver abajo) |

**Demostración de REVOKE** (`SELECT` sobre silver a analistas):

| Momento | Evidencia |
|---|---|
| Antes | ![antes](img/revoke_1_antes.png) |
| Después del GRANT indebido | ![grant](img/revoke_2_grant.png) |
| Después del REVOKE | ![revoke](img/revoke_3_despues.png) |

**Auditoría y tablas de sistema.** Error obtenido al otorgar `system.access` en Free Edition:

```
<pegar aquí el error que muestra la celda 5 de 00_setup>
```

Alternativa usada: pestaña **Lineage** de Catalog Explorer → ![lineage](img/lineage.png)

**Masking.** `dni` y `salario` llevan `MASK` declarado en el schema de las tablas del pipeline (`silver.empleados_eventos`, `silver.empleados_historia`, `gold.empleados_vista`). Las funciones `silver.mask_dni` y `silver.mask_salario` usan `is_account_group_member('electrocasa_ingenieria')`. El dueño del pipeline es miembro de `electrocasa_ingenieria`, por eso la tabla derivada `gold.empleados_vista` conserva los valores reales para ese grupo y se enmascara para el resto:

![masking ingenieria](img/masking_ingenieria.png)

> Si no se hubieran podido crear los grupos, la función compararía con `current_user() = '<correo>'` (ver comentario en `00_setup`).

---

## 8. CI/CD (Declarative Automation Bundle)

- `databricks.yml` en la raíz; recursos en `resources/`.
- Targets: **`dev`** (`electrocasa_dev`, modo development, trigger activo) y **`prod`** (`electrocasa_prod`, modo production, trigger pausado, `run_as` explícito). Ambos en el mismo workspace, diferenciados por catálogo.

### Pasos de despliegue

1. Crear los 3 grupos en la UI y agregarse a `electrocasa_ingenieria`.
2. Ejecutar `notebooks/00_setup.ipynb` con `catalogo = electrocasa_dev` (y luego `electrocasa_prod`).
3. Crear la Connection + la ingesta de Tracking (sección 3.2) y copiar el **ID de esa pipeline**.
4. En `databricks.yml`, completar `email_alertas` y `tracking_pipeline_id` (o pasarlos con `--var`).
5. Desplegar:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   ```
   (o desde el panel **Deployments** del Git Folder).
6. Subir el **lote 1** al volume (+ snapshot de marzo + `sucursales.csv`), esperar el trigger o correr `databricks bundle run electrocasa_job -t dev`.
7. Anotar los conteos (tarea `validacion`) y repetir con los lotes 2 y 3.

![bundle validate](img/bundle_validate.png)
![bundle deploy](img/bundle_deploy.png)

---

## 9. Evidencia de ejecución por lote

Conteos tomados de la salida de `02_validacion` después de cada lote.

### Bronze

| Tabla | Lote 1 | Lote 2 | Lote 3 |
|---|---|---|---|
| bronze.ventas | 6.783 | 13.622 | 18.240 |
| bronze.devoluciones | 1.387 | 3.091 | 4.217 |
| bronze.empleados_eventos | 2.257 | 3.524 | 4.448 |
| bronze.resenas | 5.292 | 9.280 | 9.280 |
| bronze.catalogo | 2.305 | 4.705 | 4.705 |
| bronze.sucursales | 40 | 40 | 40 |
| bronze.tracking_envios | 15.139 | 15.139 | 15.139 |

> Valores esperados según el README de datos; **confirmar con la salida real**.

### Silver

| Tabla | Lote 1 | Lote 2 | Lote 3 |
|---|---|---|---|
| silver.ventas | `<…>` | `<…>` | `<…>` |
| silver.devoluciones | `<…>` | `<…>` | `<…>` |
| silver.empleados_eventos | `<…>` | `<…>` | `<…>` |
| silver.empleados_historia | `<…>` | `<…>` | `<…>` |
| silver.resenas | `<…>` | `<…>` | `<…>` |
| silver.catalogo / catalogo_actual | `<…>` | `<…>` | `<…>` |
| silver.sucursales | `<…>` | `<…>` | `<…>` |
| silver.tracking | `<…>` | `<…>` | `<…>` |

### Rechazados (`politica = 'drop'`) por fuente

| Fuente | Lote 1 | Lote 2 | Lote 3 |
|---|---|---|---|
| ventas | `<…>` | `<…>` | `<…>` |
| devoluciones | `<…>` | `<…>` | `<…>` |
| empleados | `<…>` | `<…>` | `<…>` |
| resenas | `<…>` | `<…>` | `<…>` |
| catalogo | `<…>` | `<…>` | `<…>` |
| tracking | `<…>` | `<…>` | `<…>` |

**Lectura de los conteos:** Bronze crece con cada lote (incluye las reingestas), pero `silver.ventas`, `silver.devoluciones` y `silver.resenas` crecen solo con los IDs nuevos (la consulta de `02_validacion` compara filas de Bronze, IDs distintos y filas de Silver). Los eventos tardíos del lote 3 cambian versiones de junio-agosto en `silver.empleados_historia` (ver `03_monitoreo`, celda 7).

![pipeline lote 1](img/pipeline_lote1.png)
![job lote 3](img/job_lote3.png)

---

## 10. Monitoreo

- **Event log del pipeline** (`event_log(TABLE(...))`): actualizaciones y métricas por expectativa (filas OK / fallidas) → ![event log](img/event_log.png)
- **Historial del job** (`system.lakeflow.job_run_timeline`) → captura o error + alternativa (pestaña Runs del job).
- **Costos** (`system.billing.usage`, reto opcional) → captura o error.

---

## 11. Retos opcionales implementados

| Reto | Dónde |
|---|---|
| **Ingesta por metadata** | `src/electrocasa/config/fuentes.json`: Bronze (Auto Loader) y COPY INTO se generan desde ese archivo; una fuente nueva = una entrada nueva. |
| **Cuarentena con trazabilidad completa** | `silver.cuarentena`: fuente, regla, lote, archivo, fechas, registro JSON; se reprocesa al corregir la regla. |
| **VARIANT en Reseñas** | `metadata` → `VARIANT` en Silver (`try_parse_json`); `gold.resenas_negativas_por_origen`. |
| Costos medidos | Consulta a `system.billing.usage` en `03_monitoreo` (si la tabla está disponible). |

---

## 12. Antipatrones evitados

Sin credenciales en el código (Connection de UC) · sin checkpoints en `/tmp` (los administra el pipeline) · `_metadata.file_name` en vez de `input_file_name()` · schema explícito, sin `inferSchema` · Bronze sin particionar por fecha de negocio y sin lógica de negocio · sin `trigger(once=True)` ni pipelines continuos · sin clusters all-purpose en el job · sin `dbutils.fs.mount` · sin `outputMode("complete")` · sin `current_timestamp()` para ordenar la historia de empleados.
