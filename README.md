# Tarea 3 — Streaming de Datos · FPUNA MIA

**Alumno:** Gabriel Álvarez  
**Asignatura:** Streaming de Datos  
**Tema:** Apache Beam avanzado — ventanas, estado por clave e idempotencia

---

## Descripción

Pipeline de pagos en tiempo real que produce el total confirmado por comercio y
ventana de 60 segundos, tolerando eventos fuera de orden, duplicados y
reintentos al escribir.

El dataset `data/payments.jsonl` incluye todos los casos desafiantes:
eventos `PENDING` y `REJECTED` que se filtran, un `event_id` duplicado
(`p-002`), un evento muy retrasado que supera la latencia permitida (`p-007`,
169 s) y un evento fuera de orden (`p-004`, llega 53 s tarde).

---

## Ejecución rápida

### Con `uv` (recomendado)

```bash
# 1. Instalar uv (si no está instalado)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Sincronizar dependencias
uv sync

# 3. Correr la suite de pruebas
uv run pytest -v

# 4. Abrir el notebook interactivo
uv run marimo edit notebook.py
```

### Con Docker

```bash
# Construir imagen
docker build -t tarea3-beam .

# Correr suite de tests dentro del contenedor
docker run --rm tarea3-beam uv run pytest -v

# Levantar el notebook interactivo en http://localhost:2718
docker compose up
```

---

## Evidencia de ejecución

```
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-8.4.2, pluggy-1.6.0
collected 13 items

tests/test_assignment.py::test_parse_utc_returns_timezone_aware_datetime PASSED
tests/test_assignment.py::test_assign_fixed_window_uses_event_time PASSED
tests/test_assignment.py::test_duplicate_does_not_change_total PASSED
tests/test_assignment.py::test_deduplication_is_isolated_by_merchant PASSED
tests/test_assignment.py::test_stateful_dofn_keeps_keys_isolated PASSED
tests/test_assignment.py::test_out_of_order_event_uses_its_event_time_window PASSED
tests/test_assignment.py::test_late_event_within_tolerance_is_a_revision PASSED
tests/test_assignment.py::test_event_beyond_lateness_is_audited PASSED
tests/test_assignment.py::test_windowed_pipeline_produces_totals PASSED
tests/test_assignment.py::test_trigger_policy_has_lateness_and_accumulating_panes PASSED
tests/test_assignment.py::test_retries_converge_to_one_materialized_entity PASSED
tests/test_assignment.py::test_append_only_sink_materializes_every_attempt PASSED
tests/test_assignment.py::test_timer_handler_clears_state PASSED

============================== 13 passed in 2.38s ==============================
```

---

## Diseño e implementación

### 1. Contrato temporal y ventanas (`parse_utc`, `assign_fixed_window`, `summarize_payments`)

`parse_utc` convierte timestamps ISO-8601 terminados en `Z` a `datetime`
timezone-aware en UTC. `assign_fixed_window` calcula el bucket de ventana fija
a partir del epoch Unix: `start = floor(elapsed / size) * size`. La ventana
depende exclusivamente del **tiempo de evento**, no del tiempo de procesamiento.

`summarize_payments` es la implementación pura de Python que actúa como oráculo:

- Solo cuenta pagos `CONFIRMED`.
- Deduplica por `event_id` dentro del mismo `merchant_id` (el estado de
  deduplicación es independiente entre comercios).
- Calcula el atraso como `arrival_time − event_time` en segundos.
- Un evento es `too_late` si su atraso supera `allowed_lateness_seconds`.
- Un evento aceptado que llega después del cierre nominal de su ventana es una
  `revision` (`accepted=True`, `revision=True`, `reason="accepted"`).

Resultados con la configuración por defecto (120 s de latencia):

| Comercio | Ventana | Total | Eventos aceptados |
|---|---|---:|---|
| m-azul | 13:00–13:01 | 170 000 | p-001, p-004 (fuera de orden) |
| m-verde | 13:00–13:01 | 80 000 | p-002 (duplicado rechazado) |
| m-verde | 13:01–13:02 | 90 000 | p-005 |
| m-azul | 13:02–13:03 | 200 000 | p-006 |

p-003 (PENDING), p-007 (atraso 169 s > 120 s) y p-008 (REJECTED) se excluyen.

### 2. Pipeline Beam (`build_windowed_totals_pipeline`)

```
Create(events)
  → Filter(status == "CONFIRMED")
  → Map(TimestampedValue, timestamp=event_time)
  → WindowInto(FixedWindows(window_seconds))
  → Map(e → (merchant_id, amount))
  → CombinePerKey(sum)
  → Map(format_result)   # recupera límites con WindowParam
```

`WindowParam` en `beam.Map` permite acceder al objeto `window` en la función de
formato, de donde se extraen `window.start` y `window.end` como timestamps Unix.

### 3. Estado y deduplicación (`DeduplicatePayments`)

`DeduplicatePayments` es un `DoFn` con estado por clave:

- `SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())` — conjunto de
  `event_id` ya procesados. Beam aisla el estado por clave de agrupación
  (`merchant_id`), por lo que dos comercios con el mismo `event_id` no
  interfieren.
- `process()` consulta `set(seen_ids.read())` antes de emitir; si el `event_id`
  ya está, descarta el elemento. En el primer procesamiento lo añade y arma el
  timer de expiración.
- `EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)` — timer en tiempo de
  evento que dispara al final de la ventana (`window.end`).
- `expire()` llama `seen_ids.clear()`. Sin esta limpieza el estado crecería
  indefinidamente: en un pipeline de larga duración cada comercio acumularía
  todos los `event_id` vistos desde el inicio, agotando la memoria del worker.

### 4. Triggers y panes acumulativos (`build_trigger_policy`)

```python
beam.WindowInto(
    FixedWindows(60),
    trigger=AfterWatermark(
        early=AfterProcessingTime(30),   # pane estimado cada 30 s
        late=AfterCount(1),              # pane revisado en cada llegada tardía
    ),
    accumulation_mode=ACCUMULATING,      # cada pane incluye todos los eventos
    allowed_lateness=Duration(seconds=120),
)
```

`ACCUMULATING` es esencial: los panes tardíos contienen la suma total
(incluyendo eventos anteriores), no solo el incremento. Esto permite al sink
sobrescribir el total anterior con el valor actualizado sin necesidad de
lógica adicional de combinación.

### 5. Idempotencia (`make_idempotency_key`, `simulate_sink_retries`)

La clave `merchant_id|window_start` identifica unívocamente cada resultado
lógico. Con un sink UPSERT (diccionario en memoria), múltiples reintentos del
mismo resultado convergen a **una sola fila materializada** porque el segundo
intento sobreescribe la misma clave. Con un sink append-only (lista), cada
intento genera una fila nueva, lo que puede producir duplicados si el
procesamiento no es idempotente.

---

## Estructura del repositorio

```
.
├── notebook.py          # Implementación completa (8 TODOs)
├── data/
│   └── payments.jsonl   # 9 eventos de pagos
├── tests/
│   ├── conftest.py      # Fixture que extrae símbolos del notebook
│   └── test_assignment.py  # 13 tests
├── pyproject.toml       # Dependencias (apache-beam 2.74, marimo ≥ 0.19)
├── uv.lock
├── Dockerfile
└── docker-compose.yml
```
