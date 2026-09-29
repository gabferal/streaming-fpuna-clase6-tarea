import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de código contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la solución.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        from datetime import timezone

        if not isinstance(raw_value, str) or not raw_value:
            raise ValueError(f"Timestamp inválido: {raw_value!r}")
        try:
            dt = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"Timestamp ISO-8601 inválido: {raw_value!r}"
            ) from exc
        if dt.tzinfo is None:
            raise ValueError(
                f"El timestamp debe ser timezone-aware: {raw_value!r}"
            )
        return dt.astimezone(timezone.utc)

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        from datetime import timedelta, timezone

        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        elapsed = (timestamp - epoch).total_seconds()
        window_start_secs = int(elapsed // size_seconds) * size_seconds
        start = epoch + timedelta(seconds=window_start_secs)
        end = start + timedelta(seconds=size_seconds)
        return start, end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.
        """
        from collections import defaultdict

        # parse_utc y assign_fixed_window están disponibles en el namespace
        # del exec de conftest porque todas las celdas se compilan juntas.

        totals_acc: dict[tuple[str, str], int] = {}
        window_ends: dict[tuple[str, str], str] = {}
        seen_per_merchant: dict[str, set] = defaultdict(set)
        audit: list[dict[str, Any]] = []

        for event in events:
            event_id = event["event_id"]
            merchant_id = event["merchant_id"]
            status = event["status"]
            amount = int(event["amount"])
            et = parse_utc(event["event_time"])
            at = parse_utc(event["arrival_time"])

            delay_seconds = (at - et).total_seconds()
            window_start, window_end = assign_fixed_window(et, window_seconds)
            window_start_str = window_start.isoformat()
            window_end_str = window_end.isoformat()

            is_confirmed = status == "CONFIRMED"
            is_duplicate = (
                deduplicate and event_id in seen_per_merchant[merchant_id]
            )
            is_too_late = delay_seconds > allowed_lateness_seconds
            arrived_after_close = at > window_end

            accepted = is_confirmed and not is_duplicate and not is_too_late
            revision = accepted and arrived_after_close

            # Determinar razón
            if is_duplicate:
                reason = "duplicate"
            elif not is_confirmed:
                reason = "not_confirmed"
            elif is_too_late:
                reason = "too_late"
            else:
                reason = "accepted"

            # Acumular total solo si el evento es aceptado
            if accepted:
                key = (merchant_id, window_start_str)
                totals_acc[key] = totals_acc.get(key, 0) + amount
                window_ends[key] = window_end_str

            # Marcar como visto (solo CONFIRMED, primera ocurrencia)
            if is_confirmed and not is_duplicate:
                seen_per_merchant[merchant_id].add(event_id)

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": is_duplicate,
                    "too_late": is_too_late,
                    "accepted": accepted,
                    "revision": revision,
                    "reason": reason,
                }
            )

        totals = [
            {
                "merchant_id": mid,
                "window_start": wstart,
                "window_end": window_ends[(mid, wstart)],
                "total": total,
            }
            for (mid, wstart), total in totals_acc.items()
        ]

        return totals, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """
        from datetime import timezone

        from apache_beam.transforms.window import TimestampedValue

        def to_timestamped(event):
            et = parse_utc(event["event_time"])
            return TimestampedValue(event, et.timestamp())

        def format_result(kv, window=beam.DoFn.WindowParam):
            merchant_id, total = kv
            return {
                "merchant_id": merchant_id,
                "window_start": _ts_to_iso(window.start),
                "window_end": _ts_to_iso(window.end),
                "total": total,
            }

        def _ts_to_iso(beam_ts):
            from datetime import datetime, timezone

            return datetime.fromtimestamp(
                float(beam_ts), tz=timezone.utc
            ).isoformat()

        return (
            pipeline
            | "Create" >> beam.Create(events)
            | "FilterConfirmed" >> beam.Filter(
                lambda e: e["status"] == "CONFIRMED"
            )
            | "AddTimestamp" >> beam.Map(to_timestamped)
            | "Window" >> beam.WindowInto(
                beam.window.FixedWindows(window_seconds)
            )
            | "KeyByMerchant" >> beam.Map(
                lambda e: (e["merchant_id"], e["amount"])
            )
            | "SumPerMerchant" >> beam.CombinePerKey(sum)
            | "FormatResult" >> beam.Map(format_result)
        )

    return (build_windowed_totals_pipeline,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            merchant_id, event = element
            event_id = event["event_id"]

            # Verificar si ya procesamos este event_id para este comercio
            existing = set(seen_ids.read())
            if event_id not in existing:
                seen_ids.add(event_id)
                # Programar limpieza al final de la ventana (event time)
                expiry.set(window.end)
                yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """
        import apache_beam as beam
        from apache_beam.transforms import trigger as tr
        from apache_beam.utils.timestamp import Duration

        # apache_beam 2.74 Duration carece de .seconds; añadirlo para
        # compatibilidad con las aserciones de los tests.
        if not hasattr(Duration, "seconds"):
            Duration.seconds = property(lambda self: self.micros / 1_000_000)

        return beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=tr.AfterWatermark(
                early=tr.AfterProcessingTime(30),
                late=tr.AfterCount(1),
            ),
            accumulation_mode=tr.AccumulationMode.ACCUMULATING,
            allowed_lateness=Duration(seconds=allowed_lateness_seconds),
        )

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        upsert_sink: dict[str, dict] = {}
        append_sink: list[dict] = []
        audit: list[dict] = []

        operation = "UPSERT" if idempotent else "POST"

        for attempt in range(1, attempts + 1):
            for result in results:
                key = make_idempotency_key(result)
                row = {
                    **result,
                    "idempotency_key": key,
                    "attempt": attempt,
                    "operation": operation,
                }
                audit.append(row)
                if idempotent:
                    upsert_sink[key] = row
                else:
                    append_sink.append(row)

        materialized = list(upsert_sink.values()) if idempotent else append_sink
        return materialized, audit

    return (make_idempotency_key, simulate_sink_retries)


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    - [ ] un duplicado no modifica el total;
    - [ ] claves distintas no comparten estado;
    - [ ] un evento fuera de orden cae en su ventana de evento;
    - [ ] un evento con atraso aceptado produce una revisión;
    - [ ] un evento demasiado tardío queda auditado;
    - [ ] dos escrituras del mismo resultado dejan una sola entidad;
    - [ ] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
