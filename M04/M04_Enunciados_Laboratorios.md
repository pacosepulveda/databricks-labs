# M04 — Laboratorios Learn by Doing
## Delta Lake: ACID, DML, MERGE, schema, history, time travel y optimización

**Curso:** Databricks — Big Data, Spark, Delta Lake y arquitectura Medallion  
**Documento:** Enunciados para alumnos  
**Metodología:** Learn by Doing  
**Modalidad:** Individual  
**Entorno:** Azure Databricks compartido  
**Nivel:** Introductorio–intermedio

---

# 1. Objetivo

En este módulo vas a trabajar directamente con **tablas Delta Lake**.

La secuencia será:

```text
Tabla Delta
   ↓
Transaction history
   ↓
INSERT
   ↓
UPDATE
   ↓
DELETE
   ↓
MERGE
   ↓
Schema enforcement
   ↓
Schema evolution
   ↓
Time travel
   ↓
RESTORE
   ↓
VACUUM / optimization
```

El objetivo no es memorizar sintaxis, sino comprobar qué propiedades aporta Delta Lake sobre almacenamiento cloud.

---

# 2. Entorno

Todos los alumnos comparten:

```text
1 Azure Databricks Workspace
1 compute compartido
1 catálogo training
```

Cada alumno trabaja únicamente en:

```text
training.<student_id>
```

Los datos de partida se encuentran en:

```text
training.shared.m04_orders_source
```

Sustituye siempre:

```text
<student_id>
```

por tu identificador real.

---

# 3. Regla principal

Todas las operaciones destructivas (`UPDATE`, `DELETE`, `MERGE`, `RESTORE`) se realizan únicamente sobre tu propia tabla:

```text
training.<student_id>.orders_delta
```

No modifiques:

```text
training.shared
```

ni objetos de otros alumnos.

---

# 4. Ejercicio 0 — Crear tu tabla Delta

## Objetivo

Crear una tabla Delta gestionada a partir de una fuente común.

Ejecuta:

```sql
CREATE OR REPLACE TABLE training.<student_id>.orders_delta
USING DELTA
AS
SELECT *
FROM training.shared.m04_orders_source;
```

Consulta:

```sql
SELECT *
FROM training.<student_id>.orders_delta
ORDER BY order_id;
```

Cuenta:

```sql
SELECT COUNT(*)
FROM training.<student_id>.orders_delta;
```

---

# 5. Ejercicio 1 — Comprobar que es una tabla Delta

Ejecuta:

```sql
DESCRIBE DETAIL training.<student_id>.orders_delta;
```

Localiza:

- `format`;
- `location`;
- `numFiles`;
- `sizeInBytes`;
- propiedades de la tabla.

### Preguntas

1. ¿Qué formato aparece?
2. ¿Es una tabla managed o estás especificando manualmente una ruta?
3. ¿Qué diferencia existe entre “tabla” y “ficheros físicos”?

> No navegues ni modifiques manualmente la ubicación física de una managed table.

---

# 6. Ejercicio 2 — Primer vistazo al historial

Ejecuta:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;
```

Deberías observar al menos una versión inicial.

Anota:

```text
version
timestamp
operation
```

### Pregunta

¿Qué representa una versión de una tabla Delta?

---

# 7. Ejercicio 3 — INSERT crea una nueva versión

Añade un pedido:

```sql
INSERT INTO training.<student_id>.orders_delta
VALUES (
    101,
    'C050',
    'ES',
    'NEW',
    2,
    CAST(75.50 AS DECIMAL(10,2)),
    DATE '2026-09-24'
);
```

Comprueba:

```sql
SELECT *
FROM training.<student_id>.orders_delta
WHERE order_id = 101;
```

Vuelve a ejecutar:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;
```

### Preguntas

1. ¿Ha aparecido una nueva versión?
2. ¿Qué operación aparece en el historial?
3. ¿Ha desaparecido la versión anterior?

---

# 8. Ejercicio 4 — UPDATE

Actualiza:

```sql
UPDATE training.<student_id>.orders_delta
SET status = 'SHIPPED'
WHERE order_id = 101;
```

Comprueba:

```sql
SELECT *
FROM training.<student_id>.orders_delta
WHERE order_id = 101;
```

Revisa:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;
```

### Reflexión

En un conjunto de CSV o Parquet “sueltos”, ¿cómo implementarías un `UPDATE` fiable si no existiera una capa de tabla transaccional?

---

# 9. Ejercicio 5 — DELETE

Elimina:

```sql
DELETE FROM training.<student_id>.orders_delta
WHERE order_id = 101;
```

Comprueba:

```sql
SELECT *
FROM training.<student_id>.orders_delta
WHERE order_id = 101;
```

Consulta de nuevo:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;
```

### Pregunta

¿Que una fila ya no aparezca en la versión actual significa necesariamente que sus bytes se hayan eliminado inmediatamente del almacenamiento físico?

---

# 10. Ejercicio 6 — Crear una tabla de cambios

## Objetivo

Preparar un pequeño lote incremental para practicar `MERGE`.

Ejecuta:

```sql
CREATE OR REPLACE TABLE training.<student_id>.orders_updates (
    order_id BIGINT,
    customer_id STRING,
    country STRING,
    status STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    order_date DATE
)
USING DELTA;
```

Inserta:

```sql
INSERT INTO training.<student_id>.orders_updates
VALUES
    (1, 'C001', 'ES', 'SHIPPED', 3, 49.90, DATE '2026-09-01'),
    (2, 'C002', 'PT', 'CANCELLED', 1, 89.00, DATE '2026-09-02'),
    (200, 'C200', 'FR', 'NEW', 4, 19.95, DATE '2026-09-24');
```

Comprueba:

```sql
SELECT *
FROM training.<student_id>.orders_updates
ORDER BY order_id;
```

---

# 11. Ejercicio 7 — MERGE: UPDATE + INSERT

Ejecuta:

```sql
MERGE INTO training.<student_id>.orders_delta AS target
USING training.<student_id>.orders_updates AS source
ON target.order_id = source.order_id

WHEN MATCHED THEN
  UPDATE SET
    target.customer_id = source.customer_id,
    target.country = source.country,
    target.status = source.status,
    target.quantity = source.quantity,
    target.unit_price = source.unit_price,
    target.order_date = source.order_date

WHEN NOT MATCHED THEN
  INSERT (
    order_id,
    customer_id,
    country,
    status,
    quantity,
    unit_price,
    order_date
  )
  VALUES (
    source.order_id,
    source.customer_id,
    source.country,
    source.status,
    source.quantity,
    source.unit_price,
    source.order_date
  );
```

Comprueba:

```sql
SELECT *
FROM training.<student_id>.orders_delta
WHERE order_id IN (1, 2, 200)
ORDER BY order_id;
```

Revisa:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;
```

### Preguntas

1. ¿Qué filas se actualizaron?
2. ¿Qué fila se insertó?
3. ¿Por qué `MERGE` es útil para cargas incrementales?

---

# 12. Ejercicio 8 — Schema enforcement

## Objetivo

Provocar deliberadamente una escritura incompatible.

Ejecuta:

```sql
INSERT INTO training.<student_id>.orders_delta
(
    order_id,
    customer_id,
    country,
    status,
    quantity,
    unit_price,
    order_date,
    columna_inexistente
)
VALUES
(
    300,
    'C300',
    'ES',
    'NEW',
    1,
    10.00,
    DATE '2026-09-24',
    'x'
);
```

### Resultado esperado

La operación debe fallar.

### Preguntas

1. ¿Qué columna provoca el error?
2. ¿La tabla ha cambiado?
3. ¿Por qué el fallo es preferible a añadir columnas silenciosamente?

---

# 13. Ejercicio 9 — Schema evolution explícita

Ahora decidimos que la tabla necesita una nueva columna:

```text
source_system
```

Ejecuta:

```sql
ALTER TABLE training.<student_id>.orders_delta
ADD COLUMN source_system STRING;
```

Comprueba:

```sql
DESCRIBE TABLE training.<student_id>.orders_delta;
```

Actualiza registros antiguos:

```sql
UPDATE training.<student_id>.orders_delta
SET source_system = 'legacy'
WHERE source_system IS NULL;
```

Inserta uno nuevo:

```sql
INSERT INTO training.<student_id>.orders_delta
VALUES (
    301,
    'C301',
    'ES',
    'NEW',
    1,
    25.00,
    DATE '2026-09-24',
    'mobile-app'
);
```

### Reflexión

Explica la diferencia:

```text
schema enforcement
vs.
schema evolution
```

---

# 14. Ejercicio 10 — Evolución automática con `mergeSchema`

## Objetivo

Comprobar una segunda forma de añadir una columna.

Desde Python:

```python
from pyspark.sql import functions as F

extra = (
    spark.createDataFrame(
        [
            (400, "C400", "DE", "NEW", 2, 15.50, "2026-09-24", "partner", "campaign-A")
        ],
        [
            "order_id",
            "customer_id",
            "country",
            "status",
            "quantity",
            "unit_price",
            "order_date",
            "source_system",
            "campaign"
        ]
    )
    .withColumn(
        "order_date",
        F.to_date("order_date")
    )
    .withColumn(
        "unit_price",
        F.col("unit_price").cast("decimal(10,2)")
    )
)
```

Primero intenta escribir **sin** evolución:

```python
extra.write.mode("append").saveAsTable(
    "training.<student_id>.orders_delta"
)
```

Debe fallar por la columna `campaign`.

Ahora:

```python
(
    extra.write
    .option("mergeSchema", "true")
    .mode("append")
    .saveAsTable(
        "training.<student_id>.orders_delta"
    )
)
```

Comprueba:

```sql
DESCRIBE TABLE training.<student_id>.orders_delta;
```

### Pregunta

¿Por qué no debería activarse evolución automática indiscriminadamente en todos los pipelines?

---

# 15. Ejercicio 11 — History como herramienta de auditoría

Ejecuta:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;
```

Identifica operaciones correspondientes a:

- CREATE / WRITE;
- INSERT;
- UPDATE;
- DELETE;
- MERGE;
- ALTER TABLE;
- WRITE con schema evolution.

### Pregunta

¿Qué información del historial sería útil durante una investigación de un error de datos?

---

# 16. Ejercicio 12 — Time travel por versión

Elige una versión anterior de:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;
```

Por ejemplo:

```text
version = 2
```

Consulta:

```sql
SELECT *
FROM training.<student_id>.orders_delta
VERSION AS OF 2
ORDER BY order_id;
```

Compara con:

```sql
SELECT *
FROM training.<student_id>.orders_delta
ORDER BY order_id;
```

> Utiliza una versión que exista realmente en tu historial.

### Preguntas

1. ¿Has modificado la tabla actual al consultar una versión antigua?
2. ¿Qué casos de uso tiene esto?
3. ¿Es time travel equivalente a un backup de largo plazo?

---

# 17. Ejercicio 13 — Provocar un error y recuperarlo

## Objetivo

Utilizar `RESTORE` como mecanismo de recuperación.

Primero anota la versión actual:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta LIMIT 1;
```

Después realiza deliberadamente:

```sql
UPDATE training.<student_id>.orders_delta
SET unit_price = unit_price * 10;
```

Comprueba:

```sql
SELECT
    MIN(unit_price),
    MAX(unit_price),
    AVG(unit_price)
FROM training.<student_id>.orders_delta;
```

Consulta history:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;
```

Identifica la versión **anterior al UPDATE erróneo**.

Restaura:

```sql
RESTORE TABLE training.<student_id>.orders_delta
TO VERSION AS OF <version_correcta>;
```

Comprueba de nuevo los precios.

### Preguntas

1. ¿RESTORE borra las versiones posteriores?
2. ¿Aparece una nueva operación RESTORE en history?
3. ¿Por qué esto es diferente de “deshacer” borrando manualmente ficheros?

---

# 18. Ejercicio 14 — Catalog Explorer: History

Abre:

```text
Catalog
→ training
→ <student_id>
→ orders_delta
```

Busca la pestaña o sección de historial.

Compara lo que ves con:

```sql
DESCRIBE HISTORY
```

### Reflexión

¿GUI y SQL están consultando historias distintas?

---

# 19. Ejercicio 15 — VACUUM sin riesgo: DRY RUN

## Objetivo

Entender qué hace `VACUUM` sin eliminar nada.

Ejecuta:

```sql
VACUUM training.<student_id>.orders_delta DRY RUN;
```

### Preguntas

1. ¿Qué intenta localizar VACUUM?
2. ¿Por qué eliminar archivos antiguos puede limitar time travel?
3. ¿Por qué no reducimos agresivamente la retención en este laboratorio?

> No desactives los mecanismos de seguridad de retención.

---

# 20. Ejercicio 16 — Deletion vectors

## Objetivo

Conocer una optimización moderna de las operaciones de fila.

Consulta:

```sql
SHOW TBLPROPERTIES training.<student_id>.orders_delta;
```

Busca:

```text
delta.enableDeletionVectors
```

Si no está habilitado y el instructor ha validado que el runtime es compatible:

```sql
ALTER TABLE training.<student_id>.orders_delta
SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = true
);
```

Ejecuta un pequeño `DELETE`:

```sql
DELETE FROM training.<student_id>.orders_delta
WHERE order_id = 200;
```

Revisa:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;
```

### Pregunta

¿Qué ventaja puede tener marcar filas modificadas/eliminadas mediante metadata en vez de reescribir inmediatamente un fichero Parquet completo?

---

# 21. Ejercicio 17 — OPTIMIZE

Ejecuta:

```sql
OPTIMIZE training.<student_id>.orders_delta;
```

Consulta:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;
```

Busca:

```text
OPTIMIZE
```

### Reflexión

¿OPTIMIZE cambia lógicamente las filas de la tabla o reorganiza su representación física?

---

# 22. Ejercicio 18 — Liquid clustering

Este ejercicio se realiza si el runtime utilizado lo soporta.

Configura una clave de clustering:

```sql
ALTER TABLE training.<student_id>.orders_delta
CLUSTER BY (country);
```

Comprueba:

```sql
DESCRIBE DETAIL training.<student_id>.orders_delta;
```

Ejecuta:

```sql
OPTIMIZE training.<student_id>.orders_delta;
```

### Preguntas

1. ¿Por qué Databricks recomienda actualmente liquid clustering para tablas nuevas?
2. ¿Qué diferencia conceptual existe entre clustering y cambiar los datos?
3. ¿Por qué una tabla pequeña del laboratorio no sirve para hacer un benchmark serio?

---

# 23. Práctica autónoma A — Sincronización incremental con MERGE

## Contexto

Existe:

```text
training.shared.m04_customer_master
training.shared.m04_customer_changes
```

Crea tu copia:

```sql
CREATE OR REPLACE TABLE training.<student_id>.customers_delta
USING DELTA
AS
SELECT *
FROM training.shared.m04_customer_master;
```

## Tareas

1. Inspecciona la tabla.
2. Consulta su history.
3. Aplica `m04_customer_changes` mediante `MERGE`.
4. Los clientes existentes deben actualizarse.
5. Los clientes nuevos deben insertarse.
6. Comprueba el resultado.
7. Revisa `DESCRIBE HISTORY`.
8. Explica por qué esta operación es idempotente o qué condiciones necesitaría para serlo.

---

# 24. Práctica autónoma B — Recuperación ante un cambio erróneo

Sobre `customers_delta`:

1. anota la versión actual;
2. realiza un `UPDATE` incorrecto que afecte a varias filas;
3. detecta el problema mediante una consulta;
4. usa history para identificar la versión buena;
5. consulta esa versión con time travel;
6. restaura la tabla;
7. demuestra que los datos se han recuperado;
8. muestra la nueva entrada `RESTORE` en history.

### Entregable

Explica:

```text
qué ocurrió
cómo detectaste la versión correcta
por qué RESTORE no elimina la historia
```

---

# 25. Práctica autónoma C — Cambio de schema controlado

Sobre `customers_delta`:

1. intenta insertar una columna inexistente y comprueba el error;
2. añade una columna `segment STRING`;
3. actualiza varios clientes con segmentos;
4. inserta un nuevo cliente utilizando la nueva columna;
5. demuestra que el schema ha evolucionado;
6. consulta history.

### Pregunta

¿Qué ventaja aporta que el cambio de schema quede asociado a la historia de la tabla?

---

# 26. Práctica autónoma D — Diagnóstico de mantenimiento

Sobre una de tus tablas:

1. ejecuta `DESCRIBE DETAIL`;
2. ejecuta `DESCRIBE HISTORY`;
3. ejecuta `VACUUM ... DRY RUN`;
4. comprueba las propiedades;
5. ejecuta `OPTIMIZE`;
6. localiza la operación en history;
7. explica qué tareas puede automatizar predictive optimization en managed tables.

No cambies configuraciones de predictive optimization.

---

# 27. Reto final M04

Explica este flujo:

```text
Physical data files
        +
Delta transaction log
        ↓
Versioned table
        ↓
ACID transactions
        ↓
INSERT / UPDATE / DELETE / MERGE
        ↓
History / Time Travel / Restore
        ↓
Maintenance and optimization
```

Distingue además:

```text
Schema enforcement
vs.
Schema evolution
```

y:

```text
logical DELETE
vs.
physical removal by VACUUM
```

---

# 28. Qué NO hacemos todavía

No utilizamos aún:

```text
Bronze
Silver
Gold
Auto Loader
Lakeflow expectations
Medallion pipelines
```

Eso pertenece al M05.

---

# 29. Referencias oficiales

- [Azure Databricks tables](https://learn.microsoft.com/en-us/azure/databricks/tables/)
- [Delta Lake best practices](https://learn.microsoft.com/en-us/azure/databricks/delta/best-practices)
- [Schema enforcement](https://learn.microsoft.com/en-us/azure/databricks/tables/schema-enforcement)
- [Schema evolution](https://learn.microsoft.com/en-us/azure/databricks/delta/update-schema)
- [Table history and time travel](https://learn.microsoft.com/en-us/azure/databricks/delta/history)
- [MERGE](https://learn.microsoft.com/en-us/azure/databricks/delta/merge)
- [Deletion vectors](https://learn.microsoft.com/en-us/azure/databricks/delta/deletion-vectors)
- [VACUUM](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-vacuum)
- [Liquid clustering](https://learn.microsoft.com/en-us/azure/databricks/tables/clustering/)
- [Predictive optimization](https://learn.microsoft.com/en-us/azure/databricks/optimizations/predictive-optimization)