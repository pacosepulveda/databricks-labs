# M05 — Soluciones de laboratorios
## Medallion Architecture: Bronze, Silver, Gold, calidad, incrementalidad y Lakeflow

Estas soluciones muestran una implementación de referencia de la arquitectura Medallion utilizada en el módulo. Sustituye siempre `<student_id>` por tu identificador real.

Los conteos y métricas exactos dependen de los ficheros disponibles en el entorno. La validación debe hacerse con las consultas incluidas en cada sección.

---

# Ejercicio 0 — Explorar las tres capas

Separar Bronze, Silver y Gold mediante schemas aporta un límite lógico y de gobierno más claro que utilizar únicamente prefijos en los nombres.

```text
training.<student_id>_bronze
training.<student_id>_silver
training.<student_id>_gold
```

permite diferenciar:

- responsabilidades;
- permisos;
- ciclo de vida;
- descubrimiento de objetos;
- lineage;
- políticas de acceso.

`bronze_table`, `silver_table` y `gold_table` serían solo convenciones de nombres dentro de un mismo namespace.

---

# Ejercicio 1 — Preparar tu landing zone

```text
Shared source
→ copia común de los datos disponibles para el laboratorio.

Landing zone
→ zona individual donde llegan los ficheros que serán procesados por tu pipeline.
```

La landing forma parte de tu flujo de ingestión. La fuente compartida no debe utilizarse como zona de trabajo mutable.

Comprobación:

```text
%fs ls /Volumes/training/<student_id>_bronze/landing/transactions/
%fs ls /Volumes/training/<student_id>_bronze/landing/reference/
```

---

# Ejercicio 2 — Bronze: conservar el dato raw

La lectura correcta mantiene las columnas del CSV como texto y añade metadata operacional:

```python
from pyspark.sql import functions as F

bronze_transactions = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("/Volumes/training/<student_id>_bronze/landing/transactions/")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

bronze_transactions.write     .mode("overwrite")     .saveAsTable("training.<student_id>_bronze.transactions_raw")
```

Bronze debe permitir reconstruir qué se recibió y desde qué fichero.

---

# Ejercicio 3 — ¿Por qué Bronze conserva tipos flexibles?

1. Con `inferSchema=false`, las columnas procedentes del CSV se leen como `STRING`.
2. Se han añadido `_ingest_timestamp` y `_source_file`.
3. Conservar el valor recibido permite investigar errores de formato y volver a aplicar reglas distintas sin perder el dato original.
4. Bronze no significa "sin gobierno". Debe mantener trazabilidad, control de acceso, retención y una estructura de ingestión coherente.

```text
Raw
≠
descontrolado
```

---

# Ejercicio 4 — Crear reference Bronze

La solución consiste en aplicar el mismo patrón a clientes y productos:

```python
customers_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("/Volumes/training/<student_id>_bronze/landing/reference/customers.csv")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

products_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("/Volumes/training/<student_id>_bronze/landing/reference/products.csv")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

customers_raw.write     .mode("overwrite")     .saveAsTable("training.<student_id>_bronze.customers_raw")

products_raw.write     .mode("overwrite")     .saveAsTable("training.<student_id>_bronze.products_raw")
```

---

# Ejercicio 5 — Detectar problemas de calidad antes de Silver

No conviene corregir o borrar directamente en Bronze porque se perdería evidencia de lo recibido.

Bronze debe permitir responder:

```text
¿Qué llegó?
¿Desde qué fichero?
¿Cuándo se ingirió?
¿Qué valor original produjo el error?
```

Los problemas se identifican en Bronze, pero la normalización y las reglas de calidad pertenecen a Silver.

---

# Ejercicio 6 — Silver: tipar y normalizar

Responsabilidades que pasan a Silver:

- conversión de tipos;
- normalización de texto;
- estandarización de códigos;
- validación;
- deduplicación;
- integración entre entidades.

Bronze conserva el evento recibido. Silver expresa una versión utilizable y consistente del dato.

---

# Ejercicio 7 — Separar registros válidos e inválidos

1. Una quarantine conserva evidencia de los registros rechazados y permite analizarlos, corregirlos o reprocesarlos.
2. La política debe venir de reglas de calidad y gobierno definidas para el dato: no debería decidirse mediante una eliminación silenciosa accidental.

Comprobación:

```sql
SELECT COUNT(*) AS invalid_rows
FROM training.<student_id>_silver.transactions_quarantine;

SELECT COUNT(*) AS valid_rows
FROM training.<student_id>_silver.transactions_valid;
```

La suma no tiene por qué coincidir con el total de Bronze si una misma fila puede satisfacer reglas duplicadas en una implementación distinta, pero con las vistas del ejercicio cada fila queda clasificada por las condiciones indicadas.

---

# Ejercicio 8 — Deduplicación

El objetivo es que la clave de negocio quede única:

```sql
SELECT
 COUNT(*) AS rows,
 COUNT(DISTINCT transaction_id) AS distinct_ids
FROM training.<student_id>_silver.transactions;
```

El resultado esperado es:

```text
rows = distinct_ids
```

La ventana conserva una fila por `transaction_id`.

En un sistema real, el `ORDER BY` debería usar un criterio determinista de precedencia —por ejemplo, timestamp de evento, versión de origen o timestamp de ingestión fiable— para decidir qué duplicado conservar.

---

# Ejercicio 9 — Normalizar clientes y productos

La solución aplica normalizaciones consistentes:

```text
customer_name → TRIM
email         → LOWER + TRIM
country       → UPPER + TRIM
product_name  → TRIM
category      → UPPER + TRIM
```

La capa Silver no se limita a quitar errores; también crea representaciones coherentes para joins y consumo posterior.

---

# Ejercicio 10 — Conformar datos en Silver

`sales_enriched` encaja en Silver porque sigue siendo una entidad de detalle reutilizable.

Contiene:

- transacción;
- cliente;
- producto;
- atributos normalizados;
- medida derivada `amount`.

Todavía no representa una métrica agregada para un consumidor concreto.

```text
Silver → detalle limpio, integrado y reutilizable
Gold   → producto orientado a una pregunta de negocio
```

---

# Ejercicio 11 — Gold: construir métricas de negocio

Los tres productos Gold responden a preguntas concretas:

```text
daily_sales
→ ¿cuánto se vende cada día?

sales_by_country
→ ¿cuánto se vende por país?

product_performance
→ ¿qué productos generan unidades e ingresos?
```

Gold puede cambiar de granularidad respecto a Silver porque está orientado al consumo.

---

# Ejercicio 12 — Consumir Gold desde Databricks SQL

El dashboard debe consultar Gold porque así reutiliza reglas ya resueltas:

```text
Bronze
→ ingestión/trazabilidad

Silver
→ tipos/calidad/deduplicación/integración

Gold
→ métricas listas para consumo

Dashboard
→ presentación
```

Si cada dashboard repitiera limpieza y joins, existirían múltiples implementaciones de la misma lógica y sería más difícil mantener resultados consistentes.

---

# Ejercicio 13 — Llegan nuevos datos

Reejecutar todo ingenuamente con `overwrite` puede:

- reprocesar datos históricos innecesariamente;
- aumentar coste y tiempo;
- sustituir una tabla por un subconjunto si la entrada no contiene todo el histórico;
- perder la semántica incremental;
- dificultar distinguir qué llegó en cada lote.

Una reconstrucción completa puede ser válida cuando se hace deliberadamente, pero no es equivalente a procesar solo los cambios.

---

# Ejercicio 14 — MERGE incremental en Silver

Primero se carga **solo el nuevo lote** y se aplican las mismas reglas de Silver.

```python
from pyspark.sql import functions as F

day2_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("/Volumes/training/<student_id>_bronze/landing/transactions/transactions_day2.csv")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

day2_typed = (
    day2_raw
    .select(
        F.col("transaction_id").cast("long").alias("transaction_id"),
        "customer_id",
        "product_id",
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(12,2)").alias("unit_price"),
        F.upper(F.trim(F.col("country"))).alias("country"),
        F.to_date("transaction_date").alias("transaction_date"),
        "_ingest_timestamp",
        "_source_file"
    )
)

day2_clean = (
    day2_typed
    .filter(
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("product_id").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") >= 0)
        & F.col("transaction_date").isNotNull()
    )
    .dropDuplicates(["transaction_id"])
)

day2_clean.createOrReplaceTempView("day2_clean")
```

Si varios cambios distintos comparten `transaction_id`, en producción debe elegirse de forma determinista el más reciente mediante una columna de secuencia o timestamp. `dropDuplicates` es suficiente únicamente cuando el duplicado del lote representa el mismo evento.

Aplicación incremental:

```sql
MERGE INTO training.<student_id>_silver.transactions AS target
USING day2_clean AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED THEN
 UPDATE SET *

WHEN NOT MATCHED THEN
 INSERT *;
```

Comprobaciones:

```sql
SELECT
 COUNT(*) AS rows,
 COUNT(DISTINCT transaction_id) AS distinct_ids
FROM training.<student_id>_silver.transactions;

DESCRIBE HISTORY training.<student_id>_silver.transactions;
```

Relación con M04: se utiliza la misma semántica Delta `MERGE`, pero ahora forma parte de una arquitectura Medallion y de un flujo incremental.

---

# Ejercicio 15 — Recalcular Gold

No es ideal recordar manualmente el orden porque existen dependencias:

```text
transactions
customers
products
   ↓
sales_enriched
   ↓
daily_sales
sales_by_country
product_performance
```

Una capa de orquestación o un pipeline declarativo puede registrar esas dependencias y ejecutar cada paso en el orden correcto.

---

# Ejercicio 16 — Crear un Lakeflow Pipeline

El pipeline representa el flujo como un objeto de plataforma.

Nombre:

```text
M05 Medallion - <student_id>
```

La idea no es crear tres scripts desconectados, sino declarar datasets cuyas dependencias puedan ser detectadas por el motor.

---

# Ejercicio 17 — Bronze con Auto Loader

1. `read_files` permite ingerir ficheros desde la landing con semántica adecuada para el pipeline y, al utilizarse en streaming, procesarlos incrementalmente.
2. Sigue siendo Bronze porque conserva los campos recibidos sin aplicar reglas de negocio ni limpieza fuerte.
3. Se conserva la ruta del fichero en `_source_file` y se añade `_ingest_timestamp`.

```text
Landing file
   ↓
read_files
   ↓
Streaming Bronze table
```

---

# Ejercicio 18 — Silver con expectations

1. Las filas que incumplen las expectations configuradas con `ON VIOLATION DROP ROW` no llegan al resultado de esa actualización de Silver.
2. Comportamientos:
   - sin acción explícita: se registra la violación y la fila puede mantenerse;
   - `DROP ROW`: se descarta la fila inválida de la salida;
   - `FAIL UPDATE`: falla la actualización del dataset/pipeline.
3. `DROP ROW` es peligroso si la organización necesita evidencia, análisis o reproceso de los registros rechazados y no existe otra capa que los conserve.

Bronze ayuda a mantener esa trazabilidad aunque Silver descarte filas inválidas.

---

# Ejercicio 19 — Gold como materialized view

Una materialized view es apropiada porque:

- expresa una consulta declarativa;
- materializa un resultado para consumo;
- puede mantener el resultado de forma gestionada;
- evita que cada consumidor recalcule toda la agregación;
- encaja con una métrica Gold derivada de Silver.

`daily_sales_mv` cambia la granularidad desde transacciones individuales a métricas por fecha.

---

# Ejercicio 20 — Observar el Pipeline Graph

1. Databricks detecta que Silver depende de Bronze y Gold depende de Silver.
2. El gráfico representa relaciones entre datasets y estado de ejecución, no solo una lista de ficheros de código.
3. Con `FAIL UPDATE`, la actualización afectada falla en vez de publicar silenciosamente un resultado que incumple la expectativa.

Flujo esperado:

```text
landing
   ↓
transactions_stream_raw
   ↓
transactions_stream_clean
   ↓
daily_sales_mv
```

---

# Ejercicio 21 — Lineage en Unity Catalog

```text
Diseño esperado
→ describe cómo creemos que debería fluir el dato.

Lineage registrado
→ muestra relaciones observadas/registradas por la plataforma entre objetos reales.
```

El lineage permite investigar impacto y procedencia utilizando metadata de la plataforma en lugar de depender únicamente de documentación manual.

---

# Ejercicio 22 — Incrementalidad con Auto Loader

Auto Loader mantiene estado sobre los ficheros ya descubiertos/procesados. En una ejecución normal, un fichero nuevo se incorpora como nuevo input sin tratar de nuevo todos los anteriores como si nunca se hubieran visto.

Después de añadir `transactions_day3.csv`:

```sql
SELECT COUNT(*)
FROM training.<student_id>_bronze.transactions_stream_raw;
```

debe aumentar de acuerdo con las filas nuevas.

Las métricas de:

```sql
SELECT *
FROM training.<student_id>_gold.daily_sales_mv
ORDER BY transaction_date;
```

deben reflejar el nuevo conjunto válido de Silver.

---

# Práctica autónoma A — Mini lakehouse completo

Una solución compacta puede construirse en cinco pasos.

## 1. Bronze

```python
from pyspark.sql import functions as F

base = "/Volumes/training/<student_id>_bronze/landing"

transactions_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(f"{base}/transactions/")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

customers_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(f"{base}/reference/customers.csv")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

products_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(f"{base}/reference/products.csv")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

transactions_raw.write.mode("overwrite").saveAsTable(
    "training.<student_id>_bronze.transactions_raw"
)
customers_raw.write.mode("overwrite").saveAsTable(
    "training.<student_id>_bronze.customers_raw"
)
products_raw.write.mode("overwrite").saveAsTable(
    "training.<student_id>_bronze.products_raw"
)
```

## 2. Silver: tipado y calidad

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.transactions_typed AS
SELECT
 CAST(transaction_id AS BIGINT) AS transaction_id,
 customer_id,
 product_id,
 CAST(quantity AS INT) AS quantity,
 CAST(unit_price AS DECIMAL(12,2)) AS unit_price,
 UPPER(TRIM(country)) AS country,
 TO_DATE(transaction_date) AS transaction_date,
 _ingest_timestamp,
 _source_file
FROM training.<student_id>_bronze.transactions_raw;
```

```sql
CREATE OR REPLACE VIEW training.<student_id>_silver.transactions_quarantine AS
SELECT *
FROM training.<student_id>_silver.transactions_typed
WHERE
 transaction_id IS NULL
 OR customer_id IS NULL
 OR product_id IS NULL
 OR quantity <= 0
 OR unit_price < 0
 OR transaction_date IS NULL;
```

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.transactions AS
SELECT * EXCEPT (rn)
FROM (
 SELECT
   *,
   ROW_NUMBER() OVER (
     PARTITION BY transaction_id
     ORDER BY _ingest_timestamp DESC
   ) AS rn
 FROM training.<student_id>_silver.transactions_typed
 WHERE
   transaction_id IS NOT NULL
   AND customer_id IS NOT NULL
   AND product_id IS NOT NULL
   AND quantity > 0
   AND unit_price >= 0
   AND transaction_date IS NOT NULL
)
WHERE rn = 1;
```

## 3. Silver: referencias

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.customers AS
SELECT
 customer_id,
 TRIM(customer_name) AS customer_name,
 LOWER(TRIM(email)) AS email,
 UPPER(TRIM(country)) AS country
FROM training.<student_id>_bronze.customers_raw
WHERE customer_id IS NOT NULL;
```

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.products AS
SELECT
 product_id,
 TRIM(product_name) AS product_name,
 UPPER(TRIM(category)) AS category
FROM training.<student_id>_bronze.products_raw
WHERE product_id IS NOT NULL;
```

## 4. Silver: entidad conformada

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.sales_enriched AS
SELECT
 t.transaction_id,
 t.transaction_date,
 t.customer_id,
 c.customer_name,
 t.product_id,
 p.product_name,
 p.category,
 t.country,
 t.quantity,
 t.unit_price,
 t.quantity * t.unit_price AS amount
FROM training.<student_id>_silver.transactions t
LEFT JOIN training.<student_id>_silver.customers c
 ON t.customer_id = c.customer_id
LEFT JOIN training.<student_id>_silver.products p
 ON t.product_id = p.product_id;
```

## 5. Gold

```sql
CREATE OR REPLACE TABLE training.<student_id>_gold.daily_sales AS
SELECT
 transaction_date,
 SUM(amount) AS revenue,
 COUNT(*) AS transactions,
 AVG(amount) AS avg_ticket
FROM training.<student_id>_silver.sales_enriched
GROUP BY transaction_date;
```

```sql
CREATE OR REPLACE TABLE training.<student_id>_gold.sales_by_country AS
SELECT
 country,
 SUM(amount) AS revenue,
 COUNT(*) AS transactions
FROM training.<student_id>_silver.sales_enriched
GROUP BY country;
```

```sql
CREATE OR REPLACE TABLE training.<student_id>_gold.product_performance AS
SELECT
 product_id,
 product_name,
 category,
 SUM(quantity) AS units,
 SUM(amount) AS revenue
FROM training.<student_id>_silver.sales_enriched
GROUP BY product_id, product_name, category;
```

Validaciones:

```sql
SELECT COUNT(*) FROM training.<student_id>_bronze.transactions_raw;

SELECT
 COUNT(*) AS rows,
 COUNT(DISTINCT transaction_id) AS distinct_ids
FROM training.<student_id>_silver.transactions;

SELECT * FROM training.<student_id>_gold.product_performance
ORDER BY revenue DESC
LIMIT 10;
```

---

# Práctica autónoma B — Calidad y quarantine

Puede utilizarse cualquier lote que ya contenga registros inválidos.

## 1. Evidencia en Bronze

```sql
SELECT *
FROM training.<student_id>_bronze.transactions_raw
ORDER BY transaction_id;
```

No se borra el origen.

## 2. Identificar inválidos

```sql
SELECT *
FROM training.<student_id>_silver.transactions_typed
WHERE
 transaction_id IS NULL
 OR customer_id IS NULL
 OR product_id IS NULL
 OR quantity <= 0
 OR unit_price < 0
 OR transaction_date IS NULL;
```

## 3. Quarantine

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.transactions_quarantine_table AS
SELECT *
FROM training.<student_id>_silver.transactions_typed
WHERE
 transaction_id IS NULL
 OR customer_id IS NULL
 OR product_id IS NULL
 OR quantity <= 0
 OR unit_price < 0
 OR transaction_date IS NULL;
```

## 4. Conteos

```sql
SELECT COUNT(*) AS invalid_rows
FROM training.<student_id>_silver.transactions_quarantine_table;

SELECT COUNT(*) AS valid_rows
FROM training.<student_id>_silver.transactions;
```

## 5. Explicación

El origen se conserva para mantener trazabilidad, permitir investigación y poder reprocesar con reglas futuras. La quarantine evita que los registros inválidos contaminen Silver sin hacerlos desaparecer de la arquitectura.

---

# Práctica autónoma C — Incrementalidad

Ejemplo utilizando un nuevo fichero disponible en la landing, como `transactions_day3.csv`.

## 1. Medir estado anterior

```sql
SELECT COUNT(*) AS silver_before
FROM training.<student_id>_silver.transactions;

SELECT SUM(revenue) AS gold_revenue_before
FROM training.<student_id>_gold.daily_sales;
```

## 2. Cargar y limpiar solo el nuevo fichero

```python
from pyspark.sql import functions as F

inc_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("/Volumes/training/<student_id>_bronze/landing/transactions/transactions_day3.csv")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

inc_clean = (
    inc_raw
    .select(
        F.col("transaction_id").cast("long").alias("transaction_id"),
        "customer_id",
        "product_id",
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(12,2)").alias("unit_price"),
        F.upper(F.trim("country")).alias("country"),
        F.to_date("transaction_date").alias("transaction_date"),
        "_ingest_timestamp",
        "_source_file"
    )
    .filter(
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("product_id").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") >= 0)
        & F.col("transaction_date").isNotNull()
    )
    .dropDuplicates(["transaction_id"])
)

inc_clean.createOrReplaceTempView("inc_clean")
```

## 3. MERGE

```sql
MERGE INTO training.<student_id>_silver.transactions AS target
USING inc_clean AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

## 4. Reconstruir entidad conformada y Gold

Reejecuta las definiciones de:

```text
sales_enriched
daily_sales
sales_by_country
product_performance
```

## 5. Comparar

```sql
SELECT COUNT(*) AS silver_after
FROM training.<student_id>_silver.transactions;

SELECT SUM(revenue) AS gold_revenue_after
FROM training.<student_id>_gold.daily_sales;
```

El objetivo no es que todos los números aumenten siempre: una corrección puede modificar una fila existente. Lo importante es que Silver no duplique la clave y que Gold refleje el estado final correcto.

---

# Práctica autónoma D — Producto Gold

Ejemplo: **revenue_by_category**.

## Definición

Ingresos y unidades vendidas agrupados por categoría de producto.

## SQL

```sql
CREATE OR REPLACE TABLE training.<student_id>_gold.revenue_by_category AS
SELECT
 category,
 SUM(amount) AS revenue,
 SUM(quantity) AS units,
 COUNT(*) AS transactions
FROM training.<student_id>_silver.sales_enriched
GROUP BY category;
```

Consulta:

```sql
SELECT *
FROM training.<student_id>_gold.revenue_by_category
ORDER BY revenue DESC;
```

Visualización recomendada:

```text
category → eje/categoría
revenue  → medida
```

Pertenece a Gold porque responde directamente a una pregunta de negocio y presenta una agregación lista para consumo, no una entidad operacional de detalle.

---

# Práctica autónoma E — Explicar la arquitectura

| Capa | Objetivo | Tipo de datos | Transformaciones permitidas | Usuario principal | Qué no debería hacerse |
|---|---|---|---|---|---|
| Sources | Sistemas de origen | Datos originales | Fuera del lakehouse | Productores | Alterarlos desde el pipeline |
| Landing | Recepción | Ficheros recibidos | Copia/ingestión mínima | Ingeniería de datos | Aplicar lógica de negocio |
| Bronze | Trazabilidad raw | Datos casi originales + metadata | Mínimas | Ingeniería / auditoría | Borrar silenciosamente errores |
| Silver | Calidad e integración | Datos tipados, válidos y conformados | Limpiar, validar, deduplicar, integrar | Ingeniería / analítica | Acoplarse a una visualización concreta |
| Gold | Consumo | Métricas/agregaciones | Lógica de producto de datos | BI / negocio / aplicaciones | Repetir toda la limpieza raw |
| Consumer | Decisión/uso | Vistas, dashboards, apps | Presentación/consumo | Usuarios finales | Reimplementar el pipeline completo |

Flujo:

```text
Sources
   ↓
Landing
   ↓
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Consumer
```

---

# Reto final del curso

Supongamos que el nuevo fichero está disponible en:

```text
/Volumes/training/shared/m05_source/transactions_final.csv
```

## 1. Introducirlo en landing

```python
dbutils.fs.cp(
    "/Volumes/training/shared/m05_source/transactions_final.csv",
    "/Volumes/training/<student_id>_bronze/landing/transactions/transactions_final.csv"
)
```

## 2. Conservarlo en Bronze

Carga únicamente el fichero nuevo y añádelo a Bronze:

```python
from pyspark.sql import functions as F

final_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("/Volumes/training/<student_id>_bronze/landing/transactions/transactions_final.csv")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

final_raw.write     .mode("append")     .saveAsTable("training.<student_id>_bronze.transactions_raw")
```

Comprobación de trazabilidad:

```sql
SELECT *
FROM training.<student_id>_bronze.transactions_raw
WHERE _source_file LIKE '%transactions_final.csv%';
```

## 3. Validar y deduplicar el lote

```python
final_typed = (
    final_raw
    .select(
        F.col("transaction_id").cast("long").alias("transaction_id"),
        "customer_id",
        "product_id",
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(12,2)").alias("unit_price"),
        F.upper(F.trim("country")).alias("country"),
        F.to_date("transaction_date").alias("transaction_date"),
        "_ingest_timestamp",
        "_source_file"
    )
)

final_invalid = final_typed.filter(
    F.col("transaction_id").isNull()
    | F.col("customer_id").isNull()
    | F.col("product_id").isNull()
    | (F.col("quantity") <= 0)
    | (F.col("unit_price") < 0)
    | F.col("transaction_date").isNull()
)

display(final_invalid)

final_clean = (
    final_typed
    .filter(
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("product_id").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") >= 0)
        & F.col("transaction_date").isNotNull()
    )
    .dropDuplicates(["transaction_id"])
)

final_clean.createOrReplaceTempView("final_clean")
```

En producción, si existen dos versiones distintas de la misma transacción dentro del lote, la deduplicación debe utilizar una columna de secuencia o timestamp que permita elegir la versión correcta de forma determinista.

## 4. Aplicar cambios a Silver

```sql
MERGE INTO training.<student_id>_silver.transactions AS target
USING final_clean AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED THEN
 UPDATE SET *

WHEN NOT MATCHED THEN
 INSERT *;
```

Validación de unicidad:

```sql
SELECT
 COUNT(*) AS rows,
 COUNT(DISTINCT transaction_id) AS distinct_ids
FROM training.<student_id>_silver.transactions;
```

Debe cumplirse:

```text
rows = distinct_ids
```

## 5. Actualizar Silver conformada

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.sales_enriched AS
SELECT
 t.transaction_id,
 t.transaction_date,
 t.customer_id,
 c.customer_name,
 t.product_id,
 p.product_name,
 p.category,
 t.country,
 t.quantity,
 t.unit_price,
 t.quantity * t.unit_price AS amount
FROM training.<student_id>_silver.transactions t
LEFT JOIN training.<student_id>_silver.customers c
 ON t.customer_id = c.customer_id
LEFT JOIN training.<student_id>_silver.products p
 ON t.product_id = p.product_id;
```

## 6. Actualizar Gold

```sql
CREATE OR REPLACE TABLE training.<student_id>_gold.daily_sales AS
SELECT
 transaction_date,
 SUM(amount) AS revenue,
 COUNT(*) AS transactions,
 AVG(amount) AS avg_ticket
FROM training.<student_id>_silver.sales_enriched
GROUP BY transaction_date;
```

```sql
CREATE OR REPLACE TABLE training.<student_id>_gold.sales_by_country AS
SELECT
 country,
 SUM(amount) AS revenue,
 COUNT(*) AS transactions
FROM training.<student_id>_silver.sales_enriched
GROUP BY country;
```

```sql
CREATE OR REPLACE TABLE training.<student_id>_gold.product_performance AS
SELECT
 product_id,
 product_name,
 category,
 SUM(quantity) AS units,
 SUM(amount) AS revenue
FROM training.<student_id>_silver.sales_enriched
GROUP BY product_id, product_name, category;
```

## 7. Demostrar coherencia

### No hay duplicados de clave

```sql
SELECT transaction_id, COUNT(*) AS occurrences
FROM training.<student_id>_silver.transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;
```

Debe devolver cero filas.

### No quedan registros que incumplan las reglas principales

```sql
SELECT COUNT(*) AS invalid_rows
FROM training.<student_id>_silver.transactions
WHERE
 transaction_id IS NULL
 OR customer_id IS NULL
 OR product_id IS NULL
 OR quantity <= 0
 OR unit_price < 0
 OR transaction_date IS NULL;
```

Debe devolver `0`.

### Gold reconcilia con Silver

```sql
SELECT SUM(amount) AS silver_revenue
FROM training.<student_id>_silver.sales_enriched;
```

```sql
SELECT SUM(revenue) AS gold_revenue
FROM training.<student_id>_gold.daily_sales;
```

Ambos totales deben coincidir salvo diferencias de precisión derivadas de tipos/rounding si se hubieran introducido transformaciones adicionales.

## 8. Comprobar lineage

En Catalog Explorer, abre un objeto Gold y revisa Lineage.

La cadena esperada es conceptualmente:

```text
Bronze transactions_raw
        ↓
Silver transactions
        ↓
Silver sales_enriched
        ↓
Gold products
```

Si se utiliza el pipeline declarativo, la interfaz del pipeline y Unity Catalog deben reflejar además las dependencias registradas entre streaming tables/materialized views.

## 9. Relacionar M01–M05

| Módulo | Concepto utilizado en el reto |
|---|---|
| M01 | Separación storage/compute, procesamiento distribuido, shuffle |
| M02 | DataFrames, transformaciones, joins, particiones y ejecución Spark |
| M03 | Workspace, Unity Catalog, Jobs/SQL, gobierno y lineage |
| M04 | Delta, ACID, history y `MERGE` |
| M05 | Landing, Bronze, Silver, Gold, calidad e incrementalidad |

El resultado final no es solo una serie de tablas: es un flujo gobernado que conserva el dato recibido, aplica reglas explícitas, integra cambios de forma incremental y publica productos de datos coherentes.
