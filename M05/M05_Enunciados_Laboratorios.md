# M05 — Laboratorios
## Medallion Architecture: Bronze, Silver, Gold, calidad, incrementalidad y Lakeflow

**Curso:** Databricks — Big Data, Spark, Delta Lake y arquitectura Medallion
**Documento:** Enunciados para participantes
**Modalidad:** Individual
**Entorno:** Azure Databricks compartido
**Nivel:** Introductorio–intermedio

---

# 1. Objetivo

En este módulo vas a construir un **mini-lakehouse Medallion completo**.

Utilizarás lo aprendido en los módulos anteriores:

```text
Spark
+
Databricks
+
Delta Lake
+
Unity Catalog
 ↓
Medallion Architecture
```

La secuencia será:

```text
Source files
 ↓
Bronze
raw + metadata
 ↓
Silver
clean + validate + deduplicate
 ↓
Gold
business-ready
 ↓
SQL / Dashboard
```

También experimentarás con:

- ingestión incremental;
- Auto Loader;
- expectations;
- quarantine;
- `MERGE`;
- Lakeflow Pipelines;
- lineage;
- materialized views.

---

# 2. Entorno individual dentro del workspace compartido

Todos utilizamos:

```text
1 Azure Databricks Workspace
1 catálogo training
```

Cada participante tiene tres schemas:

```text
training.<student_id>_bronze
training.<student_id>_silver
training.<student_id>_gold
```

Ejemplo:

```text
training.student01_bronze
training.student01_silver
training.student01_gold
```

Tu zona Bronze contiene además un Volume:

```text
/Volumes/training/<student_id>_bronze/landing/
```

Los datos originales proporcionados por el instructor se encuentran en:

```text
/Volumes/training/shared/m05_source/
```

No modifiques objetos de otros participantes.

---

# 3. Caso de negocio

Trabajarás con una empresa de comercio electrónico.

Fuentes:

```text
customers.csv
products.csv
transactions_day1.csv
transactions_day2.csv
```

Queremos terminar con productos de datos como:

```text
daily_sales
sales_by_country
product_performance
customer_360
```

---

# 4. Ejercicio 0 — Explorar las tres capas

## Objetivo

Comprobar que Bronze, Silver y Gold son capas lógicas distintas.

Ejecuta:

```sql
SHOW SCHEMAS IN training;
```

Localiza:

```text
<student_id>_bronze
<student_id>_silver
<student_id>_gold
```

Abre después Catalog Explorer y localiza los mismos schemas.

### Pregunta

¿Por qué puede resultar útil separar las capas mediante schemas y no únicamente usando nombres como:

```text
bronze_table
silver_table
gold_table
```

?

---

# 5. Ejercicio 1 — Preparar tu landing zone

## Objetivo

Crear una copia individual de los ficheros que irán llegando al lakehouse.

Comprueba los ficheros compartidos:

```text
%fs ls /Volumes/training/shared/m05_source/
```

Copia el primer lote de transacciones a tu landing:

```python
dbutils.fs.cp(
 "/Volumes/training/shared/m05_source/transactions_day1.csv",
 "/Volumes/training/<student_id>_bronze/landing/transactions/transactions_day1.csv"
)
```

Copia también:

```python
dbutils.fs.cp(
 "/Volumes/training/shared/m05_source/customers.csv",
 "/Volumes/training/<student_id>_bronze/landing/reference/customers.csv"
)

dbutils.fs.cp(
 "/Volumes/training/shared/m05_source/products.csv",
 "/Volumes/training/<student_id>_bronze/landing/reference/products.csv"
)
```

Comprueba:

```text
%fs ls /Volumes/training/<student_id>_bronze/landing/transactions/
```

### Reflexión

¿Qué diferencia existe entre:

```text
shared source
```

y:

```text
tu landing zone
```

?

---

# 6. Ejercicio 2 — Bronze: conservar el dato raw

## Objetivo

Crear una tabla Bronze con transformación mínima.

Ejecuta:

```python
from pyspark.sql import functions as F

bronze_transactions = (
 spark.read
 .option("header", "true")
 .option("inferSchema", "false")
 .csv(
 "/Volumes/training/<student_id>_bronze/landing/transactions/"
 )
 .withColumn(
 "_ingest_timestamp",
 F.current_timestamp()
 )
 .withColumn(
 "_source_file",
 F.col("_metadata.file_path")
 )
)
```

Observa:

```python
bronze_transactions.printSchema()
display(bronze_transactions)
```

Guarda:

```python
bronze_transactions.write \
 .mode("overwrite") \
 .saveAsTable(
 "training.<student_id>_bronze.transactions_raw"
 )
```

---

# 7. Ejercicio 3 — ¿Por qué Bronze conserva tipos flexibles?

Consulta:

```sql
DESCRIBE TABLE training.<student_id>_bronze.transactions_raw;
```

### Preguntas

1. ¿Qué tipos tienen las columnas procedentes del CSV?
2. ¿Qué columnas de metadata hemos añadido?
3. ¿Por qué puede ser útil conservar el valor recibido antes de convertir tipos?
4. ¿Por qué Bronze no debe convertirse en una capa sin control?

---

# 8. Ejercicio 4 — Crear reference Bronze

Carga clientes:

```python
customers_raw = (
 spark.read
 .option("header", "true")
 .option("inferSchema", "false")
 .csv(
 "/Volumes/training/<student_id>_bronze/landing/reference/customers.csv"
 )
 .withColumn("_ingest_timestamp", F.current_timestamp())
 .withColumn("_source_file", F.col("_metadata.file_path"))
)

customers_raw.write \
 .mode("overwrite") \
 .saveAsTable(
 "training.<student_id>_bronze.customers_raw"
 )
```

Repite para productos:

```python
products_raw = (
 spark.read
 .option("header", "true")
 .option("inferSchema", "false")
 .csv(
 "/Volumes/training/<student_id>_bronze/landing/reference/products.csv"
 )
 .withColumn("_ingest_timestamp", F.current_timestamp())
 .withColumn("_source_file", F.col("_metadata.file_path"))
)

products_raw.write \
 .mode("overwrite") \
 .saveAsTable(
 "training.<student_id>_bronze.products_raw"
 )
```

---

# 9. Ejercicio 5 — Detectar problemas de calidad antes de Silver

Consulta:

```sql
SELECT *
FROM training.<student_id>_bronze.transactions_raw
ORDER BY transaction_id;
```

Busca deliberadamente:

- duplicados;
- cantidades negativas o cero;
- importes inválidos;
- países inconsistentes;
- campos vacíos.

Ejecuta:

```sql
SELECT
 COUNT(*) AS rows,
 COUNT(DISTINCT transaction_id) AS distinct_transactions
FROM training.<student_id>_bronze.transactions_raw;
```

### Pregunta

¿Por qué no deberíamos corregir o borrar estos problemas directamente en Bronze?

---

# 10. Ejercicio 6 — Silver: tipar y normalizar

## Objetivo

Crear una representación validada.

Ejecuta:

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.transactions_typed
AS
SELECT
 TRY_CAST(transaction_id AS BIGINT) AS transaction_id,
 customer_id,
 product_id,
 TRY_CAST(quantity AS INT) AS quantity,
 TRY_CAST(unit_price AS DECIMAL(12,2)) AS unit_price,
 UPPER(TRIM(country)) AS country,
 TRY_CAST(transaction_date AS DATE) AS transaction_date,
 _ingest_timestamp,
 _source_file
FROM training.<student_id>_bronze.transactions_raw;
```

Consulta:

```sql
DESCRIBE TABLE training.<student_id>_silver.transactions_typed;
```

### Reflexión

¿Qué responsabilidades han pasado de Bronze a Silver?

---

# 11. Ejercicio 7 — Separar registros válidos e inválidos

## Objetivo

No confundir calidad con eliminación silenciosa.

Crea una vista de registros inválidos:

```sql
CREATE OR REPLACE VIEW training.<student_id>_silver.transactions_quarantine
AS
SELECT *
FROM training.<student_id>_silver.transactions_typed
WHERE
 transaction_id IS NULL
 OR customer_id IS NULL
 OR TRIM(customer_id) = ''
 OR product_id IS NULL
 OR TRIM(product_id) = ''
 OR quantity <= 0
 OR unit_price < 0
 OR transaction_date IS NULL;
```

Consulta:

```sql
SELECT *
FROM training.<student_id>_silver.transactions_quarantine;
```

Ahora crea válidos:

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.transactions_valid
AS
SELECT *
FROM training.<student_id>_silver.transactions_typed
WHERE
 transaction_id IS NOT NULL
 AND customer_id IS NOT NULL
 AND TRIM(customer_id) <> ''
 AND product_id IS NOT NULL
 AND TRIM(product_id) <> ''
 AND quantity > 0
 AND unit_price >= 0
 AND transaction_date IS NOT NULL;
```

### Preguntas

1. ¿Por qué conservar quarantine puede ser mejor que hacer `DROP` sin más?
2. ¿Quién debería decidir qué hacer con un registro inválido?

---

# 12. Ejercicio 8 — Deduplicación

Busca duplicados:

```sql
SELECT
 transaction_id,
 COUNT(*) AS occurrences
FROM training.<student_id>_silver.transactions_valid
GROUP BY transaction_id
HAVING COUNT(*) > 1;
```

Deduplica con una ventana:

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.transactions
AS
SELECT * EXCEPT (rn)
FROM (
 SELECT
 *,
 ROW_NUMBER() OVER (
 PARTITION BY transaction_id
 ORDER BY _ingest_timestamp DESC
 ) AS rn
 FROM training.<student_id>_silver.transactions_valid
)
WHERE rn = 1;
```

Comprueba:

```sql
SELECT
 COUNT(*) AS rows,
 COUNT(DISTINCT transaction_id) AS distinct_ids
FROM training.<student_id>_silver.transactions;
```

---

# 13. Ejercicio 9 — Normalizar clientes y productos

Clientes:

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.customers
AS
SELECT
 customer_id,
 TRIM(customer_name) AS customer_name,
 LOWER(TRIM(email)) AS email,
 UPPER(TRIM(country)) AS country
FROM training.<student_id>_bronze.customers_raw
WHERE customer_id IS NOT NULL;
```

Productos:

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.products
AS
SELECT
 product_id,
 TRIM(product_name) AS product_name,
 UPPER(TRIM(category)) AS category
FROM training.<student_id>_bronze.products_raw
WHERE product_id IS NOT NULL;
```

---

# 14. Ejercicio 10 — Conformar datos en Silver

## Objetivo

Construir una entidad reutilizable combinando varias fuentes.

Ejecuta:

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.sales_enriched
AS
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

Consulta:

```sql
SELECT *
FROM training.<student_id>_silver.sales_enriched
ORDER BY transaction_id;
```

### Pregunta

¿Por qué `sales_enriched` encaja mejor en Silver que en Gold?

---

# 15. Ejercicio 11 — Gold: construir métricas de negocio

## Objetivo

Crear productos de consumo.

### Daily sales

```sql
CREATE OR REPLACE TABLE training.<student_id>_gold.daily_sales
AS
SELECT
 transaction_date,
 SUM(amount) AS revenue,
 COUNT(*) AS transactions,
 AVG(amount) AS avg_ticket
FROM training.<student_id>_silver.sales_enriched
GROUP BY transaction_date;
```

### Sales by country

```sql
CREATE OR REPLACE TABLE training.<student_id>_gold.sales_by_country
AS
SELECT
 country,
 SUM(amount) AS revenue,
 COUNT(*) AS transactions
FROM training.<student_id>_silver.sales_enriched
GROUP BY country;
```

### Product performance

```sql
CREATE OR REPLACE TABLE training.<student_id>_gold.product_performance
AS
SELECT
 product_id,
 product_name,
 category,
 SUM(quantity) AS units,
 SUM(amount) AS revenue
FROM training.<student_id>_silver.sales_enriched
GROUP BY
 product_id,
 product_name,
 category;
```

---

# 16. Ejercicio 12 — Consumir Gold desde Databricks SQL

Abre Databricks SQL.

Ejecuta:

```sql
SELECT *
FROM training.<student_id>_gold.sales_by_country
ORDER BY revenue DESC;
```

Después:

```sql
SELECT *
FROM training.<student_id>_gold.product_performance
ORDER BY revenue DESC
LIMIT 10;
```

Crea al menos una visualización.

### Reflexión

¿Por qué el dashboard debería consultar Gold y no repetir toda la limpieza de Bronze/Silver?

---

# 17. Ejercicio 13 — Llegan nuevos datos

## Objetivo

Pasar de una carga única a un proceso incremental.

Copia el segundo lote:

```python
dbutils.fs.cp(
 "/Volumes/training/shared/m05_source/transactions_day2.csv",
 "/Volumes/training/<student_id>_bronze/landing/transactions/transactions_day2.csv"
)
```

Comprueba:

```text
%fs ls /Volumes/training/<student_id>_bronze/landing/transactions/
```

### Pregunta

¿Qué ocurriría si volviéramos a ejecutar ingenuamente todo el pipeline con `overwrite`?

---

# 18. Ejercicio 14 — MERGE incremental en Silver

El instructor ha preparado `transactions_day2.csv` con:

- nuevos registros;
- una transacción actualizada;
- un duplicado;
- un registro inválido.

Carga exclusivamente el nuevo lote:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

day2_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(
        "/Volumes/training/<student_id>_bronze/landing/transactions/transactions_day2.csv"
    )
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)
```

Aplica las mismas reglas de tipado y calidad utilizadas en Silver:

```python
day2_typed = (
    day2_raw
    .withColumn("transaction_id", F.expr("try_cast(transaction_id as bigint)"))
    .withColumn("quantity", F.expr("try_cast(quantity as int)"))
    .withColumn("unit_price", F.expr("try_cast(unit_price as decimal(12,2))"))
    .withColumn("country", F.upper(F.trim("country")))
    .withColumn("transaction_date", F.expr("try_cast(transaction_date as date)"))
)

day2_valid = day2_typed.filter(
    F.col("transaction_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("product_id").isNotNull()
    & (F.col("quantity") > 0)
    & (F.col("unit_price") >= 0)
    & F.col("transaction_date").isNotNull()
)
```

Como el lote contiene un duplicado, deduplica antes del `MERGE` para evitar que varias filas source puedan coincidir con la misma fila target:

```python
w = Window.partitionBy("transaction_id").orderBy(
    F.col("_ingest_timestamp").desc(),
    F.col("_source_file").desc()
)

day2_clean = (
    day2_valid
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

day2_clean.createOrReplaceTempView("day2_clean")
```

Aplica:

```sql
MERGE INTO training.<student_id>_silver.transactions AS target
USING day2_clean AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED THEN
 UPDATE SET *

WHEN NOT MATCHED THEN
 INSERT *;
```

Comprueba:

```sql
DESCRIBE HISTORY training.<student_id>_silver.transactions;
```

### Reflexión

Relaciona este ejercicio con el `MERGE` de M04.

---

# 19. Ejercicio 15 — Recalcular Silver enriquecida y Gold

El `MERGE` ha actualizado:

```text
training.<student_id>_silver.transactions
```

pero `sales_enriched` sigue siendo una tabla materializada creada anteriormente. Vuelve a generarla antes de recalcular Gold:

```sql
CREATE OR REPLACE TABLE training.<student_id>_silver.sales_enriched
AS
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

Después vuelve a crear:

```text
daily_sales
sales_by_country
product_performance
```

Comprueba qué métricas han cambiado.

### Pregunta

¿Por qué no es ideal tener que recordar manualmente el orden de todas estas operaciones?

---

# 20. Parte Lakeflow — Objetivo

Hasta aquí hemos construido Medallion manualmente.

Ahora vamos a expresar una parte del mismo proceso como un **pipeline declarativo**.

La idea será:

```text
landing
 ↓
Bronze streaming table
 ↓
Silver streaming/materialized table
 ↓
Gold materialized view
```

---

# 21. Ejercicio 16 — Crear un Lakeflow Pipeline

Abre:

```text
Jobs & Pipelines
```

Crea un pipeline con nombre:

```text
M05 Medallion - <student_id>
```

Elige SQL como lenguaje si el instructor así lo ha preparado.

Utiliza Serverless si está disponible; en caso contrario usa la configuración indicada por el instructor.

---

# 22. Ejercicio 17 — Bronze con Auto Loader

Añade al pipeline una definición equivalente a:

```sql
CREATE OR REFRESH STREAMING TABLE
training.<student_id>_bronze.transactions_stream_raw
AS
SELECT
 *,
 _metadata.file_path AS _source_file,
 current_timestamp() AS _ingest_timestamp
FROM STREAM read_files(
 '/Volumes/training/<student_id>_bronze/landing/transactions/',
 format => 'csv',
 header => true,
 schema => '
 transaction_id STRING,
 customer_id STRING,
 product_id STRING,
 quantity STRING,
 unit_price STRING,
 country STRING,
 transaction_date STRING'
);
```

Ejecuta el pipeline.

Consulta:

```sql
SELECT *
FROM training.<student_id>_bronze.transactions_stream_raw;
```

### Preguntas

1. ¿Qué papel realiza `read_files` en una streaming table?
2. ¿Por qué esta tabla sigue siendo Bronze?
3. ¿Qué metadata hemos conservado?

---

# 23. Ejercicio 18 — Silver con expectations

Añade:

```sql
CREATE OR REFRESH STREAMING TABLE
training.<student_id>_silver.transactions_stream_clean (
 CONSTRAINT valid_transaction_id
 EXPECT (transaction_id IS NOT NULL)
 ON VIOLATION DROP ROW,

 CONSTRAINT valid_quantity
 EXPECT (quantity > 0)
 ON VIOLATION DROP ROW,

 CONSTRAINT valid_price
 EXPECT (unit_price >= 0)
 ON VIOLATION DROP ROW
)
AS
SELECT
 TRY_CAST(transaction_id AS BIGINT) AS transaction_id,
 customer_id,
 product_id,
 TRY_CAST(quantity AS INT) AS quantity,
 TRY_CAST(unit_price AS DECIMAL(12,2)) AS unit_price,
 UPPER(TRIM(country)) AS country,
 TRY_CAST(transaction_date AS DATE) AS transaction_date,
 _source_file,
 _ingest_timestamp
FROM STREAM(
 training.<student_id>_bronze.transactions_stream_raw
);
```

Ejecuta el pipeline.

### Observa

En la interfaz del pipeline localiza métricas de calidad si están disponibles.

### Preguntas

1. ¿Qué ocurre con las filas inválidas?
2. ¿Qué diferencia hay entre `DROP ROW`, `FAIL UPDATE` y la acción por defecto?
3. ¿Cuándo sería peligroso usar `DROP ROW` sin conservar evidencia?

---

# 24. Ejercicio 19 — Gold como materialized view

Añade:

```sql
CREATE OR REFRESH MATERIALIZED VIEW
training.<student_id>_gold.daily_sales_mv
AS
SELECT
 transaction_date,
 SUM(quantity * unit_price) AS revenue,
 COUNT(*) AS transactions,
 AVG(quantity * unit_price) AS avg_ticket
FROM training.<student_id>_silver.transactions_stream_clean
GROUP BY transaction_date;
```

Ejecuta el pipeline.

Consulta:

```sql
SELECT *
FROM training.<student_id>_gold.daily_sales_mv
ORDER BY transaction_date;
```

### Pregunta

¿Por qué una materialized view resulta apropiada para una tabla Gold agregada?

---

# 25. Ejercicio 20 — Observar el Pipeline Graph

Abre el gráfico del pipeline.

Deberías poder relacionar:

```text
landing
 ↓
transactions_stream_raw
 ↓
transactions_stream_clean
 ↓
daily_sales_mv
```

### Preguntas

1. ¿Qué dependencias detecta Databricks?
2. ¿Por qué el gráfico aporta más información que una lista de notebooks?
3. ¿Qué ocurriría si fallase una expectativa configurada como `FAIL UPDATE`?

---

# 26. Ejercicio 21 — Lineage en Unity Catalog

Desde Catalog Explorer abre:

```text
training.<student_id>_gold.daily_sales_mv
```

Busca lineage.

Identifica upstream:

```text
Gold
← Silver
← Bronze
```

Si aparece el origen correspondiente, observa la cadena completa.

### Reflexión

¿Qué diferencia existe entre:

```text
conocer cómo debería fluir el dato
```

y:

```text
tener lineage registrado por la plataforma
```

?

---

# 27. Ejercicio 22 — Incrementalidad con Auto Loader

Copia un fichero adicional proporcionado por el instructor:

```python
dbutils.fs.cp(
 "/Volumes/training/shared/m05_source/transactions_day3.csv",
 "/Volumes/training/<student_id>_bronze/landing/transactions/transactions_day3.csv"
)
```

Vuelve a ejecutar/refresh el pipeline.

Comprueba:

```sql
SELECT COUNT(*)
FROM training.<student_id>_bronze.transactions_stream_raw;
```

y:

```sql
SELECT *
FROM training.<student_id>_gold.daily_sales_mv
ORDER BY transaction_date;
```

### Pregunta

¿Por qué Auto Loader no debería volver a tratar todos los ficheros como nuevos en cada ejecución normal?

---
# 28. Práctica autónoma A — Mini lakehouse completo

Sin copiar una solución paso a paso, construye:

```text
Bronze
customers_raw
products_raw
transactions_raw

Silver
customers
products
transactions
sales_enriched

Gold
daily_sales
sales_by_country
product_performance
```

Requisitos:

- Bronze conserva metadata;
- Silver realiza tipado;
- Silver contiene al menos una regla de calidad;
- Silver deduplica;
- Silver integra clientes/productos/transacciones;
- Gold contiene métricas de negocio;
- al menos una Gold table se consulta desde Databricks SQL.

---

# 29. Práctica autónoma B — Calidad y quarantine

Introduce o utiliza un pequeño lote con datos inválidos.

Debes:

1. conservar el fichero original en landing/Bronze;
2. identificar filas inválidas;
3. crear una quarantine;
4. mantener Silver libre de esos registros;
5. contar válidos e inválidos;
6. explicar por qué no has borrado el origen.

---

# 30. Práctica autónoma C — Incrementalidad

Añade un nuevo fichero de transacciones.

Debes conseguir que:

```text
Bronze aumente
Silver incorpore cambios sin duplicar
Gold refleje los nuevos datos
```

Utiliza:

- `MERGE`;
- o el mecanismo declarativo que haya indicado el instructor.

Demuestra con consultas antes/después qué ha cambiado.

---

# 31. Práctica autónoma D — Producto Gold

Construye un producto Gold adicional que responda a una pregunta de negocio.

Opciones:

```text
customer_360
revenue_by_category
monthly_sales
top_customers
```

Debe incluir:

1. una definición clara de la métrica;
2. una consulta SQL;
3. una tabla/materialized view Gold;
4. una visualización;
5. una explicación de por qué pertenece a Gold.

---

# 32. Práctica autónoma E — Explicar la arquitectura

Dibuja o documenta:

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

Para cada capa explica:

```text
objetivo
tipo de datos
transformaciones permitidas
usuario principal
qué NO debería hacerse ahí
```

---

# 33. Reto final del curso

El instructor proporciona un nuevo fichero:

```text
transactions_final.csv
```

Contiene una combinación de:

- registros nuevos;
- duplicados;
- un dato inválido;
- una corrección sobre una transacción anterior.

Sin un guion paso a paso debes:

1. introducirlo en la landing zone;
2. conservarlo en Bronze;
3. validar y deduplicar;
4. aplicar cambios a Silver;
5. actualizar Gold;
6. comprobar lineage;
7. demostrar que las métricas finales son coherentes;
8. explicar qué componentes de M01–M05 han intervenido.

---

# 34. Qué conceptos de módulos anteriores aparecen aquí

## M01

```text
procesamiento distribuido
almacenamiento vs compute
```

## M02

```text
DataFrames
SQL
particiones
shuffle
joins
```

## M03

```text
Workspace
Unity Catalog
SQL Warehouse
Lakeflow
AI/BI
```

## M04

```text
Delta
MERGE
schema
history
```

## M05

```text
Bronze
Silver
Gold
quality
incrementality
lineage
```

---

# 35. Referencias oficiales

- [Medallion architecture](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)
- [Lakeflow Pipelines](https://learn.microsoft.com/en-us/azure/databricks/ldp/concepts/)
- [Lakeflow Pipelines tutorial](https://learn.microsoft.com/en-us/azure/databricks/ldp/tutorial-get-started)
- [Auto Loader](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
- [Auto Loader best practices](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/best-practices)
- [`read_files`](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/read_files)
- [Expectations](https://learn.microsoft.com/en-us/azure/databricks/ldp/expectations)
- [Expectation patterns](https://learn.microsoft.com/en-us/azure/databricks/ldp/expectation-patterns)
- [Unity Catalog lineage](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-lineage)
- [MERGE](https://learn.microsoft.com/en-us/azure/databricks/delta/merge)