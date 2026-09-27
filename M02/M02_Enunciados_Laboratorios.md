# M02 — Laboratorios
## Apache Spark: DataFrames, ejecución distribuida, SQL y optimización

**Curso:** Databricks — Big Data, Spark, Delta Lake y arquitectura Medallion
**Documento:** Enunciados para participantes
**Modalidad:** Individual
**Entorno:** Azure Databricks compartido
**Nivel:** Introductorio–intermedio

---

# 1. Objetivo

En M01 trabajaste con Hadoop, HDFS, YARN y MapReduce reales. En este módulo trabajarás directamente con **Apache Spark en Azure Databricks**.

La secuencia será:

```text
DataFrame
 ↓
Schema
 ↓
Transformations
 ↓
Actions
 ↓
Lazy evaluation
 ↓
Partitions
 ↓
Jobs / Stages / Tasks
 ↓
Shuffle
 ↓
Spark SQL
 ↓
Joins
 ↓
Optimization
```

No se pretende memorizar APIs. El objetivo es observar qué hace Spark y poder explicar por qué.

---

# 2. Entorno

Todos los participantes comparten:

```text
1 Azure Databricks Workspace
1 Classic Compute con acceso Standard
1 catálogo training
```

Cada participante tiene:

```text
training.<student_id>
```

y un Volume individual:

```text
/Volumes/training/<student_id>/work/
```

Los datos compartidos del módulo están en:

```text
/Volumes/training/shared/source/m02/
```

No modifiques datos ni objetos de otros participantes.

---

# 3. Ejercicio 0 — Comprobar el entorno

Ejecuta:

```python
spark.version
```

Después:

```sql
SELECT current_user();
```

Comprueba los ficheros disponibles:

```text
%fs ls /Volumes/training/shared/source/m02/
```

Deberías encontrar:

```text
sales/
customers/
products/
```

---

# 4. Ejercicio 1 — Leer un DataFrame y entender el schema

## Objetivo

Comprobar que Spark trabaja con datos estructurados y tipos.

```python
sales = (
 spark.read
 .option("header", "true")
 .option("inferSchema", "true")
 .csv("/Volumes/training/shared/source/m02/sales/")
)
```

```python
display(sales.limit(20))
sales.printSchema()
sales.count()
```

### Preguntas

1. ¿Qué columnas son numéricas?
2. ¿Qué columna representa una fecha o timestamp?
3. ¿Por qué conocer los tipos ayuda a Spark?
4. ¿`count()` es una transformación o una acción?

---

# 5. Ejercicio 2 — Transformaciones

## Objetivo

Construir un pipeline sin pedir todavía un resultado final.

```python
from pyspark.sql import functions as F

spain_sales = (
 sales
 .filter(F.col("country") == "ES")
 .select(
 "sale_id",
 "customer_id",
 "product_id",
 "quantity",
 "unit_price"
 )
 .withColumn(
 "amount",
 F.col("quantity") * F.col("unit_price")
 )
)
```

No ejecutes aún `display()`.

```python
type(spain_sales)
```

### Preguntas

1. ¿Hemos modificado `sales`?
2. ¿Qué DataFrame representa el resultado lógico?
3. ¿Qué operaciones del pipeline son transformaciones?

---

# 6. Ejercicio 3 — Lazy evaluation

Antes de mostrar datos:

```python
spain_sales.explain("formatted")
```

Ahora solicita resultados:

```python
spain_sales.count()
```

```python
display(spain_sales.limit(20))
```

### Reflexión

Explica con tus palabras:

```text
Transformation ≠ ejecución inmediata
Action = solicita un resultado
```

---

# 7. Ejercicio 4 — Escribir y volver a leer Parquet

## Objetivo

Utilizar un formato columnar sin introducir todavía Delta Lake.

Sustituye `<student_id>`:

```python
my_parquet = "/Volumes/training/<student_id>/work/m02/sales_es_parquet"
```

```python
(
 spain_sales
 .write
 .mode("overwrite")
 .parquet(my_parquet)
)
```

Lista los ficheros:

```text
%fs ls /Volumes/training/<student_id>/work/m02/sales_es_parquet
```

Vuelve a leer:

```python
sales_es_parquet = spark.read.parquet(my_parquet)

sales_es_parquet.printSchema()
display(sales_es_parquet.limit(10))
```

### Preguntas

1. ¿Se ha creado un único fichero?
2. ¿Por qué un dataset distribuido puede estar compuesto por varios ficheros?
3. ¿Qué información conserva Parquet además de los valores?

---

# 8. Ejercicio 5 — Particiones

## Objetivo

Observar cómo Spark reparte registros en particiones sin usar la API RDD.

```python
partition_distribution = (
 sales
 .withColumn(
 "partition_id",
 F.spark_partition_id()
 )
 .groupBy("partition_id")
 .count()
 .orderBy("partition_id")
)

display(partition_distribution)
```

### Preguntas

1. ¿Cuántas particiones observas?
2. ¿Todas contienen exactamente el mismo número de registros?
3. ¿Qué relación existe entre particiones y tareas paralelas?

---

# 9. Ejercicio 6 — `repartition()` y `coalesce()`

Crea ocho particiones:

```python
sales_8 = sales.repartition(8)
```

```python
display(
 sales_8
 .withColumn("partition_id", F.spark_partition_id())
 .groupBy("partition_id")
 .count()
 .orderBy("partition_id")
)
```

Reduce después a dos:

```python
sales_2 = sales_8.coalesce(2)
```

```python
display(
 sales_2
 .withColumn("partition_id", F.spark_partition_id())
 .groupBy("partition_id")
 .count()
 .orderBy("partition_id")
)
```

### Preguntas

1. ¿Qué operación redistribuye explícitamente los datos?
2. ¿Por qué `coalesce()` suele usarse para reducir particiones?
3. ¿Es cierto que “más particiones = siempre más rápido”?

---

# 10. Ejercicio 7 — Narrow vs wide transformations

### Caso A — Filter

```python
filtered = sales.filter(
 F.col("quantity") >= 3
)

filtered.explain("formatted")
```

### Caso B — GroupBy

```python
by_country = (
 sales
 .groupBy("country")
 .agg(
 F.sum(
 F.col("quantity") * F.col("unit_price")
 ).alias("revenue")
 )
)

by_country.explain("formatted")
```

Busca en el segundo plan:

```text
Exchange
```

### Preguntas

1. ¿Por qué `filter()` puede trabajar partición a partición?
2. ¿Por qué `groupBy("country")` necesita juntar datos relacionados?
3. ¿Cuál es más probable que provoque shuffle?

---

# 11. Ejercicio 8 — Job, Stage y Task

Ejecuta:

```python
result = (
 sales
 .repartition(8)
 .groupBy("country")
 .agg(
 F.sum("quantity").alias("units")
 )
)

display(result)
```

Ahora abre:

```text
Compute
→ compute compartido
→ Spark UI
```

Busca:

```text
Jobs
Stages
Executors
SQL / DataFrame
```

Observa:

- jobs completados;
- stages;
- número de tasks;
- shuffle read/write si aparece;
- DAG.

> El compute es compartido, por lo que puede haber actividad de otros participantes. El objetivo es reconocer la estructura Job → Stage → Task, no memorizar un Job ID concreto.

---

# 12. Ejercicio 9 — DataFrame y SQL

Crea una vista temporal:

```python
sales.createOrReplaceTempView("sales_m02")
```

### DataFrame API

```python
df_result = (
 sales
 .groupBy("country")
 .agg(
 F.sum(
 F.col("quantity") * F.col("unit_price")
 ).alias("revenue")
 )
 .orderBy(F.desc("revenue"))
)

display(df_result)
```

### SQL

```sql
SELECT
 country,
 SUM(quantity * unit_price) AS revenue
FROM sales_m02
GROUP BY country
ORDER BY revenue DESC;
```

### Preguntas

1. ¿Obtienes el mismo resultado?
2. ¿SQL y DataFrame utilizan motores diferentes?
3. ¿Qué interfaz te resulta más natural?

---

# 13. Ejercicio 10 — Joins

Lee clientes:

```python
customers = (
 spark.read
 .option("header", "true")
 .option("inferSchema", "true")
 .csv("/Volumes/training/shared/source/m02/customers/")
)

customers.count()
customers.printSchema()
```

Join:

```python
sales_with_customer = (
 sales
 .join(
 customers,
 on="customer_id",
 how="inner"
 )
)
```

```python
display(
 sales_with_customer
 .select(
 "sale_id",
 "customer_id",
 "customer_name",
 "country",
 "quantity",
 "unit_price"
 )
 .limit(20)
)
```

```python
sales_with_customer.explain("formatted")
```

---

# 14. Ejercicio 11 — Broadcast join

```python
from pyspark.sql.functions import broadcast

broadcast_join = (
 sales
 .join(
 broadcast(customers),
 on="customer_id",
 how="inner"
 )
)

broadcast_join.explain("formatted")
```

Busca, si aparece:

```text
BroadcastHashJoin
```

### Preguntas

1. ¿Qué dataset estamos enviando a los workers?
2. ¿Por qué solo tiene sentido si es suficientemente pequeño?
3. ¿Qué coste intentamos evitar?

---

# 15. Ejercicio 12 — Cache

```python
high_value = (
 sales
 .withColumn(
 "amount",
 F.col("quantity") * F.col("unit_price")
 )
 .filter(F.col("amount") >= 200)
)
```

```python
high_value.cache()
high_value.count()
```

Reutiliza:

```python
display(
 high_value
 .groupBy("country")
 .agg(F.sum("amount").alias("revenue"))
)
```

Libera al terminar:

```python
high_value.unpersist()
```

### Preguntas

1. ¿Cuándo tiene sentido cachear?
2. ¿Por qué cachear todo sería mala idea?
3. ¿Por qué `unpersist()` es especialmente importante en un compute compartido?

---

# 16. Ejercicio 13 — Adaptive Query Execution (AQE)

Comprueba:

```python
spark.conf.get("spark.sql.adaptive.enabled")
```

Ejecuta:

```python
aqe_test = (
 sales
 .repartition(16)
 .groupBy("country")
 .agg(F.sum("quantity").alias("units"))
)

aqe_test.explain("formatted")
display(aqe_test)
aqe_test.explain("formatted")
```

La salida exacta depende del runtime y de optimizaciones de Databricks. Busca referencias a:

```text
AdaptiveSparkPlan
```

si aparecen.

### Reflexión

¿Qué significa que Spark pueda utilizar estadísticas reales obtenidas durante la ejecución para adaptar un plan?

---

# 17. Ejercicio 14 — Data skew

Crea deliberadamente datos desequilibrados:

```python
skewed = (
 spark.range(0, 500_000)
 .withColumn(
 "group_key",
 F.when(F.col("id") < 450_000, "HOT")
 .otherwise(
 F.concat(
 F.lit("K"),
 (F.col("id") % 10).cast("string")
 )
 )
 )
)
```

```python
display(
 skewed
 .groupBy("group_key")
 .count()
 .orderBy(F.desc("count"))
)
```

Redistribuye por clave:

```python
skewed_by_key = skewed.repartition(8, "group_key")
```

```python
display(
 skewed_by_key
 .withColumn(
 "partition_id",
 F.spark_partition_id()
 )
 .groupBy("partition_id")
 .count()
 .orderBy("partition_id")
)
```

### Preguntas

1. ¿Está equilibrado el trabajo?
2. ¿Qué problema causa la clave `HOT`?
3. ¿Por qué añadir más workers no garantiza resolver un skew extremo?

---

# 18. Ejercicio 15 — Built-in functions antes que UDF

Lee productos:

```python
products = (
 spark.read
 .option("header", "true")
 .option("inferSchema", "true")
 .csv("/Volumes/training/shared/source/m02/products/")
)
```

Normaliza:

```python
clean_products = (
 products
 .withColumn(
 "product_name_clean",
 F.upper(F.trim(F.col("product_name")))
 )
 .withColumn(
 "category_clean",
 F.upper(F.trim(F.col("category")))
 )
)

display(clean_products.limit(20))
```

### Pregunta

¿Por qué conviene utilizar primero funciones nativas de Spark cuando resuelven correctamente el problema?

---

# 19. Ejercicio 16 — Structured Streaming

## Objetivo

Comprobar que Spark puede trabajar con un flujo mediante APIs estructuradas.

Sustituye `<student_id>`:

```python
stream = (
 spark.readStream
 .format("rate")
 .option("rowsPerSecond", 5)
 .load()
)

query = (
 stream
 .writeStream
 .format("memory")
 .queryName("m02_rate_<student_id>")
 .outputMode("append")
 .start()
)
```

Espera unos segundos y consulta:

```python
display(
 spark.sql(
 "SELECT * FROM m02_rate_<student_id> "
 "ORDER BY timestamp DESC LIMIT 20"
 )
)
```

```python
spark.sql(
 "SELECT COUNT(*) FROM m02_rate_<student_id>"
).show()
```

Detén siempre la consulta:

```python
query.stop()
```

### Preguntas

1. ¿El DataFrame de streaming representa un conjunto cerrado de filas?
2. ¿Por qué es importante detener la consulta?
3. ¿Qué similitud existe entre DataFrames batch y streaming?

---

# 20. Práctica autónoma A — Analítica de ventas

Utiliza:

```text
/Volumes/training/shared/source/m02/sales/
```

## Tareas

1. Lee los datos.
2. Muestra el schema.
3. Cuenta las ventas.
4. Calcula `amount = quantity * unit_price`.
5. Obtén ingresos por país.
6. Obtén ingresos por producto.
7. Muestra los 10 productos con más ingresos.
8. Identifica transformaciones y acciones.
9. Ejecuta `explain("formatted")` sobre una agregación.
10. Localiza un `Exchange`.
11. Abre Spark UI e identifica al menos un Job y sus Stages.

### Entregable

En una celda Markdown explica:

```text
¿Dónde se produce shuffle y por qué?
```

---

# 21. Práctica autónoma B — Clientes + ventas

Utiliza:

```text
sales/
customers/
```

## Tareas

1. Lee ambos datasets.
2. Comprueba sus schemas.
3. Haz join por `customer_id`.
4. Calcula ingresos por cliente.
5. Muestra los 10 clientes con mayor facturación.
6. Repite el join utilizando `broadcast(customers)`.
7. Compara ambos planes con `explain("formatted")`.
8. Explica cuándo un broadcast join es razonable y cuándo no.

---

# 22. Práctica autónoma C — Diagnóstico de rendimiento

Crea:

```python
data = spark.range(0, 1_000_000)
```

Construye un pipeline que:

1. añada `group_id = id % 100`;
2. filtre registros pares;
3. reparta en 8 particiones;
4. agrupe por `group_id`;
5. cuente registros;
6. ordene el resultado.

Debes identificar:

- operaciones narrow;
- operaciones wide;
- shuffle;
- acción;
- Jobs/Stages en Spark UI.

Después responde:

> ¿Qué revisarías si el dataset fuera 1.000 veces mayor?

No es necesario proponer una única solución. Justifica el razonamiento.

---

# 23. Reto final del M02

Explica:

```text
Source data
 ↓
DataFrame
 ↓
Transformations
 ↓
Logical plan
 ↓
Physical plan
 ↓
Job
 ↓
Stages
 ↓
Tasks over partitions
 ↓
Shuffle when required
 ↓
Result
```

Relaciona además:

```text
MapReduce del M01
vs.
DAG de Spark del M02
```

---

# 24. Referencias oficiales

- [Standard compute overview — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/compute/standard-overview)
- [Classic compute overview and permissions](https://learn.microsoft.com/en-us/azure/databricks/compute/use-compute)
- [Apache Spark 4.2.0 Quick Start](https://spark.apache.org/docs/4.2.0/quick-start.html)
- [Spark SQL guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Spark performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning)
- [Spark Web UI](https://spark.apache.org/docs/4.2.0/web-ui.html)
- [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)