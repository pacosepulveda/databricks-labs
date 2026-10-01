# M02 — Soluciones de laboratorios
## Apache Spark: DataFrames, ejecución distribuida, SQL y optimización

Estas soluciones permiten contrastar el resultado de los ejercicios con el comportamiento esperado de Spark. Los números exactos de particiones, tasks, Jobs o tiempos pueden variar según el runtime, el tamaño de los datos y el estado del compute compartido.

---

# Ejercicio 0 — Comprobar el entorno

`spark.version` debe devolver la versión de Spark utilizada por el runtime activo.

`SELECT current_user();` devuelve la identidad autenticada.

```text
%fs ls /Volumes/training/shared/source/m02/
```

debe mostrar las carpetas de datos compartidos:

```text
sales/
customers/
products/
```

---

# Ejercicio 1 — Leer un DataFrame y entender el schema

El resultado exacto de `printSchema()` es la referencia para los tipos inferidos.

1. Las columnas utilizadas para cálculos son, como mínimo, `quantity` y `unit_price`. Los identificadores también pueden aparecer como numéricos si todos sus valores permiten inferirlos de esa forma.
2. La columna de fecha o timestamp es la que `printSchema()` muestre con un tipo temporal. Si el CSV se ha inferido como texto, aparecerá como `string`; esa diferencia demuestra por qué la inferencia no siempre sustituye a un schema explícito.
3. Los tipos permiten validar operaciones, elegir representaciones eficientes, optimizar expresiones y evitar conversiones innecesarias.
4. `count()` es una **acción** porque solicita un resultado y desencadena la ejecución del plan pendiente.

---

# Ejercicio 2 — Transformaciones

1. No se ha modificado `sales`. Los DataFrames son inmutables: cada operación devuelve un nuevo DataFrame lógico.
2. `spain_sales` representa el resultado lógico del pipeline.
3. `filter()`, `select()` y `withColumn()` son transformaciones.

Hasta que se ejecuta una acción, Spark puede construir y optimizar el plan sin materializar el resultado completo.

---

# Ejercicio 3 — Lazy evaluation

`explain("formatted")` permite inspeccionar el plan sin pedir el resultado de negocio.

```text
Transformation = describe cómo transformar los datos.
Action = exige un resultado y dispara la ejecución necesaria.
```

Ejemplos de transformaciones:

```text
filter
select
withColumn
groupBy + agg como construcción del plan
```

Ejemplos de acciones:

```text
count
collect
show
write
display, cuando necesita materializar resultados
```

La evaluación perezosa permite a Catalyst optimizar el conjunto del pipeline antes de ejecutarlo.

---

# Ejercicio 4 — Escribir y volver a leer Parquet

1. Normalmente se crean varios ficheros `part-...`, no necesariamente uno solo.
2. Cada partición de salida puede ser escrita en paralelo por una task, por lo que un dataset distribuido suele representarse mediante múltiples ficheros.
3. Parquet conserva, además de los valores, schema/tipos y metadata de organización columnar que permiten leer únicamente las columnas necesarias y aplicar optimizaciones.

El número exacto de ficheros depende del número de particiones efectivas durante la escritura.

---

# Ejercicio 5 — Particiones

1. El número debe tomarse del resultado de `partition_distribution`; depende de cómo se haya leído el dataset.
2. No tienen por qué contener exactamente el mismo número de registros.
3. Una partición es una unidad de paralelismo: durante una etapa, una task procesa normalmente una partición.

Más particiones pueden ofrecer más paralelismo, pero también aumentan scheduling, metadata y overhead.

---

# Ejercicio 6 — `repartition()` y `coalesce()`

1. `repartition(8)` redistribuye explícitamente los datos y normalmente provoca shuffle.
2. `coalesce(2)` se usa habitualmente para reducir particiones intentando evitar una redistribución completa.
3. No. Más particiones no significa siempre más velocidad. Un número excesivo genera tasks pequeñas y overhead; un número insuficiente reduce paralelismo y puede crear particiones muy grandes.

---

# Ejercicio 7 — Narrow vs wide transformations

1. `filter()` puede evaluar cada fila dentro de su partición actual sin necesitar datos de otras particiones, por eso es una transformación narrow.
2. `groupBy("country")` necesita reunir en la misma partición lógica los registros con la misma clave.
3. `groupBy()` es la operación más probable que provoque shuffle.

En el plan físico, el límite entre etapas suele hacerse visible mediante un operador `Exchange`.

---

# Ejercicio 8 — Job, Stage y Task

La estructura esperada es:

```text
Action
 ↓
Job
 ↓
Stages
 ↓
Tasks
 ↓
Partitions
```

En este ejercicio hay al menos una redistribución explícita por `repartition(8)` y otra necesidad de agrupar por `country`. El plan puede ser simplificado u optimizado por Spark, por lo que el número exacto de stages y tasks debe observarse en la Spark UI.

### Qué significa `Exchange`

`Exchange` indica una redistribución física de datos. Normalmente marca un **shuffle boundary**: los registros deben moverse entre particiones para satisfacer una distribución requerida por el siguiente operador.

### Qué significa `explain("formatted")`

La salida formatted separa el plan físico en operadores y muestra detalles de cada uno. Es especialmente útil para localizar:

- scans;
- filters;
- projections;
- joins;
- aggregations;
- `Exchange`;
- estrategias de join.

---

# Ejercicio 9 — DataFrame y SQL

1. Sí, ambas versiones deben producir los mismos ingresos por país.
2. No utilizan motores distintos. Tanto SQL como DataFrame API terminan expresándose como planes lógicos que Catalyst analiza y optimiza antes de ejecutarse con Spark.
3. La elección de interfaz depende del caso: SQL suele ser natural para analítica declarativa; DataFrame API facilita composición con Python/Scala y lógica programática.

Una forma útil de comprobar la equivalencia es ejecutar:

```python
df_result.explain("formatted")
```

y comparar el tipo de operaciones con el plan de la consulta SQL.

---

# Ejercicio 10 — Joins

El join relaciona ventas con clientes mediante `customer_id`.

```python
sales_with_customer = (
    sales
    .join(customers, on="customer_id", how="inner")
)
```

El plan físico decidirá la estrategia concreta de join. Dependiendo del tamaño estimado de `customers`, Spark puede elegir automáticamente broadcast o una estrategia con shuffle.

Por eso el resultado de `explain("formatted")` es más importante que asumir una estrategia concreta.

---

# Ejercicio 11 — Broadcast join

1. Se envía una copia de `customers` a los ejecutores.
2. Solo es razonable si el dataset es suficientemente pequeño para ser distribuido y mantenido en memoria sin presión excesiva.
3. Se intenta evitar el shuffle de la tabla grande. Cada partición de `sales` puede resolver el join localmente contra la copia de `customers`.

En el plan físico debe buscarse:

```text
BroadcastExchange
BroadcastHashJoin
```

El broadcast no hace que un join sea siempre mejor; es una estrategia adecuada cuando uno de los lados es pequeño.

---

# Ejercicio 12 — Cache

1. Tiene sentido cachear cuando un DataFrame costoso de calcular va a reutilizarse varias veces.
2. Cachear todo consume memoria y puede expulsar datos útiles, provocar spill o competir con otras cargas.
3. `unpersist()` libera explícitamente esos recursos, algo especialmente importante en compute compartido.

`cache()` por sí solo no obliga necesariamente a materializar todos los datos. La acción `count()` fuerza su cálculo y hace que pueda quedar almacenado para reutilizaciones posteriores.

---

# Ejercicio 13 — Adaptive Query Execution

Si AQE está activado:

```python
spark.conf.get("spark.sql.adaptive.enabled")
```

debe devolver un valor equivalente a `true`.

`AdaptiveSparkPlan` indica que Spark puede revisar decisiones del plan físico utilizando estadísticas obtenidas durante la ejecución. Por ejemplo, puede:

- combinar particiones de shuffle pequeñas;
- cambiar algunas estrategias de join;
- mitigar determinados casos de skew;
- ajustar decisiones que antes se tomaban solo con estimaciones previas.

AQE no elimina la necesidad de diseñar bien los datos y las transformaciones; mejora decisiones de ejecución cuando dispone de información real.

---

# Ejercicio 14 — Data skew

1. No. La clave `HOT` concentra aproximadamente el 90 % de los registros y crea una carga muy desigual.
2. Todos los registros de la misma clave deben acabar juntos para operaciones que agrupen por esa clave. Una partición puede convertirse en cuello de botella.
3. Añadir workers no divide automáticamente una única clave extremadamente dominante. Si el trabajo asociado a `HOT` sigue concentrado en una sola partición lógica, muchos recursos pueden quedar ociosos mientras una task tarda mucho más.

Además, `repartition(8, "group_key")` no "arregla" necesariamente el skew: distribuye por hash de la clave, por lo que todos los `HOT` continúan juntos.

---

# Ejercicio 15 — Built-in functions antes que UDF

Las funciones nativas de Spark son preferibles porque Catalyst conoce su semántica y puede integrarlas en el plan optimizado.

Ventajas habituales:

- mejor optimización;
- menor coste de serialización;
- ejecución más eficiente;
- mejor integración con code generation;
- planes más transparentes.

Una UDF puede ser necesaria cuando la lógica no puede expresarse con funciones nativas, pero no debería ser la primera opción por comodidad.

---

# Ejercicio 16 — Structured Streaming

1. No. El DataFrame de streaming representa una fuente que puede seguir recibiendo nuevas filas.
2. Es importante detener la query para liberar recursos y evitar que una ejecución siga activa innecesariamente.
3. Batch y streaming comparten el modelo estructurado de DataFrames y muchas operaciones. La diferencia principal es que en streaming el conjunto de entrada evoluciona con el tiempo y Spark mantiene un proceso incremental.

La tabla en memoria `m02_rate_<student_id>` solo existe mientras el contexto correspondiente mantenga ese resultado.

---

# Práctica autónoma A — Analítica de ventas

Una solución completa:

```python
from pyspark.sql import functions as F

sales_a = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/training/shared/source/m02/sales/")
)

sales_a.printSchema()
print("Ventas:", sales_a.count())

sales_amount = sales_a.withColumn(
    "amount",
    F.col("quantity") * F.col("unit_price")
)

revenue_country = (
    sales_amount
    .groupBy("country")
    .agg(F.sum("amount").alias("revenue"))
    .orderBy(F.desc("revenue"))
)

revenue_product = (
    sales_amount
    .groupBy("product_id")
    .agg(F.sum("amount").alias("revenue"))
    .orderBy(F.desc("revenue"))
)

display(revenue_country)
display(revenue_product.limit(10))

revenue_country.explain("formatted")
```

Transformaciones:

```text
withColumn
groupBy
agg
orderBy
limit
```

Acciones/materialización:

```text
count
display
```

El shuffle se produce en la agregación por país/producto porque los registros de una misma clave pueden estar repartidos entre varias particiones. En el plan debe buscarse `Exchange`.

En Spark UI debe localizarse el Job provocado por una acción y observar sus Stages. Un `Exchange` suele separar etapas porque crea una dependencia wide.

---

# Práctica autónoma B — Clientes + ventas

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

sales_b = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/training/shared/source/m02/sales/")
)

customers_b = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/training/shared/source/m02/customers/")
)

sales_b.printSchema()
customers_b.printSchema()

joined = sales_b.join(customers_b, "customer_id", "inner")

top_customers = (
    joined
    .withColumn("amount", F.col("quantity") * F.col("unit_price"))
    .groupBy("customer_id", "customer_name")
    .agg(F.sum("amount").alias("revenue"))
    .orderBy(F.desc("revenue"))
)

display(top_customers.limit(10))

joined.explain("formatted")

joined_broadcast = sales_b.join(
    broadcast(customers_b),
    "customer_id",
    "inner"
)

joined_broadcast.explain("formatted")
```

Comparación:

- join normal: Spark elige estrategia según estadísticas y configuración;
- join con `broadcast(customers_b)`: se solicita explícitamente broadcast;
- es razonable si `customers_b` es pequeño;
- no es razonable si el lado broadcast ocupa demasiada memoria o cuesta demasiado distribuirlo.

---

# Práctica autónoma C — Diagnóstico de rendimiento

```python
from pyspark.sql import functions as F

data = spark.range(0, 1_000_000)

result = (
    data
    .withColumn("group_id", F.col("id") % 100)
    .filter((F.col("id") % 2) == 0)
    .repartition(8)
    .groupBy("group_id")
    .count()
    .orderBy("group_id")
)

result.explain("formatted")
display(result)
```

Clasificación aproximada:

| Operación | Tipo |
|---|---|
| `withColumn` | narrow |
| `filter` | narrow |
| `repartition(8)` | wide, provoca shuffle |
| `groupBy("group_id")` | wide, requiere distribución por clave |
| `count` dentro de la agregación | parte de la agregación |
| `orderBy` global | wide, puede requerir otro shuffle |
| `display` | acción/materialización |

Si el dataset fuera 1.000 veces mayor, convendría revisar:

- tamaño total y selectividad de filtros;
- número y tamaño de particiones;
- skew de claves;
- shuffle read/write;
- spill a disco;
- estrategia de joins, si los hubiera;
- tamaño de ficheros de entrada;
- uso de funciones nativas;
- reutilización y cache solo cuando aporte valor;
- Spark UI y plan físico antes de aumentar compute.

La respuesta correcta no es simplemente "añadir más workers": primero hay que localizar el cuello de botella.

---

# Reto final M02

```text
Source data
 ↓
DataFrame
 ↓
Transformations
 ↓
Logical plan
 ↓
Catalyst optimization
 ↓
Physical plan
 ↓
Action
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

Relación con MapReduce:

| Hadoop MapReduce | Spark |
|---|---|
| Job MapReduce | Job/DAG |
| Map tasks | Tasks sobre particiones |
| Shuffle | Exchange / shuffle |
| Reduce tasks | Stages posteriores al shuffle, agregaciones o joins |
| Flujo rígido Map → Reduce | DAG con múltiples operadores y stages |
| Escritura entre fases como modelo clásico | Puede mantener resultados intermedios en memoria/disco según el plan |

Spark no elimina los fundamentos distribuidos del M01. Los generaliza dentro de un DAG de ejecución y permite encadenar más operaciones antes de materializar resultados.
