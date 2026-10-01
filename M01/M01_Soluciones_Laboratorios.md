# M01 — Soluciones de laboratorios
## Hadoop real en Azure + puente hacia Spark y Databricks

Estas soluciones sirven para comprobar el razonamiento y los resultados obtenidos durante los ejercicios. Algunos valores dependen del estado del clúster o del compute en el momento de la ejecución; cuando sea así se indica expresamente.

---

# Ejercicio 1 — Entrar en el clúster

1. La terminal pertenece a `hadoop-master`, no al equipo local. `hostname` debe devolver el nombre del nodo remoto.
2. En el entorno del curso debe aparecer Hadoop 3.5.0.
3. No es necesario instalar Hadoop localmente porque los comandos se ejecutan en la VM remota, donde Hadoop ya está instalado y configurado.
4. Azure Bastion permite acceder a la VM a través del portal sin exponer directamente SSH mediante una IP pública.

Las variables:

```bash
LAB_USER="$(id -un | cut -d@ -f1)"
HDFS_HOME="/training/users/$LAB_USER"
```

construyen una ruta HDFS individual a partir del usuario autenticado.

---

# Ejercicio 2 — Localizar DataNodes mediante HDFS

En el entorno preparado, el fichero compartido debe aparecer replicado en los tres DataNodes disponibles.

1. DataNodes distintos: normalmente 3.
2. Factor de replicación: 3.
3. El NameNode mantiene la metadata del filesystem y conoce qué bloques pertenecen a cada fichero y en qué DataNodes se encuentran.
4. Los DataNodes almacenan físicamente las réplicas de los bloques.

`hdfs fsck ... -locations` permite ver esa relación entre bloque lógico y ubicaciones físicas.

---

# Ejercicio 3 — Ver los nodos de YARN

| Sistema | Componente maestro | Componente worker | Responsabilidad |
|---|---|---|---|
| HDFS | NameNode | DataNode | Almacenamiento distribuido |
| YARN | ResourceManager | NodeManager | Gestión de recursos y ejecución |

HDFS responde a la pregunta **dónde están los datos**. YARN responde a **dónde y con qué recursos se ejecuta una aplicación**.

---

# Ejercicio 4 — Crear la zona HDFS individual

Separar zonas de trabajo evita colisiones de nombres, borrados accidentales y modificaciones sobre datos de otros usuarios. También permite aplicar permisos de forma independiente.

`chmod 700` deja la carpeta accesible únicamente para su propietario dentro del modelo de permisos HDFS.

---

# Ejercicio 5 — Filesystem local frente a HDFS

1. No son el mismo objeto. El fichero de `/tmp` reside en el filesystem local de `hadoop-master`; la copia de HDFS pertenece al filesystem distribuido.
2. `hdfs dfs -put` transfiere el contenido desde el filesystem local hacia HDFS.
3. El NameNode mantiene el namespace, la metadata y la localización de los bloques. No almacena normalmente el contenido de los bloques de usuario.

---

# Ejercicio 6 — Bloques y replicación

Para este fichero pequeño:

1. Debe utilizar un único bloque HDFS porque su tamaño es muy inferior al tamaño máximo de bloque.
2. Con factor de replicación 3, las réplicas deben aparecer en los tres DataNodes disponibles.
3. HDFS no rellena un bloque completo: un fichero pequeño ocupa un bloque lógico, pero solo consume físicamente los bytes necesarios.
4. La replicación permite seguir leyendo los datos aunque una de las réplicas o uno de los DataNodes deje de estar disponible.

---

# Ejercicio 7 — Observar el NameNode

JMX y `hdfs fsck` no consultan dos HDFS diferentes.

- JMX expone métricas y estado global del NameNode.
- `fsck` inspecciona el estado y la distribución de ficheros/bloques concretos.

Ambos reflejan el mismo HDFS desde perspectivas diferentes.

---

# Ejercicio 8 — Ejecutar Hadoop MapReduce WordCount

La ejecución crea una aplicación YARN. En la salida deben aparecer referencias a una aplicación, mapas, reducers y progreso.

La cadena conceptual es:

```text
HDFS input
  ↓
Map
  ↓
Shuffle / Sort
  ↓
Reduce
  ↓
HDFS output
```

---

# Ejercicio 9 — Consultar el resultado

1. Los datos de entrada estaban en HDFS, dentro de `$HDFS_HOME/input`.
2. El resultado queda persistido en HDFS, en `$HDFS_HOME/output_wordcount`.
3. Map procesa registros de entrada y genera pares intermedios clave/valor.
4. Shuffle redistribuye los pares para que los valores de una misma clave lleguen al mismo reducer.
5. Reduce agrega o combina los valores asociados a cada clave.
6. El resultado permanece porque compute y almacenamiento tienen ciclos de vida distintos: la aplicación termina, pero los ficheros HDFS continúan almacenados.

---

# Ejercicio 10 — Observar MapReduce en YARN

1. HDFS almacena datos; YARN gestiona recursos y ejecución.
2. Cada worker ofrece recursos mediante un NodeManager.
3. MapReduce aparece como aplicación YARN porque utiliza YARN para solicitar y coordinar recursos de ejecución.
4. El compute existe mientras la aplicación necesita recursos; los datos pueden permanecer antes y después de la ejecución.

---

# Ejercicio 11 — HDFS y tolerancia a fallos

1. Sí. Mientras exista otra réplica accesible, el bloque puede seguir leyéndose.
2. El NameNode conoce las ubicaciones.
3. Los DataNodes almacenan las réplicas.
4. Con replicación 1, la pérdida del único DataNode que contiene un bloque puede hacer que ese bloque quede temporal o permanentemente inaccesible.

---

# Ejercicio 12 — Crear un dataset distribuido en Spark

1. Sí, los registros aparecen distribuidos entre particiones. El número exacto puede variar según el compute y el plan.
2. No se ven directamente DataNodes.
3. No se ven directamente NodeManagers.
4. No. Spark sigue ejecutando trabajo distribuido; Databricks abstrae gran parte de la infraestructura subyacente.

Una partición Spark representa una unidad lógica de datos que puede ser procesada por una task.

---

# Ejercicio 13 — Repartition y shuffle

`repartition(8)` fuerza una redistribución para producir ocho particiones. Esa redistribución requiere movimiento de datos entre ejecutores y aparece en el plan físico como un `Exchange`.

La relación conceptual es:

```text
Hadoop MapReduce: Map → Shuffle → Reduce
Spark: transformaciones → Exchange/Shuffle → agregación u otra operación wide
```

Spark no elimina el coste del shuffle; ofrece un motor más flexible para encadenar operaciones en un DAG.

---

# Ejercicio 14 — WordCount con Spark

La solución transforma cada línea en palabras, agrupa por palabra y cuenta ocurrencias:

```python
from pyspark.sql import functions as F

text = spark.read.text("/Volumes/training/shared/source/words.txt")

word_count = (
    text
    .select(
        F.explode(
            F.split(F.lower(F.col("value")), r"\s+")
        ).alias("word")
    )
    .filter(F.col("word") != "")
    .groupBy("word")
    .count()
    .orderBy(F.desc("count"), "word")
)

display(word_count)
```

`explode`, `filter` y la preparación de columnas son transformaciones. `groupBy().count()` necesita agrupar por clave y puede provocar shuffle.

---

# Práctica autónoma A — HDFS y MapReduce

Una ejecución completa puede hacerse así:

```bash
hdfs dfs -mkdir -p "$HDFS_HOME/input"

hdfs dfs -cp   /training/shared/access_log.txt   "$HDFS_HOME/input/access_log.txt"

hdfs dfs -rm -r -f "$HDFS_HOME/output_logs"

hadoop jar   "$HADOOP_HOME"/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar   wordcount   "$HDFS_HOME/input/access_log.txt"   "$HDFS_HOME/output_logs"
```

Consulta de bloques y réplicas:

```bash
hdfs fsck   "$HDFS_HOME/input/access_log.txt"   -files   -blocks   -locations
```

Consulta de la aplicación YARN:

```bash
yarn application -list -appStates ALL
```

Consulta del resultado:

```bash
hdfs dfs -cat "$HDFS_HOME/output_logs/part-r-*"
```

Para extraer solo los códigos HTTP:

```bash
hdfs dfs -cat "$HDFS_HOME/output_logs/part-r-*"   | grep -E '^(200|404|500)[[:space:]]'
```

El fichero es pequeño, por lo que normalmente utiliza un bloque. El factor de replicación esperado es 3. El ID de aplicación y los conteos exactos deben tomarse de la ejecución real.

---

# Práctica autónoma B — El mismo problema en Spark

```python
from pyspark.sql import functions as F

logs = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/training/shared/source/access_log.csv")
)

logs.printSchema()

total = logs.count()
print("Registros:", total)

by_status = (
    logs
    .groupBy("status")
    .count()
    .orderBy(F.desc("count"))
)

display(by_status)
by_status.explain("formatted")
```

En el plan debe aparecer un `Exchange` asociado a la agregación por `status`, porque registros de la misma clave pueden encontrarse inicialmente en particiones distintas.

Comparación conceptual:

| Concepto | Hadoop MapReduce | Spark |
|---|---|---|
| Datos distribuidos | HDFS | Fuentes distribuidas / almacenamiento cloud |
| Paralelismo | Maps y reducers | Tasks sobre particiones |
| Reagrupación por clave | Shuffle | Shuffle / Exchange |
| Plan de trabajo | Fases MapReduce | DAG con múltiples operadores |
| Persistencia del resultado | Escritura explícita en HDFS | Solo si se escribe; un DataFrame por sí solo no es persistente |
| Infraestructura visible | Alta | Más abstraída en Databricks |

---

# Reto final — Arquitectura

| Problema | Hadoop tradicional | Entorno moderno del curso |
|---|---|---|
| Almacenamiento persistente | HDFS | Almacenamiento cloud gobernado mediante tablas/Volumes |
| Distribución física de datos | DataNodes y bloques HDFS | Gestionada/abstraída |
| Gestión de recursos | YARN | Compute gestionado |
| Motor batch clásico | MapReduce | Spark |
| Movimiento por clave | Shuffle | Shuffle / Exchange |
| Interfaz | CLI + APIs internas | Notebooks + SQL + GUI + CLI |

La idea clave es que cambian las capas de abstracción y las herramientas, pero siguen existiendo problemas fundamentales de sistemas distribuidos: particionado, paralelismo, movimiento de datos, tolerancia a fallos, persistencia y separación entre almacenamiento y compute.
