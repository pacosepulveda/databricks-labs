# M01 — Laboratorios Learn by Doing
## Hadoop real en Azure + puente hacia Spark y Databricks

**Curso:** Databricks — Big Data, Spark, Delta Lake y arquitectura Medallion  
**Documento:** Enunciados para alumnos  
**Metodología:** Learn by Doing  
**Modalidad:** Individual  
**Entornos:** Hadoop sobre Azure Virtual Machines + Azure Databricks  
**Nivel:** Introductorio–intermedio

---

# 1. Objetivo

En este módulo vas a trabajar primero con un **clúster Hadoop real** y después compararás esa experiencia con un entorno moderno basado en Spark y Databricks.

El objetivo es aprender haciendo:

```text
concepto
   ↓
experimento
   ↓
observación
   ↓
explicación
   ↓
práctica autónoma
```

Trabajarás directamente con HDFS, NameNode, DataNodes, YARN, ResourceManager, NodeManagers y Hadoop MapReduce. Después utilizarás Azure Databricks para observar cómo conceptos como particiones, procesamiento paralelo y shuffle siguen existiendo, aunque la infraestructura esté mucho más abstraída.

---

# 2. Entornos del módulo

## Entorno A — Hadoop real en Azure

El instructor proporcionará acceso a un clúster Hadoop compartido desplegado sobre Azure Virtual Machines:

```text
Azure Virtual Network
│
├── HADOOP-MASTER
│   ├── HDFS NameNode
│   └── YARN ResourceManager
│
├── HADOOP-W01
│   ├── HDFS DataNode
│   └── YARN NodeManager
│
├── HADOOP-W02
│   ├── HDFS DataNode
│   └── YARN NodeManager
│
└── HADOOP-W03
    ├── HDFS DataNode
    └── YARN NodeManager
```

Cada alumno tendrá un usuario Linux individual, acceso SSH al nodo master y un directorio HDFS individual:

```text
/training/<student_id>
```

## Entorno B — Azure Databricks

Todos los alumnos utilizarán:

```text
1 Azure Databricks Workspace
1 Compute compartido
1 catálogo de formación
```

Cada alumno tendrá su propia zona lógica, por ejemplo:

```text
training.student01
```

---

# 3. Normas

No debes modificar directorios HDFS de otros alumnos, borrar datos compartidos, cambiar configuraciones del clúster, crear o destruir compute, cambiar permisos ni modificar objetos de otros alumnos.

Utiliza siempre tu identificador `<student_id>` en las rutas indicadas.

---

# 4. Parte A — Hadoop real

## Ejercicio 1 — Entrar en el clúster

### Objetivo

Acceder al nodo master y comprobar que estamos trabajando en un clúster Hadoop real.

El instructor te proporcionará:

```text
MASTER_HOST
student_id
credencial de acceso
```

Desde PowerShell, Terminal o cualquier cliente OpenSSH, conecta al master creando además dos túneles locales para las interfaces web de Hadoop:

```bash
ssh \
  -L 9870:127.0.0.1:9870 \
  -L 8088:127.0.0.1:8088 \
  <student_id>@<MASTER_HOST>
```

Una vez dentro:

```bash
hostname
whoami
hadoop version
hdfs version
yarn version
```

### Preguntas

1. ¿Estás conectado a tu ordenador local o al nodo master?
2. ¿Qué tecnologías aparecen instaladas?
3. ¿Por qué no has tenido que instalar Hadoop en tu equipo?

---

## Ejercicio 2 — Ver los DataNodes de HDFS

### Objetivo

Comprobar que HDFS está distribuido entre varios nodos.

Ejecuta:

```bash
hdfs dfsadmin -report
```

Localiza:

```text
Live datanodes
```

### Preguntas

1. ¿Cuántos DataNodes están activos?
2. ¿Qué información muestra HDFS de cada DataNode?
3. ¿Qué ocurriría si no existiera ningún DataNode?

---

## Ejercicio 3 — Ver los nodos de YARN

### Objetivo

Distinguir almacenamiento distribuido de gestión de recursos.

Ejecuta:

```bash
yarn node -list
```

Completa:

| Sistema | Componente maestro | Componente worker | Responsabilidad |
|---|---|---|---|
| HDFS | ______ | ______ | Almacenamiento |
| YARN | ______ | ______ | Recursos y ejecución |

---

## Ejercicio 4 — Tu directorio HDFS

Comprueba el directorio común:

```bash
hdfs dfs -ls /training
```

Crea tu zona de trabajo si no existe:

```bash
hdfs dfs -mkdir -p /training/<student_id>/input
```

Comprueba:

```bash
hdfs dfs -ls /training/<student_id>
```

---

## Ejercicio 5 — Local filesystem vs HDFS

### Objetivo

Distinguir entre el disco local del master y HDFS.

Crea un fichero local:

```bash
cat > /tmp/words_<student_id>.txt <<'EOF'
spark hadoop databricks
hadoop hdfs spark
databricks delta spark
hadoop yarn mapreduce
spark delta lakehouse
EOF
```

Comprueba:

```bash
cat /tmp/words_<student_id>.txt
```

Ahora mira HDFS:

```bash
hdfs dfs -ls /training/<student_id>/input
```

El fichero todavía no debería aparecer allí.

Cópialo a HDFS:

```bash
hdfs dfs -put   /tmp/words_<student_id>.txt   /training/<student_id>/input/
```

Comprueba:

```bash
hdfs dfs -ls -h /training/<student_id>/input
```

Lee el fichero desde HDFS:

```bash
hdfs dfs -cat   /training/<student_id>/input/words_<student_id>.txt
```

### Preguntas

1. ¿Es el fichero local el mismo objeto que el fichero de HDFS?
2. ¿Qué comando ha transferido el fichero al sistema distribuido?
3. ¿Por qué HDFS necesita un NameNode?

---

## Ejercicio 6 — Bloques y replicación

Ejecuta:

```bash
hdfs fsck   /training/<student_id>/input/words_<student_id>.txt   -files   -blocks   -locations
```

Busca:

- número de bloques;
- factor de replicación;
- ubicaciones.

### Preguntas

1. ¿Cuántos bloques utiliza el fichero?
2. ¿En qué DataNodes aparecen las réplicas?
3. ¿Por qué un fichero tan pequeño tiene tan pocos bloques?
4. ¿Qué ventaja aporta la replicación?

---

## Ejercicio 7 — Abrir el NameNode

Mantén abierta la conexión SSH del ejercicio 1.

En tu navegador abre:

```text
http://localhost:9870
```

Busca:

- capacidad;
- DataNodes activos;
- uso de almacenamiento;
- información del filesystem.

### Reflexión

Relaciona `hdfs dfsadmin -report` con la interfaz del NameNode. ¿Son dos sistemas diferentes o dos formas de observar el mismo sistema?

---

## Ejercicio 8 — Ejecutar Hadoop MapReduce WordCount

Asegúrate de que no existe la salida:

```bash
hdfs dfs -rm -r -f   /training/<student_id>/output_wordcount
```

Ejecuta:

```bash
hadoop jar   "$HADOOP_HOME"/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar   wordcount   /training/<student_id>/input   /training/<student_id>/output_wordcount
```

Observa la salida y busca referencias a map, reduce, application y progress.

---

## Ejercicio 9 — Consultar el resultado

Lista la salida:

```bash
hdfs dfs -ls   /training/<student_id>/output_wordcount
```

Muestra el resultado:

```bash
hdfs dfs -cat   /training/<student_id>/output_wordcount/part-r-*
```

### Preguntas

1. ¿Dónde estaban los datos de entrada?
2. ¿Dónde queda el resultado?
3. ¿Qué función tiene la fase Map?
4. ¿Qué ocurre durante Shuffle?
5. ¿Qué función tiene Reduce?

---

## Ejercicio 10 — Ver MapReduce en YARN

Ejecuta:

```bash
yarn application -list -appStates ALL
```

En tu navegador abre:

```text
http://localhost:8088
```

Busca:

- Applications;
- Nodes;
- estado;
- duración;
- recursos.

### Preguntas

1. ¿Qué diferencia existe entre HDFS y YARN?
2. ¿Por qué el resultado sigue existiendo aunque la aplicación haya terminado?
3. ¿Qué componente ofrece recursos en cada worker?

---

## Ejercicio 11 — Razonar sobre tolerancia a fallos

No vamos a apagar nodos.

Utiliza la información obtenida con `hdfs fsck ... -locations` y responde:

1. Si una copia de un bloque estuviera en Worker 1 y otra en Worker 2, ¿qué ocurriría si Worker 1 dejara de estar disponible?
2. ¿Quién conoce las ubicaciones?
3. ¿Quién almacena físicamente cada réplica?

---

# 5. Parte B — Del Hadoop visible al Spark abstraído

Cierra la sesión SSH:

```bash
exit
```

Abre el Azure Databricks workspace proporcionado por el instructor.

---

## Ejercicio 12 — Crear un dataset distribuido

```python
from pyspark.sql import functions as F

numbers = spark.range(0, 1_000_000)

partitioned = numbers.withColumn(
    "partition_id",
    F.spark_partition_id()
)

distribution = (
    partitioned
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

display(distribution)
```

### Preguntas

1. ¿Los datos aparecen repartidos entre varias particiones?
2. ¿Ves directamente DataNodes?
3. ¿Ves directamente NodeManagers?
4. ¿Significa que ya no existe procesamiento distribuido?

---

## Ejercicio 13 — Repartition y shuffle

```python
numbers_8 = numbers.repartition(8)
```

Después:

```python
distribution_8 = (
    numbers_8
    .withColumn(
        "partition_id",
        F.spark_partition_id()
    )
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

display(distribution_8)
```

Inspecciona:

```python
distribution_8.explain("formatted")
```

Busca:

```text
Exchange
```

### Reflexión

En Hadoop acabamos de observar:

```text
Map
↓
Shuffle
↓
Reduce
```

En Spark sigue existiendo movimiento de datos, pero el modelo de ejecución es más flexible.

---

## Ejercicio 14 — WordCount con Spark

El instructor ha preparado:

```text
/Volumes/training/shared/source/words.txt
```

Ejecuta:

```python
text = spark.read.text(
    "/Volumes/training/shared/source/words.txt"
)

words_df = (
    text
    .select(
        F.explode(
            F.split(
                F.lower(F.col("value")),
                r"\s+"
            )
        ).alias("word")
    )
    .filter(F.col("word") != "")
)

word_count = (
    words_df
    .groupBy("word")
    .count()
    .orderBy(F.desc("count"), "word")
)

display(word_count)
```

Compara:

```text
Hadoop MapReduce
HDFS → Map → Shuffle → Reduce → HDFS

Spark
DataFrame → transformations → shuffle → aggregation → result
```

---

# 6. Práctica autónoma A — HDFS y MapReduce

Existe un fichero compartido HDFS:

```text
/training/shared/access_log.txt
```

Cada línea contiene:

```text
status path
```

### Tareas

1. Copia el fichero a tu directorio:

```bash
hdfs dfs -cp   /training/shared/access_log.txt   /training/<student_id>/input/access_log.txt
```

2. Comprueba que existe.
3. Usa MapReduce WordCount para contar las palabras.
4. Localiza en el resultado los códigos `200`, `404` y `500`.
5. Comprueba el job en YARN.
6. Utiliza `hdfs fsck` para mostrar bloques y ubicaciones.

### Entregable

Anota:

- número de bloques;
- factor de replicación;
- DataNodes donde reside;
- ID de la aplicación YARN;
- conteos de 200, 404 y 500.

---

# 7. Práctica autónoma B — El mismo problema en Spark

En Azure Databricks utiliza:

```text
/Volumes/training/shared/source/access_log.csv
```

Debes:

1. leer el CSV;
2. mostrar su schema;
3. contar registros;
4. agrupar por `status`;
5. ordenar por frecuencia;
6. inspeccionar `explain("formatted")`;
7. localizar un `Exchange`.

### Pregunta final

Explica qué elementos conceptuales se mantienen entre Hadoop MapReduce y Spark y cuáles han cambiado.

---

# 8. Reto final — Arquitectura

Completa:

| Problema | Hadoop tradicional | Entorno moderno del curso |
|---|---|---|
| Almacenamiento persistente | ______ | ______ |
| Distribución física de datos | ______ | Gestionada/abstraída |
| Gestión de recursos | ______ | Compute gestionado |
| Motor batch clásico | ______ | Spark |
| Movimiento por clave | Shuffle | ______ |
| Interfaz | CLI + UIs | Notebooks + SQL + GUI + CLI |

---

# 9. Resultado esperado

Al terminar deberías poder explicar:

> HDFS distribuye y replica datos entre DataNodes y utiliza un NameNode para gestionar metadata. YARN administra recursos mediante ResourceManager y NodeManagers. MapReduce procesa datos mediante Map, Shuffle y Reduce. Spark mantiene muchos de los problemas fundamentales del procesamiento distribuido, como particiones y shuffle, pero ofrece un modelo de ejecución más flexible y plataformas como Databricks ocultan gran parte de la infraestructura.

---

# 10. Referencias oficiales

- [Azure Linux virtual machines](https://learn.microsoft.com/en-us/azure/virtual-machines/linux/)
- [Azure Network Security Groups](https://learn.microsoft.com/en-us/azure/virtual-network/network-security-groups-overview)
- [HDFS Architecture](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [YARN Architecture](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html)
- [MapReduce Tutorial](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
- [Apache Spark SQL/DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Azure Databricks compute](https://learn.microsoft.com/en-us/azure/databricks/compute/)