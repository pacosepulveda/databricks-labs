# M01 — Laboratorios
## Hadoop real en Azure + puente hacia Spark y Databricks

**Curso:** Databricks — Big Data, Spark, Delta Lake y arquitectura Medallion  
**Documento:** Enunciados de laboratorio  
**Modalidad:** Individual  
**Entornos:** Hadoop sobre Azure Virtual Machines + Azure Databricks  
**Nivel:** Introductorio–intermedio

---

# 1. Objetivo

En este módulo se trabajará primero con un **clúster Hadoop real** y después se comparará esa experiencia con un entorno moderno basado en Spark y Databricks.

Se utilizarán directamente HDFS, NameNode, DataNodes, YARN, ResourceManager, NodeManagers, Hadoop MapReduce, bloques, replicación y shuffle.

Después se utilizará Azure Databricks para observar cómo conceptos como particiones, procesamiento paralelo y shuffle siguen existiendo aunque la infraestructura esté mucho más abstraída.

---

# 2. Entornos del módulo

## Entorno A — Hadoop real en Azure

```text
Azure Virtual Network
│
├── HADOOP-MASTER
│   ├── HDFS NameNode
│   ├── YARN ResourceManager
│   └── MapReduce JobHistoryServer
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

El acceso al nodo master se realiza mediante:

```text
Navegador
   ↓
Azure Portal
   ↓
Microsoft Entra ID + MFA
   ↓
Azure Bastion
   ↓
HADOOP-MASTER
```

El nodo master no dispone de una IP pública de acceso directo.

## Entorno B — Azure Databricks

Se utilizará un entorno compartido con:

```text
1 Azure Databricks Workspace
1 Compute compartido
1 catálogo training
```

Cada identidad dispone de una zona lógica propia.

---

# 3. Normas del entorno

No se deben modificar zonas HDFS ajenas, borrar datos compartidos, cambiar configuraciones del clúster, detener servicios Hadoop, modificar permisos globales, acceder directamente a los workers ni crear, eliminar o modificar recursos de Azure.

---

# 4. Parte A — Hadoop real

## Ejercicio 1 — Entrar en el clúster

### Objetivo

Acceder al nodo master mediante Azure Bastion y comprobar que se está trabajando en un clúster Hadoop real.

### Acceso

1. Abre Azure Portal con la identidad proporcionada.
2. Accede al grupo de recursos del laboratorio.
3. Abre la máquina virtual `hadoop-master`.
4. Selecciona **Connect → Bastion**.
5. Utiliza autenticación con **Microsoft Entra ID**.

Una vez dentro:

```bash
hostname
whoami
hadoop version
hdfs version
yarn version
```

Define dos variables que se reutilizarán durante el laboratorio:

```bash
LAB_USER="$(id -un | cut -d@ -f1)"
HDFS_HOME="/training/users/$LAB_USER"

echo "$LAB_USER"
echo "$HDFS_HOME"
```

### Preguntas

1. ¿La terminal pertenece al equipo local o a `hadoop-master`?
2. ¿Qué versión de Hadoop aparece?
3. ¿Por qué no ha sido necesario instalar Hadoop localmente?
4. ¿Qué función cumple Azure Bastion en este acceso?

---

## Ejercicio 2 — Localizar DataNodes mediante HDFS

### Objetivo

Comprobar que un fichero de HDFS está almacenado de forma distribuida.

Existe un fichero compartido:

```text
/training/shared/access_log.txt
```

Inspecciónalo:

```bash
hdfs fsck \
  /training/shared/access_log.txt \
  -files \
  -blocks \
  -locations
```

Busca el número de bloques, el factor de replicación y las ubicaciones de las réplicas.

### Preguntas

1. ¿Cuántos DataNodes distintos aparecen en las ubicaciones?
2. ¿Cuál es el factor de replicación?
3. ¿Qué componente conoce la ubicación de los bloques?
4. ¿Qué componentes almacenan físicamente las réplicas?

---

## Ejercicio 3 — Ver los nodos de YARN

### Objetivo

Distinguir almacenamiento distribuido de gestión de recursos.

```bash
yarn node -list
```

Completa:

| Sistema | Componente maestro | Componente worker | Responsabilidad |
|---|---|---|---|
| HDFS | ______ | ______ | Almacenamiento |
| YARN | ______ | ______ | Recursos y ejecución |

---

## Ejercicio 4 — Crear la zona HDFS individual

```bash
echo "$LAB_USER"
echo "$HDFS_HOME"

hdfs dfs -mkdir -p "$HDFS_HOME/input"
hdfs dfs -chmod 700 "$HDFS_HOME"

hdfs dfs -ls "$HDFS_HOME"
```

### Reflexión

¿Por qué es útil separar las zonas de trabajo aunque el clúster sea compartido?

---

## Ejercicio 5 — Filesystem local frente a HDFS

Crea un fichero local:

```bash
cat > "/tmp/words_${LAB_USER}.txt" <<'EOF'
spark hadoop databricks
hadoop hdfs spark
databricks delta spark
hadoop yarn mapreduce
spark delta lakehouse
EOF
```

Comprueba:

```bash
cat "/tmp/words_${LAB_USER}.txt"
hdfs dfs -ls "$HDFS_HOME/input"
```

Cópialo a HDFS:

```bash
hdfs dfs -put \
  "/tmp/words_${LAB_USER}.txt" \
  "$HDFS_HOME/input/"
```

Comprueba:

```bash
hdfs dfs -ls -h "$HDFS_HOME/input"

hdfs dfs -cat \
  "$HDFS_HOME/input/words_${LAB_USER}.txt"
```

### Preguntas

1. ¿El fichero local y el fichero de HDFS son el mismo objeto?
2. ¿Qué comando ha transferido el contenido a HDFS?
3. ¿Qué función desempeña el NameNode?

---

## Ejercicio 6 — Bloques y replicación

```bash
hdfs fsck \
  "$HDFS_HOME/input/words_${LAB_USER}.txt" \
  -files \
  -blocks \
  -locations
```

Busca número de bloques, factor de replicación y ubicaciones.

### Preguntas

1. ¿Cuántos bloques utiliza el fichero?
2. ¿En qué DataNodes aparecen las réplicas?
3. ¿Por qué un fichero tan pequeño utiliza un solo bloque?
4. ¿Qué ventaja aporta la replicación?

---

## Ejercicio 7 — Observar el NameNode sin exponer su interfaz web

El puerto web del NameNode no está publicado hacia Internet. Desde `hadoop-master` consulta su endpoint JMX interno:

```bash
curl -s \
  "http://hadoop-master:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" \
  | python3 -m json.tool
```

Localiza, si aparecen en la respuesta:

```text
NumLiveDataNodes
NumDeadDataNodes
CapacityTotal
CapacityRemaining
```

### Reflexión

Relaciona esta información con `hdfs fsck ... -locations`. ¿Son dos sistemas diferentes o dos formas de consultar el mismo HDFS?

---

## Ejercicio 8 — Ejecutar Hadoop MapReduce WordCount

Asegúrate de que no existe la salida:

```bash
hdfs dfs -rm -r -f \
  "$HDFS_HOME/output_wordcount"
```

Ejecuta:

```bash
hadoop jar \
  "$HADOOP_HOME"/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  wordcount \
  "$HDFS_HOME/input" \
  "$HDFS_HOME/output_wordcount"
```

Busca referencias a `application`, `map`, `reduce`, `progress` y YARN.

---

## Ejercicio 9 — Consultar el resultado

```bash
hdfs dfs -ls \
  "$HDFS_HOME/output_wordcount"

hdfs dfs -cat \
  "$HDFS_HOME/output_wordcount/part-r-*"
```

### Preguntas

1. ¿Dónde estaban los datos de entrada?
2. ¿Dónde queda el resultado?
3. ¿Qué función tiene la fase Map?
4. ¿Qué ocurre durante Shuffle?
5. ¿Qué función tiene Reduce?
6. ¿Por qué el resultado permanece cuando la aplicación ha terminado?

---

## Ejercicio 10 — Observar MapReduce en YARN

```bash
yarn application -list -appStates ALL
```

Consulta también la API REST interna del ResourceManager:

```bash
curl -s \
  "http://hadoop-master:8088/ws/v1/cluster/nodes" \
  | python3 -m json.tool
```

Para consultar aplicaciones finalizadas:

```bash
curl -s \
  "http://hadoop-master:8088/ws/v1/cluster/apps?states=FINISHED" \
  | python3 -m json.tool \
  | head -n 120
```

### Preguntas

1. ¿Qué diferencia existe entre HDFS y YARN?
2. ¿Qué componente ofrece recursos en cada worker?
3. ¿Por qué MapReduce aparece como una aplicación YARN?
4. ¿Qué diferencia hay entre el ciclo de vida del compute y el de los datos?

---

## Ejercicio 11 — HDFS y tolerancia a fallos

No se detendrá ningún nodo.

```bash
hdfs fsck \
  "$HDFS_HOME/input/words_${LAB_USER}.txt" \
  -files \
  -blocks \
  -locations
```

Responde:

1. Si una réplica dejara de estar disponible, ¿podría seguir leyéndose el bloque mientras existan otras réplicas?
2. ¿Quién conoce las ubicaciones de las réplicas?
3. ¿Qué componente almacena físicamente cada réplica?
4. ¿Qué riesgo existiría con un factor de replicación 1?

---

# 5. Parte B — Del Hadoop visible al Spark abstraído

Cuando se indique:

```bash
exit
```

Abre el Azure Databricks workspace proporcionado.

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
2. ¿Se ven directamente DataNodes?
3. ¿Se ven directamente NodeManagers?
4. ¿Significa eso que ya no existe procesamiento distribuido?

---

## Ejercicio 13 — Repartition y shuffle

```python
numbers_8 = numbers.repartition(8)

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
distribution_8.explain("formatted")
```

Busca:

```text
Exchange
```

### Reflexión

```text
Hadoop:
Map → Shuffle → Reduce

Spark:
transformations → shuffle → aggregation
```

---

## Ejercicio 14 — WordCount con Spark

El entorno Databricks dispone de:

```text
/Volumes/training/shared/source/words.txt
```

```python
from pyspark.sql import functions as F

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

Existe:

```text
/training/shared/access_log.txt
```

Cada línea contiene:

```text
status path
```

## Tareas

```bash
hdfs dfs -mkdir -p "$HDFS_HOME/input"

hdfs dfs -cp \
  /training/shared/access_log.txt \
  "$HDFS_HOME/input/access_log.txt"

hdfs dfs -ls "$HDFS_HOME/input"

hdfs dfs -rm -r -f \
  "$HDFS_HOME/output_logs"

hadoop jar \
  "$HADOOP_HOME"/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  wordcount \
  "$HDFS_HOME/input/access_log.txt" \
  "$HDFS_HOME/output_logs"

hdfs dfs -cat \
  "$HDFS_HOME/output_logs/part-r-*"

yarn application -list -appStates ALL

hdfs fsck \
  "$HDFS_HOME/input/access_log.txt" \
  -files \
  -blocks \
  -locations
```

## Entregable

Anota:

- número de bloques;
- factor de replicación;
- DataNodes donde reside;
- ID de la aplicación YARN;
- conteos de `200`, `404` y `500`.

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

## Pregunta final

Explica qué elementos conceptuales se mantienen entre Hadoop MapReduce y Spark y cuáles han cambiado.

---

# 8. Reto final — Arquitectura

| Problema | Hadoop tradicional | Entorno moderno del curso |
|---|---|---|
| Almacenamiento persistente | ______ | ______ |
| Distribución física de datos | ______ | Gestionada/abstraída |
| Gestión de recursos | ______ | Compute gestionado |
| Motor batch clásico | ______ | Spark |
| Movimiento por clave | Shuffle | ______ |
| Interfaz | CLI + APIs internas | Notebooks + SQL + GUI + CLI |

---

# 9. Resultado esperado

> HDFS distribuye y replica datos entre DataNodes y utiliza un NameNode para gestionar metadata. YARN administra recursos mediante ResourceManager y NodeManagers. MapReduce procesa datos mediante Map, Shuffle y Reduce. Spark mantiene muchos de los problemas fundamentales del procesamiento distribuido, como particiones y shuffle, pero ofrece un modelo de ejecución más flexible y plataformas como Databricks abstraen gran parte de la infraestructura.

---

# 10. Referencias oficiales

- [Azure Bastion](https://learn.microsoft.com/azure/bastion/)
- [Microsoft Entra authentication for Azure Linux VMs](https://learn.microsoft.com/entra/identity/devices/howto-vm-sign-in-azure-ad-linux)
- [Apache Hadoop 3.5.0](https://hadoop.apache.org/docs/r3.5.0/)
- [HDFS Architecture](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [HDFS Commands Guide](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html)
- [YARN Commands](https://hadoop.apache.org/docs/r3.5.0/hadoop-yarn/hadoop-yarn-site/YarnCommands.html)
- [ResourceManager REST APIs](https://hadoop.apache.org/docs/r3.5.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html)
- [MapReduce Tutorial](https://hadoop.apache.org/docs/r3.5.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
- [Apache Spark SQL/DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Azure Databricks compute](https://learn.microsoft.com/azure/databricks/compute/)
