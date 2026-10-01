# M03 — Soluciones de laboratorios
## Databricks: workspace, compute, Unity Catalog, SQL, Jobs, AI/BI y MLflow

Estas soluciones resumen el comportamiento esperado de la plataforma y ofrecen una implementación de referencia para las prácticas autónomas. Sustituye siempre `<student_id>` por tu identificador real.

---

# Ejercicio 0 — Tour del workspace

| Elemento | ¿Código/objeto? | ¿Compute? | ¿Gobierno? | ¿Orquestación? |
|---|---:|---:|---:|---:|
| Notebook | Sí | No | No | No |
| SQL Warehouse | No | Sí | No | No |
| Unity Catalog | No | No | Sí | No |
| Lakeflow Job | Sí, objeto de plataforma | No por sí mismo | No | Sí |
| Dashboard | Sí, objeto de consumo | No | No | No |

La idea principal es que Databricks integra varias capas, pero no deben confundirse entre sí.

---

# Ejercicio 1 — Notebook no es compute

1. El notebook no desaparece al desconectarlo del compute.
2. El código sigue guardado en el Workspace.
3. `spark.range(5)` era ejecutado por el compute al que estaba conectado el notebook.
4. El Workspace organiza objetos de trabajo y colaboración; el Compute aporta CPU, memoria y procesos para ejecutar código.

```text
Notebook = código/interfaz
Compute  = capacidad de ejecución
```

---

# Ejercicio 2 — Tu identidad dentro de Databricks

`current_user()` muestra la identidad autenticada.

`current_catalog()` y `current_schema()` muestran el contexto SQL actual.

Después de:

```sql
USE CATALOG training;
USE SCHEMA <student_id>;
```

el contexto esperado es:

```text
catalog = training
schema  = <student_id>
```

Un mismo workspace puede contener usuarios con privilegios distintos porque la autorización sobre los datos se aplica en Unity Catalog, no simplemente por estar dentro del mismo workspace.

---

# Ejercicio 3 — Catalog Explorer y namespace de tres niveles

```text
training.shared.sales
```

significa:

```text
training  = catalog
shared    = schema
sales     = object/table
```

El nombre completo permite identificar un objeto sin depender del catálogo o schema activo de la sesión.

---

# Ejercicio 4 — Crear tu primer objeto gobernado

Una variable o DataFrame temporal existe dentro de la sesión de ejecución y no constituye por sí solo un objeto persistente de gobierno.

```text
DataFrame temporal
- pertenece a la sesión
- no tiene nombre estable en Unity Catalog
- desaparece cuando deja de existir la sesión si no se escribe

Tabla Unity Catalog
- tiene nombre estable catalog.schema.table
- persiste
- tiene owner y permisos
- puede ser localizada por otros consumidores autorizados
- participa en gobierno y lineage
```

---

# Ejercicio 5 — Ver permisos

1. No es esperable tener los mismos permisos sobre `training.<student_id>` y `training.shared`.
2. En el schema individual se necesita capacidad para crear y modificar objetos; en `shared` normalmente basta con lectura.
3. Asociar permisos a objetos gobernados permite aplicar control centralizado y consistente independientemente de si se accede mediante notebook, SQL u otra interfaz autorizada.

---

# Ejercicio 6 — Comprobar que el aislamiento funciona

La consulta:

```sql
SELECT *
FROM training.shared.sales
LIMIT 10;
```

debe funcionar si existe permiso de lectura.

La creación en:

```text
training.shared
```

debe fallar si el aislamiento está correctamente configurado.

Ese error demuestra que la identidad puede consumir datos compartidos sin adquirir privilegios de modificación sobre ellos.

---

# Ejercicio 7 — Tabla, vista y Volume

| Objeto | Contiene/representa | Uso típico |
|---|---|---|
| Table | Datos tabulares persistentes gobernados | Analítica, transformación, consumo |
| View | Consulta lógica con nombre | Abstracción, reutilización, seguridad lógica |
| Volume | Ficheros no tabulares o rutas de ficheros gobernadas | CSV, JSON, modelos, artefactos y ficheros de trabajo |

Una vista no duplica necesariamente los datos de la tabla origen: normalmente guarda la definición lógica de la consulta.

---

# Ejercicio 8 — Workspace Browser

1. No. Una carpeta de Workspace no pertenece a Unity Catalog.
2. No. Una carpeta de Workspace no es un schema.
3. El Workspace organiza notebooks, código y otros artefactos de trabajo.

Comparación:

```text
Workspace folder → organización de artefactos
Unity Catalog schema → namespace y gobierno de datos/objetos
```

---

# Ejercicio 9 — SQL Warehouse

1. La consulta la ejecuta el SQL Warehouse seleccionado.
2. No tiene por qué ser el mismo compute que ejecuta los notebooks Spark.
3. El SQL Warehouse está orientado a cargas SQL/BI y permite separar sus recursos, escalado y ciclo de vida de otros tipos de compute.

```text
Notebook Spark → compute asociado al notebook
Databricks SQL → SQL Warehouse
```

---

# Ejercicio 10 — Visualización desde SQL

Crear un gráfico no modifica los datos de `training.shared.sales`.

La visualización cambia la **representación** del resultado, no el contenido de la tabla.

---

# Ejercicio 11 — Crear un AI/BI Dashboard

```text
Notebook
→ entorno interactivo para código, exploración y desarrollo

SQL Query
→ consulta declarativa para obtener un resultado tabular

Dashboard
→ producto de consumo que presenta indicadores y visualizaciones
```

Los tres pueden trabajar sobre los mismos datos gobernados, pero cumplen funciones distintas.

---

# Ejercicio 12 — Crear un Lakeflow Job

El Job convierte una ejecución manual en una unidad repetible.

La relación correcta es:

```text
Job
└── Task
    └── Notebook
        └── Compute
```

El notebook contiene lógica. El Job define cómo y cuándo ejecutar esa lógica.

---

# Ejercicio 13 — Observar una ejecución de Lakeflow Jobs

1. No. El notebook es un artefacto de código; el Job es un objeto de orquestación.
2. El Job aporta ejecución repetible, estado, historial, logs, parámetros, dependencias y posibilidad de planificación.
3. Para convertirlo en periódico se necesita un trigger/schedule y, si hay varias tareas, pueden definirse dependencias y políticas de ejecución.

---

# Ejercicio 14 — Job con parámetro

Parametrizar evita duplicar código.

```text
Mismo notebook
+ country=PT
+ country=ES
= dos ejecuciones de la misma lógica
```

Ventajas:

- una única implementación;
- menos duplicación;
- mantenimiento centralizado;
- comportamiento reproducible;
- posibilidad de reutilizar el mismo Job con distintos valores.

---

# Ejercicio 15 — Primera experiencia con MLflow

Imprimir:

```text
0.85
```

solo muestra un valor en una salida.

MLflow registra el valor dentro de un **run** y lo relaciona con metadata como:

- Run ID;
- parámetros;
- métricas;
- momento de ejecución;
- experimento;
- otros artefactos que puedan añadirse.

Eso permite comparar ejecuciones y conservar trazabilidad de experimentos.

---

# Ejercicio 16 — GUI y SQL representan el mismo gobierno

No son dos sistemas de permisos.

Catalog Explorer y:

```sql
SHOW GRANTS
```

son dos interfaces sobre los mismos objetos y privilegios de Unity Catalog.

---

# Ejercicio 17 — Línea de comandos con Databricks CLI

1. No. La CLI se autentica contra el workspace indicado mediante `--host`.
2. Los permisos no aumentan por utilizar CLI. Se aplican los privilegios de la identidad autenticada.
3. GUI, SQL, CLI y API permiten distintos patrones de uso sobre los mismos recursos: interacción humana, automatización, scripts, integración y operaciones reproducibles.

Ejemplo de comprobación:

```bash
databricks current-user me
databricks workspace list /Users/<TU_USUARIO>
```

La identidad y los permisos deben ser coherentes con los observados desde la plataforma.

---

# Práctica autónoma A — Explora y documenta tu plataforma

Una respuesta válida puede estructurarse así:

| Elemento | Qué es | Para qué sirve | Dónde localizarlo |
|---|---|---|---|
| Usuario | Identidad autenticada | Autorización y auditoría | `current_user()` / perfil |
| Compute | Recurso de ejecución | Ejecutar Spark/Python | Compute |
| `training.<student_id>` | Schema individual | Crear objetos propios | Catalog Explorer |
| `training.shared.sales` | Tabla compartida | Fuente común de datos | Catalog Explorer |
| `m03_demo` | Tabla gobernada | Persistir datos del ejercicio | Tu schema |
| `m03_high_value` | View | Reutilizar una consulta | Tu schema |
| SQL Warehouse | Compute SQL | Ejecutar consultas BI/SQL | SQL Warehouses |
| Job | Orquestación | Ejecutar lógica repetible | Jobs & Pipelines |
| Dashboard | Producto de consumo | Presentar métricas | Dashboards |
| MLflow run | Registro de experimento | Trazar parámetros/métricas | Experiments |

Los nombres concretos del compute, warehouse, Job, dashboard y Run ID deben tomarse del entorno real.

---

# Práctica autónoma B — Pipeline analítico sencillo

## 1. Notebook: calcular ventas por producto

```python
from pyspark.sql import functions as F

product_sales = (
    spark.table("training.shared.sales")
    .withColumn(
        "amount",
        F.col("quantity") * F.col("unit_price")
    )
    .groupBy("product_id")
    .agg(
        F.sum("amount").alias("revenue"),
        F.sum("quantity").alias("units"),
        F.count("*").alias("transactions")
    )
)

(
    product_sales
    .write
    .mode("overwrite")
    .saveAsTable("training.<student_id>.m03_product_sales")
)
```

## 2. Comprobación

```sql
SELECT *
FROM training.<student_id>.m03_product_sales
ORDER BY revenue DESC
LIMIT 10;
```

## 3. Automatización

Crea una Notebook Task que ejecute el notebook anterior dentro de un Lakeflow Job.

La ejecución debe poder repetirse sin crear tablas duplicadas porque se utiliza:

```text
mode("overwrite")
```

para reconstruir el producto del ejercicio.

## 4. Consulta SQL

```sql
SELECT
 product_id,
 revenue,
 units,
 transactions
FROM training.<student_id>.m03_product_sales
ORDER BY revenue DESC
LIMIT 10;
```

## 5. Dashboard

Utiliza el resultado anterior para una visualización Top 10 Products.

Flujo final:

```text
training.shared.sales
        ↓
Notebook
        ↓
training.<student_id>.m03_product_sales
        ↓
Lakeflow Job
        ↓
Databricks SQL
        ↓
Dashboard
```

---

# Práctica autónoma C — Comprueba el gobierno

## Lectura compartida

```sql
SELECT *
FROM training.shared.sales
LIMIT 5;
```

Debe funcionar.

## Creación individual

```sql
CREATE OR REPLACE TABLE training.<student_id>.m03_governance_test
AS SELECT 1 AS id;
```

Debe funcionar.

## Intento de creación compartida

```sql
CREATE TABLE training.shared.m03_governance_test
AS SELECT 1 AS id;
```

Debe ser rechazado si los privilegios están configurados como se espera.

## Grants

```sql
SHOW GRANTS ON SCHEMA training.<student_id>;
SHOW GRANTS ON SCHEMA training.shared;
```

## Explicación

Un único workspace puede ser multiusuario porque compartir interfaz y recursos no implica compartir todos los privilegios. Unity Catalog aplica autorización sobre catálogos, schemas y objetos, mientras cada identidad mantiene sus propios permisos.

---

# Reto final M03

Una representación completa:

```text
User / Identity
      ↓
Workspace
 ├── Notebook ─────────────┐
 ├── Job ──────────────────┤
 ├── SQL Query ────────────┤
 └── Dashboard             │
                           ↓
                  Execution resources
                  ├── Spark Compute
                  └── SQL Warehouse
                           ↓
                    Unity Catalog
                  ├── permissions
                  ├── namespace
                  ├── metadata
                  └── lineage
                           ↓
                          Data

MLflow
└── registra experiments/runs, parámetros, métricas y artefactos
```

Interpretación:

- **Workspace** organiza el trabajo.
- **Compute** ejecuta código.
- **SQL Warehouse** ejecuta cargas SQL/BI.
- **Unity Catalog** gobierna los objetos y el acceso.
- **Lakeflow Jobs** orquesta ejecuciones.
- **Dashboards** presentan resultados.
- **MLflow** registra experimentos y ejecuciones de ML.

La plataforma integra todas estas capacidades, pero cada una resuelve una responsabilidad diferente.
