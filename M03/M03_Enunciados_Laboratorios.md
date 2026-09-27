# M03 — Laboratorios
## Databricks: workspace, compute, Unity Catalog, SQL, Jobs, AI/BI y MLflow

**Curso:** Databricks — Big Data, Spark, Delta Lake y arquitectura Medallion
**Documento:** Enunciados para participantes
**Modalidad:** Individual
**Entorno:** Azure Databricks compartido
**Nivel:** Introductorio–intermedio

---

# 1. Objetivo

En M01 trabajaste con la infraestructura de Hadoop y en M02 con Apache Spark como motor de procesamiento.

En este módulo el objetivo es comprender **Databricks como plataforma**.

Trabajarás con:

- Workspace;
- notebooks;
- compute;
- Catalog Explorer;
- Unity Catalog;
- permisos;
- SQL Warehouse;
- Databricks SQL;
- Lakeflow Jobs;
- AI/BI dashboards;
- MLflow;
- una introducción a Databricks CLI.

La idea central es distinguir:

```text
Workspace
Compute
Governance
Storage
Orchestration
SQL / BI / ML
```

No son lo mismo, aunque formen parte de la misma plataforma.

---

# 2. Entorno compartido

Todos los participantes utilizan:

```text
1 Azure Databricks Workspace
1 compute compartido
1 SQL Warehouse compartido
1 catálogo training
```

Cada participante dispone de un schema individual:

```text
training.<student_id>
```

y puede leer datos comunes desde:

```text
training.shared
```

Utiliza siempre tu identificador real en lugar de:

```text
<student_id>
```

---

# 3. Regla principal

No necesitas permisos administrativos.

No debes:

- crear workspaces;
- cambiar configuración global;
- crear compute;
- modificar permisos de otros participantes;
- borrar objetos compartidos;
- modificar tablas de otros participantes.

Aprenderás Databricks trabajando con permisos de usuario realistas.

---

# 4. Ejercicio 0 — Tour del workspace

## Objetivo

Identificar las áreas principales de la plataforma.

En el menú lateral localiza, si están disponibles:

```text
Workspace
Catalog
Compute
Jobs & Pipelines
SQL
Dashboards / AI/BI
Experiments
```

No hagas cambios todavía.

### Pregunta

Clasifica cada elemento:

| Elemento | ¿Código/objeto? | ¿Compute? | ¿Gobierno? | ¿Orquestación? |
|---|---:|---:|---:|---:|
| Notebook | | | | |
| SQL Warehouse | | | | |
| Unity Catalog | | | | |
| Lakeflow Job | | | | |
| Dashboard | | | | |

---

# 5. Ejercicio 1 — Notebook no es compute

## Objetivo

Separar la interfaz de trabajo del recurso que ejecuta el código.

Abre tu notebook M03.

En la parte superior comprueba a qué compute está conectado.

Ejecuta:

```python
spark.version
```

Después:

```python
spark.range(5).show()
```

Desconecta el notebook del compute mediante la interfaz y observa qué cambia.

Vuelve a conectarlo cuando el instructor lo indique.

### Preguntas

1. ¿El notebook desaparece al desconectarlo?
2. ¿El código sigue guardado?
3. ¿Qué componente ejecutaba `spark.range(5)`?
4. ¿Qué diferencia hay entre Workspace y Compute?

---

# 6. Ejercicio 2 — Tu identidad dentro de Databricks

Ejecuta:

```sql
SELECT current_user();
```

Consulta:

```sql
SELECT current_catalog();
```

Después:

```sql
SELECT current_schema();
```

Cambia al catálogo de formación:

```sql
USE CATALOG training;
```

Cambia a tu schema:

```sql
USE SCHEMA <student_id>;
```

Comprueba:

```sql
SELECT
 current_user(),
 current_catalog(),
 current_schema();
```

### Reflexión

¿Por qué es útil que una misma persona pueda trabajar en el mismo workspace pero con permisos diferentes sobre los datos?

---

# 7. Ejercicio 3 — Catalog Explorer y namespace de tres niveles

## Objetivo

Relacionar la interfaz gráfica con:

```text
catalog.schema.object
```

Abre:

```text
Catalog
```

Localiza:

```text
training
```

Dentro busca:

```text
shared
<student_id>
```

Ahora vuelve al notebook:

```sql
SHOW CATALOGS;
```

Después:

```sql
SHOW SCHEMAS IN training;
```

Y:

```sql
SHOW TABLES IN training.shared;
```

### Pregunta

Explica:

```text
training.shared.sales
```

¿Qué representa cada parte?

---

# 8. Ejercicio 4 — Crear tu primer objeto gobernado

## Objetivo

Crear una tabla en tu propio schema sin entrar todavía en los detalles internos de Delta Lake.

Ejecuta:

```sql
CREATE OR REPLACE TABLE training.<student_id>.m03_demo
AS
SELECT
 id,
 CONCAT('item-', id) AS item_name,
 id * 10 AS value
FROM range(10);
```

Consulta:

```sql
SELECT *
FROM training.<student_id>.m03_demo;
```

Busca la tabla mediante Catalog Explorer.

### Observa

- catálogo;
- schema;
- nombre;
- owner;
- columnas;
- permisos disponibles.

### Pregunta

¿Qué diferencia hay entre crear una variable/DataFrame temporal y crear una tabla gobernada por Unity Catalog?

---

# 9. Ejercicio 5 — Ver permisos

Ejecuta:

```sql
SHOW GRANTS
ON SCHEMA training.<student_id>;
```

Después:

```sql
SHOW GRANTS
ON SCHEMA training.shared;
```

### Preguntas

1. ¿Tienes los mismos permisos en ambos schemas?
2. ¿Por qué no?
3. ¿Qué ventaja aporta que los permisos estén asociados a objetos gobernados?

---

# 10. Ejercicio 6 — Comprobar que el aislamiento funciona

## Objetivo

Ver una denegación de acceso controlada.

Lee una tabla compartida:

```sql
SELECT *
FROM training.shared.sales
LIMIT 10;
```

Ahora intenta crear una tabla en el schema compartido:

```sql
CREATE TABLE training.shared.no_deberia_funcionar
AS SELECT 1 AS id;
```

### Resultado esperado

La lectura debería funcionar.

La creación debería fallar por permisos.

> Si la creación funciona, avisa al instructor: la configuración del laboratorio concede más permisos de los previstos.

### Reflexión

¿Por qué un error de permisos puede ser una señal de que el gobierno está funcionando correctamente?

---

# 11. Ejercicio 7 — Tabla, vista y Volume

## Objetivo

Diferenciar varios tipos de objetos de Unity Catalog.

Crea una vista:

```sql
CREATE OR REPLACE VIEW training.<student_id>.m03_high_value
AS
SELECT *
FROM training.shared.sales
WHERE quantity * unit_price >= 200;
```

Consulta:

```sql
SELECT *
FROM training.<student_id>.m03_high_value
LIMIT 20;
```

En Catalog Explorer localiza:

- una Table;
- tu View;
- el Volume individual asignado.

Completa:

| Objeto | Contiene/representa | Uso típico |
|---|---|---|
| Table | | |
| View | | |
| Volume | | |

---

# 12. Ejercicio 8 — Workspace Browser

## Objetivo

Organizar objetos de trabajo.

En:

```text
Workspace
```

entra en tu Home.

Crea una carpeta:

```text
M03_Lab
```

Dentro crea un notebook:

```text
01_platform_exploration
```

Añade una celda Markdown:

```markdown
# M03 Platform Exploration

Usuario:
Objetivo:
```

Guarda y vuelve al navegador.

### Preguntas

1. ¿La carpeta del Workspace pertenece a Unity Catalog?
2. ¿Una carpeta del Workspace es un schema?
3. ¿Qué tipo de objetos organizarías aquí?

---

# 13. Ejercicio 9 — SQL Warehouse

## Objetivo

Comprobar que las consultas SQL no necesitan utilizar el mismo compute que nuestros notebooks Spark.

Abre:

```text
SQL
```

Selecciona el SQL Warehouse proporcionado por el instructor.

Ejecuta:

```sql
SELECT
 country,
 COUNT(*) AS transactions,
 SUM(quantity * unit_price) AS revenue
FROM training.shared.sales
GROUP BY country
ORDER BY revenue DESC;
```

### Preguntas

1. ¿Qué recurso ha ejecutado esta consulta?
2. ¿Es necesariamente el mismo compute que utilizaste en el notebook?
3. ¿Por qué Databricks dispone de compute especializado para SQL?

---

# 14. Ejercicio 10 — Visualización desde SQL

Sobre el resultado anterior crea una visualización.

Representa:

```text
country
vs.
revenue
```

Utiliza un gráfico apropiado.

Ponle un nombre:

```text
Revenue by Country - <student_id>
```

### Reflexión

¿Hemos cambiado los datos o únicamente su forma de presentación?

---

# 15. Ejercicio 11 — Crear un AI/BI Dashboard

## Objetivo

Convertir una consulta en un pequeño producto de consumo.

Crea un dashboard llamado:

```text
M03 Dashboard - <student_id>
```

Incluye como mínimo:

1. Revenue by Country.
2. Total Revenue.
3. Number of Transactions.

Consulta para los indicadores:

```sql
SELECT
 SUM(quantity * unit_price) AS total_revenue,
 COUNT(*) AS transactions
FROM training.shared.sales;
```

Publica o previsualiza el dashboard según indique el instructor.

### Pregunta

¿Qué diferencia conceptual existe entre:

```text
Notebook
SQL Query
Dashboard
```

?

---

# 16. Ejercicio 12 — Crear un Lakeflow Job

## Objetivo

Convertir un notebook interactivo en una ejecución repetible.

En tu carpeta `M03_Lab` crea un notebook:

```text
02_job_task
```

Contenido:

```python
from pyspark.sql import functions as F

result = (
 spark.table("training.shared.sales")
 .groupBy("country")
 .agg(
 F.sum(
 F.col("quantity") * F.col("unit_price")
 ).alias("revenue")
 )
)

display(result)
```

Ahora abre:

```text
Jobs & Pipelines
```

Crea un Job:

```text
M03 Job - <student_id>
```

Añade una Notebook Task que ejecute `02_job_task`.

Selecciona el compute indicado por el instructor.

Ejecuta:

```text
Run now
```

---

# 17. Ejercicio 13 — Observar una ejecución de Lakeflow Jobs

Abre la ejecución anterior.

Localiza:

- estado;
- duración;
- task;
- start time;
- logs/output.

Ejecuta de nuevo el job.

### Preguntas

1. ¿El notebook y el job son el mismo objeto?
2. ¿Qué aporta el job respecto a ejecutar una celda manualmente?
3. ¿Qué capacidades necesitaríamos para convertirlo en un proceso periódico?

---

# 18. Ejercicio 14 — Job con parámetro

Modifica el notebook `02_job_task` para utilizar un parámetro.

Añade:

```python
dbutils.widgets.text("country", "ES")

country = dbutils.widgets.get("country")
```

Modifica el pipeline:

```python
result = (
 spark.table("training.shared.sales")
 .filter(F.col("country") == country)
 .agg(
 F.sum(
 F.col("quantity") * F.col("unit_price")
 ).alias("revenue")
 )
)

display(result)
```

En el Job configura:

```text
country = PT
```

Ejecuta.

Después cambia:

```text
country = ES
```

y vuelve a ejecutar.

### Reflexión

¿Por qué parametrizar un job es mejor que duplicar el notebook para cada país?

---

# 19. Ejercicio 15 — Primera experiencia con MLflow

## Objetivo

Entender el concepto de experiment tracking sin construir todavía un proyecto de ML complejo.

Ejecuta en tu notebook:

```python
import mlflow

with mlflow.start_run():
 mlflow.log_param("student", "<student_id>")
 mlflow.log_param("module", "M03")
 mlflow.log_metric("sample_metric", 0.85)
```

Abre:

```text
Experiments
```

o el experimento asociado al notebook.

Busca el run.

### Identifica

- Run ID;
- parámetros;
- métricas;
- fecha/hora.

### Pregunta

¿Qué información aporta MLflow que no obtendríamos simplemente imprimiendo `0.85` en una celda?

---

# 20. Ejercicio 16 — GUI y SQL representan el mismo gobierno

Vuelve a:

```text
Catalog Explorer
```

Abre tu tabla:

```text
training.<student_id>.m03_demo
```

Observa sus permisos.

Después ejecuta:

```sql
SHOW GRANTS
ON TABLE training.<student_id>.m03_demo;
```

### Reflexión

¿La interfaz gráfica y el SQL están gestionando dos sistemas de permisos diferentes?

---

# 21. Ejercicio 17 — Línea de comandos con Databricks CLI

## Objetivo

Comprobar que Databricks también expone sus recursos mediante CLI.

Este ejercicio se realiza únicamente si el instructor ha dejado Databricks CLI instalado en el equipo.

Comprueba:

```bash
databricks version
```

Autentícate usando el workspace proporcionado:

```bash
databricks auth login --host <WORKSPACE_URL>
```

Sigue el flujo de autenticación en navegador.

Lista objetos de tu home:

```bash
databricks workspace list /Users/<TU_USUARIO>
```

Obtén información del usuario autenticado:

```bash
databricks current-user me
```

### Preguntas

1. ¿La CLI utiliza otro workspace distinto?
2. ¿Los permisos cambian por usar CLI?
3. ¿Por qué una plataforma ofrece GUI, SQL, CLI y API para los mismos recursos?

> Si el instructor indica que la CLI no forma parte del entorno de los equipos, este ejercicio se realizará como demostración y no será evaluable.

---

# 22. Práctica autónoma A — Explora y documenta tu plataforma

Sin seguir un guion de clics exacto, documenta:

1. tu usuario;
2. el compute que puedes utilizar;
3. tu schema;
4. una tabla compartida;
5. tu tabla `m03_demo`;
6. tu vista `m03_high_value`;
7. el SQL Warehouse;
8. tu Job;
9. tu Dashboard;
10. tu experimento/run de MLflow.

Para cada elemento indica:

```text
qué es
para qué sirve
dónde lo localizas
```

---

# 23. Práctica autónoma B — Pipeline analítico sencillo

## Objetivo

Combinar diferentes capacidades de Databricks.

Debes crear:

```text
Notebook
 ↓
tabla/vista individual
 ↓
Lakeflow Job
 ↓
SQL Query
 ↓
Dashboard
```

Requisitos:

1. Utiliza `training.shared.sales`.
2. Calcula ventas por producto.
3. Guarda el resultado como una vista o tabla en tu schema.
4. Automatiza el cálculo con un Job.
5. Consulta el resultado desde Databricks SQL.
6. Crea una visualización con Top 10 productos.
7. Añádela a un dashboard.

---

# 24. Práctica autónoma C — Comprueba el gobierno

Debes demostrar, sin modificar objetos ajenos, que:

1. puedes leer `training.shared.sales`;
2. puedes crear objetos en `training.<student_id>`;
3. no puedes crear objetos en `training.shared`;
4. puedes inspeccionar tus grants;
5. puedes localizar los mismos objetos mediante Catalog Explorer.

Explica:

> ¿Por qué un único workspace puede seguir siendo un entorno multiusuario seguro?

---

# 25. Reto final M03

Dibuja o explica este flujo:

```text
User
 ↓
Workspace
 ├── Notebook
 ├── Job
 ├── Query
 └── Dashboard
 │
 ▼
 Compute
 │
 ▼
 Unity Catalog
 │
 ▼
 Data
```

Añade:

```text
SQL Warehouse
MLflow
```

en el lugar que consideres correcto.

---

# 26. Referencias oficiales

- [Workspace browser](https://learn.microsoft.com/en-us/azure/databricks/workspace/workspace-browser)
- [Standard compute](https://learn.microsoft.com/en-us/azure/databricks/compute/standard-overview)
- [Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/)
- [Manage privileges in Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/)
- [Databricks SQL](https://learn.microsoft.com/en-us/azure/databricks/sql/)
- [Lakeflow Jobs quickstart](https://learn.microsoft.com/en-us/azure/databricks/jobs/jobs-quickstart)
- [Databricks AI/BI](https://learn.microsoft.com/en-us/azure/databricks/ai-bi/)
- [Databricks CLI](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/)
- [Databricks CLI authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/authentication)
- [MLflow on Databricks](https://learn.microsoft.com/en-us/azure/databricks/mlflow/)