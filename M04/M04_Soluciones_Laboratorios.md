# M04 — Soluciones de laboratorios
## Delta Lake: ACID, DML, MERGE, schema, history, time travel y optimización

Estas soluciones permiten comprobar el estado esperado de una tabla Delta después de cada operación. Sustituye `<student_id>` por tu identificador real. Los números de versión concretos pueden variar si una operación se repite; utiliza siempre `DESCRIBE HISTORY` para localizar la versión correcta.

---

# Ejercicio 0 — Crear tu tabla Delta

```sql
CREATE OR REPLACE TABLE training.<student_id>.orders_delta
USING DELTA
AS
SELECT *
FROM training.shared.m04_orders_source;
```

Comprobación:

```sql
SELECT COUNT(*) AS rows
FROM training.<student_id>.orders_delta;

SELECT *
FROM training.<student_id>.orders_delta
ORDER BY order_id;
```

La tabla resultante es una copia individual sobre la que pueden probarse operaciones destructivas sin modificar la fuente compartida.

---

# Ejercicio 1 — Comprobar que es una tabla Delta

1. `DESCRIBE DETAIL` debe mostrar `delta` como formato.
2. Es una tabla **managed** porque se ha creado como objeto de Unity Catalog sin especificar una `LOCATION` externa.
3. La tabla es el objeto lógico gobernado: nombre, schema, permisos, metadata y transaction log. Los ficheros físicos son la representación almacenada de los datos.

```text
Tabla Delta
= metadata + transaction log + ficheros de datos
```

No debe confundirse el objeto lógico con un único fichero Parquet.

---

# Ejercicio 2 — Primer vistazo al historial

Una versión Delta representa un **snapshot lógico consistente** de la tabla después de un commit.

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;
```

Cada operación confirmada crea una nueva entrada de historial con información como versión, timestamp y tipo de operación.

---

# Ejercicio 3 — INSERT crea una nueva versión

1. Sí, el `INSERT` confirmado crea una nueva versión.
2. El historial debe registrar una operación de escritura/insert.
3. La versión anterior no desaparece del historial.

Delta mantiene una secuencia de commits. La versión actual cambia, pero las anteriores pueden seguir siendo consultables mientras existan los ficheros necesarios y no hayan sido eliminados por políticas de retención.

---

# Ejercicio 4 — UPDATE

Después de:

```sql
UPDATE training.<student_id>.orders_delta
SET status = 'SHIPPED'
WHERE order_id = 101;
```

la fila 101 debe mostrar el nuevo estado y aparecer una nueva versión en history.

En Parquet o CSV sueltos no existe una semántica de `UPDATE` de tabla transaccional. Habría que localizar datos afectados, reescribir ficheros y coordinar el cambio de forma segura. Delta aporta esa capa transaccional.

---

# Ejercicio 5 — DELETE

Que una fila no aparezca en la versión actual **no implica** que sus bytes hayan desaparecido inmediatamente del almacenamiento.

Delta puede dejar ficheros antiguos disponibles para:

- time travel;
- recuperación;
- consistencia entre versiones.

La eliminación física de ficheros no referenciados corresponde posteriormente a `VACUUM`, respetando la retención configurada.

---

# Ejercicio 6 — Crear una tabla de cambios

La tabla `orders_updates` representa un lote incremental con tres casos:

```text
order_id 1   → registro existente que debe actualizarse
order_id 2   → registro existente que debe actualizarse
order_id 200 → registro nuevo que debe insertarse
```

La comprobación debe hacerse con:

```sql
SELECT *
FROM training.<student_id>.orders_updates
ORDER BY order_id;
```

---

# Ejercicio 7 — MERGE: UPDATE + INSERT

Después del `MERGE`:

1. `order_id = 1` y `order_id = 2` deben quedar actualizados con los valores de la fuente.
2. `order_id = 200` debe quedar insertado.
3. `MERGE` es útil porque expresa en una única transacción lógica cómo sincronizar registros existentes y nuevos.

Comprobación:

```sql
SELECT *
FROM training.<student_id>.orders_delta
WHERE order_id IN (1, 2, 200)
ORDER BY order_id;

DESCRIBE HISTORY training.<student_id>.orders_delta;
```

Es un patrón habitual de **upsert**.

---

# Ejercicio 8 — Schema enforcement

1. `columna_inexistente` provoca el error.
2. La tabla no debe cambiar porque la escritura incompatible no se confirma.
3. El fallo evita que errores de productores o cambios accidentales modifiquen silenciosamente el contrato de datos.

Schema enforcement protege la estructura esperada.

---

# Ejercicio 9 — Schema evolution explícita

```text
Schema enforcement
→ rechaza escrituras incompatibles con el schema actual.

Schema evolution
→ modifica deliberadamente el schema para aceptar un cambio conocido.
```

Después de:

```sql
ALTER TABLE training.<student_id>.orders_delta
ADD COLUMN source_system STRING;
```

la columna forma parte del contrato de la tabla. Los registros anteriores pueden completarse con `legacy` y los nuevos pueden informar su origen.

---

# Ejercicio 10 — Evolución automática con `mergeSchema`

La primera escritura debe fallar porque `campaign` no existe en el destino.

Con:

```python
.option("mergeSchema", "true")
```

Delta puede incorporar la nueva columna durante la escritura.

No conviene activarlo indiscriminadamente porque un error de productor, un nombre de columna mal escrito o un cambio no revisado podría convertirse en un cambio permanente de schema.

```text
Flexibilidad sin control
≠
gobierno de schema
```

---

# Ejercicio 11 — History como herramienta de auditoría

Durante una investigación son especialmente útiles:

- timestamp;
- operación;
- versión;
- usuario/identidad cuando esté disponible;
- parámetros de operación;
- métricas de filas/ficheros afectadas cuando estén disponibles;
- versión anterior y posterior al cambio.

El historial ayuda a responder:

```text
qué cambió
cuándo
mediante qué operación
qué versión era correcta
```

---

# Ejercicio 12 — Time travel por versión

1. No. Consultar `VERSION AS OF` no modifica la versión actual.
2. Casos de uso: auditoría, comparación antes/después, investigación de errores, reproducción de resultados y recuperación.
3. No. Time travel no sustituye a una estrategia de backup de largo plazo. Depende de que se conserven los ficheros requeridos por las versiones antiguas.

Ejemplo:

```sql
SELECT *
FROM training.<student_id>.orders_delta
VERSION AS OF <version_existente>
ORDER BY order_id;
```

---

# Ejercicio 13 — Provocar un error y recuperarlo

Flujo correcto:

```text
versión correcta
   ↓
UPDATE erróneo
   ↓
nueva versión incorrecta
   ↓
RESTORE TO VERSION AS OF <versión correcta>
   ↓
nueva versión que recupera el estado anterior
```

1. `RESTORE` no borra las versiones posteriores del historial.
2. Sí, aparece una nueva operación `RESTORE`.
3. No se manipulan ficheros manualmente. Delta crea un nuevo commit transaccional que hace que el estado actual vuelva a corresponder con el snapshot elegido.

Comprobación:

```sql
DESCRIBE HISTORY training.<student_id>.orders_delta;

SELECT
 MIN(unit_price),
 MAX(unit_price),
 AVG(unit_price)
FROM training.<student_id>.orders_delta;
```

---

# Ejercicio 14 — Catalog Explorer: History

Catalog Explorer y `DESCRIBE HISTORY` muestran distintas interfaces sobre la misma historia transaccional de la tabla.

No existen dos historiales independientes.

---

# Ejercicio 15 — VACUUM sin riesgo: DRY RUN

1. `VACUUM` localiza ficheros que ya no están referenciados por versiones que deban conservarse y que superan el umbral de retención.
2. Si esos ficheros se eliminan físicamente, versiones antiguas que dependan de ellos pueden dejar de ser consultables mediante time travel.
3. Reducir agresivamente la retención aumenta el riesgo de eliminar datos todavía necesarios para recuperación, auditoría o procesos concurrentes.

```sql
VACUUM training.<student_id>.orders_delta DRY RUN;
```

solo muestra candidatos; no elimina los ficheros.

---

# Ejercicio 16 — Deletion vectors

Una deletion vector **no es una columna visible de la tabla**.

Permite registrar mediante estructuras de metadata qué filas de un fichero están lógicamente eliminadas o modificadas, evitando tener que reescribir inmediatamente todo el fichero Parquet para ciertas operaciones.

Ventajas:

- menos reescritura;
- menor write amplification;
- operaciones de fila potencialmente más rápidas.

La representación física puede consolidarse posteriormente mediante operaciones de mantenimiento que reescriban los ficheros afectados.

---

# Ejercicio 17 — OPTIMIZE

`OPTIMIZE` no cambia el significado lógico de las filas.

Su objetivo es mejorar la representación física de los datos, por ejemplo compactando ficheros y, cuando hay clustering, reorganizando el layout según las claves definidas.

Después debe aparecer una operación `OPTIMIZE` en history.

```text
Mismo contenido lógico
≠
misma disposición física
```

---

# Ejercicio 18 — Liquid clustering

1. Liquid clustering permite adaptar el layout de los datos sin depender de una estrategia rígida de particionado tradicional. Databricks lo recomienda para tablas nuevas.
2. Clustering cambia la organización física utilizada para acelerar lecturas; no cambia los valores lógicos de las filas.
3. Una tabla pequeña no permite medir de forma seria beneficios de data skipping, tamaño de ficheros, paralelismo o coste de reescritura.

Después de cambiar una clave de clustering, `OPTIMIZE` aplica progresivamente esa organización a los datos que reescribe.

---

# Práctica autónoma A — Sincronización incremental con MERGE

## 1. Crear la copia

```sql
CREATE OR REPLACE TABLE training.<student_id>.customers_delta
USING DELTA
AS
SELECT *
FROM training.shared.m04_customer_master;
```

## 2. Inspeccionar

```sql
SELECT *
FROM training.<student_id>.customers_delta
ORDER BY customer_id;

DESCRIBE HISTORY training.<student_id>.customers_delta;
```

## 3. Aplicar cambios

```sql
MERGE INTO training.<student_id>.customers_delta AS target
USING training.shared.m04_customer_changes AS source
ON target.customer_id = source.customer_id

WHEN MATCHED THEN
  UPDATE SET *

WHEN NOT MATCHED THEN
  INSERT *;
```

## 4. Comprobar

```sql
SELECT *
FROM training.<student_id>.customers_delta
ORDER BY customer_id;

DESCRIBE HISTORY training.<student_id>.customers_delta;
```

## 5. Idempotencia

El resultado es semánticamente idempotente si:

- `customer_id` identifica de forma única cada entidad;
- la fuente no contiene varias filas conflictivas para la misma clave;
- repetir el lote produce los mismos valores finales;
- no existen columnas cuyo valor cambie en cada ejecución de forma no determinista.

Repetir un `MERGE` puede generar actividad o una nueva operación, pero el **estado de negocio** debe quedar igual si la entrada es la misma y las reglas son deterministas.

---

# Práctica autónoma B — Recuperación ante un cambio erróneo

## 1. Anotar la versión correcta

```sql
DESCRIBE HISTORY training.<student_id>.customers_delta;
```

Guarda la versión actual como `<version_correcta>`.

## 2. Introducir un error visible

```sql
UPDATE training.<student_id>.customers_delta
SET country = 'ZZ'
WHERE country IS NOT NULL;
```

## 3. Detectarlo

```sql
SELECT country, COUNT(*) AS rows
FROM training.<student_id>.customers_delta
GROUP BY country
ORDER BY rows DESC;
```

## 4. Ver el estado bueno sin modificar la tabla

```sql
SELECT *
FROM training.<student_id>.customers_delta
VERSION AS OF <version_correcta>
ORDER BY customer_id;
```

## 5. Restaurar

```sql
RESTORE TABLE training.<student_id>.customers_delta
TO VERSION AS OF <version_correcta>;
```

## 6. Verificar

```sql
SELECT country, COUNT(*) AS rows
FROM training.<student_id>.customers_delta
GROUP BY country
ORDER BY rows DESC;

DESCRIBE HISTORY training.<student_id>.customers_delta;
```

La historia conserva tanto el cambio erróneo como el `RESTORE`. Restaurar no reescribe el pasado; crea un nuevo estado actual basado en un snapshot anterior.

---

# Práctica autónoma C — Cambio de schema controlado

## 1. Provocar un error

```sql
INSERT INTO training.<student_id>.customers_delta
(
 customer_id,
 columna_inexistente
)
VALUES
(
 'C999',
 'x'
);
```

Debe fallar.

## 2. Evolucionar el schema

```sql
ALTER TABLE training.<student_id>.customers_delta
ADD COLUMN segment STRING;
```

## 3. Clasificar clientes existentes

```sql
UPDATE training.<student_id>.customers_delta
SET segment = CASE
  WHEN country = 'ES' THEN 'CORE'
  ELSE 'STANDARD'
END
WHERE segment IS NULL;
```

## 4. Insertar un cliente nuevo

Comprueba primero las columnas reales:

```sql
DESCRIBE TABLE training.<student_id>.customers_delta;
```

Si la tabla contiene las columnas habituales `customer_id`, `customer_name`, `email`, `country` y `segment`:

```sql
INSERT INTO training.<student_id>.customers_delta
(
 customer_id,
 customer_name,
 email,
 country,
 segment
)
VALUES
(
 'C999',
 'New Customer',
 'new.customer@example.com',
 'ES',
 'NEW'
);
```

## 5. Verificar

```sql
DESCRIBE TABLE training.<student_id>.customers_delta;

SELECT *
FROM training.<student_id>.customers_delta
WHERE customer_id = 'C999';

DESCRIBE HISTORY training.<student_id>.customers_delta;
```

Ventaja: el cambio queda integrado en la evolución del objeto gobernado y puede correlacionarse con el momento y las operaciones que lo utilizaron.

---

# Práctica autónoma D — Diagnóstico de mantenimiento

```sql
DESCRIBE DETAIL training.<student_id>.customers_delta;

DESCRIBE HISTORY training.<student_id>.customers_delta;

VACUUM training.<student_id>.customers_delta DRY RUN;

SHOW TBLPROPERTIES training.<student_id>.customers_delta;

OPTIMIZE training.<student_id>.customers_delta;

DESCRIBE HISTORY training.<student_id>.customers_delta;
```

Interpretación:

- `DESCRIBE DETAIL`: formato, ubicación, tamaño, número de ficheros y características de la tabla;
- `DESCRIBE HISTORY`: commits y operaciones;
- `VACUUM ... DRY RUN`: ficheros candidatos a limpieza;
- `SHOW TBLPROPERTIES`: propiedades Delta;
- `OPTIMIZE`: mantenimiento del layout físico.

En Unity Catalog managed tables, **predictive optimization** puede decidir y ejecutar automáticamente:

```text
OPTIMIZE
VACUUM
ANALYZE
```

El objetivo es ejecutar mantenimiento cuando resulte útil, en lugar de aplicar una frecuencia manual idéntica a todas las tablas.

---

# Reto final M04

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

## Schema enforcement vs schema evolution

```text
Schema enforcement
→ protege el contrato actual y rechaza escrituras incompatibles.

Schema evolution
→ modifica deliberadamente el contrato para aceptar una nueva estructura.
```

## Logical DELETE vs physical removal

```text
DELETE
→ crea una nueva versión en la que la fila ya no forma parte del resultado actual.

VACUUM
→ elimina físicamente ficheros no referenciados que superan la retención.
```

Entre ambos momentos, los datos antiguos pueden seguir siendo necesarios para time travel y recuperación.

La aportación esencial de Delta Lake es que los ficheros dejan de tratarse como elementos aislados y pasan a formar parte de una tabla versionada con semántica transaccional.
