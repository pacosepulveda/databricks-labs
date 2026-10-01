# LAB_ML01 — Pipeline end-to-end de Machine Learning

## Objetivo

Construir un pipeline completo de clasificación binaria para predecir churn y recorrer las fases principales de un proyecto de Machine Learning dentro de Databricks.

No se pretende profundizar en la teoría matemática de la regresión logística. El objetivo es entender el flujo de trabajo:

```text
datos
→ features
→ train/test
→ preprocesamiento
→ entrenamiento
→ evaluación
→ tracking
→ inferencia
```

## Datos

Entrenamiento:

```text
/Volumes/training/shared/source/ml/customer_churn_gold.csv
```

Scoring:

```text
/Volumes/training/shared/source/ml/customer_churn_scoring.csv
```

La tabla de entrenamiento contiene:

- `customer_id`;
- variables de cliente;
- 11 variables numéricas utilizables como features;
- `churn` como etiqueta binaria.

El dataset de scoring contiene nuevos clientes sin la columna `churn`.

---

## Parte 1 — Cargar y explorar

1. Lee el CSV de entrenamiento con Spark.
2. Muestra una muestra.
3. Revisa el schema.
4. Cuenta los registros.
5. Comprueba la distribución de `churn`.

Responde:

- ¿el problema está perfectamente balanceado?
- ¿qué representa la clase positiva?

---

## Parte 2 — Preparar features y label

1. Identifica las columnas numéricas.
2. Excluye `customer_id` y `churn`.
3. Comprueba que dispones de 11 features.
4. Convierte el subconjunto necesario a pandas.
5. Construye `X` e `y`.

---

## Parte 3 — Train/test

Divide los datos:

```text
80% train
20% test
```

Utiliza una semilla fija y conserva aproximadamente la proporción de churn mediante estratificación.

Pregunta:

> ¿Por qué no evaluamos el modelo con los mismos datos que utilizamos para entrenarlo?

---

## Parte 4 — Preprocesamiento y entrenamiento

Construye un pipeline de scikit-learn con:

```text
StandardScaler
→ LogisticRegression
```

Entrena únicamente con el conjunto de entrenamiento.

Pregunta:

> ¿Por qué el scaler debe aprender sus parámetros solo con train?

---

## Parte 5 — Evaluación

Calcula:

- accuracy;
- precision;
- recall;
- F1;
- ROC-AUC;
- matriz de confusión.

Interpreta especialmente precision y recall.

No busques una métrica "perfecta": el objetivo es comprender qué mide cada una.

---

## Parte 6 — MLflow

Registra una ejecución con:

- tipo de modelo;
- número de features;
- tamaño de train/test;
- accuracy;
- precision;
- recall;
- F1;
- ROC-AUC;
- modelo entrenado.

Localiza el run en MLflow.

---

## Parte 7 — Inferencia

1. Lee `customer_churn_scoring.csv`.
2. Aplica exactamente las mismas features.
3. Genera:
   - `predicted_churn`;
   - `churn_probability`.
4. Conserva `customer_id`.
5. Convierte el resultado de nuevo en un Spark DataFrame.
6. Ordena los clientes por mayor probabilidad de churn.

---

## Preguntas finales

1. ¿Qué diferencia hay entre entrenamiento e inferencia?
2. ¿Por qué el preprocesamiento forma parte del pipeline del modelo?
3. ¿Por qué ROC-AUC y F1 aportan información diferente a accuracy?
4. ¿Qué aporta MLflow frente a ejecutar una celda y anotar manualmente el resultado?
5. ¿Qué necesitaríamos añadir para convertir este notebook en un proceso productivo?
