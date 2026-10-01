# LAB_ML02 — Redes neuronales con PyTorch

## Objetivo

Resolver el mismo problema de churn del LAB_ML01 utilizando una red neuronal sencilla con PyTorch.

El objetivo no es construir una red profunda ni maximizar la métrica. El laboratorio permite observar directamente:

```text
tensor
batch
epoch
forward
loss
zero_grad
backward
optimizer.step
validation
test
```

## Prerrequisito

PyTorch debe estar disponible en el Compute:

```python
import torch
print(torch.__version__)
```

## Datos

```text
/Volumes/training/shared/source/ml/customer_churn_gold.csv
/Volumes/training/shared/source/ml/customer_churn_scoring.csv
```

Se reutilizan las mismas 11 features numéricas de LAB_ML01.

---

## Parte 1 — Preparar los datos

1. Lee los datos con Spark.
2. Selecciona las 11 features numéricas y la etiqueta `churn`.
3. Convierte a pandas.
4. Divide los datos en:
   - train;
   - validation;
   - test.
5. Ajusta `StandardScaler` únicamente con train.
6. Transforma validation y test utilizando ese mismo scaler.

Pregunta:

> ¿Por qué validation y test no deben influir en el ajuste del scaler?

---

## Parte 2 — Tensores y DataLoader

Convierte los arrays a tensores `float32`.

Crea un `TensorDataset` y un `DataLoader` para train.

Utiliza mini-batches.

Preguntas:

1. ¿Qué diferencia existe entre un DataFrame y un tensor?
2. ¿Qué representa un batch?
3. ¿Por qué no es necesario procesar todos los registros a la vez?

---

## Parte 3 — Crear la red

Utiliza:

```text
11 inputs
  ↓
Linear(11, 32)
  ↓
ReLU
  ↓
Linear(32, 16)
  ↓
ReLU
  ↓
Linear(16, 1)
```

La última capa devuelve un **logit**.

Utiliza:

```text
BCEWithLogitsLoss
Adam
```

---

## Parte 4 — Entrenamiento

En cada batch:

1. `optimizer.zero_grad()`;
2. forward;
3. calcular loss;
4. `loss.backward()`;
5. `optimizer.step()`.

Repite durante varias epochs.

Después de cada epoch calcula también validation loss.

Pregunta:

> ¿Qué indicaría que train loss sigue bajando mientras validation loss empieza a empeorar?

---

## Parte 5 — Test

Evalúa el modelo final sobre test.

Calcula:

- accuracy;
- precision;
- recall;
- F1;
- ROC-AUC;
- matriz de confusión.

No utilices test para ajustar el modelo.

---

## Parte 6 — MLflow

Registra:

- arquitectura;
- epochs;
- batch size;
- learning rate;
- métricas de test;
- modelo PyTorch.

Utiliza `mlflow.pytorch.log_model(model, "model")`.

---

## Parte 7 — Scoring

1. Lee los 500 clientes sin etiqueta.
2. Aplica el scaler entrenado con train.
3. Convierte a tensor.
4. Ejecuta la red en modo evaluación.
5. Aplica sigmoid a los logits.
6. Genera:
   - `predicted_churn`;
   - `churn_probability`.
7. Devuelve el resultado a Spark.

---

## Preguntas finales

1. ¿Qué aprende realmente una capa `Linear`?
2. ¿Qué función cumple ReLU?
3. ¿Por qué utilizamos `model.eval()` durante evaluación?
4. ¿Qué diferencia hay entre un logit y una probabilidad?
5. ¿Qué señales utilizarías para detectar overfitting?
6. ¿En qué se parece este flujo al pipeline de regresión logística de LAB_ML01?
