# Machine Learning — Laboratorios adicionales

Esta carpeta contiene dos laboratorios guiados de Machine Learning integrados en el curso de Databricks.

## Contenido

### LAB_ML01 — Pipeline end-to-end de Machine Learning

- [Enunciado](LAB_ML01_Enunciado.md)
- [Notebook de trabajo](LAB_ML01_Notebook.ipynb)
- [Solución guiada](LAB_ML01_Solucion_Guiada.ipynb)

Recorrido:

```text
Gold CSV
→ Spark DataFrame
→ selección de features
→ pandas
→ train/test
→ StandardScaler
→ Logistic Regression
→ métricas
→ MLflow
→ scoring
→ Spark DataFrame
```

### LAB_ML02 — Redes neuronales con PyTorch

- [Enunciado](LAB_ML02_Enunciado.md)
- [Notebook de trabajo](LAB_ML02_Notebook.ipynb)
- [Solución guiada](LAB_ML02_Solucion_Guiada.ipynb)

Recorrido:

```text
Gold CSV
→ train/validation/test
→ normalización
→ tensores
→ DataLoader
→ red neuronal
→ forward/loss/backprop
→ epochs
→ evaluación
→ MLflow
→ scoring
```

## Datos

Los notebooks utilizan:

```text
/Volumes/training/shared/source/ml/customer_churn_gold.csv
/Volumes/training/shared/source/ml/customer_churn_scoring.csv
```

El primer fichero contiene aproximadamente 6.000 clientes con la etiqueta `churn`. El segundo contiene aproximadamente 500 clientes nuevos sin etiqueta para realizar inferencia.

Los notebooks seleccionan las 11 columnas numéricas disponibles como features y excluyen `customer_id` y `churn`.

## Prerrequisitos

LAB_ML01 utiliza las librerías habituales del runtime de Databricks: Spark, pandas, scikit-learn y MLflow.

LAB_ML02 requiere PyTorch disponible en el Compute. Compruébalo antes de ejecutar:

```python
import torch
print(torch.__version__)
```

Si el entorno no dispone de PyTorch, debe instalarse como librería del Compute antes de ejecutar el laboratorio.
