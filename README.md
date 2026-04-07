# ¿Qué detiene el tiempo de las mujeres?

Repositorio del reanálisis empírico y soporte metodológico para el artículo **“¿Qué detiene el tiempo de las mujeres? Infraestructura hídrica, cuidado y no participación económica en Bolivia”**.

Este proyecto contiene el entorno reproducible de datos construido a partir de los microdatos públicos del Censo de Población y Vivienda 2024 de Bolivia.

## Estructura

- `rebuild_local_analysis.py`: script principal de reconstrucción empírica.
- `protocolo_reanalisis_local.md`: protocolo técnico del reanálisis.
- `outputs/`: salidas descriptivas y resultados de los modelos estadísticos.

## Requisitos

El reanálisis utiliza:

- Python 3.12 o superior
- `duckdb`
- `numpy`
- `openpyxl`
- `pandas`
- `pyarrow`
- `scipy`
- `statsmodels`

Las versiones utilizadas en este proyecto están en `requirements.txt`.

## Microdatos

Los archivos crudos del censo no se incluyen en este repositorio. Para ejecutar el script es necesario disponer de:

- `Persona_CPV-2024.csv`
- `Vivienda_CPV-2024.csv`

El script los buscará por defecto en:

1. `data/raw/` dentro del repositorio
2. `../CENSO_2024_bolivia/` como directorio hermano del proyecto

También puede indicarse otra ruta mediante:

```bash
.venv/bin/python rebuild_local_analysis.py --data-dir /ruta/a/microdatos
```

o con la variable de entorno:

```bash
export CPV2024_DATA_DIR=/ruta/a/microdatos
```

## Entorno local

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Ejecución del reanálisis

```bash
.venv/bin/python rebuild_local_analysis.py
```

Para forzar la reconstrucción del parquet intermedio:

```bash
.venv/bin/python rebuild_local_analysis.py --force-parquet
```

## Salidas principales

Después de correr el script, el directorio `outputs/` contendrá:

- `table1_sample_characteristics.csv`
- `omega_weights.csv`
- `omega_distribution.csv`
- `censo_variables_utilizadas.csv`
- `model_base_logit.csv`
- `model_extended_logit.csv`
- `model_components_logit.csv`
- `key_findings.json`
- `reanalysis_summary.md`

## Nota de reproducibilidad

El artículo remite a este repositorio como soporte de replicación del procesamiento, la construcción de variables, las estimaciones y las tablas reportadas en el manuscrito.
