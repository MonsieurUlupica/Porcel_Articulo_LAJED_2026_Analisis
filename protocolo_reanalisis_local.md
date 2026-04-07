# Protocolo de reanálisis local del Censo de Población y Vivienda 2024

## Objetivo

Este protocolo documenta la reconstrucción local del análisis empírico del artículo a partir de los archivos completos del Censo de Población y Vivienda 2024. El propósito es dejar trazable la construcción de la base analítica, la definición de las variables principales, la estrategia de estimación y las salidas que alimentan el manuscrito.

## Insumos requeridos

El script principal espera encontrar dos archivos en un directorio de datos:

- `Persona_CPV-2024.csv`
- `Vivienda_CPV-2024.csv`

Por defecto, el reanálisis busca esos archivos en una de estas rutas:

1. `data/raw/` dentro del repositorio
2. `../CENSO_2024_bolivia/` en el directorio hermano del proyecto

También puede indicarse una ruta explícita mediante:

- el argumento `--data-dir`
- o la variable de entorno `CPV2024_DATA_DIR`

## Universo analítico

- Unidad de análisis: persona residente en vivienda particular.
- Integración de bases: unión de los archivos de Persona y Vivienda mediante el identificador `i00`.
- Restricción etaria: personas entre 15 y 65 años.
- Filtro de vivienda: se conservan únicamente viviendas particulares (`V01_TIPOVIV` entre 1 y 6) con condición de ocupación compatible con presencia o ausencia temporal de personas (`V02_CONDOCUP` entre 0 y 2).

## Variable dependiente

La variable dependiente identifica la no participación económica por labores domésticas o de cuidado:

- `captura_tiempo = 1` si `P48_NOCU = 6`
- `captura_tiempo = 0` en otro caso

Según el diccionario oficial del censo, `P48_NOCU = 6` corresponde a:

`Realizó labores de su casa o cuidado de los miembros de su hogar`.

## Variables explicativas principales

### Índice hídrico

El índice de restricción hídrica replica la construcción empleada en el manuscrito:

\[
\Omega_i = w(v07_i)\cdot \phi(v08_i)
\]

donde:

- `v07_i` representa la procedencia principal del agua
- `v08_i` representa la modalidad de distribución del agua en la vivienda

El script guarda tanto las ponderaciones empleadas como la distribución empírica del índice y de sus dos componentes.

### Covariables demográficas

- sexo
- edad
- edad al cuadrado
- escolaridad equivalente
- ruralidad
- efectos fijos departamentales

### Controles materiales del hogar

La especificación ampliada incorpora:

- material del piso
- fuente de energía eléctrica
- servicio sanitario
- desagüe
- tenencia de la vivienda
- total de personas en el hogar
- número de dormitorios

## Estrategia de estimación

El reanálisis produce cuatro bloques principales:

1. Descriptivos de la muestra analítica completa.
2. Documentación del índice hídrico y de las variables censales utilizadas.
3. Modelos principales:
   - Logit base
   - Logit con interacción
   - Logit ampliado con controles materiales
   - Logit alternativo con componentes directos del agua
4. Sensibilidad funcional:
   - Probit
   - Modelo de probabilidad lineal con HC3

## Optimización computacional

Los descriptivos se calculan sobre el universo analítico completo. Las estimaciones se realizan sobre celdas agrupadas exactas de éxitos y fracasos por combinación observada de covariables discretas. Este procedimiento preserva la información del universo completo y evita recurrir a submuestreo aleatorio.

## Productos generados

El script principal escribe en `Porcel_Articulo_LAJED_2026_Analisis/outputs/`:

- `table1_sample_characteristics.csv`
- `omega_weights.csv`
- `omega_distribution.csv`
- `v07_distribution.csv`
- `v08_distribution.csv`
- `censo_variables_utilizadas.csv`
- `p48_nocu_documentation.txt`
- `model_base_logit.csv`
- `model_interaction_logit.csv`
- `model_interaction_probit.csv`
- `model_interaction_lpm.csv`
- `model_extended_logit.csv`
- `model_components_logit.csv`
- `model_comparison_key_terms.csv`
- `key_findings.json`
- `reanalysis_summary.md`

El archivo `analytic_sample.parquet` es un derivado intermedio grande y puede regenerarse en cualquier momento.

## Comando de ejecución

```bash
.venv/bin/python Porcel_Articulo_LAJED_2026_Analisis/rebuild_local_analysis.py --data-dir /ruta/al/directorio/de/microdatos
```

Si los microdatos ya se encuentran en una de las rutas por defecto, basta con:

```bash
.venv/bin/python Porcel_Articulo_LAJED_2026_Analisis/rebuild_local_analysis.py
```

## Lectura sustantiva

El reanálisis permite evaluar tres preguntas centrales:

1. Si la brecha de género en la no participación económica por cuidados sigue siendo amplia y estable.
2. Si la asociación entre las condiciones hídricas del hogar y la variable dependiente persiste al introducir controles materiales adicionales.
3. Si la interacción entre género y restricción hídrica se mantiene o debe tratarse como un resultado sensible a la forma funcional.
