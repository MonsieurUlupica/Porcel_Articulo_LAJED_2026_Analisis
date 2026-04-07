# Síntesis del reanálisis local

## Universo analítico

- Observaciones analíticas: 7,325,783
- Mujeres: 3,715,280
- Hombres: 3,610,503
- Municipios observados: 343

## Descriptivos principales

- Tasa total de no participación por cuidados: 13.01%
- Tasa femenina: 21.16%
- Tasa masculina: 4.62%
- Índice hídrico medio: 3.85
- Mediana del índice hídrico: 1.00
- Percentil 75 del índice hídrico: 3.00

## Coeficientes de referencia

- Logit base: `omega = 0.00369`, `mujer = 1.89537`
- Logit con interacción: `omega = 0.00558`, `interacción = -0.00265`
- Logit ampliado: `omega = 0.00309`, `interacción = -0.00417`, `mujer = 1.91378`

## Lectura sintética

- La brecha de género en la no participación económica por cuidados se mantiene amplia en todas las especificaciones.
- La asociación entre restricción hídrica y la variable dependiente persiste después de incorporar controles materiales del hogar.
- La interacción entre género y restricción hídrica continúa siendo sensible a la forma funcional y debe leerse con cautela.
- La modalidad de distribución del agua dentro o fuera de la vivienda resulta más informativa que la sola procedencia del recurso.

## Archivos clave

- `analysis/outputs/table1_sample_characteristics.csv`
- `analysis/outputs/omega_weights.csv`
- `analysis/outputs/omega_distribution.csv`
- `analysis/outputs/censo_variables_utilizadas.csv`
- `analysis/outputs/model_base_logit.csv`
- `analysis/outputs/model_extended_logit.csv`
- `analysis/outputs/model_components_logit.csv`
