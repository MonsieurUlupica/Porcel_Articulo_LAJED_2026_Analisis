#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "analysis" / "outputs"
ANALYTIC_PARQUET = OUT_DIR / "analytic_sample.parquet"

DATA_CANDIDATES = [
    ROOT / "data" / "raw",
    ROOT.parent / "CENSO_2024_bolivia",
]

EDUCATION_MAP = {1: 0, 2: 1, 3: 3, 7: 6, 8: 12, 9: 14, 10: 15, 11: 17, 12: 19, 13: 21}

# Réplica de la ponderación usada en la especificación original, preservada por comparabilidad.
W_MAP_REPLICA = {1: 1.0, 4: 3.0, 6: 3.0, 8: 5.0, 5: 10.0, 7: 10.0}
PHI_MAP_REPLICA = {1: 1.0, 2: 1.5, 3: 3.0}
W_DEFAULT = 2.0
PHI_DEFAULT = 1.0

V07_LABELS = {
    1: "Cañería de red",
    2: "Pileta pública",
    3: "Cosecha de agua de lluvia",
    4: "Pozo excavado o perforado con bomba",
    5: "Pozo no protegido o sin bomba",
    6: "Manantial o vertiente protegida",
    7: "Río, acequia o vertiente no protegida",
    8: "Carro repartidor (aguatero)",
    9: "Otro",
}

V08_LABELS = {
    1: "Por cañería dentro de la vivienda",
    2: "Por cañería fuera de la vivienda, pero dentro del lote o terreno",
    3: "No se distribuye por cañería",
}

P48_OPTIONS = {
    1: "Buscó trabajo por primera vez",
    2: "Buscó trabajo habiendo trabajado antes",
    3: "Estuvo como pasante o aprendiz sin recibir pago",
    4: "Estuvo estudiando",
    5: "Está jubilada(o), es pensionista o rentista",
    6: "Realizó labores de su casa o cuidado de los miembros de su hogar",
    7: "Otro (especifique)",
    9: "Sin especificar",
}

VARIABLE_CATALOG = [
    {
        "archivo": "PERSONA",
        "variable": "P25_SEXO",
        "pregunta_o_etiqueta": "25. Es mujer u hombre",
        "uso_analitico": "Indicador de género",
    },
    {
        "archivo": "PERSONA",
        "variable": "P26_EDAD",
        "pregunta_o_etiqueta": "26. Cuantos años cumplidos tiene",
        "uso_analitico": "Filtro etario y controles de edad y edad al cuadrado",
    },
    {
        "archivo": "PERSONA",
        "variable": "P41A_NIVEL",
        "pregunta_o_etiqueta": "41.A. Cuál es el último curso o año que aprobó y en que nivel educativo (Nivel)",
        "uso_analitico": "Proxy de escolaridad equivalente",
    },
    {
        "archivo": "PERSONA",
        "variable": "P48_NOCU",
        "pregunta_o_etiqueta": "48.A. Las últimas 4 semanas: (Condición de inactividad en las últimas 4 semanas)",
        "uso_analitico": "Variable dependiente",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "URBRUR",
        "pregunta_o_etiqueta": "Área Urbana - Rural",
        "uso_analitico": "Control de ruralidad",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "V01_TIPOVIV",
        "pregunta_o_etiqueta": "1. La vivienda es: (Tipo de vivienda)",
        "uso_analitico": "Filtro de viviendas particulares",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "V02_CONDOCUP",
        "pregunta_o_etiqueta": "2. La vivienda esta: (Condición de ocupación de la vivienda)",
        "uso_analitico": "Filtro de viviendas ocupadas o temporalmente ausentes",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "V06_PISO",
        "pregunta_o_etiqueta": "6. Cuál es el material más utilizado en los pisos de esta vivienda",
        "uso_analitico": "Control material del hogar",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "V07_AGUAPRO",
        "pregunta_o_etiqueta": "7. Principalmente, el agua que usan en la vivienda proviene de:",
        "uso_analitico": "Componente de aprovisionamiento del índice hídrico",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "V08_AGUADIST",
        "pregunta_o_etiqueta": "8. El agua que usan en la vivienda se distribuye:",
        "uso_analitico": "Componente de distribución del índice hídrico",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "V09_ENERGIA",
        "pregunta_o_etiqueta": "9. De donde proviene la energía eléctrica que usan en la vivienda",
        "uso_analitico": "Control material del hogar",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "V14_DORMIT",
        "pregunta_o_etiqueta": "14. De estos cuartos o habitaciones, Cuántos se utilizan solo para dormir",
        "uso_analitico": "Control material del hogar y razón personas por dormitorio",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "V15_SERVSAN",
        "pregunta_o_etiqueta": "15. Tienen, baño o letrina",
        "uso_analitico": "Control material del hogar",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "V16_DESAGUE",
        "pregunta_o_etiqueta": "16. El baño o letrina tiene desagüe:",
        "uso_analitico": "Control material del hogar",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "V17_TENENCIA",
        "pregunta_o_etiqueta": "17. La vivienda que ocupan es: (Tenencia de la vivienda)",
        "uso_analitico": "Control material del hogar",
    },
    {
        "archivo": "VIVIENDA",
        "variable": "TOT_PERS",
        "pregunta_o_etiqueta": "Total personas",
        "uso_analitico": "Tamaño del hogar y razón personas por dormitorio",
    },
    {
        "archivo": "SISTEMA",
        "variable": "IDEP, IPROV, IMUN",
        "pregunta_o_etiqueta": "Identificadores territoriales derivados del operativo censal",
        "uso_analitico": "Efectos fijos departamentales y conteo territorial",
    },
]


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def resolve_data_dir(cli_data_dir: str | None) -> Path:
    candidates: list[Path] = []
    if cli_data_dir:
        candidates.append(Path(cli_data_dir).expanduser().resolve())

    env_data_dir = os.environ.get("CPV2024_DATA_DIR")
    if env_data_dir:
        candidates.append(Path(env_data_dir).expanduser().resolve())

    candidates.extend(path.resolve() for path in DATA_CANDIDATES)

    for candidate in candidates:
        persona = candidate / "Persona_CPV-2024.csv"
        vivienda = candidate / "Vivienda_CPV-2024.csv"
        if persona.exists() and vivienda.exists():
            return candidate

    searched = "\n".join(str(path) for path in candidates)
    raise FileNotFoundError(
        "No se encontró el directorio de microdatos del censo.\n"
        "Busqué en:\n"
        f"{searched}\n"
        "Use --data-dir o la variable de entorno CPV2024_DATA_DIR."
    )


def build_analytic_parquet(data_dir: Path, force: bool = False) -> None:
    if ANALYTIC_PARQUET.exists() and not force:
        log(f"Base analítica ya disponible: {ANALYTIC_PARQUET.name}")
        return

    log("Construyendo base analítica local en parquet desde los CSV completos...")
    con = duckdb.connect()
    persona = (data_dir / "Persona_CPV-2024.csv").as_posix()
    vivienda = (data_dir / "Vivienda_CPV-2024.csv").as_posix()
    target = ANALYTIC_PARQUET.as_posix()

    query = f"""
    COPY (
        SELECT
            TRY_CAST(p.idep AS SMALLINT) AS idep,
            TRY_CAST(p.iprov AS SMALLINT) AS iprov,
            TRY_CAST(p.imun AS INTEGER) AS imun,
            p.i00 AS i00,
            TRY_CAST(p.p25_sexo AS SMALLINT) AS p25_sexo,
            TRY_CAST(p.p26_edad AS SMALLINT) AS edad,
            TRY_CAST(p.p41a_nivel AS SMALLINT) AS p41a_nivel,
            TRY_CAST(p.p48_nocu AS SMALLINT) AS p48_nocu,
            TRY_CAST(v.v01_tipoviv AS SMALLINT) AS v01_tipoviv,
            TRY_CAST(v.v02_condocup AS SMALLINT) AS v02_condocup,
            TRY_CAST(v.urbrur AS SMALLINT) AS urbrur,
            TRY_CAST(v.v06_piso AS SMALLINT) AS v06_piso,
            TRY_CAST(v.v07_aguapro AS SMALLINT) AS v07_aguapro,
            TRY_CAST(v.v08_aguadist AS SMALLINT) AS v08_aguadist,
            TRY_CAST(v.v09_energia AS SMALLINT) AS v09_energia,
            TRY_CAST(v.v14_dormit AS SMALLINT) AS v14_dormit,
            TRY_CAST(v.v15_servsan AS SMALLINT) AS v15_servsan,
            TRY_CAST(v.v16_desague AS SMALLINT) AS v16_desague,
            TRY_CAST(v.v17_tenencia AS SMALLINT) AS v17_tenencia,
            TRY_CAST(v.tot_pers AS SMALLINT) AS tot_pers
        FROM read_csv_auto('{persona}', delim=';', header=true, all_varchar=true, ignore_errors=true) p
        INNER JOIN read_csv_auto('{vivienda}', delim=';', header=true, all_varchar=true, ignore_errors=true) v
        USING (i00)
        WHERE TRY_CAST(p.p26_edad AS INTEGER) BETWEEN 15 AND 65
          AND TRY_CAST(v.v01_tipoviv AS INTEGER) BETWEEN 1 AND 6
          AND TRY_CAST(v.v02_condocup AS INTEGER) BETWEEN 0 AND 2
    ) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    con.execute(query)
    con.close()
    log("Base analítica creada.")


def load_analytic_frame() -> pd.DataFrame:
    log("Cargando base analítica...")
    df = pd.read_parquet(ANALYTIC_PARQUET)

    int_cols = [
        "idep",
        "iprov",
        "imun",
        "p25_sexo",
        "edad",
        "p41a_nivel",
        "p48_nocu",
        "v01_tipoviv",
        "v02_condocup",
        "urbrur",
        "v06_piso",
        "v07_aguapro",
        "v08_aguadist",
        "v09_energia",
        "v14_dormit",
        "v15_servsan",
        "v16_desague",
        "v17_tenencia",
        "tot_pers",
    ]
    for col in int_cols:
        df[col] = df[col].fillna(0).astype("int16")

    df["capital_humano"] = df["p41a_nivel"].map(EDUCATION_MAP).fillna(0).astype("float32")

    omega_base = df["v07_aguapro"].map(W_MAP_REPLICA).fillna(W_DEFAULT)
    omega_dist = df["v08_aguadist"].map(PHI_MAP_REPLICA).fillna(PHI_DEFAULT)
    df["omega"] = (omega_base * omega_dist).astype("float32")

    df["mujer"] = (df["p25_sexo"] == 1).astype("int8")
    df["ruralidad"] = (df["urbrur"] == 2).astype("int8")
    df["captura_tiempo"] = (df["p48_nocu"] == 6).astype("int8")
    df["inter_omega_mujer"] = (df["omega"] * df["mujer"]).astype("float32")
    df["inter_rural_mujer"] = (df["ruralidad"] * df["mujer"]).astype("int8")
    df["edad2"] = (df["edad"].astype("int32") ** 2).astype("int32")

    dormit_safe = np.where(df["v14_dormit"].to_numpy() > 0, df["v14_dormit"].to_numpy(), 1)
    df["personas_por_dormitorio"] = (df["tot_pers"].to_numpy() / dormit_safe).astype("float32")

    log(f"Base cargada: {len(df):,} observaciones.")
    return df


def write_table1(df: pd.DataFrame) -> None:
    log("Generando Tabla 1 descriptiva...")

    women = df["mujer"] == 1
    men = df["mujer"] == 0

    def summarise(mask: pd.Series | np.ndarray | None, series: str, stat: str) -> float:
        data = df.loc[mask, series] if mask is not None else df[series]
        if stat == "mean":
            return float(data.mean())
        if stat == "std":
            return float(data.std())
        if stat == "median":
            return float(data.median())
        if stat == "p75":
            return float(data.quantile(0.75))
        raise ValueError(stat)

    rows = [
        ("Observaciones", float(len(df)), float(women.sum()), float(men.sum())),
        ("Municipios observados", float(df[["idep", "iprov", "imun"]].drop_duplicates().shape[0]), np.nan, np.nan),
        ("Edad media", summarise(None, "edad", "mean"), summarise(women, "edad", "mean"), summarise(men, "edad", "mean")),
        ("Edad DE", summarise(None, "edad", "std"), summarise(women, "edad", "std"), summarise(men, "edad", "std")),
        (
            "Escolaridad equivalente media",
            summarise(None, "capital_humano", "mean"),
            summarise(women, "capital_humano", "mean"),
            summarise(men, "capital_humano", "mean"),
        ),
        (
            "Escolaridad equivalente DE",
            summarise(None, "capital_humano", "std"),
            summarise(women, "capital_humano", "std"),
            summarise(men, "capital_humano", "std"),
        ),
        ("Ruralidad (%)", float(df["ruralidad"].mean() * 100), float(df.loc[women, "ruralidad"].mean() * 100), float(df.loc[men, "ruralidad"].mean() * 100)),
        (
            "No participación por cuidados (%)",
            float(df["captura_tiempo"].mean() * 100),
            float(df.loc[women, "captura_tiempo"].mean() * 100),
            float(df.loc[men, "captura_tiempo"].mean() * 100),
        ),
        ("Índice hídrico medio", summarise(None, "omega", "mean"), summarise(women, "omega", "mean"), summarise(men, "omega", "mean")),
        ("Índice hídrico DE", summarise(None, "omega", "std"), summarise(women, "omega", "std"), summarise(men, "omega", "std")),
        ("Índice hídrico mediana", summarise(None, "omega", "median"), summarise(women, "omega", "median"), summarise(men, "omega", "median")),
        ("Índice hídrico P75", summarise(None, "omega", "p75"), summarise(women, "omega", "p75"), summarise(men, "omega", "p75")),
        (
            "Personas por dormitorio media",
            summarise(None, "personas_por_dormitorio", "mean"),
            summarise(women, "personas_por_dormitorio", "mean"),
            summarise(men, "personas_por_dormitorio", "mean"),
        ),
    ]

    pd.DataFrame(rows, columns=["variable", "total", "mujeres", "hombres"]).to_csv(
        OUT_DIR / "table1_sample_characteristics.csv", index=False
    )


def write_omega_metadata(df: pd.DataFrame) -> None:
    log("Documentando el índice hídrico y sus componentes...")

    weight_rows = []
    for code, label in V07_LABELS.items():
        weight_rows.append(
            {
                "componente": "V07_AGUAPRO",
                "codigo": code,
                "categoria": label,
                "peso_aplicado": W_MAP_REPLICA.get(code, W_DEFAULT),
                "tipo_asignacion": "mapa_original" if code in W_MAP_REPLICA else "valor_por_defecto",
            }
        )
    for code, label in V08_LABELS.items():
        weight_rows.append(
            {
                "componente": "V08_AGUADIST",
                "codigo": code,
                "categoria": label,
                "peso_aplicado": PHI_MAP_REPLICA.get(code, PHI_DEFAULT),
                "tipo_asignacion": "mapa_original" if code in PHI_MAP_REPLICA else "valor_por_defecto",
            }
        )
    pd.DataFrame(weight_rows).to_csv(OUT_DIR / "omega_weights.csv", index=False)

    omega_dist = (
        df["omega"]
        .value_counts(dropna=False)
        .rename_axis("omega")
        .reset_index(name="n")
        .sort_values("omega")
    )
    omega_dist["pct"] = omega_dist["n"] / omega_dist["n"].sum() * 100
    omega_dist.to_csv(OUT_DIR / "omega_distribution.csv", index=False)

    v07_dist = (
        df["v07_aguapro"]
        .value_counts(dropna=False)
        .rename_axis("codigo")
        .reset_index(name="n")
        .sort_values("codigo")
    )
    v07_dist["categoria"] = v07_dist["codigo"].map(V07_LABELS).fillna("Sin especificar")
    v07_dist["pct"] = v07_dist["n"] / v07_dist["n"].sum() * 100
    v07_dist.to_csv(OUT_DIR / "v07_distribution.csv", index=False)

    v08_dist = (
        df["v08_aguadist"]
        .value_counts(dropna=False)
        .rename_axis("codigo")
        .reset_index(name="n")
        .sort_values("codigo")
    )
    v08_dist["categoria"] = v08_dist["codigo"].map(V08_LABELS).fillna("Sin especificar")
    v08_dist["pct"] = v08_dist["n"] / v08_dist["n"].sum() * 100
    v08_dist.to_csv(OUT_DIR / "v08_distribution.csv", index=False)


def write_variable_catalog() -> None:
    log("Generando catálogo de variables censales utilizadas...")
    pd.DataFrame(VARIABLE_CATALOG).to_csv(OUT_DIR / "censo_variables_utilizadas.csv", index=False)

    p48_text = [
        "P48_NOCU",
        "Pregunta: 48.A. Las últimas 4 semanas: (Condición de inactividad en las últimas 4 semanas)",
        "",
        "Opciones:",
    ]
    p48_text.extend([f"{code}. {label}" for code, label in P48_OPTIONS.items()])
    (OUT_DIR / "p48_nocu_documentation.txt").write_text("\n".join(p48_text), encoding="utf-8")


def grouped_cells(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    grouped = (
        df.groupby(columns, dropna=False, observed=True)
        .agg(successes=("captura_tiempo", "sum"), n=("captura_tiempo", "size"))
        .reset_index()
    )
    grouped["failures"] = grouped["n"] - grouped["successes"]
    return grouped


def build_design_matrix(grouped: pd.DataFrame, numeric_cols: list[str], categorical_cols: list[str]) -> pd.DataFrame:
    parts = [pd.DataFrame({"const": np.ones(len(grouped), dtype=np.float32)})]

    if numeric_cols:
        parts.append(grouped[numeric_cols].astype(np.float32))

    for col in categorical_cols:
        dummies = pd.get_dummies(
            grouped[col].fillna(0).astype("int32"),
            prefix=col,
            drop_first=True,
            dtype=np.float32,
        )
        parts.append(dummies)

    return pd.concat(parts, axis=1)


def tidy_result(result, model_name: str) -> pd.DataFrame:
    conf = np.asarray(result.conf_int())
    out = pd.DataFrame(
        {
            "term": result.params.index,
            "estimate": result.params.values,
            "std_error": result.bse,
            "statistic": result.tvalues,
            "p_value": result.pvalues,
            "conf_low": conf[:, 0],
            "conf_high": conf[:, 1],
            "model": model_name,
        }
    )
    return out


def fit_grouped_glm(
    grouped: pd.DataFrame,
    numeric_cols: list[str],
    categorical_cols: list[str],
    model_name: str,
    link_name: str = "logit",
) -> pd.DataFrame:
    X = build_design_matrix(grouped, numeric_cols, categorical_cols)
    y = grouped[["successes", "failures"]].to_numpy(dtype=np.float64)

    if link_name == "logit":
        link = sm.families.links.Logit()
    elif link_name == "probit":
        link = sm.families.links.Probit()
    else:
        raise ValueError(link_name)

    model = sm.GLM(y, X, family=sm.families.Binomial(link=link))
    result = model.fit(maxiter=100, disp=0)
    tidy = tidy_result(result, model_name)
    tidy.to_csv(OUT_DIR / f"{model_name}.csv", index=False)
    return tidy


def fit_grouped_lpm(grouped: pd.DataFrame, numeric_cols: list[str], categorical_cols: list[str], model_name: str) -> pd.DataFrame:
    X = build_design_matrix(grouped, numeric_cols, categorical_cols)
    y = grouped["successes"] / grouped["n"]
    result = sm.WLS(y, X, weights=grouped["n"]).fit(cov_type="HC3")
    tidy = tidy_result(result, model_name)
    tidy.to_csv(OUT_DIR / f"{model_name}.csv", index=False)
    return tidy


def extract_key_terms(tidies: list[pd.DataFrame]) -> pd.DataFrame:
    wanted = ["omega", "mujer", "inter_omega_mujer", "capital_humano", "edad", "edad2", "ruralidad", "inter_rural_mujer"]
    subset = [tidy.loc[tidy["term"].isin(wanted)] for tidy in tidies]
    comparison = pd.concat(subset, ignore_index=True)
    comparison.to_csv(OUT_DIR / "model_comparison_key_terms.csv", index=False)
    return comparison


def run_models(df: pd.DataFrame) -> tuple[dict[str, dict[str, float | None] | int | float], dict[str, pd.DataFrame]]:
    log("Estimando modelos sobre celdas agrupadas exactas...")

    base_numeric = ["omega", "mujer", "capital_humano", "edad", "edad2", "ruralidad", "inter_rural_mujer"]
    interaction_numeric = base_numeric + ["inter_omega_mujer"]
    dept_cat = ["idep"]

    base_group = grouped_cells(df, ["omega", "mujer", "capital_humano", "edad", "edad2", "ruralidad", "inter_rural_mujer", "idep"])
    interaction_group = grouped_cells(df, ["omega", "mujer", "inter_omega_mujer", "capital_humano", "edad", "edad2", "ruralidad", "inter_rural_mujer", "idep"])
    extended_group = grouped_cells(
        df,
        [
            "omega",
            "mujer",
            "inter_omega_mujer",
            "capital_humano",
            "edad",
            "edad2",
            "ruralidad",
            "inter_rural_mujer",
            "idep",
            "v06_piso",
            "v09_energia",
            "v15_servsan",
            "v16_desague",
            "v17_tenencia",
            "tot_pers",
            "v14_dormit",
        ],
    )
    components_group = grouped_cells(
        df,
        [
            "mujer",
            "capital_humano",
            "edad",
            "edad2",
            "ruralidad",
            "inter_rural_mujer",
            "idep",
            "v07_aguapro",
            "v08_aguadist",
        ],
    )

    log(f"Celdas modelo base: {len(base_group):,}")
    log(f"Celdas modelo interacción: {len(interaction_group):,}")
    log(f"Celdas modelo ampliado: {len(extended_group):,}")
    log(f"Celdas modelo componentes: {len(components_group):,}")

    m_base_logit = fit_grouped_glm(base_group, base_numeric, dept_cat, "model_base_logit", link_name="logit")
    m_inter_logit = fit_grouped_glm(interaction_group, interaction_numeric, dept_cat, "model_interaction_logit", link_name="logit")
    m_inter_probit = fit_grouped_glm(interaction_group, interaction_numeric, dept_cat, "model_interaction_probit", link_name="probit")
    m_inter_lpm = fit_grouped_lpm(interaction_group, interaction_numeric, dept_cat, "model_interaction_lpm")
    m_extended_logit = fit_grouped_glm(
        extended_group,
        interaction_numeric + ["tot_pers", "v14_dormit"],
        ["idep", "v06_piso", "v09_energia", "v15_servsan", "v16_desague", "v17_tenencia"],
        "model_extended_logit",
        link_name="logit",
    )
    m_components_logit = fit_grouped_glm(
        components_group,
        ["mujer", "capital_humano", "edad", "edad2", "ruralidad", "inter_rural_mujer"],
        ["idep", "v07_aguapro", "v08_aguadist"],
        "model_components_logit",
        link_name="logit",
    )

    comparison = extract_key_terms(
        [m_base_logit, m_inter_logit, m_inter_probit, m_inter_lpm, m_extended_logit, m_components_logit]
    )

    findings: dict[str, dict[str, float | None] | int | float] = {
        "n_analytic": int(len(df)),
        "n_mujeres": int(df["mujer"].sum()),
        "n_hombres": int((1 - df["mujer"]).sum()),
        "municipios_observados": int(df[["idep", "iprov", "imun"]].drop_duplicates().shape[0]),
        "outcome_rate_total_pct": float(df["captura_tiempo"].mean() * 100),
        "outcome_rate_mujeres_pct": float(df.loc[df["mujer"] == 1, "captura_tiempo"].mean() * 100),
        "outcome_rate_hombres_pct": float(df.loc[df["mujer"] == 0, "captura_tiempo"].mean() * 100),
        "omega_mean": float(df["omega"].mean()),
        "omega_median": float(df["omega"].median()),
        "omega_p75": float(df["omega"].quantile(0.75)),
    }

    def lookup(model: pd.DataFrame, term: str) -> dict[str, float | None]:
        row = model.loc[model["term"] == term]
        if row.empty:
            return {"estimate": None, "p_value": None}
        return {"estimate": float(row["estimate"].iloc[0]), "p_value": float(row["p_value"].iloc[0])}

    findings["coefficients"] = {
        "base_logit_omega": lookup(m_base_logit, "omega"),
        "interaction_logit_omega": lookup(m_inter_logit, "omega"),
        "interaction_logit_inter_omega_mujer": lookup(m_inter_logit, "inter_omega_mujer"),
        "interaction_probit_inter_omega_mujer": lookup(m_inter_probit, "inter_omega_mujer"),
        "interaction_lpm_inter_omega_mujer": lookup(m_inter_lpm, "inter_omega_mujer"),
        "extended_logit_omega": lookup(m_extended_logit, "omega"),
        "extended_logit_inter_omega_mujer": lookup(m_extended_logit, "inter_omega_mujer"),
        "base_logit_mujer": lookup(m_base_logit, "mujer"),
        "extended_logit_mujer": lookup(m_extended_logit, "mujer"),
        "components_v08_2": lookup(m_components_logit, "v08_aguadist_2"),
        "components_v08_3": lookup(m_components_logit, "v08_aguadist_3"),
    }

    (OUT_DIR / "key_findings.json").write_text(json.dumps(findings, indent=2, ensure_ascii=False), encoding="utf-8")
    return findings, {
        "model_base_logit": m_base_logit,
        "model_interaction_logit": m_inter_logit,
        "model_interaction_probit": m_inter_probit,
        "model_interaction_lpm": m_inter_lpm,
        "model_extended_logit": m_extended_logit,
        "model_components_logit": m_components_logit,
        "comparison": comparison,
    }


def write_summary_markdown(findings: dict[str, dict[str, float | None] | int | float]) -> None:
    coeffs = findings["coefficients"]  # type: ignore[index]
    summary = f"""# Síntesis del reanálisis local

## Universo analítico

- Observaciones analíticas: {findings['n_analytic']:,}
- Mujeres: {findings['n_mujeres']:,}
- Hombres: {findings['n_hombres']:,}
- Municipios observados: {findings['municipios_observados']:,}

## Descriptivos principales

- Tasa total de no participación por cuidados: {findings['outcome_rate_total_pct']:.2f}%
- Tasa femenina: {findings['outcome_rate_mujeres_pct']:.2f}%
- Tasa masculina: {findings['outcome_rate_hombres_pct']:.2f}%
- Índice hídrico medio: {findings['omega_mean']:.2f}
- Mediana del índice hídrico: {findings['omega_median']:.2f}
- Percentil 75 del índice hídrico: {findings['omega_p75']:.2f}

## Coeficientes de referencia

- Logit base: `omega = {coeffs['base_logit_omega']['estimate']:.5f}`, `mujer = {coeffs['base_logit_mujer']['estimate']:.5f}`
- Logit con interacción: `omega = {coeffs['interaction_logit_omega']['estimate']:.5f}`, `interacción = {coeffs['interaction_logit_inter_omega_mujer']['estimate']:.5f}`
- Logit ampliado: `omega = {coeffs['extended_logit_omega']['estimate']:.5f}`, `interacción = {coeffs['extended_logit_inter_omega_mujer']['estimate']:.5f}`, `mujer = {coeffs['extended_logit_mujer']['estimate']:.5f}`

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
"""
    (OUT_DIR / "reanalysis_summary.md").write_text(summary, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reanálisis local del CPV 2024")
    parser.add_argument("--data-dir", type=str, default=None, help="Directorio que contiene Persona_CPV-2024.csv y Vivienda_CPV-2024.csv.")
    parser.add_argument("--force-parquet", action="store_true", help="Reconstruye la base analítica parquet desde los CSV.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dirs()
    data_dir = resolve_data_dir(args.data_dir)
    log(f"Usando microdatos desde: {data_dir}")
    build_analytic_parquet(data_dir=data_dir, force=args.force_parquet)
    df = load_analytic_frame()
    write_table1(df)
    write_omega_metadata(df)
    write_variable_catalog()
    findings, _ = run_models(df)
    write_summary_markdown(findings)
    log("Proceso completo.")
    log(json.dumps(findings, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
