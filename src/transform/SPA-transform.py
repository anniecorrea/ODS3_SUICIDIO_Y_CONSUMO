import argparse
import json
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, Tuple, Union, Optional

import pandas as pd


BAD_SET = {"", "nan", "sin dato", "upz sin asignar", "s/d", "n.a.", "n.a", "na"}


# ----------------------------- utilidades -----------------------------
def _normalize_text(v: Union[str, float, None]) -> Optional[str]:
    """Minúsculas, sin acentos y trim. Mantiene None/NaN."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    return s


def unify_missing(series: pd.Series, bad: Iterable[str] = BAD_SET) -> pd.Series:
    """Unifica variantes 'sin dato' y NaN en el string 'sin dato'."""
    s = series.map(_normalize_text, na_action="ignore")
    mask = s.isna() | s.isin(set(bad))
    return s.where(~mask, "sin dato")


def standardize_sex(series: pd.Series) -> pd.Series:
    """Estandariza sexo a {'hombre','mujer','otro','sin dato'}."""
    s = series.map(_normalize_text, na_action="ignore")
    map_basic = {
        "h": "hombre",
        "m": "mujer",
        "masculino": "hombre",
        "femenino": "mujer",
        "male": "hombre",
        "female": "mujer",
    }
    s = s.replace(map_basic)
    s = s.where(s.isin({"hombre", "mujer", "sin dato"}), "otro")
    s = s.fillna("sin dato")
    return s


def _is_yes(series: pd.Series) -> pd.Series:
    """Detecta 'sí' en columnas de sitio (sí/si/true/1/x). Devuelve 0/1."""
    s = series.map(_normalize_text, na_action="ignore")
    return s.isin({"si", "sí", "si.", "sí.", "true", "1", "x"}).astype(int)


def normalize_ciclo_vida(series: pd.Series) -> pd.Series:
    """Extrae solo el nombre del ciclo de vida, removiendo rangos de edad."""
    s = series.map(_normalize_text, na_action="ignore")
    # Mapeo de variantes al nombre estándar
    mapping = {
        "0  5 primera infancia": "primera infancia",
        "6  11 infancia": "infancia",
        "12  17 adolescencia": "adolescencia",
        "18  28 juventud": "juventud",
        "29  59 adultez": "adultez",
        ">60 vejez": "vejez",
        # También mapear variantes sin números
        "primera infancia": "primera infancia",
        "infancia": "infancia",
        "adolescencia": "adolescencia",
        "juventud": "juventud",
        "adultez": "adultez",
        "vejez": "vejez",
    }
    return s.replace(mapping).fillna("sin dato")


def normalize_nivel_educativo(series: pd.Series) -> pd.Series:
    """Normaliza nivel educativo para consistencia entre datasets."""
    s = series.map(_normalize_text, na_action="ignore")
    # Unificar variantes de tecnico
    s = s.str.replace("tecnico pos secundaria", "tecnico post-secundaria", regex=False)
    s = s.str.replace("tecnico post secundaria", "tecnico post-secundaria", regex=False)
    # Unificar completo/completa, incompleto/incompleta
    s = s.str.replace(" completo", " completa", regex=False)
    s = s.str.replace(" incompleto", " incompleta", regex=False)
    return s.fillna("sin dato")


def quality_report(df: pd.DataFrame, key_cols: Iterable[str]) -> Dict[str, object]:
    """Reporte simple de calidad para dejar trazabilidad de la transformación."""
    rep = {
        "rows": int(len(df)),
        "nulls_by_col": df[key_cols].isna().sum().to_dict(),
        "sin_dato_by_col": {
            c: int((df[c] == "sin dato").sum())
            for c in key_cols
            if df[c].dtype == "object" or "string" in str(df[c].dtype)
        },
        "duplicates_by_key": int(df.duplicated(subset=list(key_cols), keep=False).sum()),
    }
    return rep


# ----------------------------- pipeline -----------------------------
def transform_spa(
    spa_raw: pd.DataFrame,
    drop_sin_dato_upz: bool = False,
    aggregate: bool = True,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """
    Transforma el dataframe SPA en una tabla tidy.
    Params
    ------
    spa_raw: DataFrame original
    drop_sin_dato_upz: elimina filas con upz = 'sin dato' (por defecto no)
    aggregate: agrega por clave y suma casos (por defecto sí)
    Returns
    -------
    (spa_tidy, qc_report)
    """
    # 1) seleccionar y renombrar
    expected = ["ANO", "NOMBREUPZ", "SEXO", "CURSO_DE_VIDA", "NIVELEDUCATIVO", "CASOS"]
    cols = [c for c in expected if c in spa_raw.columns]
    if not set(["ANO", "NOMBREUPZ", "CASOS"]).issubset(cols):
        missing = set(["ANO", "NOMBREUPZ", "CASOS"]) - set(cols)
        raise ValueError(f"Faltan columnas requeridas: {sorted(missing)}")

    df = spa_raw[cols].copy().rename(
        columns={
            "ANO": "anio",
            "NOMBREUPZ": "upz",
            "SEXO": "sexo",
            "CURSO_DE_VIDA": "ciclo_vida",
            "NIVELEDUCATIVO": "nivel_educativo",
            "CASOS": "casos",
        }
    )

    # --- sitios de consumo -> flags 0/1 usando el texto original ---
    site_cols = [c for c in spa_raw.columns if str(c).upper().startswith("SITIOHABITUALCONSUMO_")]
    site_flags = {c: _is_yes(spa_raw[c]) for c in site_cols} if site_cols else {}
    sites_df = pd.DataFrame(site_flags, index=spa_raw.index) if site_flags else pd.DataFrame(index=spa_raw.index)
    sites_df = sites_df.loc[df.index]  # alinear con df recortado

    # 2) normalizaciones y unificación de 'sin dato'
    df["upz"] = unify_missing(df["upz"])
    df["sexo"] = standardize_sex(df["sexo"])
    df["ciclo_vida"] = normalize_ciclo_vida(df["ciclo_vida"])
    df["nivel_educativo"] = normalize_nivel_educativo(df["nivel_educativo"])

    # 3) tipos numéricos
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["casos"] = pd.to_numeric(df["casos"], errors="coerce").fillna(0).astype(int)

    # 3.1) limpieza estricta de la clave (sin NaN ni 'sin dato')
    key_cols = ["anio", "upz", "sexo", "ciclo_vida", "nivel_educativo"]
    df = df.dropna(subset=key_cols).copy()
    for c in ["upz", "sexo", "ciclo_vida", "nivel_educativo"]:
        s = df[c].astype("string").str.strip().str.lower()
        df = df.loc[~s.isin(BAD_SET)].copy()
    # mantener alineado sites_df después de los filtros
    sites_df = sites_df.loc[df.index]

    # 4) (opcional) excluir filas sin UPZ explícitamente
    if drop_sin_dato_upz:
        df = df.loc[df["upz"] != "sin dato"].copy()
        sites_df = sites_df.loc[df.index]

    # 5) agregación por clave (y sitios ponderados por 'casos')
    if aggregate:
        # base: total de casos por clave
        grouped = df.groupby(key_cols, dropna=False, as_index=False)["casos"].sum()

        if not sites_df.empty:
            weighted = df[key_cols + ["casos"]].join(sites_df)
            for c in sites_df.columns:
                weighted[c] = weighted[c] * weighted["casos"]  # sum(casos * flag)
            agg_sites = weighted.groupby(key_cols, dropna=False)[sites_df.columns].sum().reset_index()
            spa_tidy = grouped.merge(agg_sites, on=key_cols, how="left")
        else:
            spa_tidy = grouped
    else:
        # sin agregación: adjunta flags 0/1 fila a fila
        spa_tidy = df.join(sites_df)

    # 6) reporte
    qc = quality_report(spa_tidy, key_cols)

    return spa_tidy, qc


# ----------------------------- CLI -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Transformación SPA a formato tidy.")
    p.add_argument("--input", required=True, type=Path, help="Ruta del CSV de consumo de sustancias (SPA).")
    p.add_argument("--output", required=True, type=Path, help="Ruta de salida para spa_tidy.csv")
    p.add_argument("--drop-sin-dato", action="store_true", help="Excluye filas con upz = 'sin dato'.")
    p.add_argument("--no-group", action="store_true", help="No agregar; conserva filas sin agrupar.")
    p.add_argument("--report", type=Path, default=None, help="Ruta para guardar QC en JSON.")
    return p.parse_args()


def main():
    args = parse_args()
    # lectura
    spa_raw = pd.read_csv(args.input)

    # transformación
    tidy, qc = transform_spa(
        spa_raw,
        drop_sin_dato_upz=args.drop_sin_dato,
        aggregate=not args.no_group,
    )

    # escritura
    args.output.parent.mkdir(parents=True, exist_ok=True)
    tidy.to_csv(args.output, index=False)
    print(f"[OK] Guardado: {args.output}  ({len(tidy)} filas)")

    # reporte
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(qc, f, ensure_ascii=False, indent=2)
        print(f"[OK] Reporte QC: {args.report}")
    else:
        print("QC:", qc)


if __name__ == "__main__":
    main()