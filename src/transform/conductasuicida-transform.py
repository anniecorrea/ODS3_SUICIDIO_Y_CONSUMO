import argparse
import json
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, Tuple, List, Union, Optional

import pandas as pd

BAD_SET = {"", "nan", "sin dato", "upz sin asignar", "s/d", "n.a.", "n.a", "na"}

RISK_COLS: List[str] = [
    "enfermedades_dolorosas",
    "maltrato_sexual",
    "muerte_familiar",
    "conflicto_pareja",
    "problemas_economicos",
    "esc_educ",
    "problemas_juridicos",
    "problemas_laborales",
    "suicidio_amigo",
]


# ----------------------------- utilidades -----------------------------
def _normalize_text(v: Union[str, float, None]) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    return s


def unify_missing(series: pd.Series, bad: Iterable[str] = BAD_SET) -> pd.Series:
    s = series.map(_normalize_text, na_action="ignore")
    mask = s.isna() | s.isin(set(bad))
    return s.where(~mask, "sin dato")


def standardize_sex(series: pd.Series) -> pd.Series:
    s = series.map(_normalize_text, na_action="ignore")
    s = s.replace({
        "h": "hombre", "m": "mujer",
        "masculino": "hombre", "femenino": "mujer",
        "male": "hombre", "female": "mujer",
    })
    s = s.where(s.isin({"hombre", "mujer", "sin dato"}), "otro").fillna("sin dato")
    return s


def normalize_clasif(series: pd.Series) -> pd.Series:
    s = series.map(_normalize_text, na_action="ignore").replace({
        "ideacion": "ideacion",
        "ideación": "ideacion",
        "ideacion suicida": "ideacion",
        "intento": "intento",
        "intento suicida": "intento",
    }).fillna("sin dato")
    return s.where(s.isin({"ideacion", "intento", "sin dato"}), "otra")


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
    return {
        "rows": int(len(df)),
        "nulls_by_col": df[key_cols].isna().sum().to_dict(),
        "sin_dato_by_col": {
            c: int((df[c] == "sin dato").sum())
            for c in key_cols
            if df[c].dtype == "object" or "string" in str(df[c].dtype)
        },
        "duplicates_by_key": int(df.duplicated(subset=list(key_cols), keep=False).sum()),
    }


# ----------------------------- pipeline -----------------------------
def transform_conductasuicida(
    sui_raw: pd.DataFrame,
    split_clasif: bool = False,
    drop_sin_dato_upz: bool = False,
    aggregate: bool = True,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    # 1) seleccionar y renombrar (ajusta si tus nombres difieren)
    expected = ["ano_notificacion", "nombre_upz", "sexo", "ciclovital", "niveleducativo"]
    cols = [c for c in expected if c in sui_raw.columns]
    if not set(["ano_notificacion", "nombre_upz"]).issubset(cols):
        missing = set(["ano_notificacion", "nombre_upz"]) - set(cols)
        raise ValueError(f"Faltan columnas requeridas: {sorted(missing)}")

    df = sui_raw[cols].copy().rename(columns={
        "ano_notificacion": "anio",
        "nombre_upz": "upz",
        "sexo": "sexo",
        "ciclovital": "ciclo_vida",
        "niveleducativo": "nivel_educativo",
    })

    # 2) normalización ligera
    df["upz"] = unify_missing(df["upz"])
    df["sexo"] = standardize_sex(df["sexo"])
    df["ciclo_vida"] = normalize_ciclo_vida(df["ciclo_vida"])
    df["nivel_educativo"] = normalize_nivel_educativo(df["nivel_educativo"])

    # 3) tipos y 'casos'
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["casos"] = 1

    # 4) factores -> 0/1
    present_risks = [c for c in RISK_COLS if c in sui_raw.columns]
    for c in present_risks:
        vals = pd.to_numeric(sui_raw[c], errors="coerce")
        df[c] = (vals.fillna(0) > 0).astype(int)

    # 5) clasificacion opcional
    if split_clasif:
        col_cand = next((c for c in ["clasificaciondelaconducta", "clasificacion_conducta", "clasificacion"]
                         if c in sui_raw.columns), None)
        df["clasificacion"] = normalize_clasif(sui_raw[col_cand]) if col_cand else "sin dato"

    # 6) limpieza estricta de la clave (sin NaN ni 'sin dato')
    key = ["anio", "upz", "sexo", "ciclo_vida", "nivel_educativo"]
    df = df.dropna(subset=key).copy()
    for c in ["upz", "sexo", "ciclo_vida", "nivel_educativo"]:
        s = df[c].astype("string").str.strip().str.lower()
        df = df.loc[~s.isin(BAD_SET)].copy()

    # 7) excluir upz = 'sin dato' (opcional; redundante si aplicaste limpieza estricta)
    if drop_sin_dato_upz:
        df = df.loc[df["upz"] != "sin dato"].copy()

    # 8) agregación
    group_key = key + (["clasificacion"] if split_clasif else [])
    # Compatible con Python 3.7 - usar {**dict1, **dict2} en lugar de dict1 | dict2
    agg = {**{"casos": "sum"}, **{c: "sum" for c in present_risks}}
    if aggregate:
        df = df.groupby(group_key, dropna=False, as_index=False).agg(agg)

    # 9) QC
    qc = quality_report(df, group_key)

    return df, qc


# ----------------------------- CLI -----------------------------
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Transformación Conductas Suicidas a formato tidy.")
    p.add_argument("--input", required=True, type=Path, help="Ruta del CSV de Conductas Suicidas.")
    p.add_argument("--output", required=True, type=Path, help="Ruta de salida para suicida_tidy.csv")
    p.add_argument("--split-clasif", action="store_true", help="Agrega 'clasificacion' (ideacion/intento/otra/sin dato).")
    p.add_argument("--drop-sin-dato", action="store_true", help="Excluye filas con upz = 'sin dato'.")
    p.add_argument("--no-group", action="store_true", help="No agregar (conserva filas).")
    p.add_argument("--report", type=Path, default=None, help="Ruta para guardar QC en JSON.")
    return p.parse_args()


def main():
    args = _parse_args()

    sui_raw = pd.read_csv(args.input)

    tidy, qc = transform_conductasuicida(
        sui_raw,
        split_clasif=args.split_clasif,
        drop_sin_dato_upz=args.drop_sin_dato,
        aggregate=not args.no_group,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    tidy.to_csv(args.output, index=False)
    print(f"[OK] Guardado: {args.output}  ({len(tidy)} filas)")

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(qc, f, ensure_ascii=False, indent=2)
        print(f"[OK] Reporte QC: {args.report}")
    else:
        print("QC:", qc)


if __name__ == "__main__":
    main()
