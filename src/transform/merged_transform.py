import argparse
import json
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple, Union

import pandas as pd

KEY_BASE = ["anio", "upz", "sexo", "ciclo_vida", "nivel_educativo"]

RISK_COLS = [
    "enfermedades_dolorosas","maltrato_sexual","muerte_familiar","conflicto_pareja",
    "problemas_economicos","esc_educ","problemas_juridicos","problemas_laborales","suicidio_amigo"
]

# ----------------- utilidades internas -----------------
def _normalize_text(s: pd.Series) -> pd.Series:
    """minúsculas, sin acentos y trim (defensivo)."""
    if s.dtype.name.startswith(("Int", "int", "Float", "float")):
        return s
    x = s.astype("string").str.strip().str.lower()
    return x.map(lambda v: unicodedata.normalize("NFKD", v).encode("ascii","ignore").decode("utf-8") if v is not None else v)

def _prepare_spa(spa: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    df = spa.copy()
    for c in [c for c in KEY_BASE if c in df.columns]:
        df[c] = _normalize_text(df[c])
    if "clasificacion" in df.columns:
        df["clasificacion"] = _normalize_text(df["clasificacion"])
    if "casos" in df.columns and "casos_spa" not in df.columns:
        df = df.rename(columns={"casos":"casos_spa"})
    site_cols = [c for c in df.columns if c.startswith("SITIOHABITUALCONSUMO_")]
    if "casos_spa" in df.columns:
        df["casos_spa"] = pd.to_numeric(df["casos_spa"], errors="coerce").fillna(0).astype(int)
    for c in site_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df, site_cols

def _prepare_sui(sui: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    df = sui.copy()
    for c in [c for c in KEY_BASE if c in df.columns]:
        df[c] = _normalize_text(df[c])
    if "clasificacion" in df.columns:
        df["clasificacion"] = _normalize_text(df["clasificacion"])
    if "casos" in df.columns and "casos_sui" not in df.columns:
        df = df.rename(columns={"casos":"casos_sui"})
    risk_cols = [c for c in RISK_COLS if c in df.columns]
    if "casos_sui" in df.columns:
        df["casos_sui"] = pd.to_numeric(df["casos_sui"], errors="coerce").fillna(0).astype(int)
    for c in risk_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df, risk_cols

def _qc_merge(spa: pd.DataFrame, sui: pd.DataFrame, merged: pd.DataFrame, key: List[str]) -> Dict[str, object]:
    spa_only = pd.merge(spa[key], sui[key].drop_duplicates(), on=key, how="left", indicator=True)
    sui_only = pd.merge(sui[key], spa[key].drop_duplicates(), on=key, how="left", indicator=True)
    return {
        "rows_spa": int(len(spa)),
        "rows_sui": int(len(sui)),
        "rows_merged": int(len(merged)),
        "spa_without_match": int((spa_only["_merge"]=="left_only").sum()),
        "sui_without_match": int((sui_only["_merge"]=="left_only").sum()),
        "key_used": key,
    }

def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

# ----------------- API principal (para DAG/notebook) -----------------
def merge_spa_sui(
    spa_df: pd.DataFrame,
    sui_df: pd.DataFrame,
    *,
    how: str = "inner",
    with_clasif: bool = False,
    add_pcts: bool = True
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """Une SPA y Suicidas (ambos tidy). Devuelve (merged, qc)."""
    spa, site_cols = _prepare_spa(spa_df)
    sui, risk_cols = _prepare_sui(sui_df)

    key = KEY_BASE.copy()
    if with_clasif and ("clasificacion" in spa.columns) and ("clasificacion" in sui.columns):
        key += ["clasificacion"]

    merged = pd.merge(spa, sui, on=key, how=how, suffixes=("_spa","_sui"))

    # relleno defensivo
    for c in site_cols:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0).astype(int)
    for c in risk_cols:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0).astype(int)
    for c in ["casos_spa","casos_sui"]:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0).astype(int)

    # porcentajes derivados (opcional)
    if add_pcts:
        if "casos_sui" in merged.columns:
            denom_sui = merged["casos_sui"].replace({0: pd.NA})
            for c in risk_cols:
                if c in merged.columns:
                    merged[f"pct_{c}"] = (merged[c] / denom_sui).astype("Float64").round(4)
        if "casos_spa" in merged.columns:
            denom_spa = merged["casos_spa"].replace({0: pd.NA})
            for c in site_cols:
                if c in merged.columns:
                    merged[f"pct_{c.lower()}"] = (merged[c] / denom_spa).astype("Float64").round(4)

    qc = _qc_merge(spa, sui, merged, key)
    return merged, qc

# ----------------- Helper IO para uso simple -----------------
def load_and_merge(
    spa_csv: Union[str, Path],
    sui_csv: Union[str, Path],
    *,
    how: str = "inner",
    with_clasif: bool = False,
    add_pcts: bool = True
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    spa = _load_csv(Path(spa_csv))
    sui = _load_csv(Path(sui_csv))
    return merge_spa_sui(spa, sui, how=how, with_clasif=with_clasif, add_pcts=add_pcts)

# ----------------- MAIN (para correr local por CLI) -----------------
def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Merge SPA y Conductas Suicidas (salidas tidy).")
    ap.add_argument("--spa", required=True, type=Path, help="CSV tidy de SPA.")
    ap.add_argument("--sui", required=True, type=Path, help="CSV tidy de Conductas Suicidas.")
    ap.add_argument("--output", required=True, type=Path, help="CSV de salida del merge.")
    ap.add_argument("--how", choices=["inner","left","right","outer"], default="inner", help="Tipo de merge (default: inner).")
    ap.add_argument("--with-clasif", action="store_true", help="Usar 'clasificacion' en la clave si existe en ambos.")
    ap.add_argument("--add-pcts", action="store_true", help="Agregar columnas de porcentajes derivados.")
    ap.add_argument("--report", type=Path, default=None, help="Guardar JSON con QC del merge.")
    return ap.parse_args()

def main():
    args = _parse_args()

    merged, qc = load_and_merge(
        spa_csv=args.spa,
        sui_csv=args.sui,
        how=args.how,
        with_clasif=args.with_clasif,
        add_pcts=args.add_pcts,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.output, index=False, encoding="utf-8")
    print(f"[OK] Merge guardado: {args.output}  ({len(merged)} filas)")
    print("Clave usada:", qc["key_used"])

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(qc, f, ensure_ascii=False, indent=2)
        print(f"[OK] Reporte QC: {args.report}")
    else:
        print("QC:", qc)

if __name__ == "__main__":
    main()
