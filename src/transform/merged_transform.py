# merge_lib.py
# -*- coding: utf-8 -*-
"""
Funciones para unir SPA (consumo) y Conductas Suicidas ya transformados (tidy).

Requiere que ambos DF tengan al menos las columnas de la clave:
  anio, upz, sexo, ciclo_vida, nivel_educativo
SPA debe tener 'casos' (o 'casos_spa'); Suicidas, 'casos' (o 'casos_sui').

API principal:
  merged, qc = merge_spa_sui(spa_df, sui_df, how="inner", with_clasif=False, add_pcts=True)

Donde:
  - how: inner/left/right/outer
  - with_clasif: si True y ambos DF traen 'clasificacion', se incluye en la clave
  - add_pcts: agrega porcentajes derivados (sitios/factores)

QC incluye filas sin match por lado, filas totales, y la clave usada.
"""

from __future__ import annotations
from typing import Dict, List, Tuple
import unicodedata
import pandas as pd

KEY_BASE = ["anio", "upz", "sexo", "ciclo_vida", "nivel_educativo"]

RISK_COLS = [
    "enfermedades_dolorosas","maltrato_sexual","muerte_familiar","conflicto_pareja",
    "problemas_economicos","esc_educ","problemas_juridicos","problemas_laborales","suicidio_amigo"
]

def _normalize_text(s: pd.Series) -> pd.Series:
    """minúsculas, sin acentos y trim (defensivo)."""
    if s.dtype.name.startswith(("Int", "int", "Float", "float")):
        return s
    x = s.astype("string").str.strip().str.lower()
    return x.map(lambda v: unicodedata.normalize("NFKD", v).encode("ascii","ignore").decode("utf-8") if v is not None else v)

def _prepare_spa(spa: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    df = spa.copy()
    # normaliza clave
    for c in [c for c in KEY_BASE if c in df.columns]:
        df[c] = _normalize_text(df[c])
    # renombra casos
    if "casos" in df.columns and "casos_spa" not in df.columns:
        df = df.rename(columns={"casos":"casos_spa"})
    # detectar sitios
    site_cols = [c for c in df.columns if c.startswith("SITIOHABITUALCONSUMO_")]
    # tipos
    if "casos_spa" in df.columns:
        df["casos_spa"] = pd.to_numeric(df["casos_spa"], errors="coerce").fillna(0).astype(int)
    for c in site_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df, site_cols

def _prepare_sui(sui: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    df = sui.copy()
    # normaliza clave
    for c in [c for c in KEY_BASE if c in df.columns]:
        df[c] = _normalize_text(df[c])
    if "clasificacion" in df.columns:
        df["clasificacion"] = _normalize_text(df["clasificacion"])
    # renombra casos
    if "casos" in df.columns and "casos_sui" not in df.columns:
        df = df.rename(columns={"casos":"casos_sui"})
    # detectar riesgos
    risk_cols = [c for c in RISK_COLS if c in df.columns]
    # tipos
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

def merge_spa_sui(
    spa_df: pd.DataFrame,
    sui_df: pd.DataFrame,
    *,
    how: str = "inner",
    with_clasif: bool = False,
    add_pcts: bool = True
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """
    Une SPA y Suicidas (ambos tidy). Devuelve (merged, qc).
    """
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
        # riesgos sobre total suicidas
        if "casos_sui" in merged.columns:
            denom_sui = merged["casos_sui"].replace({0: pd.NA})
            for c in risk_cols:
                if c in merged.columns:
                    merged[f"pct_{c}"] = (merged[c] / denom_sui).astype("Float64").round(4)
        # sitios sobre total SPA
        if "casos_spa" in merged.columns:
            denom_spa = merged["casos_spa"].replace({0: pd.NA})
            for c in site_cols:
                if c in merged.columns:
                    merged[f"pct_{c.lower()}"] = (merged[c] / denom_spa).astype("Float64").round(4)

    qc = _qc_merge(spa, sui, merged, key)
    return merged, qc

# -------- helpers opcionales para DAG/IO --------
def load_and_merge(
    spa_csv: str,
    sui_csv: str,
    *,
    how: str = "inner",
    with_clasif: bool = False,
    add_pcts: bool = True
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """Carga CSVs y llama merge_spa_sui()."""
    spa = pd.read_csv(spa_csv)
    sui = pd.read_csv(sui_csv)
    return merge_spa_sui(spa, sui, how=how, with_clasif=with_clasif, add_pcts=add_pcts)
