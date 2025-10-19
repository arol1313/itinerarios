# -*- coding: utf-8 -*-
"""
PLANIFICADOR_ABC.py
Uso rápido (CLI):
  python PLANIFICADOR_ABC.py A ADG_A_0156_01 ADG_A_0156_02 --plan-xlsx PLAN_As.xlsx
  python PLANIFICADOR_ABC.py B AFD_B_3003 ADG_B_3002 --plan-xlsx PLAN_Bs.xlsx
  (opcional) añade --misma-familia para restringir por familia
  (opcional) --export-master para exportar tablas maestras (ref_b, ref_c, map_b_a, map_c_b)

Requiere en la carpeta:
  - RDs_GradoA_Consolidado_por_familia.xlsx
  - RDs_GradoB_Consolidado_por_familia.xlsx
  - RDs_GradoC_Consolidado_por_familia.xlsx
"""

import argparse, re, sys
from pathlib import Path
import pandas as pd

F_A = "RDs_GradoA_Consolidado_por_familia.xlsx"
F_B = "RDs_GradoB_Consolidado_por_familia.xlsx"
F_C = "RDs_GradoC_Consolidado_por_familia.xlsx"

# ----------------- Helpers -----------------
def prefer_fix(path: str) -> str:
    p = Path(path); p_fix = p.with_name("fix_" + p.name)
    return str(p_fix if p_fix.exists() else p)

def nz(x, default=""):
    try:
        if not pd.api.types.is_scalar(x):
            return default
    except Exception:
        pass
    try:
        if pd.isna(x): return default
    except Exception:
        pass
    s = str(x).strip()
    return s if s else default

def to_int_or_blank(x):
    try:
        if pd.isna(x): return ""
        m = re.search(r"\d+", str(x))
        return int(m.group(0)) if m else ""
    except Exception:
        return ""

def dedup_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated()]

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\s+", " ", c).strip() for c in df.columns]
    df.columns = [c.lower() for c in df.columns]
    return df

def load_all_sheets(xlsx_path):
    xlsx_path = Path(xlsx_path)
    xl = pd.ExcelFile(xlsx_path)
    frames = []
    for sh in xl.sheet_names:
        df = xl.parse(sh); df["_sheet"] = sh
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def coalesce_cols(df, cols):
    out = pd.Series([""]*len(df), index=df.index, dtype=object)
    for c in cols:
        if c in df.columns:
            cand = df[c].astype(str).str.strip()
            out = out.where(out != "", cand.where(cand != "", out))
    return out

def first_nonempty(series):
    for x in series:
        sx = str(x).strip()
        if sx and sx.lower() not in ("nan", "none"):
            return sx
    return ""

def parse_nom_from_completo(s):
    s = nz(s)
    if not s: return ""
    m = re.match(r"^[A-Z]{3}_[A-Z]_\d{4}(?:_[0-9A-Z]+)?\.\s*(.+)$", s)
    return m.group(1).strip() if m else ""

def extract_cod_b(s):
    s = nz(s)
    if not s: return ""
    m = re.search(r"\b([A-Z]{3}_B_\d{4}(?:_[0-9A-Z]+)?)\b", s)
    return m.group(1) if m else ""

def ensure_cod_b(df: pd.DataFrame, col_candidates) -> pd.Series:
    cod = df.get("cod_b", pd.Series([""]*len(df)))
    cod = cod.astype(str).str.strip()
    if (cod == "").all():
        assembled = pd.Series([""]*len(df), index=df.index, dtype=object)
        for c in col_candidates:
            if c in df.columns:
                extra = df[c].astype(str).map(extract_cod_b)
                assembled = assembled.where(assembled != "", extra.where(extra != "", assembled))
        cod = assembled
    return cod.astype(str).str.strip()

# ----------------- Carga y normalización -----------------
def cargar_grado_A():
    dfA = load_all_sheets(prefer_fix(F_A))
    dfA = dedup_columns(normalize_columns(dfA))
    alias = {
        "familia":"familia",
        "cert_b_completo":"cert_b_completo",
        "cod_cert_b":"cod_b",
        "nom_cert_b":"nom_b",
        "acreditación parcial de competencia":"acred_parcial",
        "acreditacion parcial de competencia":"acred_parcial",
        "cod_acred_parc":"cod_a",
        "nom_acred_parcial":"nom_a",
        "formación a cursar":"ra_texto",
        "formacion a cursar":"ra_texto",
        "duración en el ámbito de gestión del mefd en horas":"horas_a",
        "duracion en el ambito de gestion del mefd en horas":"horas_a",
    }
    for k,v in alias.items():
        if k in dfA.columns: dfA.rename(columns={k:v}, inplace=True)
    for need in ["familia","cert_b_completo","cod_b","nom_b","acred_parcial","cod_a","nom_a","ra_texto","horas_a"]:
        if need not in dfA.columns: dfA[need] = ""
    # completar códigos B desde CERT_B_COMPLETO si faltan
    dfA["cod_b"] = dfA["cod_b"].astype(str).str.strip()
    mask = dfA["cod_b"] == ""
    dfA.loc[mask, "cod_b"] = dfA.loc[mask, "cert_b_completo"].map(extract_cod_b)
    dfA["cod_a"] = dfA["cod_a"].astype(str).str.strip()
    dfA["horas_a"] = dfA["horas_a"].map(to_int_or_blank)
    return dfA

def cargar_grado_B():
    dfB = load_all_sheets(prefer_fix(F_B))
    dfB = dedup_columns(normalize_columns(dfB))
    rename_min = {
        "familia":"familia",
        "cert_padre_cod":"cod_c",
        "cert_padre_denom":"nom_c",
        "cert_padre_completo":"cert_c_completo",
        "cod_cert_comp":"cod_b",
        "nom_cert_comp":"nom_b",
        "duracion_mefd_h":"horas_b",
        "formación a cursar":"form_b_texto",
        "formacion a cursar":"form_b_texto",
        "formacion_codigo":"form_b_codigo",
        "formacion_titulo":"form_b_titulo",
        "formación codigo":"form_b_codigo",
        "formación titulo":"form_b_titulo",
    }
    for k,v in rename_min.items():
        if k in dfB.columns: dfB.rename(columns={k:v}, inplace=True)

    # Asegurar cod_b
    dfB["cod_b"] = ensure_cod_b(dfB, col_candidates=[
        "cod_b","cert_c_completo","nom_b","cert_padre_completo","form_b_texto","form_b_titulo"
    ])

    # --- Construcción de nom_b (prioridades específicas a tus ficheros) ---
    # Prioridad: 1) cert_comp_titulo  2) nom_b  3) form_b_titulo  (nom_c NO se usa)
    posibles = []
    if "cert_comp_titulo" in dfB.columns: posibles.append("cert_comp_titulo")
    if "nom_b" in dfB.columns: posibles.append("nom_b")
    if "form_b_titulo" in dfB.columns: posibles.append("form_b_titulo")
    # Aliases habituales
    for alt in ["nom_cert_b", "denominacion_b", "denominación_b", "titulo_b", "título_b", "nombre_b"]:
        if alt in dfB.columns and alt not in posibles:
            posibles.append(alt)
    # Heurística (evitar nom_c que es del C)
    import re as _re
    regex_nom = _re.compile(r"(nom|denomin|t[ií]tulo|titulo|desc)", _re.I)
    for c in dfB.columns:
        lc = c.lower().strip()
        if lc == "nom_c":  # evitar mezclar nombre del C
            continue
        if c not in posibles and regex_nom.search(c):
            posibles.append(c)

    dfB["nom_b"] = coalesce_cols(dfB, posibles)
    dfB["nom_b"] = dfB["nom_b"].astype(str).str.strip()
    dfB.loc[dfB["nom_b"].str.lower().isin(["", "nan", "none"]), "nom_b"] = ""

    dfB["horas_b"] = dfB.get("horas_b", pd.Series([""]*len(dfB))).map(to_int_or_blank)
    dfB["cod_c"] = dfB.get("cod_c", pd.Series([""]*len(dfB))).astype(str).str.strip()
    dfB["cod_b"] = dfB["cod_b"].astype(str).str.strip()

    dfB = dedup_columns(dfB)
    return dfB

def cargar_grado_C():
    dfC = load_all_sheets(prefer_fix(F_C))
    dfC = dedup_columns(normalize_columns(dfC))
    alias = {"familia":"familia","denominacion":"nom_c","denominación":"nom_c","codigo":"cod_c","código":"cod_c","duracion":"horas_c","duración":"horas_c"}
    for k,v in alias.items():
        if k in dfC.columns: dfC.rename(columns={k:v}, inplace=True)
    dfC["cod_c"] = dfC.get("cod_c", pd.Series([""]*len(dfC))).astype(str).str.strip()
    dfC["horas_c"] = dfC.get("horas_c", pd.Series([""]*len(dfC))).map(to_int_or_blank)
    return dfC[["familia","cod_c","nom_c","horas_c"]].drop_duplicates()

# ----------------- Relaciones y referencias -----------------
def construir_mapas(dfA, dfB, dfC):
    map_b_a = (
        dfA[["cod_b","cod_a","nom_a","ra_texto","horas_a","familia"]]
        .dropna(subset=["cod_b","cod_a"])
        .query("cod_b != '' and cod_a != ''")
        .drop_duplicates()
    )
    map_c_b = (
        dfB[["cod_c","cod_b","familia"]]
        .dropna(subset=["cod_c","cod_b"])
        .query("cod_c != '' and cod_b != ''")
        .drop_duplicates()
    )

    # refs B (usar primer nombre no vacío)
    ref_b_B = (
        dfB.groupby("cod_b", as_index=False)
           .agg(nom_b=("nom_b", first_nonempty),
                horas_b=("horas_b","first"),
                familias=("familia", lambda s: ", ".join(sorted(set(nz(x) for x in s if nz(x))))))
    )

    # fallback nombre B desde A (CERT_B_COMPLETO)
    dfA_bnames = dfA[["cod_b","cert_b_completo"]].dropna().copy()
    dfA_bnames["fallback_nom_b"] = dfA_bnames["cert_b_completo"].map(parse_nom_from_completo)
    dfA_bnames = dfA_bnames[dfA_bnames["fallback_nom_b"]!=""].drop_duplicates(subset=["cod_b"])[["cod_b","fallback_nom_b"]]

    ref_b = ref_b_B.merge(dfA_bnames, on="cod_b", how="left")
    def pick_nom_b(row):
        cur = nz(row.get("nom_b"))
        if cur: return cur
        return nz(row.get("fallback_nom_b"))
    ref_b["nom_b"] = ref_b.apply(pick_nom_b, axis=1)
    ref_b.drop(columns=["fallback_nom_b"], inplace=True, errors="ignore")

    # refs C
    ref_c = (
        dfC.groupby("cod_c", as_index=False)
           .agg(nom_c=("nom_c","first"),
                horas_c=("horas_c","first"),
                familias=("familia", lambda s: ", ".join(sorted(set(nz(x) for x in s if nz(x))))))
    )
    return map_b_a, map_c_b, ref_b, ref_c

# ----------------- Planificación -----------------
def _list_b_for_as(map_b_a, misma_familia, familias_usuario):
    b_to_as, horas_a, fam_b = {}, {}, {}
    for _, r in map_b_a.iterrows():
        b = nz(r.get("cod_b")); a = nz(r.get("cod_a")); fam = nz(r.get("familia"))
        if not b or not a: continue
        if misma_familia and familias_usuario and fam and fam not in familias_usuario:
            continue
        b_to_as.setdefault(b, set()).add(a)
        fam_b.setdefault(b, set()).add(fam)
        try:
            h = int(r.get("horas_a")); 
            if a and h: horas_a[a] = h
        except Exception: pass
    fam_b = {k: ", ".join(sorted(v - {""})) for k, v in fam_b.items()}
    return b_to_as, horas_a, fam_b

def _list_c_for_bs(map_c_b, ref_b, misma_familia, familias_usuario):
    c_to_bs, fam_c, horas_b = {}, {}, {}
    for _, r in map_c_b.iterrows():
        c = nz(r.get("cod_c")); b = nz(r.get("cod_b")); fam = nz(r.get("familia"))
        if not c or not b: continue
        if misma_familia and familias_usuario and fam and fam not in familias_usuario:
            continue
        c_to_bs.setdefault(c, set()).add(b)
        fam_c.setdefault(c, set()).add(fam)
    fam_c = {k: ", ".join(sorted(v - {""})) for k, v in fam_c.items()}
    for _, r in ref_b.iterrows():
        b = nz(r.get("cod_b"))
        try:
            h = int(r.get("horas_b"))
            if b and h: horas_b[b] = h
        except Exception: pass
    return c_to_bs, horas_b, fam_c

def plan_desde_As(cods_a_usuario, map_b_a, map_c_b, ref_b, ref_c, misma_familia=False):
    cods_a_usuario = {nz(x) for x in cods_a_usuario if nz(x)}
    fams_usuario = set(map_b_a[map_b_a["cod_a"].isin(cods_a_usuario)]["familia"].dropna().map(str).map(str.strip))
    b_to_as, horas_a, fam_b = _list_b_for_as(map_b_a, misma_familia, fams_usuario if misma_familia else set())
    ref_b_idx = ref_b.set_index("cod_b", drop=False); ref_c_idx = ref_c.set_index("cod_c", drop=False)

    rows_b = []
    for b, req_as in b_to_as.items():
        cubiertas = sorted(req_as & cods_a_usuario)
        faltan = sorted(req_as - cods_a_usuario)
        horas_pend = sum(horas_a.get(a, 0) for a in faltan)
        cobertura = 100.0 * (len(cubiertas) / len(req_as)) if req_as else 0.0
        nom_b = nz(ref_b_idx.at[b, "nom_b"] if b in ref_b_idx.index else "")
        h_b   = nz(ref_b_idx.at[b, "horas_b"] if b in ref_b_idx.index else "")
        fams_b= nz(ref_b_idx.at[b, "familias"] if b in ref_b_idx.index else "")
        rows_b.append({
            "cod_b": b, "nom_b": nom_b, "familias_b": fams_b, "horas_b": h_b,
            "a_requeridas": ", ".join(sorted(req_as)),
            "a_cubiertas": ", ".join(cubiertas),
            "a_faltan": ", ".join(faltan),
            "horas_pendientes": horas_pend, "cobertura_pct": round(cobertura, 2),
        })
    df_b = pd.DataFrame(rows_b).sort_values(["cobertura_pct","cod_b"], ascending=[False, True])

    c_to_bs, horas_b, fam_c = _list_c_for_bs(map_c_b, ref_b, misma_familia, fams_usuario if misma_familia else set())
    rows_c = []
    for c, req_bs in c_to_bs.items():
        nom_c = nz(ref_c_idx.at[c, "nom_c"] if c in ref_c_idx.index else "")
        fams_c= nz(ref_c_idx.at[c, "familias"] if c in ref_c_idx.index else "")
        rows_c.append({
            "cod_c": c, "nom_c": nom_c, "familias_c": fams_c,
            "b_requeridos": ", ".join(sorted(req_bs)),
        })
    df_c = pd.DataFrame(rows_c).sort_values("cod_c")
    return df_b, df_c

def plan_desde_Bs(cods_b_usuario, map_b_a, map_c_b, ref_b, ref_c, misma_familia=False):
    cods_b_usuario = {nz(x) for x in cods_b_usuario if nz(x)}
    fams_usuario = set(map_c_b[map_c_b["cod_b"].isin(cods_b_usuario)]["familia"].dropna().map(str).map(str.strip))
    c_to_bs, horas_b, fam_c = _list_c_for_bs(map_c_b, ref_b, misma_familia, fams_usuario if misma_familia else set())
    ref_c_idx = ref_c.set_index("cod_c", drop=False)
    b_to_as = map_b_a.groupby("cod_b")["cod_a"].apply(lambda s: sorted(set([nz(x) for x in s if nz(x)])))

    rows_c, rows_b_detalle = [], []
    for c, req_bs in c_to_bs.items():
        cubiertos = sorted(req_bs & cods_b_usuario)
        faltan = sorted(req_bs - cods_b_usuario)
        horas_pend = sum(horas_b.get(b, 0) for b in faltan)
        nom_c = nz(ref_c_idx.at[c, "nom_c"] if c in ref_c_idx.index else "")
        fams_c= nz(ref_c_idx.at[c, "familias"] if c in ref_c_idx.index else "")
        rows_c.append({
            "cod_c": c, "nom_c": nom_c, "familias_c": fams_c,
            "b_requeridos": ", ".join(sorted(req_bs)),
            "b_cubiertos": ", ".join(cubiertos),
            "b_faltan": ", ".join(faltan),
            "horas_pendientes_b": horas_pend,
        })
        for b in faltan:
            req_as = b_to_as.get(b, [])
            rows_b_detalle.append({
                "cod_c": c, "cod_b_faltante": b,
                "a_requeridas_para_b": ", ".join(req_as),
                "horas_b_estimada": horas_b.get(b, "")
            })
    df_c = pd.DataFrame(rows_c).sort_values("cod_c")
    df_b_detalle = pd.DataFrame(rows_b_detalle).sort_values(["cod_c","cod_b_faltante"])
    return df_c, df_b_detalle

# ----------------- CLI -----------------
def main():
    ap = argparse.ArgumentParser(description="Planificador A→B→C (CLI)")
    ap.add_argument("nivel", nargs="?", choices=["A","B"], help="Punto de partida (A o B)")
    ap.add_argument("codigos", nargs="*", help="Códigos de partida (A o B)")
    ap.add_argument("--misma-familia", action="store_true", help="Restringe por misma familia")
    ap.add_argument("--plan-xlsx", default="", help="Ruta de salida Excel del plan")
    ap.add_argument("--export-master", action="store_true", help="Exporta ref_b, ref_c, map_b_a, map_c_b")
    args = ap.parse_args()

    # Permitir solo exportar maestros
    if args.export_master and not args.nivel:
        dfA = cargar_grado_A(); dfB = cargar_grado_B(); dfC = cargar_grado_C()
        map_b_a, map_c_b, ref_b, ref_c = construir_mapas(dfA, dfB, dfC)
        out = Path("MASTER_RELACIONES.xlsx")
        with pd.ExcelWriter(out, engine="openpyxl") as w:
            map_b_a.to_excel(w, sheet_name="map_b_a", index=False)
            map_c_b.to_excel(w, sheet_name="map_c_b", index=False)
            ref_b.to_excel(w, sheet_name="ref_b", index=False)
            ref_c.to_excel(w, sheet_name="ref_c", index=False)
        print(f"[OK] Exportado {out}")
        return 0

    if not args.nivel:
        ap.print_help()
        print("\nEjemplos:")
        print("  python PLANIFICADOR_ABC.py A ADG_A_0156_01 ADG_A_0156_02 --plan-xlsx PLAN_As.xlsx")
        print("  python PLANIFICADOR_ABC.py B AFD_B_3003 ADG_B_3002 --plan-xlsx PLAN_Bs.xlsx")
        return 1

    dfA = cargar_grado_A()
    dfB = cargar_grado_B()
    dfC = cargar_grado_C()
    map_b_a, map_c_b, ref_b, ref_c = construir_mapas(dfA, dfB, dfC)

    if args.nivel == "A":
        cods_a = set(args.codigos)
        df_b, df_c = plan_desde_As(cods_a, map_b_a, map_c_b, ref_b, ref_c, misma_familia=args.misma_familia)
        print("\n=== B alcanzables desde A ===")
        print(df_b.head(50).to_string(index=False))
        print("\n=== C relacionados (requisitos B) ===")
        print(df_c.head(50).to_string(index=False))
        if args.plan_xlsx:
            with pd.ExcelWriter(args.plan_xlsx, engine="openpyxl") as w:
                df_b.to_excel(w, sheet_name="B_posibles", index=False)
                df_c.to_excel(w, sheet_name="C_relacionados", index=False)
            print(f"[OK] Guardado plan en {args.plan_xlsx}")
    else:
        cods_b = set(args.codigos)
        df_c, df_b_det = plan_desde_Bs(cods_b, map_b_a, map_c_b, ref_b, ref_c, misma_familia=args.misma_familia)
        print("\n=== C alcanzables desde B ===")
        print(df_c.head(50).to_string(index=False))
        print("\n=== Detalle de A requeridas para B faltantes ===")
        print(df_b_det.head(50).to_string(index=False))
        if args.plan_xlsx:
            with pd.ExcelWriter(args.plan_xlsx, engine="openpyxl") as w:
                df_c.to_excel(w, sheet_name="C_posibles", index=False)
                df_b_det.to_excel(w, sheet_name="Detalle_A_para_B_falt", index=False)
            print(f"[OK] Guardado plan en {args.plan_xlsx}")

if __name__ == "__main__":
    sys.exit(main())
