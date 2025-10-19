# app_planificador_abc.py
# -*- coding: utf-8 -*-
import re, io, traceback, unicodedata, html
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Iterable, Tuple
import pandas as pd
import streamlit as st

# === Integración ESCO PRO ===
try:
    from esco_mapper_pro import map_competencias_a_esco_pro, esco_search_skills
except Exception:
    map_competencias_a_esco_pro = None
    esco_search_skills = None

# (Opcional) módulo externo; si no existe, devolvemos vacíos
try:
    from perfil_competencias import perfil_competencias
except Exception:
    def perfil_competencias(*args, **kwargs):
        # comp_A, comp_B, comp_C
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

# ========= Config =========
F_A = "RDs_GradoA_Consolidado_por_familia.xlsx"
F_B = "RDs_GradoB_Consolidado_por_familia.xlsx"
F_C = "RDs_GradoC_Consolidado_por_familia.xlsx"

def prefer_fix(path: str) -> str:
    p = Path(path); p_fix = p.with_name("fix_" + p.name)
    return str(p_fix if p_fix.exists() else p)

# ========= Helpers =========
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

def download_excel_button(dfs_dict: Dict[str, pd.DataFrame], filename: str, label: str):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        for sheet, df in dfs_dict.items():
            (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_excel(
                w, sheet_name=sheet[:31], index=False
            )
    st.download_button(
        label=label,
        data=bio.getvalue(),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

def export_master_with_timestamp(dfs_dict: Dict[str, pd.DataFrame], base_name: str = "PLAN_MAESTRO"):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{base_name}_{ts}.xlsx"
    download_excel_button(dfs_dict, filename, f"💾 Exportar maestro ({filename})")

def extract_codes(df: pd.DataFrame, col: str) -> str:
    if df is None or len(df) == 0 or col not in df.columns:
        return ""
    vals = sorted({nz(x) for x in df[col].tolist() if nz(x)})
    return " ".join(vals)

def copy_to_clipboard_button(label: str, text: str, key: str):
    import streamlit.components.v1 as components
    safe_text = html.escape(text, quote=True)
    btn_html = f"""
    <button id="copybtn_{key}" style="
        padding:0.5rem 0.75rem;border-radius:8px;border:1px solid #e0e0e0;
        cursor:pointer;background:#f7f7f7;">{html.escape(label)}</button>
    <span id="copystate_{key}" style="margin-left:8px;color:#666;"></span>
    <script>
    const btn_{key} = document.getElementById("copybtn_{key}");
    const st_{key} = document.getElementById("copystate_{key}");
    if (btn_{key}) {{
        btn_{key}.onclick = async () => {{
            try {{
                await navigator.clipboard.writeText("{safe_text}");
                st_{key}.textContent = "copiado ✓";
                setTimeout(()=>{{ st_{key}.textContent=""; }}, 1800);
            }} catch(e) {{
                st_{key}.textContent = "no se pudo copiar";
                setTimeout(()=>{{ st_{key}.textContent=""; }}, 2500);
            }}
        }}
    }}
    </script>
    """
    components.html(btn_html, height=40)

# ======== Normalización de familias ========
def norm_txt(s: str) -> str:
    s = nz(s)
    if not s:
        return ""
    s = " ".join(s.split())
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.casefold()

def canonicalize_families(series_iterables: Iterable[pd.Series]):
    by_norm = {}
    for ser in series_iterables:
        for raw in ser:
            f = nz(raw)
            if not f:
                continue
            k = norm_txt(f)
            by_norm.setdefault(k, []).append(f)
    canon = {}
    for k, vals in by_norm.items():
        freq = {}
        for v in vals:
            freq[v] = freq.get(v, 0) + 1
        best = sorted(freq.items(), key=lambda x: (x[1], len(x[0])), reverse=True)[0][0]
        canon[k] = best
    etiquetas = sorted(set(canon.values()))
    return etiquetas, canon

# ========= Estado global =========
if "export_pack" not in st.session_state:
    st.session_state["export_pack"] = {}
if "esco_queue" not in st.session_state:
    st.session_state["esco_queue"] = []
if "last_frames" not in st.session_state:
    st.session_state["last_frames"] = {}
if "esco_last" not in st.session_state:
    st.session_state["esco_last"] = {
        "df_s": pd.DataFrame(),
        "df_o": pd.DataFrame(),
        "df_unificado": pd.DataFrame(),
        "metrics": {}
    }

def set_export_sheet(name: str, df: pd.DataFrame):
    if df is None: return
    st.session_state["export_pack"][name] = df.copy()

def get_export_pack() -> Dict[str, pd.DataFrame]:
    return dict(st.session_state.get("export_pack", {}))

def set_last(name: str, df: pd.DataFrame):
    st.session_state["last_frames"][name] = df.copy() if df is not None else None

def get_last(name: str) -> pd.DataFrame:
    return st.session_state["last_frames"].get(name)

def add_to_esco_queue(texts: List[str], source_tag: str):
    seen = set([t.strip() for t in st.session_state["esco_queue"]])
    added = 0
    for t in texts:
        s = nz(t)
        if not s: continue
        if s not in seen:
            st.session_state["esco_queue"].append(s)
            seen.add(s)
            added += 1
    if added > 0:
        st.success(f"Se añadieron {added} elementos a la bandeja ESCO ({source_tag}).")
    else:
        st.info("Nada nuevo que añadir a la bandeja ESCO.")

def get_esco_queue_df() -> pd.DataFrame:
    items = st.session_state.get("esco_queue", [])
    if not items:
        return pd.DataFrame()
    return pd.DataFrame({"texto": [str(x).strip() for x in items if str(x).strip()]})

# ========= Carga & normalización =========
@st.cache_data(show_spinner=False)
def cargar_grado_A():
    dfA = pd.DataFrame()
    xl = pd.ExcelFile(prefer_fix(F_A))
    parts = []
    for sh in xl.sheet_names:
        tmp = xl.parse(sh); tmp["_sheet"] = sh; parts.append(tmp)
    if parts: dfA = pd.concat(parts, ignore_index=True)
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
    for need in ["familia","cert_b_completo","cod_b","nom_b","acred_parcial","cod_a","nom_a","ra_texto","horas_a","fuente_url","rd_id","rd_num","fecha_rd"]:
        if need not in dfA.columns: dfA[need] = ""
    dfA["cod_b"] = dfA["cod_b"].astype(str).str.strip()
    mask = dfA["cod_b"] == ""
    dfA.loc[mask, "cod_b"] = dfA.loc[mask, "cert_b_completo"].map(parse_nom_from_completo).map(lambda _: extract_cod_b(_))
    dfA["cod_a"] = dfA["cod_a"].astype(str).str.strip()
    dfA["horas_a"] = dfA["horas_a"].map(to_int_or_blank)
    return dfA

@st.cache_data(show_spinner=False)
def cargar_grado_B():
    dfB = pd.DataFrame()
    xl = pd.ExcelFile(prefer_fix(F_B))
    parts = []
    for sh in xl.sheet_names:
        tmp = xl.parse(sh); tmp["_sheet"] = sh; parts.append(tmp)
    if parts: dfB = pd.concat(parts, ignore_index=True)

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
    }
    for k,v in rename_min.items():
        if k in dfB.columns: dfB.rename(columns={k:v}, inplace=True)

    dfB["cod_b"] = ensure_cod_b(dfB, col_candidates=[
        "cod_b","cert_c_completo","nom_b","cert_padre_completo","form_b_texto","form_b_titulo"
    ])

    posibles = []
    if "cert_comp_titulo" in dfB.columns: posibles.append("cert_comp_titulo")
    if "nom_b" in dfB.columns: posibles.append("nom_b")
    if "form_b_titulo" in dfB.columns: posibles.append("form_b_titulo")
    for alt in ["nom_cert_b", "denominacion_b", "denominación_b", "titulo_b", "título_b", "nombre_b"]:
        if alt in dfB.columns and alt not in posibles:
            posibles.append(alt)
    import re as _re
    regex_nom = _re.compile(r"(nom|denomin|t[ií]tulo|titulo|desc)", _re.I)
    for c in dfB.columns:
        lc = c.lower().strip()
        if lc == "nom_c":
            continue
        if c not in posibles and regex_nom.search(c):
            posibles.append(c)

    dfB["nom_b"] = coalesce_cols(dfB, posibles).astype(str).str.strip()
    dfB.loc[dfB["nom_b"].str.lower().isin(["", "nan", "none"]), "nom_b"] = ""

    for col in ["fuente_url","rd_id","rd_num","fecha_rd"]:
        if col not in dfB.columns: dfB[col] = ""

    dfB["horas_b"] = dfB.get("horas_b", pd.Series([""]*len(dfB))).map(to_int_or_blank)
    dfB["cod_c"] = dfB.get("cod_c", pd.Series([""]*len(dfB))).astype(str).str.strip()
    dfB["cod_b"] = dfB["cod_b"].astype(str).str.strip()

    dfB = dedup_columns(dfB)
    return dfB

@st.cache_data(show_spinner=False)
def cargar_grado_C():
    dfC = pd.DataFrame()
    xl = pd.ExcelFile(prefer_fix(F_C))
    parts = [xl.parse(sh).assign(_sheet=sh) for sh in xl.sheet_names]
    if parts: dfC = pd.concat(parts, ignore_index=True)

    dfC = dedup_columns(normalize_columns(dfC))
    alias = {"familia":"familia","denominacion":"nom_c","denominación":"nom_c",
             "codigo":"cod_c","código":"cod_c","duracion":"horas_c","duración":"horas_c"}
    for k,v in alias.items():
        if k in dfC.columns: dfC.rename(columns={k:v}, inplace=True)

    for col in ["comp_general","comp_prof","codigo_comp","estandar_comp","estándar_comp","modulos_prof","modulos_profesionales","fuente_url","rd_id","rd_num","fecha_rd"]:
        if col not in dfC.columns: dfC[col] = ""

    nivel_col = None
    for cand in ["nivel","nivel_c","level","nivel (c)","nivel c"]:
        if cand in dfC.columns:
            nivel_col = cand; break
    if nivel_col is None:
        dfC["nivel_c"] = ""
    else:
        dfC["nivel_c"] = dfC[nivel_col]

    dfC["cod_c"] = dfC.get("cod_c", pd.Series([""]*len(dfC))).astype(str).str.strip()
    dfC["horas_c"] = dfC.get("horas_c", pd.Series([""]*len(dfC))).map(to_int_or_blank)

    def norm_level(x):
        s = nz(x).lower()
        m = re.search(r"\d+", s)
        return m.group(0) if m else ("Desconocido" if s=="" else s)
    dfC["nivel_c"] = dfC["nivel_c"].map(norm_level)

    return dfC[["familia","cod_c","nom_c","horas_c","nivel_c","comp_general","comp_prof","codigo_comp","estandar_comp","fuente_url","rd_id","rd_num","fecha_rd"]].drop_duplicates()

# ========= Relaciones y refs + Niveles inferidos =========
@st.cache_data(show_spinner=False)
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

    ref_c = (
        dfC.groupby("cod_c", as_index=False)
           .agg(nom_c=("nom_c","first"),
                horas_c=("horas_c","first"),
                familias=("familia", lambda s: ", ".join(sorted(set(nz(x) for x in s if nz(x))))),
                nivel_c=("nivel_c", first_nonempty),
                comp_general=("comp_general", first_nonempty),
                comp_prof=("comp_prof", first_nonempty))
    )

    b2niveles = (map_c_b.merge(dfC[["cod_c","nivel_c"]], on="cod_c", how="left")
                      .groupby("cod_b")["nivel_c"]
                      .apply(lambda s: sorted(set([nz(x) for x in s if nz(x)])))
                      .to_dict())
    def pick_b_level(levels):
        if not levels: return "Desconocido"
        uniq = [x for x in levels if x]
        return "/".join(uniq) if len(uniq)>1 else uniq[0]

    ref_b_B = (
        dfB.groupby("cod_b", as_index=False)
           .agg(nom_b=("nom_b", first_nonempty),
                horas_b=("horas_b","first"),
                familias=("familia", lambda s: ", ".join(sorted(set(nz(x) for x in s if nz(x))))))
    )
    dfA_bnames = dfA[["cod_b","cert_b_completo"]].dropna().copy()
    dfA_bnames["fallback_nom_b"] = dfA_bnames["cert_b_completo"].map(parse_nom_from_completo)
    dfA_bnames = dfA_bnames[dfA_bnames["fallback_nom_b"]!=""].drop_duplicates(subset=["cod_b"])[["cod_b","fallback_nom_b"]]

    ref_b = ref_b_B.merge(dfA_bnames, on="cod_b", how="left")
    ref_b["nom_b"] = ref_b.apply(lambda r: nz(r["nom_b"]) or nz(r["fallback_nom_b"]), axis=1)
    ref_b.drop(columns=["fallback_nom_b"], inplace=True, errors="ignore")
    ref_b["nivel_b"] = ref_b["cod_b"].map(lambda cb: pick_b_level(b2niveles.get(cb, [])))

    a2niveles = (map_b_a.merge(ref_b[["cod_b","nivel_b"]], on="cod_b", how="left")
                      .groupby("cod_a")["nivel_b"]
                      .apply(lambda s: sorted(set([nz(x) for x in s if nz(x)])))
                      .to_dict())
    return map_b_a, map_c_b, ref_b, ref_c, a2niveles

# ========= UI =========
st.set_page_config(page_title="Planificador A ↔ B ↔ C + ESCO (PRO)", layout="wide")
st.title("Planificador de itinerarios: A → B → C (multi-familia) + Niveles + Perfil + ESCO (PRO)")

with st.sidebar:
    st.header("Fuentes de datos")
    st.caption("Se usan automáticamente ficheros fix_* si existen.")
    st.write(f"- A: `{prefer_fix(F_A)}`")
    st.write(f"- B: `{prefer_fix(F_B)}`")
    st.write(f"- C: `{prefer_fix(F_C)}`")

    misma_familia = not st.checkbox("Ignorar familia en las vinculaciones", value=True)
    exigir_mismo_nivel = st.checkbox("Exigir mismo nivel (A→B y B→C)", value=True)
    debug_mode = st.toggle("Mostrar trazas de error (debug)", value=False)

# Carga + familias/niveles
try:
    dfA = cargar_grado_A(); dfB = cargar_grado_B(); dfC = cargar_grado_C()
    map_b_a, map_c_b, ref_b, ref_c, a2niveles = construir_mapas(dfA, dfB, dfC)

    familias_all, fam_canon_map = canonicalize_families([
        dfA.get("familia", pd.Series()).astype(str),
        dfB.get("familia", pd.Series()).astype(str),
        dfC.get("familia", pd.Series()).astype(str),
    ])
    niveles_all = sorted({nz(x) for x in ref_c.get("nivel_c", pd.Series())} - {""}) or ["Desconocido"]

    with st.sidebar:
        st.header("Filtros previos")
        fam_sel = st.multiselect("Familias (opcional)", familias_all, default=[])
        niv_sel = st.multiselect("Niveles (opcional)", niveles_all, default=[])

    norm_sel = {norm_txt(x) for x in (fam_sel or [])}

    nb_series = ref_b["nom_b"] if "nom_b" in ref_b.columns else pd.Series([], dtype=object)
    if isinstance(nb_series, pd.DataFrame):
        nb_series = nb_series.iloc[:, 0]
    num_blank = (nb_series.astype(str).str.strip() == "").sum()
    if num_blank > 0:
        st.warning(f"⚠️ Quedan {num_blank} códigos B sin nombre. Se aplicó coalesce y fallback desde A.")
except Exception as e:
    if debug_mode:
        st.exception(e)
        st.code(traceback.format_exc())
    else:
        st.error(f"Error cargando datos: {e}")
    st.stop()

# === Export Master + Bandeja ESCO ===
if "export_pack" not in st.session_state:
    st.session_state["export_pack"] = {}

with st.sidebar:
    st.header("Exportación Maestro")
    if st.button("💾 Exportar todo lo calculado (con timestamp)"):
        pack = dict(st.session_state.get("export_pack", {}))
        esco_df = get_esco_queue_df()
        if not esco_df.empty:
            pack["ESCO_bandeja"] = esco_df
        if not pack:
            st.warning("Aún no hay resultados calculados para exportar.")
        else:
            export_master_with_timestamp(pack, base_name="PLAN_MAESTRO")
    st.caption("Incluye lo último calculado en cada pestaña y la bandeja ESCO.")

    st.divider()
    st.header("Bandeja ESCO")
    st.caption("Acumula textos enviados con “Mandar a ESCO”.")
    esco_items = st.session_state.get("esco_queue", [])
    st.write(f"Ítems en bandeja: **{len(esco_items)}**")
    if esco_items:
        with st.expander("👁️ Vista previa de la bandeja ESCO (sin mapear)"):
            st.dataframe(pd.DataFrame({"texto": esco_items}), use_container_width=True, hide_index=True)
        if st.button("🧹 Vaciar bandeja ESCO"):
            st.session_state["esco_queue"] = []
            st.success("Bandeja ESCO vaciada.")

tabs = st.tabs([
    "🔎 Desde A → B (filtro por familia/nivel)",
    "🔀 Desde B → C (filtro por familia/nivel)",
    "🎯 Perfil de Competencias",
    "🧭 Mapeo a ESCO (PRO)",
    "📘 Fichas acciones formativas"
])

# --- Planificación desde A ---
def _list_b_for_as(map_b_a, misma_familia, familias_usuario):
    b_to_as, horas_a, fam_b = {}, {}, {}
    for _, r in map_b_a.iterrows():
        b = nz(r.get("cod_b")); a = nz(r.get("cod_a")); fam = nz(r.get("familia"))
        if not b or not a: continue
        if misma_familia and familias_usuario and fam and (fam not in familias_usuario):
            continue
        b_to_as.setdefault(b, set()).add(a)
        fam_b.setdefault(b, set()).add(fam)
        try:
            h = int(r.get("horas_a"))
            if a and h: horas_a[a] = h
        except Exception: pass
    fam_b = {k: ", ".join(sorted(v - {""})) for k, v in fam_b.items()}
    return b_to_as, horas_a, fam_b

def _list_c_for_bs(map_c_b, ref_b, misma_familia, familias_usuario):
    c_to_bs, fam_c, horas_b = {}, {}, {}
    for _, r in map_c_b.iterrows():
        c = nz(r.get("cod_c")); b = nz(r.get("cod_b")); fam = nz(r.get("familia"))
        if not c or not b: continue
        if misma_familia and familias_usuario and fam and (fam not in familias_usuario):
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

def plan_desde_As(cods_a_usuario, map_b_a, map_c_b, ref_b, ref_c,
                  misma_familia=False, niveles_perm=None, exigir_mismo_nivel=False, a2niveles=None):
    niveles_perm = set(niveles_perm or [])
    cods_a_usuario = {nz(x) for x in cods_a_usuario if nz(x)}
    fams_usuario = set(map_b_a[map_b_a["cod_a"].isin(cods_a_usuario)]["familia"].dropna().map(str).map(str.strip))
    b_to_as, horas_a, fam_b = _list_b_for_as(map_b_a, misma_familia, fams_usuario if misma_familia else set())
    ref_b_idx = ref_b.set_index("cod_b", drop=False); ref_c_idx = ref_c.set_index("cod_c", drop=False)

    niveles_A_sel = set()
    if exigir_mismo_nivel and a2niveles:
        for a in cods_a_usuario:
            for n in a2niveles.get(a, []):
                if n: niveles_A_sel.add(n)

    rows_b = []
    for b, req_as in b_to_as.items():
        nivel_b = nz(ref_b_idx.at[b, "nivel_b"] if b in ref_b_idx.index else "Desconocido")
        if niveles_perm and (nivel_b not in niveles_perm):
            continue
        if exigir_mismo_nivel and niveles_A_sel and (nivel_b not in niveles_A_sel):
            continue

        cubiertas = sorted(req_as & cods_a_usuario)
        faltan = sorted(req_as - cods_a_usuario)
        horas_pend = sum(horas_a.get(a, 0) for a in faltan)
        cobertura = 100.0 * (len(cubiertas) / len(req_as)) if req_as else 0.0
        nom_b = nz(ref_b_idx.at[b, "nom_b"] if b in ref_b_idx.index else "")
        h_b   = nz(ref_b_idx.at[b, "horas_b"] if b in ref_b_idx.index else "")
        fams_b= nz(ref_b_idx.at[b, "familias"] if b in ref_b_idx.index else "")
        rows_b.append({
            "cod_b": b, "nom_b": nom_b, "nivel_b": nivel_b,
            "familias_b": fams_b, "horas_b": h_b,
            "a_requeridas": ", ".join(sorted(req_as)),
            "a_cubiertas": ", ".join(cubiertas),
            "a_faltan": ", ".join(faltan),
            "horas_pendientes": horas_pend, "cobertura_pct": round(cobertura, 2),
        })
    df_b = pd.DataFrame(rows_b).sort_values(["cobertura_pct","cod_b"], ascending=[False, True])

    c_to_bs, horas_b, fam_c = _list_c_for_bs(map_c_b, ref_b, misma_familia, fams_usuario if misma_familia else set())
    rows_c = []
    for c, req_bs in c_to_bs.items():
        en_ref = c in ref_c_idx.index
        nom_c   = nz(ref_c_idx.at[c, "nom_c"]    if en_ref else "")
        fams_c_ = nz(ref_c_idx.at[c, "familias"] if en_ref else "")
        nivel_c = nz(ref_c_idx.at[c, "nivel_c"]  if en_ref else "Desconocido")
        if niveles_perm and (nivel_c not in niveles_perm):
            continue
        rows_c.append({
            "cod_c": c, "nom_c": nom_c, "nivel_c": nivel_c, "familias_c": fams_c_,
            "b_requeridos": ", ".join(sorted(req_bs)),
            "nota": "" if en_ref else "⚠️ C no encontrado en consolidado C",
        })
    df_c = pd.DataFrame(rows_c).sort_values("cod_c")
    return df_b, df_c

with tabs[0]:
    st.subheader("Selecciona tus acreditaciones (Grado A)")
    catA = dfA[["familia","cod_a","nom_a","cod_b","ra_texto"]].drop_duplicates()
    if fam_sel:
        catA = catA[catA["familia"].apply(lambda f: norm_txt(f) in norm_sel)]
    if niv_sel:
        niveles_A = []
        for _, r in catA.iterrows():
            levels = a2niveles.get(nz(r["cod_a"]), [])
            niveles_A.append("/".join(levels) if levels else "Desconocido")
        catA = catA.assign(nivel_a_inferido=niveles_A)
        catA = catA[catA["nivel_a_inferido"].apply(lambda s: bool(set(s.split("/")) & set(niv_sel)))]
    catA = catA.sort_values(["familia","cod_a"])
    catA["etiqueta"] = catA.apply(
        lambda r: f"{nz(r['cod_a'])} — {nz(r['nom_a'])}  (Fam: {nz(r['familia'])})", axis=1
    )
    etiqueta_to_codA = dict(zip(catA["etiqueta"], catA["cod_a"]))
    seleccion = st.multiselect(
        "Busca y marca uno o varios Grado A:",
        options=catA["etiqueta"].tolist(),
        default=[]
    )
    cods_a_usuario = {etiqueta_to_codA[e] for e in seleccion}

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Calcular B alcanzables", type="primary", use_container_width=True):
            if not cods_a_usuario:
                st.warning("Selecciona al menos un Grado A.")
            else:
                df_B_posibles, df_C_rel = plan_desde_As(
                    cods_a_usuario, map_b_a, map_c_b, ref_b, ref_c,
                    misma_familia=misma_familia,
                    niveles_perm=niv_sel if niv_sel else None,
                    exigir_mismo_nivel=exigir_mismo_nivel,
                    a2niveles=a2niveles
                )
                st.markdown("### Resultado: **B alcanzables**")

                filtrar_b_con_cubiertas = st.checkbox(
                    "Mostrar solo B con alguna A cubierta",
                    value=True,
                    key="flt_b_cubiertas"
                )
                df_B_view = df_B_posibles.copy()
                if "a_cubiertas" in df_B_view.columns and filtrar_b_con_cubiertas:
                    df_B_view = df_B_view[df_B_view["a_cubiertas"].astype(str).str.strip() != ""]
                st.dataframe(df_B_view, use_container_width=True, hide_index=True)

                st.markdown("### C relacionados (requisitos B)")
                st.dataframe(df_C_rel, use_container_width=True, hide_index=True)

                set_export_sheet("B_posibles_desde_A", df_B_view)
                set_export_sheet("C_relacionados_desde_A", df_C_rel)
                set_last("B_posibles_desde_A", df_B_view)
                set_last("C_relacionados_desde_A", df_C_rel)

                cods_b_str = extract_codes(df_B_view, "cod_b")
                cods_c_str = extract_codes(df_C_rel, "cod_c")
                st.markdown("**Copiar códigos mostrados:**")
                copy_to_clipboard_button("📋 Copiar B (listado)", cods_b_str, key="a2b_copy_b")
                st.write("")
                copy_to_clipboard_button("📋 Copiar C (relacionados)", cods_c_str, key="a2b_copy_c")

                download_excel_button(
                    {"B_posibles": df_B_view, "C_relacionados": df_C_rel},
                    "PLAN_desde_As.xlsx",
                    "💾 Descargar Excel (B_posibles + C_relacionados)"
                )

    with c2:
        if st.button("🎯 Ver perfil de competencias (desde A seleccionadas)", use_container_width=True):
            if not cods_a_usuario:
                st.warning("Selecciona al menos un Grado A.")
            else:
                comp_A, comp_B, comp_C = perfil_competencias(
                    as_hechas=cods_a_usuario, bs_hechos=set(), cs_hechos=set(),
                    dfA=dfA, dfB=dfB, dfC=dfC, map_b_a=map_b_a
                )
                st.markdown("### Perfil de Competencias — desde A")
                st.markdown("**Competencias desde A (RAs)**")
                st.dataframe(comp_A, use_container_width=True, hide_index=True)
                if comp_B is not None and len(comp_B) > 0:
                    with st.expander("Competencias derivadas desde B (si aplica)"):
                        st.dataframe(comp_B, use_container_width=True, hide_index=True)
                if comp_C is not None and len(comp_C) > 0:
                    with st.expander("Competencias derivadas desde C (si tu consolidado C las incluye)"):
                        st.dataframe(comp_C, use_container_width=True, hide_index=True)

                set_export_sheet("Perfil_competencias_desde_A", comp_A)
                set_export_sheet("Perfil_competencias_desde_A_via_B", comp_B if comp_B is not None else pd.DataFrame())
                set_export_sheet("Perfil_competencias_desde_A_via_C", comp_C if comp_C is not None else pd.DataFrame())
                set_last("Perfil_A", comp_A)
                set_last("Perfil_A_via_B", comp_B if comp_B is not None else pd.DataFrame())
                set_last("Perfil_A_via_C", comp_C if comp_C is not None else pd.DataFrame())

                st.markdown("**Copiar códigos A seleccionados:**")
                copy_to_clipboard_button("📋 Copiar A (selección)", " ".join(sorted(cods_a_usuario)), key="copy_sel_a")

                download_excel_button(
                    {"Competencias_desde_A": comp_A,
                     "Competencias_desde_B": comp_B if comp_B is not None else pd.DataFrame(),
                     "Competencias_desde_C": comp_C if comp_C is not None else pd.DataFrame()},
                    "PERFIL_COMPETENCIAS_desde_A.xlsx",
                    "💾 Descargar Perfil (Excel)"
                )

    with c3:
        st.markdown("#### Mandar a ESCO (bandeja)")
        if st.button("➕ RAs (texto) de A seleccionados → ESCO"):
            ra_texts = dfA[dfA["cod_a"].isin(cods_a_usuario)].get("ra_texto", pd.Series([], dtype=object)).astype(str).tolist()
            nom_a_texts = dfA[dfA["cod_a"].isin(cods_a_usuario)].get("nom_a", pd.Series([], dtype=object)).astype(str).tolist()
            add_to_esco_queue(ra_texts + nom_a_texts, "A (RAs/nom_a)")

        if st.button("➕ Nombres de B alcanzables → ESCO"):
            df_Bp = get_last("B_posibles_desde_A")
            if df_Bp is None or df_Bp.empty:
                st.info("Primero calcula B alcanzables.")
            else:
                add_to_esco_queue(df_Bp.get("nom_b", pd.Series([], dtype=object)).astype(str).tolist(), "B (nombres)")

        if st.button("➕ Nombres de C relacionados → ESCO"):
            df_Cr = get_last("C_relacionados_desde_A")
            if df_Cr is None or df_Cr.empty:
                st.info("Primero calcula C relacionados.")
            else:
                add_to_esco_queue(df_Cr.get("nom_c", pd.Series([], dtype=object)).astype(str).tolist(), "C (nombres)")

# --- Planificación desde B ---
def plan_desde_Bs(cods_b_usuario, map_b_a, map_c_b, ref_b, ref_c,
                  misma_familia=False, niveles_perm=None, exigir_mismo_nivel=False):
    niveles_perm = set(niveles_perm or [])
    cods_b_usuario = {nz(x) for x in cods_b_usuario if nz(x)}
    fams_usuario = set(map_c_b[map_c_b["cod_b"].isin(cods_b_usuario)]["familia"].dropna().map(str).map(str.strip))
    c_to_bs, horas_b, fam_c = _list_c_for_bs(map_c_b, ref_b, misma_familia, fams_usuario if misma_familia else set())
    ref_c_idx = ref_c.set_index("cod_c", drop=False)
    ref_b_idx = ref_b.set_index("cod_b", drop=False)

    niveles_B_sel = set()
    if exigir_mismo_nivel:
        for b in cods_b_usuario:
            n = nz(ref_b_idx.at[b, "nivel_b"] if b in ref_b_idx.index else "")
            if n: niveles_B_sel.add(n)

    b_to_as = map_b_a.groupby("cod_b")["cod_a"].apply(lambda s: sorted(set([nz(x) for x in s if nz(x)])))

    rows_c, rows_b_detalle = [], []
    for c, req_bs in c_to_bs.items():
        en_ref = c in ref_c_idx.index
        nivel_c = nz(ref_c_idx.at[c, "nivel_c"] if en_ref else "Desconocido")
        if niveles_perm and (nivel_c not in niveles_perm):
            continue
        if exigir_mismo_nivel and niveles_B_sel and (nivel_c not in niveles_B_sel):
            continue

        cubiertos = sorted(req_bs & cods_b_usuario)
        faltan = sorted(req_bs - cods_b_usuario)
        horas_pend = sum(horas_b.get(b, 0) for b in faltan)
        nom_c = nz(ref_c_idx.at[c, "nom_c"] if en_ref else "")
        fams_c_ = nz(ref_c_idx.at[c, "familias"] if en_ref else "")
        rows_c.append({
            "cod_c": c, "nom_c": nom_c, "nivel_c": nivel_c, "familias_c": fams_c_,
            "b_requeridos": ", ".join(sorted(req_bs)),
            "b_cubiertos": ", ".join(cubiertos),
            "b_faltan": ", ".join(faltan),
            "horas_pendientes_b": horas_pend,
            "nota": "" if en_ref else "⚠️ C no encontrado en consolidado C",
        })
        for b in faltan:
            req_as = b_to_as.get(b, [])
            rows_b_detalle.append({
                "cod_c": c, "nivel_c": nivel_c,
                "cod_b_faltante": b,
                "a_requeridas_para_b": ", ".join(req_as),
                "horas_b_estimada": horas_b.get(b, "")
            })
    df_c = pd.DataFrame(rows_c).sort_values("cod_c")
    df_b_detalle = pd.DataFrame(rows_b_detalle).sort_values(["cod_c","cod_b_faltante"])
    return df_c, df_b_detalle

with tabs[1]:
    st.subheader("Selecciona tus certificados de competencia (Grado B)")
    catB = ref_b[["familias","cod_b","nom_b","horas_b","nivel_b"]].drop_duplicates()
    if fam_sel:
        def fams_str_to_norm_set(s: str):
            toks = [t.strip() for t in str(s).split(",")]
            return {norm_txt(t) for t in toks if t}
        catB = catB[catB["familias"].apply(lambda s: bool(fams_str_to_norm_set(s) & norm_sel))]
    if niv_sel:
        catB = catB[catB["nivel_b"].apply(lambda s: any(n==s or n in s.split("/") for n in niv_sel))]
    catB = catB.sort_values(["cod_b"])

    catB["etiqueta"] = catB.apply(
        lambda r: f"{nz(r['cod_b'])} — {nz(r['nom_b'],'«sin nombre»')}  (Nivel: {nz(r['nivel_b'])}; Fams: {nz(r['familias'],'—')}, {nz(r['horas_b'])} h)",
        axis=1
    )
    etiqueta_to_codB = dict(zip(catB["etiqueta"], catB["cod_b"]))
    seleccion_b = st.multiselect(
        "Busca y marca uno o varios Grado B:",
        options=catB["etiqueta"].tolist(),
        default=[]
    )
    cods_b_usuario = {etiqueta_to_codB[e] for e in seleccion_b}

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Calcular C alcanzables", type="primary", use_container_width=True, key="btn_b2c"):
            if not cods_b_usuario:
                st.warning("Selecciona al menos un Grado B.")
            else:
                df_C_posibles, df_B_det = plan_desde_Bs(
                    cods_b_usuario, map_b_a, map_c_b, ref_b, ref_c,
                    misma_familia=misma_familia,
                    niveles_perm=niv_sel if niv_sel else None,
                    exigir_mismo_nivel=exigir_mismo_nivel
                )
                st.markdown("### Resultado: **C alcanzables**")

                filtrar_c_con_b_cubiertos = st.checkbox(
                    "Mostrar solo C con algún B cubierto",
                    value=True,
                    key="flt_c_cubiertos"
                )
                df_C_view = df_C_posibles.copy()
                if "b_cubiertos" in df_C_view.columns and filtrar_c_con_b_cubiertos:
                    df_C_view = df_C_view[df_C_view["b_cubiertos"].astype(str).str.strip() != ""]
                st.dataframe(df_C_view, use_container_width=True, hide_index=True)

                st.markdown("### Detalle de A requeridas para B faltantes")
                st.dataframe(df_B_det, use_container_width=True, hide_index=True)

                set_export_sheet("C_posibles_desde_B", df_C_view)
                set_export_sheet("Detalle_A_para_B_faltantes", df_B_det)
                set_last("C_posibles_desde_B", df_C_view)
                set_last("Detalle_A_para_B_faltantes", df_B_det)

                cods_c_str = extract_codes(df_C_view, "cod_c")
                cods_bfalt_str = extract_codes(df_B_det, "cod_b_faltante")
                st.markdown("**Copiar códigos mostrados:**")
                copy_to_clipboard_button("📋 Copiar C (alcanzables)", cods_c_str, key="b2c_copy_c")
                st.write("")
                copy_to_clipboard_button("📋 Copiar B faltantes", cods_bfalt_str, key="b2c_copy_bfalt")

                download_excel_button(
                    {"C_posibles": df_C_view, "Detalle_A_para_B_falt": df_B_det},
                    "PLAN_desde_Bs.xlsx",
                    "💾 Descargar Excel (C_posibles + Detalle_A_para_B_falt)"
                )

    with c2:
        if st.button("🎯 Ver perfil de competencias (desde B seleccionados)", use_container_width=True):
            if not cods_b_usuario:
                st.warning("Selecciona al menos un Grado B.")
            else:
                comp_A, comp_B, comp_C = perfil_competencias(
                    as_hechas=set(), bs_hechos=cods_b_usuario, cs_hechos=set(),
                    dfA=dfA, dfB=dfB, dfC=dfC, map_b_a=map_b_a
                )
                st.markdown("### Perfil de Competencias — desde B")
                st.dataframe(comp_B, use_container_width=True, hide_index=True)
                if comp_A is not None and len(comp_A) > 0:
                    with st.expander("Acreditaciones A asociadas a los B seleccionados"):
                        st.dataframe(comp_A, use_container_width=True, hide_index=True)
                if comp_C is not None and len(comp_C) > 0:
                    with st.expander("Competencias desde C (si tu consolidado C trae UCs)"):
                        st.dataframe(comp_C, use_container_width=True, hide_index=True)

                set_export_sheet("Perfil_competencias_desde_B", comp_B)
                set_export_sheet("Perfil_competencias_desde_B_incl_A", comp_A if comp_A is not None else pd.DataFrame())
                set_export_sheet("Perfil_competencias_desde_B_incl_C", comp_C if comp_C is not None else pd.DataFrame())
                set_last("Perfil_B", comp_B)
                set_last("Perfil_B_incl_A", comp_A if comp_A is not None else pd.DataFrame())
                set_last("Perfil_B_incl_C", comp_C if comp_C is not None else pd.DataFrame())

                st.markdown("**Copiar códigos B seleccionados:**")
                copy_to_clipboard_button("📋 Copiar B (selección)", " ".join(sorted(cods_b_usuario)), key="copy_sel_b")

                download_excel_button(
                    {"Competencias_desde_B": comp_B,
                     "Competencias_desde_A": comp_A if comp_A is not None else pd.DataFrame(),
                     "Competencias_desde_C": comp_C if comp_C is not None else pd.DataFrame()},
                    "PERFIL_COMPETENCIAS_desde_B.xlsx",
                    "💾 Descargar Perfil (Excel)"
                )

    with c3:
        st.markdown("#### Mandar a ESCO (bandeja)")
        if st.button("➕ Nombres de B seleccionados → ESCO"):
            names = ref_b[ref_b["cod_b"].isin(cods_b_usuario)].get("nom_b", pd.Series([], dtype=object)).astype(str).tolist()
            add_to_esco_queue(names, "B (seleccionados)")
        if st.button("➕ Nombres de C alcanzables → ESCO"):
            df_Cp = get_last("C_posibles_desde_B")
            if df_Cp is None or df_Cp.empty:
                st.info("Primero calcula C alcanzables.")
            else:
                add_to_esco_queue(df_Cp.get("nom_c", pd.Series([], dtype=object)).astype(str).tolist(), "C (alcanzables)")

        if st.button("➕ RAs de B faltantes (detalle A) → ESCO"):
            df_Bdet = get_last("Detalle_A_para_B_faltantes")
            if df_Bdet is None or df_Bdet.empty:
                st.info("Primero calcula C alcanzables para ver B faltantes.")
            else:
                ras = []
                for s in df_Bdet.get("a_requeridas_para_b", pd.Series([], dtype=object)).astype(str).tolist():
                    ras.extend([x.strip() for x in s.split(",") if x.strip()])
                if ras:
                    ra_texts = dfA[dfA["cod_a"].isin(ras)].get("ra_texto", pd.Series([], dtype=object)).astype(str).tolist()
                    add_to_esco_queue(ra_texts, "A (RAs para B faltantes)")

# --- Tab 3: Perfil de Competencias directo ---
with tabs[2]:
    st.subheader("Genera un perfil de competencias directamente")
    st.caption("Opcional: escribe manualmente códigos A/B/C para componer un perfil.")
    codsA_txt = st.text_area("Códigos A (separados por espacio/coma)", "")
    codsB_txt = st.text_area("Códigos B (separados por espacio/coma)", "")
    codsC_txt = st.text_area("Códigos C (separados por espacio/coma)", "")
    def split_codes(s):
        s = (s or "").replace(",", " ")
        return {c.strip() for c in s.split() if c.strip()}
    codsA = split_codes(codsA_txt); codsB = split_codes(codsB_txt); codsC = split_codes(codsC_txt)
    if st.button("Calcular perfil directo"):
        comp_A, comp_B, comp_C = perfil_competencias(
            as_hechas=codsA, bs_hechos=codsB, cs_hechos=codsC,
            dfA=dfA, dfB=dfB, dfC=dfC, map_b_a=map_b_a
        )
        st.markdown("### Resultado de competencias")
        with st.expander("Competencias desde A", expanded=True):
            st.dataframe(comp_A, use_container_width=True, hide_index=True)
        with st.expander("Competencias desde B"):
            st.dataframe(comp_B, use_container_width=True, hide_index=True)
        with st.expander("Competencias desde C"):
            st.dataframe(comp_C, use_container_width=True, hide_index=True)

        set_export_sheet("Perfil_competencias_directo_A", comp_A)
        set_export_sheet("Perfil_competencias_directo_B", comp_B)
        set_export_sheet("Perfil_competencias_directo_C", comp_C)
        set_last("Perfil_directo_A", comp_A)
        set_last("Perfil_directo_B", comp_B)
        set_last("Perfil_directo_C", comp_C)

        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("➕ RAs (A) → ESCO", key="dir_ra_a"):
                add_to_esco_queue(comp_A.get("ra_texto", pd.Series([], dtype=object)).astype(str).tolist(), "A (directo)")
        with c2:
            if st.button("➕ Nombres B → ESCO", key="dir_nom_b"):
                add_to_esco_queue(comp_B.get("nom_b", pd.Series([], dtype=object)).astype(str).tolist(), "B (directo)")
        with c3:
            if st.button("➕ Nombres C → ESCO", key="dir_nom_c"):
                add_to_esco_queue(comp_C.get("nom_c", pd.Series([], dtype=object)).astype(str).tolist(), "C (directo)")

        download_excel_button(
            {"Competencias_desde_A": comp_A,
             "Competencias_desde_B": comp_B,
             "Competencias_desde_C": comp_C},
            "PERFIL_COMPETENCIAS_directo.xlsx",
            "💾 Descargar Perfil (Excel)"
        )

# --- Tab 4: Mapeo a ESCO (PRO) ---
with tabs[3]:
    st.subheader("Mapeo de competencias → ESCO (skills & occupations) — PRO")
    st.caption("API ESCO en quick mode + fuzzy. Resultados persistentes y exportables.")

    # Fuente de entradas
    queue_items = [t.strip() for t in st.session_state.get("esco_queue", []) if str(t).strip()]
    n_queue = len(queue_items)
    fuente = st.radio(
        "¿Qué entradas usar?",
        ["Solo cuadro de texto", f"Solo bandeja ESCO ({n_queue})", f"Ambos: cuadro + bandeja ({n_queue})"],
        horizontal=False
    )
    if fuente != "Solo bandeja ESCO":
        if "esco_textarea" not in st.session_state:
            st.session_state["esco_textarea"] = "\n".join(queue_items)
        if st.button("⤵️ Copiar bandeja → cuadro"):
            st.session_state["esco_textarea"] = "\n".join(queue_items)
        comps_txt = st.text_area(
            "Cuadro de texto (una competencia por línea):",
            st.session_state["esco_textarea"],
            height=220,
            key="esco_textarea"
        )
        from_text = [t.strip() for t in comps_txt.splitlines() if t.strip()]
    else:
        from_text = []
    from_queue = queue_items if fuente != "Solo cuadro de texto" else []
    to_map = list(dict.fromkeys(from_text + from_queue))  # dedup

    # Parámetros + ping API
    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        top_k_skills = st.slider("Top-K skills ESCO", 1, 50, 8, 1)
    with c2:
        top_k_occs = st.slider("Top-K ocupaciones", 1, 50, 8, 1)
    with c3:
        alpha = st.number_input("Peso fuzzy (α)", 0.0, 1.0, 0.60, 0.05)
    timeout_read = st.slider("Timeout de lectura (s)", 10, 120, 45, 5)
    api_limit = st.slider("Resultados por llamada (limit)", 5, 50, 25, 5)

    synonyms_txt = st.text_input("Sinónimos (opcional, CSV: origen=dest1|dest2; ...)",
                                 value="ofimática=ofimatica|office automation; almacén=warehouse|logistics")
    syn_map = {}
    for pair in synonyms_txt.split(";"):
        par = pair.strip()
        if not par or "=" not in par: continue
        k, vs = par.split("=", 1)
        syn_map[k.strip().lower()] = [v.strip() for v in vs.split("|") if v.strip()]
    expanded = []
    for t in to_map:
        expanded.append(t)
        low = t.lower()
        for k, repls in syn_map.items():
            if k in low:
                for r in repls:
                    expanded.append(low.replace(k, r))
    to_map = list(dict.fromkeys(expanded))

    ping = st.button("🔎 Probar API ESCO")
    if ping:
        if esco_search_skills is None:
            st.error("No se encontró esco_mapper_pro.py o faltan dependencias.")
        else:
            try:
                test = esco_search_skills("ofimática", lang="es", limit=3)
                st.success(f"API OK. Ejemplo (ofimática): {len(test)} resultados.")
                if test:
                    st.dataframe(pd.DataFrame(test), use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"No se pudo contactar la API ESCO: {e}")

    st.markdown(f"**Entradas listas para mapear:** `{len(to_map)}`")

    lanzar = st.button("▶️ Ejecutar mapeo", type="primary", use_container_width=True)
    if lanzar:
        if not to_map:
            st.warning("No hay entradas. Escribe en el cuadro o añade a la bandeja ESCO.")
        elif map_competencias_a_esco_pro is None:
            st.error("No se encontró esco_mapper_pro.py. Colócalo junto a la app.")
        else:
            with st.status("Consultando ESCO…", expanded=True) as s:
                st.write("Llamando a /search (skills y ocupaciones), con reintentos…")
                try:
                    df_s, df_o, metrics = map_competencias_a_esco_pro(
                        to_map, language="es",
                        top_k_skills=top_k_skills,
                        top_k_occs=top_k_occs,
                        alpha_fuzzy=alpha,
                        timeout_connect=10,
                        timeout_read=timeout_read,
                        api_limit=api_limit
                    )
                    st.write(f"Hecho en {metrics.get('elapsed_s', '?')} s · llamadas: skills={metrics.get('calls_skills',0)}, occs={metrics.get('calls_occs',0)}.")
                    st.session_state["esco_last"]["df_s"] = df_s
                    st.session_state["esco_last"]["df_o"] = df_o
                    st.session_state["esco_last"]["metrics"] = metrics
                    s.update(label="ESCO consultado con éxito ✅", state="complete")
                except Exception as e:
                    st.error(f"Error llamando a ESCO: {e}")

    df_s = st.session_state["esco_last"]["df_s"]
    df_o = st.session_state["esco_last"]["df_o"]
    metrics = st.session_state["esco_last"]["metrics"] or {}
    st.info(f"Resultados actuales — inputs: {metrics.get('n_inputs', 0)} · filas skills: {len(df_s)} · filas ocupaciones: {len(df_o)}")

    min_score = st.slider("Puntaje mínimo (mostrar)", 0.0, 1.0, 0.30, 0.05)
    top_k_por_input = st.number_input("Máx. matches por input", 1, 50, 8, 1)

    def vista(df: pd.DataFrame, kind: str):
        if df is None or df.empty:
            st.warning(f"Sin filas en {kind}.")
            return pd.DataFrame()
        if "input_text" not in df.columns or "score" not in df.columns:
            st.error(f"{kind}: falta 'input_text' o 'score' en columnas.")
            return pd.DataFrame()
        base = df.copy()
        base = base[base["score"] >= min_score]
        base.sort_values(["input_text","score"], ascending=[True, False], inplace=True)
        base["rank_in_input"] = base.groupby("input_text").cumcount() + 1
        base = base[base["rank_in_input"] <= top_k_por_input]
        if base.empty and len(df) > 0:
            st.warning(f"{kind}: sin filas tras filtros (sube Top-K o baja el score).")
        return base

    df_s_view = vista(df_s, "skills ESCO")
    df_o_view = vista(df_o, "ocupaciones ESCO")

    st.markdown("### Skills ESCO")
    st.dataframe(df_s_view, use_container_width=True, hide_index=True)

    st.markdown("### Ocupaciones relacionadas")
    st.dataframe(df_o_view, use_container_width=True, hide_index=True)

    st.markdown("## Tabla unificada: Origen → Skill ESCO (+ ocupaciones)")
    def _safe(df, cols):
        if df is None or df.empty:
            return pd.DataFrame(columns=cols)
        for c in cols:
            if c not in df.columns: df[c] = ""
        return df[cols].copy()
    cols_s = ["input_text", "esco_skill_label", "esco_skill_uri", "score", "method"]
    df_map = _safe(df_s_view, cols_s)
    df_map.rename(columns={
        "input_text": "competencia_origen",
        "esco_skill_label": "esco_skill",
        "esco_skill_uri": "esco_skill_uri",
        "score": "score",
        "method": "metodo"
    }, inplace=True)

    if not df_o_view.empty:
        dfo = _safe(df_o_view, ["input_text","esco_skill_uri","esco_occ_label","esco_occ_uri","score"]).copy()
        dfo.rename(columns={
            "input_text": "competencia_origen",
            "esco_occ_label": "esco_occupation",
            "esco_occ_uri": "esco_occupation_uri",
            "score": "occ_score"
        }, inplace=True)
        dfo_grp = (dfo
            .assign(pair=lambda r: r["esco_occupation"] + " ⟨" + r["esco_occupation_uri"] + "⟩")
            .groupby(["competencia_origen","esco_skill_uri"], as_index=False)
            .agg(ocupaciones=("pair", lambda s: " | ".join(list(dict.fromkeys([str(x) for x in s]))))))
        df_map = df_map.merge(dfo_grp, on=["competencia_origen","esco_skill_uri"], how="left")
    else:
        if not df_map.empty:
            df_map["ocupaciones"] = ""

    df_map.sort_values(["competencia_origen","score"], ascending=[True, False], inplace=True)
    st.dataframe(df_map, use_container_width=True, hide_index=True)
    st.session_state["esco_last"]["df_unificado"] = df_map.copy()
    st.session_state["export_pack"]["ESCO_mapeo_unificado"] = df_map.copy()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        df_map.to_excel(w, sheet_name="ESCO_mapeo_unificado", index=False)
        df_s_view.to_excel(w, sheet_name="ESCO_skills_raw", index=False)
        df_o_view.to_excel(w, sheet_name="ESCO_occupations_raw", index=False)
    st.download_button(
        "💾 Descargar Excel (unificado + crudo)",
        data=bio.getvalue(),
        file_name=f"ESCO_mapeo_unificado_{ts}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
    def extract_uris(df, col):
        if df is None or df.empty or col not in df.columns: return ""
        vals = sorted({str(x).strip() for x in df[col].tolist() if str(x).strip()})
        return " ".join(vals)
    uris = extract_uris(df_map, "esco_skill_uri")
    copy_to_clipboard_button("📋 Copiar URIs de skills (visibles)", uris, key="esco_copy_uris")

# ---------- NUEVO: exportar ficha SIEMPRE en PDF ELEGANTE ----------
def exportar_ficha_pdf(ficha: dict, tipo: str):
    """
    Genera PDF elegante de la ficha (A/B/C) usando ReportLab Platypus,
    con celdas que envuelven texto y anchos de columna optimizados.
    Devuelve (pdf_bytes, file_name).
    """
    import io as _io
    from datetime import datetime
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                    TableStyle, PageBreak, KeepTogether)
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus.flowables import HRFlowable
    from reportlab.pdfgen import canvas

    # -------- utilidades --------
    def _nz(x, d=""):
        s = str(x or "").strip()
        return s if s else d

    # Estilos de párrafo (para permitir WRAP dentro de celdas)
    styles = getSampleStyleSheet()
    cellH = ParagraphStyle("cellH", parent=styles["Heading5"], fontName="Helvetica-Bold",
                           fontSize=9, leading=12, textColor=colors.HexColor("#1f2d3d"), spaceAfter=0, spaceBefore=0)
    cellP = ParagraphStyle("cellP", parent=styles["BodyText"], fontName="Helvetica",
                           fontSize=9, leading=12, textColor=colors.HexColor("#111827"),
                           wordWrap="CJK")  # fuerza el wrap
    # Anchura útil del frame (A4 con márgenes 2cm)
    PAGE_W = A4[0] - 4*cm  # ≈ 17 cm

    def _p(txt):  # conv a Paragraph
        return Paragraph(_nz(txt).replace("\n", "<br/>"), cellP)

    def df_to_table(df, col_ren=None, widths_by_col=None, zebra=True):
        """
        Convierte df en Table con:
        - títulos como Paragraph
        - celdas como Paragraph (wrap)
        - anchos por nombre de columna (cm)
        """
        if df is None or getattr(df, "empty", True):
            return None
        _df = df.copy()
        if col_ren:
            _df.rename(columns=col_ren, inplace=True)
        cols = list(_df.columns)

        # Construir filas con Paragraph (cabecera y cuerpo)
        data = [[Paragraph(str(c), cellH) for c in cols]]
        for _, r in _df.iterrows():
            row = [ _p(r[c]) for c in cols ]
            data.append(row)

        # Calcular colWidths
        # Reglas por defecto: códigos estrechos (2.2–3.0 cm), descripciones anchas.
        # Puedes ajustar aquí si lo ves necesario.
        default_widths = []
        if widths_by_col:
            # Asignación por nombre; el resto se reparte
            assigned = 0.0
            widths = []
            for c in cols:
                w = widths_by_col.get(c)
                if isinstance(w, (int, float)):
                    widths.append(w*cm)
                    assigned += w
                else:
                    widths.append(None)
            remaining = PAGE_W - sum([w or 0 for w in widths])
            flex_cols = [i for i,w in enumerate(widths) if w is None]
            share = max(remaining/len(flex_cols), 2.0*cm) if flex_cols else 0
            for i in flex_cols:
                widths[i] = share
            col_widths = widths
        else:
            # Heurístico según nombre
            col_widths = []
            for c in cols:
                cl = str(c).lower()
                if any(k in cl for k in ["código", "codigo", "cod ", "cod_", "cod.", "nivel", "horas"]):
                    col_widths.append(2.4*cm)
                elif any(k in cl for k in ["familia", "familias", "módulo", "modulo"]):
                    col_widths.append(4.0*cm)
                elif any(k in cl for k in ["ra (resumen)", "estándar", "estandar", "competencia", "ocupaciones", "nombre"]):
                    col_widths.append(8.0*cm)
                else:
                    col_widths.append(3.2*cm)
            # Ajuste global si excede
            total = sum(col_widths)
            if total > PAGE_W:
                scale = PAGE_W / total
                col_widths = [w*scale for w in col_widths]

        tbl = Table(data, hAlign="LEFT", colWidths=col_widths, repeatRows=1)

        base = [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f0f3f7")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#1f2d3d")),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
            ("TOPPADDING", (0, 0), (-1, 0), 6),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cfd8e3")),
            # Envoltura de texto en celdas (funciona con Paragraph + wordWrap en estilo)
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]
        if zebra and len(data) > 2:
            base.append(("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]))

        tbl.setStyle(TableStyle(base))
        return tbl

    # -------- cabecera/pie de página --------
    def header_footer(canv: canvas.Canvas, doc):
        canv.saveState()
        w, h = A4
        # Cabecera
        canv.setFillColor(colors.HexColor("#1f2d3d"))
        canv.setFont("Helvetica-Bold", 11)
        canv.drawString(2*cm, h - 1.5*cm, "Fichas de acciones formativas · Planificador A→B→C")
        canv.setFillColor(colors.HexColor("#8795a1"))
        canv.setFont("Helvetica", 8)
        canv.drawRightString(w - 2*cm, h - 1.5*cm, datetime.now().strftime("%Y-%m-%d %H:%M"))
        # Línea cabecera
        canv.setStrokeColor(colors.HexColor("#e2e8f0"))
        canv.setLineWidth(0.6)
        canv.line(1.7*cm, h - 1.7*cm, w - 1.7*cm, h - 1.7*cm)
        # Pie
        canv.setStrokeColor(colors.HexColor("#e2e8f0"))
        canv.line(1.7*cm, 1.7*cm, w - 1.7*cm, 1.7*cm)
        canv.setFillColor(colors.HexColor("#64748b"))
        canv.setFont("Helvetica", 8)
        canv.drawString(2*cm, 1.1*cm, "Fuente: BOE (deep-links incluidos) · Exportado desde la app")
        canv.drawRightString(w - 2*cm, 1.1*cm, f"Página {doc.page}")
        canv.restoreState()

    # -------- estilos para títulos fuera de tabla --------
    H1 = ParagraphStyle(
        "H1",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#0f172a"),
        spaceAfter=8,
        spaceBefore=6,
    )
    H2 = ParagraphStyle(
        "H2",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=16,
        textColor=colors.HexColor("#1f2937"),
        spaceAfter=6,
        spaceBefore=14,
    )
    P = ParagraphStyle(
        "P",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10,
        leading=14,
        textColor=colors.HexColor("#111827"),
    )
    MUTE = ParagraphStyle(
        "MUTE",
        parent=P,
        textColor=colors.HexColor("#6b7280"),
        fontSize=9,
    )
    TAG = ParagraphStyle(
        "TAG",
        parent=P,
        backColor=colors.HexColor("#eef2ff"),
        textColor=colors.HexColor("#3730a3"),
        fontName="Helvetica-Bold",
        fontSize=9,
        leftIndent=3,
        rightIndent=3,
        spaceAfter=4,
    )

    # -------- datos base --------
    tipo = _nz(tipo).upper()
    cod = _nz(ficha.get(f"cod_{tipo.lower()}"))
    nom = _nz(ficha.get(f"nom_{tipo.lower()}"))
    lvl = _nz(ficha.get("nivel"))
    hrs = _nz(ficha.get("horas"))
    fam = _nz(ficha.get("familia" if tipo == "A" else "familias"))
    fuente = _nz(ficha.get("fuente_url"))
    rd = _nz(ficha.get("rd_num"))
    fecha = _nz(ficha.get("fecha_rd"))

    # -------- documento --------
    buf = _io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2.2*cm, bottomMargin=2.2*cm
    )
    story = []

    # Portada/encabezado de ficha
    story.append(Paragraph(f"Ficha {tipo}", TAG))
    story.append(Paragraph(f"{cod} — {nom}", H1))
    meta = []
    if fam: meta.append(f"<b>Familia(s):</b> {fam}")
    if lvl: meta.append(f"<b>Nivel:</b> {lvl}")
    if hrs: meta.append(f"<b>Horas:</b> {hrs}")
    story.append(Paragraph(" · ".join(meta) if meta else "—", P))
    if fuente or rd or fecha:
        story.append(Paragraph(
            f"<b>BOE:</b> {fuente or '—'} &nbsp;&nbsp; <b>RD:</b> {rd or '—'} &nbsp;&nbsp; <b>Fecha:</b> {fecha or '—'}",
            MUTE
        ))
    story.append(Spacer(1, 6))
    story.append(HRFlowable(width="100%", color=colors.HexColor("#e5e7eb"), thickness=0.8))
    story.append(Spacer(1, 10))

    # Contenido por tipo (usamos df_to_table con widths_by_col para columnas largas)
    if tipo == "A":
        # RA (texto largo fuera de tabla para mejor flujo)
        ra = _nz(ficha.get("ra_texto"))
        story.append(Paragraph("Resultados de aprendizaje (RA)", H2))
        story.append(Paragraph(ra or "—", P))

        # B relacionados
        Bs = ficha.get("Bs")
        if Bs is not None and not Bs.empty:
            story.append(Paragraph("B relacionados", H2))
            tbl = df_to_table(
                Bs[["cod_b","nom_b","nivel_b","familias","horas_b"]].rename(columns={
                    "cod_b":"Código B","nom_b":"Nombre B","nivel_b":"Nivel","familias":"Familias","horas_b":"Horas"
                }),
                widths_by_col={"Código B":2.6, "Nombre B":8.0, "Nivel":2.4, "Familias":3.5, "Horas":2.0}
            )
            story.append(tbl or Paragraph("—", P))

        # C relacionados
        Cs = ficha.get("Cs")
        if Cs is not None and not Cs.empty:
            story.append(Paragraph("C relacionados (vía B)", H2))
            tbl = df_to_table(
                Cs[["cod_c","nom_c","nivel_c","familias","horas_c"]].rename(columns={
                    "cod_c":"Código C","nom_c":"Nombre C","nivel_c":"Nivel","familias":"Familias","horas_c":"Horas"
                }),
                widths_by_col={"Código C":2.8, "Nombre C":8.2, "Nivel":2.4, "Familias":3.2, "Horas":1.8}
            )
            story.append(tbl or Paragraph("—", P))

    elif tipo == "B":
        # A que lo componen (RA puede ser largo → columna ancha)
        As = ficha.get("As")
        story.append(Paragraph("A que lo componen", H2))
        if As is not None and not As.empty:
            tbl = df_to_table(
                As[["cod_a","nom_a","ra_texto","horas_a","familia"]].rename(columns={
                    "cod_a":"Código A","nom_a":"Nombre A","ra_texto":"RA (resumen)","horas_a":"Horas","familia":"Familia"
                }),
                widths_by_col={"Código A":2.8, "Nombre A":5.2, "RA (resumen)":8.0, "Horas":2.0, "Familia":3.0}
            )
            story.append(tbl)
        else:
            story.append(Paragraph("—", P))

        # C en los que participa
        Cs = ficha.get("Cs")
        story.append(Paragraph("C en los que participa", H2))
        if Cs is not None and not Cs.empty:
            tbl = df_to_table(
                Cs[["cod_c","nom_c","nivel_c","familias","horas_c"]].rename(columns={
                    "cod_c":"Código C","nom_c":"Nombre C","nivel_c":"Nivel","familias":"Familias","horas_c":"Horas"
                }),
                widths_by_col={"Código C":2.8, "Nombre C":8.2, "Nivel":2.4, "Familias":3.2, "Horas":1.8}
            )
            story.append(tbl)
        else:
            story.append(Paragraph("—", P))

    else:  # C
        # Competencias (fuera de tabla)
        story.append(Paragraph("Competencia general", H2))
        story.append(Paragraph(_nz(ficha.get("comp_general")) or "—", P))
        story.append(Spacer(1, 6))
        story.append(Paragraph("Competencias profesionales y para la empleabilidad", H2))
        story.append(Paragraph(_nz(ficha.get("comp_prof")) or "—", P))

        # UCs (columna Estándar ancha)
        UCs = ficha.get("UCs")
        story.append(Paragraph("Estándares (UC)", H2))
        if UCs is not None and not UCs.empty:
            tbl = df_to_table(
                UCs[["codigo_comp","estandar_comp"]].rename(columns={
                    "codigo_comp":"Código UC","estandar_comp":"Estándar"
                }),
                widths_by_col={"Código UC":3.0, "Estándar":13.8}
            )
            story.append(tbl)
        else:
            story.append(Paragraph("—", P))

        # Módulos
        Mods = ficha.get("Modulos")
        story.append(Paragraph("Módulos profesionales del certificado", H2))
        if Mods is not None and not Mods.empty:
            tbl = df_to_table(
                Mods.rename(columns={"modulo_profesional":"Módulo profesional"}),
                widths_by_col={"Módulo profesional":16.8}
            )
            story.append(tbl)
        else:
            story.append(Paragraph("—", P))

        # B y A por B
        Bs = ficha.get("Bs")
        story.append(Paragraph("B que lo componen", H2))
        if Bs is not None and not Bs.empty:
            tbl = df_to_table(
                Bs[["cod_b","nom_b","nivel_b","familias","horas_b"]].rename(columns={
                    "cod_b":"Código B","nom_b":"Nombre B","nivel_b":"Nivel","familias":"Familias","horas_b":"Horas"
                }),
                widths_by_col={"Código B":2.8, "Nombre B":8.0, "Nivel":2.4, "Familias":3.2, "Horas":1.8}
            )
            story.append(tbl)
        else:
            story.append(Paragraph("—", P))

        BAs = ficha.get("B_As")
        story.append(Paragraph("A por cada B", H2))
        if BAs is not None and not BAs.empty:
            for b, sub in BAs.groupby("cod_b"):
                story.append(Paragraph(f"<b>{b}</b>", P))
                sub = sub[["cod_a","nom_a","ra_texto"]].drop_duplicates()
                tbl = df_to_table(
                    sub.rename(columns={
                        "cod_a":"Código A","nom_a":"Nombre A","ra_texto":"RA (resumen)"
                    }),
                    widths_by_col={"Código A":2.8, "Nombre A":5.2, "RA (resumen)":9.0},
                    zebra=False
                )
                story.append(KeepTogether([tbl]))
                story.append(Spacer(1, 6))
        else:
            story.append(Paragraph("—", P))

    # -------- construir PDF --------
    doc.build(
        story,
        onFirstPage=header_footer,
        onLaterPages=header_footer
    )
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return buf.getvalue(), f"FICHA_{tipo}_{cod}_{ts}.pdf"

# --- Tab 5: Fichas acciones formativas ---
@st.cache_data(show_spinner=False)
def ficha_A(cod_a: str, dfA, dfB, dfC, map_b_a, map_c_b, ref_b, ref_c, a2niveles):
    cod_a = nz(cod_a)
    infoA = dfA[dfA["cod_a"] == cod_a].copy()
    if infoA.empty:
        return {}
    nom_a = first_nonempty(infoA["nom_a"]) if "nom_a" in infoA.columns else ""
    fam_a = first_nonempty(infoA["familia"]) if "familia" in infoA.columns else ""
    horas_a = first_nonempty(infoA["horas_a"]) if "horas_a" in infoA.columns else ""
    nivel_a = "/".join(sorted(set(a2niveles.get(cod_a, [])))) or "Desconocido"
    ra_texto = " ".join(sorted(set(infoA.get("ra_texto", pd.Series()).astype(str).str.strip().tolist())))
    fuente_url = first_nonempty(infoA.get("fuente_url", pd.Series()))
    rd_id = first_nonempty(infoA.get("rd_id", pd.Series()))
    rd_num = first_nonempty(infoA.get("rd_num", pd.Series()))
    fecha_rd = first_nonempty(infoA.get("fecha_rd", pd.Series()))
    b_rows = map_b_a[map_b_a["cod_a"] == cod_a][["cod_b"]].drop_duplicates()
    b_rows = b_rows.merge(ref_b, on="cod_b", how="left")
    b_rows = b_rows[["cod_b","nom_b","nivel_b","familias","horas_b"]].drop_duplicates().sort_values("cod_b")
    cs = map_c_b[map_c_b["cod_b"].isin(b_rows["cod_b"])][["cod_c"]].drop_duplicates()
    cs = cs.merge(ref_c, on="cod_c", how="left")
    cs = cs[["cod_c","nom_c","nivel_c","familias","horas_c"]].drop_duplicates().sort_values("cod_c")
    return {
        "cod_a": cod_a, "nom_a": nom_a, "familia": fam_a, "nivel": nivel_a, "horas": horas_a,
        "ra_texto": ra_texto,
        "fuente_url": fuente_url, "rd_id": rd_id, "rd_num": rd_num, "fecha_rd": fecha_rd,
        "Bs": b_rows, "Cs": cs
    }

@st.cache_data(show_spinner=False)
def ficha_B(cod_b: str, dfA, dfB, dfC, map_b_a, map_c_b, ref_b, ref_c):
    cod_b = nz(cod_b)
    infoB = ref_b[ref_b["cod_b"] == cod_b].copy()
    if infoB.empty:
        return {}
    nom_b = first_nonempty(infoB["nom_b"])
    fams_b = first_nonempty(infoB["familias"])
    horas_b = first_nonempty(infoB["horas_b"])
    nivel_b = first_nonempty(infoB["nivel_b"])
    rawB = dfB[dfB["cod_b"] == cod_b].copy()
    fuente_url = first_nonempty(rawB.get("fuente_url", pd.Series()))
    rd_id = first_nonempty(rawB.get("rd_id", pd.Series()))
    rd_num = first_nonempty(rawB.get("rd_num", pd.Series()))
    fecha_rd = first_nonempty(rawB.get("fecha_rd", pd.Series()))
    a_rows = map_b_a[map_b_a["cod_b"] == cod_b][["cod_a","nom_a","ra_texto","horas_a","familia"]].drop_duplicates()
    a_rows = a_rows.sort_values("cod_a")
    c_rows = map_c_b[map_c_b["cod_b"] == cod_b][["cod_c"]].drop_duplicates()
    c_rows = c_rows.merge(ref_c, on="cod_c", how="left")
    c_rows = c_rows[["cod_c","nom_c","nivel_c","familias","horas_c"]].drop_duplicates().sort_values("cod_c")
    return {
        "cod_b": cod_b, "nom_b": nom_b, "familias": fams_b, "nivel": nivel_b, "horas": horas_b,
        "fuente_url": fuente_url, "rd_id": rd_id, "rd_num": rd_num, "fecha_rd": fecha_rd,
        "As": a_rows, "Cs": c_rows
    }

@st.cache_data(show_spinner=False)
def ficha_C(cod_c: str, dfA, dfB, dfC, map_b_a, map_c_b, ref_b, ref_c):
    cod_c = nz(cod_c)
    infoC = ref_c[ref_c["cod_c"] == cod_c].copy()
    if infoC.empty:
        return {}
    nom_c = first_nonempty(infoC["nom_c"])
    fams_c = first_nonempty(infoC["familias"])
    horas_c = first_nonempty(infoC["horas_c"])
    nivel_c = first_nonempty(infoC["nivel_c"])
    rawC = dfC[dfC["cod_c"] == cod_c].copy()
    fuente_url = first_nonempty(rawC.get("fuente_url", pd.Series()))
    rd_id = first_nonempty(rawC.get("rd_id", pd.Series()))
    rd_num = first_nonempty(rawC.get("rd_num", pd.Series()))
    fecha_rd = first_nonempty(rawC.get("fecha_rd", pd.Series()))
    comp_general = first_nonempty(rawC.get("comp_general", pd.Series()))
    comp_prof = first_nonempty(rawC.get("comp_prof", pd.Series()))
    if ("codigo_comp" in rawC.columns) or ("estandar_comp" in rawC.columns) or ("estándar_comp" in rawC.columns):
        uc_df = rawC.copy()
        if "estándar_comp" in uc_df.columns and "estandar_comp" not in uc_df.columns:
            uc_df.rename(columns={"estándar_comp":"estandar_comp"}, inplace=True)
        for c in ["codigo_comp","estandar_comp"]:
            if c not in uc_df.columns: uc_df[c] = ""
        uc_df = uc_df[["codigo_comp","estandar_comp"]].drop_duplicates().sort_values("codigo_comp")
    else:
        uc_df = pd.DataFrame(columns=["codigo_comp","estandar_comp"])
    mod_cols = [c for c in rawC.columns if c.lower().startswith("modulos_prof")]
    if mod_cols:
        mods = []
        for _, r in rawC.iterrows():
            for mc in mod_cols:
                val = nz(r[mc])
                if val:
                    mods.append(val)
        mods_df = pd.DataFrame({"modulo_profesional": sorted(set(mods))})
    else:
        mods_df = pd.DataFrame(columns=["modulo_profesional"])
    b_rows = map_c_b[map_c_b["cod_c"] == cod_c][["cod_b"]].drop_duplicates()
    b_rows = b_rows.merge(ref_b, on="cod_b", how="left")
    b_rows = b_rows[["cod_b","nom_b","nivel_b","familias","horas_b"]].drop_duplicates().sort_values("cod_b")
    ba = map_b_a.merge(b_rows[["cod_b"]], on="cod_b", how="inner")[["cod_b","cod_a","nom_a","ra_texto","horas_a","familia"]]
    ba = ba.drop_duplicates().sort_values(["cod_b","cod_a"])
    return {
        "cod_c": cod_c, "nom_c": nom_c, "familias": fams_c, "nivel": nivel_c, "horas": horas_c,
        "fuente_url": fuente_url, "rd_id": rd_id, "rd_num": rd_num, "fecha_rd": fecha_rd,
        "comp_general": comp_general, "comp_prof": comp_prof,
        "UCs": uc_df, "Modulos": mods_df, "Bs": b_rows, "B_As": ba
    }

def exportar_ficha_excel(ficha: dict, tipo: str):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        pd.DataFrame([ficha]).drop(columns=["Bs","Cs","UCs","Modulos","B_As","As"], errors="ignore").to_excel(w, "Resumen", index=False)
        if tipo == "A":
            ficha.get("Bs", pd.DataFrame()).to_excel(w, "B_relacionados", index=False)
            ficha.get("Cs", pd.DataFrame()).to_excel(w, "C_relacionados", index=False)
        elif tipo == "B":
            ficha.get("As", pd.DataFrame()).to_excel(w, "A_componentes", index=False)
            ficha.get("Cs", pd.DataFrame()).to_excel(w, "C_relacionados", index=False)
        elif tipo == "C":
            ficha.get("Bs", pd.DataFrame()).to_excel(w, "B_componentes", index=False)
            ficha.get("B_As", pd.DataFrame()).to_excel(w, "A_por_B", index=False)
            ficha.get("UCs", pd.DataFrame()).to_excel(w, "UCs", index=False)
            ficha.get("Modulos", pd.DataFrame()).to_excel(w, "Modulos", index=False)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return bio.getvalue(), f"FICHA_{tipo}_{nz(ficha.get('cod_'+tipo.lower()))}_{ts}.xlsx"

with tabs[4]:
    st.subheader("Fichas acciones formativas")
    st.caption("Explora la ficha de un A/B/C con filtros por Familia y Nivel. Incluye exportación y deep-links al BOE.")

    colf1, colf2, colf3 = st.columns([1,1,2])
    with colf1:
        tipo = st.radio("Tipo", ["A", "B", "C"], horizontal=True, index=0, key="ficha_tipo")
    with colf2:
        fam_filter = st.multiselect("Familias", familias_all, default=[])
    with colf3:
        niv_filter = st.multiselect("Niveles", niveles_all, default=[])

    q = st.text_input("Buscar por código o nombre contiene…", "")

    if tipo == "A":
        cat = dfA[["familia","cod_a","nom_a","horas_a"]].drop_duplicates()
        cat["nivel_a"] = cat["cod_a"].map(lambda c: "/".join(sorted(set(a2niveles.get(nz(c), [])))) or "Desconocido")
        if fam_filter:
            sel_norm = {norm_txt(x) for x in fam_filter}
            cat = cat[cat["familia"].apply(lambda s: norm_txt(s) in sel_norm)]
        if niv_filter:
            cat = cat[cat["nivel_a"].apply(lambda s: bool(set(s.split("/")) & set(niv_filter)))]
        if q:
            qq = q.lower().strip()
            cat = cat[cat.apply(lambda r: qq in nz(r["cod_a"]).lower() or qq in nz(r["nom_a"]).lower(), axis=1)]
        cat = cat.sort_values(["familia","cod_a"])
        cat["etiqueta"] = cat.apply(lambda r: f"{r['cod_a']} — {nz(r['nom_a'])} (Fam: {nz(r['familia'])}, Nivel: {nz(r['nivel_a'])})", axis=1)
        opt_map = dict(zip(cat["etiqueta"], cat["cod_a"]))
    elif tipo == "B":
        cat = ref_b[["familias","cod_b","nom_b","horas_b","nivel_b"]].drop_duplicates()
        if fam_filter:
            sel_norm = {norm_txt(x) for x in fam_filter}
            def fams_ok(s):
                toks = [t.strip() for t in str(s).split(",")]
                return bool({norm_txt(t) for t in toks if t} & sel_norm)
            cat = cat[cat["familias"].apply(fams_ok)]
        if niv_filter:
            cat = cat[cat["nivel_b"].apply(lambda s: any(n==s or n in s.split("/") for n in niv_filter))]
        if q:
            qq = q.lower().strip()
            cat = cat[cat.apply(lambda r: qq in nz(r["cod_b"]).lower() or qq in nz(r["nom_b"]).lower(), axis=1)]
        cat = cat.sort_values(["cod_b"])
        cat["etiqueta"] = cat.apply(lambda r: f"{r['cod_b']} — {nz(r['nom_b'],'«sin nombre»')} (Nivel: {nz(r['nivel_b'])})", axis=1)
        opt_map = dict(zip(cat["etiqueta"], cat["cod_b"]))
    else:
        cat = ref_c[["familias","cod_c","nom_c","horas_c","nivel_c"]].drop_duplicates()
        if fam_filter:
            sel_norm = {norm_txt(x) for x in fam_filter}
            def fams_ok(s):
                toks = [t.strip() for t in str(s).split(",")]
                return bool({norm_txt(t) for t in toks if t} & sel_norm)
            cat = cat[cat["familias"].apply(fams_ok)]
        if niv_filter:
            cat = cat[cat["nivel_c"].apply(lambda s: s in niv_filter)]
        if q:
            qq = q.lower().strip()
            cat = cat[cat.apply(lambda r: qq in nz(r["cod_c"]).lower() or qq in nz(r["nom_c"]).lower(), axis=1)]
        cat = cat.sort_values(["cod_c"])
        cat["etiqueta"] = cat.apply(lambda r: f"{r['cod_c']} — {nz(r['nom_c'])} (Nivel: {nz(r['nivel_c'])})", axis=1)
        opt_map = dict(zip(cat["etiqueta"], cat["cod_c"]))

    sel = st.selectbox(f"Selecciona {tipo}…", options=["—"] + cat["etiqueta"].tolist(), index=0)
    if sel == "—":
        st.info("Elige un elemento para ver su ficha.")
        st.stop()

    cod_sel = opt_map.get(sel, "")
    if not cod_sel:
        st.warning("No se pudo resolver el código seleccionado.")
        st.stop()

    if tipo == "A":
        F = ficha_A(cod_sel, dfA, dfB, dfC, map_b_a, map_c_b, ref_b, ref_c, a2niveles)
        if not F: st.error("No se pudo construir la ficha A."); st.stop()
        st.markdown(f"### {F['cod_a']} — {F['nom_a']}")
        topcols = st.columns([2,1,1,1])
        with topcols[0]: st.markdown(f"**Familia:** {F['familia']}")
        with topcols[1]: st.markdown(f"**Nivel (inferido):** {F['nivel']}")
        with topcols[2]: st.markdown(f"**Horas:** {F['horas']}")
        with topcols[3]:
            if F.get("fuente_url"):
                st.markdown(f"[**BOE**]({F['fuente_url']})")
            else:
                st.markdown("**BOE:** —")
        st.caption(f"RD: {nz(F.get('rd_num'))} · Fecha: {nz(F.get('fecha_rd'))} · ID: {nz(F.get('rd_id'))}")

        st.markdown("#### RA (texto completo)")
        st.write(F["ra_texto"] or "—")
        st.markdown("#### B a los que contribuye")
        st.dataframe(F["Bs"], use_container_width=True, hide_index=True)
        st.markdown("#### C relacionados (vía B)")
        st.dataframe(F["Cs"], use_container_width=True, hide_index=True)

        st.markdown("**Copiar códigos:**")
        copy_to_clipboard_button("📋 Copiar este A", F["cod_a"], key="fichaA_copyA")
        st.write("")
        copy_to_clipboard_button("📋 Copiar B relacionados", " ".join(F["Bs"]["cod_b"].tolist()) if not F["Bs"].empty else "", key="fichaA_copyB")
        st.write("")
        copy_to_clipboard_button("📋 Copiar C relacionados", " ".join(F["Cs"]["cod_c"].tolist()) if not F["Cs"].empty else "", key="fichaA_copyC")

        xls_bytes, xls_name = exportar_ficha_excel(F, "A")
        st.download_button("💾 Exportar ficha (Excel)", data=xls_bytes, file_name=xls_name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        pdf_bytes, pdf_name = exportar_ficha_pdf(F, "A")
        st.download_button("🖨️ Exportar ficha (PDF)", data=pdf_bytes, file_name=pdf_name, mime="application/pdf", use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            if st.button("➕ Mandar RA a ESCO", key="fichaA_esco_ra"):
                add_to_esco_queue([F["ra_texto"]], f"Ficha A {F['cod_a']} (RA)")
        with c2:
            if st.button("➕ Mandar nombre A a ESCO", key="fichaA_esco_nom"):
                add_to_esco_queue([F["nom_a"]], f"Ficha A {F['cod_a']} (nombre)")

    elif tipo == "B":
        F = ficha_B(cod_sel, dfA, dfB, dfC, map_b_a, map_c_b, ref_b, ref_c)
        if not F: st.error("No se pudo construir la ficha B."); st.stop()
        st.markdown(f"### {F['cod_b']} — {F['nom_b'] or '«sin nombre»'}")
        topcols = st.columns([2,1,1,1])
        with topcols[0]: st.markdown(f"**Familia(s):** {F['familias']}")
        with topcols[1]: st.markdown(f"**Nivel:** {F['nivel']}")
        with topcols[2]: st.markdown(f"**Horas:** {F['horas']}")
        with topcols[3]:
            if F.get("fuente_url"):
                st.markdown(f"[**BOE**]({F['fuente_url']})")
            else:
                st.markdown("**BOE:** —")
        st.caption(f"RD: {nz(F.get('rd_num'))} · Fecha: {nz(F.get('fecha_rd'))} · ID: {nz(F.get('rd_id'))}")

        st.markdown("#### A que lo componen (con RA)")
        st.dataframe(F["As"], use_container_width=True, hide_index=True)
        st.markdown("#### C en los que participa")
        st.dataframe(F["Cs"], use_container_width=True, hide_index=True)

        st.markdown("**Copiar códigos:**")
        copy_to_clipboard_button("📋 Copiar este B", F["cod_b"], key="fichaB_copyB")
        st.write("")
        copy_to_clipboard_button("📋 Copiar A (componentes)", " ".join(F["As"]["cod_a"].tolist()) if not F["As"].empty else "", key="fichaB_copyA")
        st.write("")
        copy_to_clipboard_button("📋 Copiar C (relacionados)", " ".join(F["Cs"]["cod_c"].tolist()) if not F["Cs"].empty else "", key="fichaB_copyC")

        xls_bytes, xls_name = exportar_ficha_excel(F, "B")
        st.download_button("💾 Exportar ficha (Excel)", data=xls_bytes, file_name=xls_name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        pdf_bytes, pdf_name = exportar_ficha_pdf(F, "B")
        st.download_button("🖨️ Exportar ficha (PDF)", data=pdf_bytes, file_name=pdf_name, mime="application/pdf", use_container_width=True)
        # (sin “Mandar a ESCO” en B por ahora)

    else:
        F = ficha_C(cod_sel, dfA, dfB, dfC, map_b_a, map_c_b, ref_b, ref_c)
        if not F: st.error("No se pudo construir la ficha C."); st.stop()
        st.markdown(f"### {F['cod_c']} — {F['nom_c']}")
        topcols = st.columns([2,1,1,1])
        with topcols[0]: st.markdown(f"**Familia(s):** {F['familias']}")
        with topcols[1]: st.markdown(f"**Nivel:** {F['nivel']}")
        with topcols[2]: st.markdown(f"**Horas:** {F['horas']}")
        with topcols[3]:
            if F.get("fuente_url"):
                st.markdown(f"[**BOE**]({F['fuente_url']})")
            else:
                st.markdown("**BOE:** —")
        st.caption(f"RD: {nz(F.get('rd_num'))} · Fecha: {nz(F.get('fecha_rd'))} · ID: {nz(F.get('rd_id'))}")

        st.markdown("#### Competencia general")
        st.write(F["comp_general"] or "—")
        st.markdown("#### Competencias profesionales/empleabilidad")
        st.write(F["comp_prof"] or "—")
        st.markdown("#### Estándares (UC)")
        st.dataframe(F["UCs"], use_container_width=True, hide_index=True)
        st.markdown("#### Módulos profesionales del certificado")
        st.dataframe(F["Modulos"], use_container_width=True, hide_index=True)
        st.markdown("#### B que lo componen")
        st.dataframe(F["Bs"], use_container_width=True, hide_index=True)
        st.markdown("#### A por cada B")
        st.dataframe(F["B_As"], use_container_width=True, hide_index=True)

        st.markdown("**Copiar códigos:**")
        copy_to_clipboard_button("📋 Copiar este C", F["cod_c"], key="fichaC_copyC")
        st.write("")
        copy_to_clipboard_button("📋 Copiar B (componentes)", " ".join(F["Bs"]["cod_b"].tolist()) if not F["Bs"].empty else "", key="fichaC_copyB")
        st.write("")
        copy_to_clipboard_button("📋 Copiar A (por B)", " ".join(F["B_As"]["cod_a"].tolist()) if not F["B_As"].empty else "", key="fichaC_copyA")
        st.write("")
        copy_to_clipboard_button("📋 Copiar UCs", " ".join(F["UCs"]["codigo_comp"].tolist()) if not F["UCs"].empty else "", key="fichaC_copyUC")

        xls_bytes, xls_name = exportar_ficha_excel(F, "C")
        st.download_button("💾 Exportar ficha (Excel)", data=xls_bytes, file_name=xls_name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        pdf_bytes, pdf_name = exportar_ficha_pdf(F, "C")
        st.download_button("🖨️ Exportar ficha (PDF)", data=pdf_bytes, file_name=pdf_name, mime="application/pdf", use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            if st.button("➕ Mandar COMP_GENERAL/PROF a ESCO", key="fichaC_esco_txt"):
                texts = [F.get("comp_general",""), F.get("comp_prof","")]
                add_to_esco_queue([t for t in texts if t], f"Ficha C {F['cod_c']} (texto competencias)")
        with c2:
            if st.button("➕ Mandar UCs a ESCO", key="fichaC_esco_uc"):
                add_to_esco_queue(F["UCs"]["estandar_comp"].astype(str).tolist() if not F["UCs"].empty else [], f"Ficha C {F['cod_c']} (UCs)")

# ========= Aviso de calidad =========
faltan_A = dfA[(dfA.get("cod_b","")=="") | (dfA.get("cod_a","")=="")]
faltan_B = dfB[(dfB.get("cod_b","")=="") | (dfB.get("cod_c","")=="")]
if (len(faltan_A) > 0) or (len(faltan_B) > 0):
    st.info("⚠️ Detectados posibles códigos faltantes en A/B. Si usas versiones 'fix_*', la app las prioriza.")
