# esco_mapper_pro.py
# -*- coding: utf-8 -*-
"""
Mapeo de textos de competencias a ESCO (skills & occupations).
- Multi-query (reformular consultas)
- Bonus por palabras clave
- Fuzzy + score ESCO fusionado
- Reintentos y timeouts configurables
- Parseo HAL correcto + fallback a /suggest2
Devuelve DataFrames listos para la UI.
"""
from __future__ import annotations
import time, re, html, logging
from typing import List, Dict, Any, Tuple
import requests
import pandas as pd

# ======== Utilidades base ========
def _nz(x, default: str = "") -> str:
    try:
        if x is None:
            return default
        s = str(x).strip()
        return s if s else default
    except Exception:
        return default

# ======== Fuzzy: rapidfuzz (preferido) o difflib (fallback) ========
try:
    from rapidfuzz import fuzz
    def fuzzy_sim(a: str, b: str) -> float:
        a = _nz(a).lower()
        b = _nz(b).lower()
        if not a or not b:
            return 0.0
        return fuzz.token_set_ratio(a, b) / 100.0
except Exception:
    import difflib
    def fuzzy_sim(a: str, b: str) -> float:
        a = _nz(a).lower()
        b = _nz(b).lower()
        if not a or not b:
            return 0.0
        return difflib.SequenceMatcher(None, a, b).ratio()

STOP_ES = {
    "la","el","los","las","de","del","y","o","u","a","en","con","por","para","al","lo",
    "un","una","unos","unas","que","se","su","sus","como","más","mas","menos","entre",
    "sobre","sin","es","son","ser","estar","esta","este","estos","estas","cada","donde",
    "cuando","también","tambien","muy","si","no","ya","le","les","me","te","mi","tu",
    "tras","hacia","hasta","desde","ante","bajo","cual","cuales","cuyo","cuyos","cuyas",
    "etc","ej","según","segun"
}

def simple_keywords(text: str, max_kw: int = 8) -> List[str]:
    t = _nz(text).lower()
    t = re.sub(r"\bra\s*\d+\.\s*", " ", t, flags=re.I)  # quita "RA1.", etc.
    t = re.sub(r"[^\wáéíóúñü ]+", " ", t, flags=re.I)
    toks = [w.strip() for w in t.split() if len(w.strip()) >= 3]
    toks = [w for w in toks if w not in STOP_ES]
    freq: Dict[str, int] = {}
    for w in toks:
        freq[w] = freq.get(w, 0) + 1
    ranked = sorted(freq.items(), key=lambda x: (x[1], len(x[0])), reverse=True)
    return [w for w, _ in ranked[:max_kw]]

def rewrite_queries(q: str) -> List[str]:
    """Variantes para ampliar el recall en ESCO."""
    q = _nz(q)
    if not q:
        return []
    out = [q]
    q2 = re.sub(r"\bra\s*\d+\.\s*", " ", q, flags=re.I).strip()
    if q2 and q2 != q:
        out.append(q2)
    short = " ".join(q2.split()[:12]).strip()
    if short and short not in out:
        out.append(short)
    kws = simple_keywords(q)
    if kws:
        kw_line = " ".join(kws)
        if kw_line not in out:
            out.append(kw_line)
    seen, uniq = set(), []
    for v in out:
        if v and v not in seen:
            uniq.append(v); seen.add(v)
    return uniq

def bonus_keywords(label: str, kws: List[str]) -> float:
    if not kws:
        return 0.0
    lab = _nz(label).lower()
    hits = sum(1 for k in kws if k.lower() in lab)
    return min(0.15, 0.03 * hits)  # bonus suave acumulable hasta 0.15

# ======== Cliente ESCO ========
# Usamos /search como base y /suggest2 como fallback
ESCO_API_BASE = "https://ec.europa.eu/esco/api"
USER_AGENT = "Planificador-ABC/1.0 (+contacto)"
DEFAULT_LANG = "es"

class ESCOClient:
    def __init__(self, timeout_connect=10, timeout_read=45, retries=2, backoff=0.8):
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=20, max_retries=0)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            "Accept": "application/json,application/json;charset=UTF-8",
            "User-Agent": USER_AGENT,
        })
        self.timeout_connect = timeout_connect
        self.timeout_read = timeout_read
        self.retries = retries
        self.backoff = backoff

    def _call(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        last_err = None
        for i in range(self.retries + 1):
            try:
                r = self.session.get(
                    url,
                    params=params,
                    timeout=(self.timeout_connect, self.timeout_read),
                )
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_err = e
                if i < self.retries:
                    time.sleep(self.backoff * (i + 1))
        raise last_err

    def _parse_hal_results(self, data: Any) -> List[Dict[str, Any]]:
        """
        Normaliza posibles estructuras HAL:
        - _embedded.results (más común)
        - items (fallback raro)
        - lista directa
        """
        if isinstance(data, dict):
            emb = data.get("_embedded") or {}
            items = emb.get("results") or emb.get("resource") or []
            if not items and isinstance(data.get("items"), list):
                items = data["items"]
        elif isinstance(data, list):
            items = data
        else:
            items = []
        return items if isinstance(items, list) else []

    def _normalize_item(self, it: Dict[str, Any]) -> Dict[str, Any]:
        # ESCO devuelve a veces 'title' (full=false) y a veces 'preferredLabel'
        label = _nz(it.get("preferredLabel")) or _nz(it.get("title")) or _nz(it.get("label"))
        uri   = _nz(it.get("uri")) or _nz(it.get("id"))
        score = float(it.get("score", 0.0) or 0.0)
        return {"label": label, "uri": uri, "score": score}

    def search(self, text: str, type_: str, lang: str = DEFAULT_LANG, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Envuelve /api/search de ESCO.
        type_ : 'skill' | 'occupation'
        Retorna lista normalizada con campos: label, uri, score
        """
        text = _nz(text)
        if not text:
            return []
        # /search rápido: full=false
        url = f"{ESCO_API_BASE}/search"
        params = {
            "text": text,
            "type": [type_],      # array es más compatible
            "language": lang,
            "limit": max(1, min(50, int(limit))),
            "offset": 0,
            "full": "false",
        }
        try:
            data = self._call(url, params)
            raw = self._parse_hal_results(data)
            items = []
            for it in raw:
                norm = self._normalize_item(it)
                if norm["label"] and norm["uri"]:
                    items.append(norm)
            if items:
                return items
        except Exception as e:
            logging.warning(f"[ESCO] /search fallo: {e}")

        # Fallback a /suggest2 (rápido y a veces más robusto)
        try:
            sug_url = f"{ESCO_API_BASE}/suggest2"
            sug_params = {
                "text": text,
                "type": [type_],
                "language": lang,
                "limit": max(1, min(30, int(limit))),
                "offset": 0,
                "alt": "true",
            }
            data = self._call(sug_url, sug_params)
            raw = self._parse_hal_results(data)
            items = []
            for it in raw:
                norm = self._normalize_item(it)
                if norm["label"] and norm["uri"]:
                    items.append(norm)
            return items
        except Exception as e2:
            logging.error(f"[ESCO] /suggest2 fallo: {e2}")
            return []

# helpers directos para la app (firmas compatibles con tu código)
def esco_search_skills(text: str, lang="es", limit=25,
                       timeout_connect=10, timeout_read=45) -> List[Dict[str, Any]]:
    cli = ESCOClient(timeout_connect=timeout_connect, timeout_read=timeout_read)
    items = cli.search(text, type_="skill", lang=lang, limit=limit)
    out = []
    for it in items:
        out.append({
            "esco_skill_label": it["label"],
            "esco_skill_uri": it["uri"],
            "score": it.get("score", 0.0) or 0.0
        })
    return out

def esco_search_occupations(text: str, lang="es", limit=25,
                            timeout_connect=10, timeout_read=45) -> List[Dict[str, Any]]:
    cli = ESCOClient(timeout_connect=timeout_connect, timeout_read=timeout_read)
    items = cli.search(text, type_="occupation", lang=lang, limit=limit)
    out = []
    for it in items:
        out.append({
            "esco_occ_label": it["label"],
            "esco_occ_uri": it["uri"],
            "score": it.get("score", 0.0) or 0.0
        })
    return out

# ======== Motor principal (multi-query + fuzzy + bonus KW) ========
def map_competencias_a_esco_pro(
    textos: List[str],
    language: str = "es",
    top_k_skills: int = 8,
    top_k_occs: int = 8,
    alpha_fuzzy: float = 0.6,
    timeout_connect: int = 10,
    timeout_read: int = 45,
    api_limit: int = 25,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Retorna:
      df_skills [input_text, esco_skill_label, esco_skill_uri, score, method]
      df_occs   [input_text, esco_occ_label, esco_occ_uri, score, method]
      metrics   {elapsed_s, n_inputs, calls_skills, calls_occs}
    """
    t0 = time.time()
    textos = [t for t in (_nz(x) for x in textos) if t]
    rows_s, rows_o = [], []
    metrics = {"calls_skills": 0, "calls_occs": 0, "n_inputs": len(textos)}

    cli = ESCOClient(timeout_connect=timeout_connect, timeout_read=timeout_read)

    for q in textos:
        VARS = rewrite_queries(q)
        kws = simple_keywords(q)

        # ----- SKILLS -----
        merged_s: Dict[str, Dict[str, Any]] = {}
        for v in VARS:
            # idioma pedido
            try:
                res = cli.search(v, type_="skill", lang=language, limit=api_limit)
                metrics["calls_skills"] += 1
            except Exception:
                res = []
            # fallback en inglés si vacío/errores
            if not res:
                try:
                    res = cli.search(v, type_="skill", lang="en", limit=api_limit)
                    metrics["calls_skills"] += 1
                except Exception:
                    res = []

            for it in res:
                es = it.get("score", 0.0) or 0.0
                fz = fuzzy_sim(q, it.get("label", ""))
                bns = bonus_keywords(it.get("label",""), kws)
                score = alpha_fuzzy * fz + (1.0 - alpha_fuzzy) * es + bns
                uri = _nz(it.get("uri"))
                cand = {
                    "input_text": q,
                    "esco_skill_label": _nz(it.get("label")),
                    "esco_skill_uri": uri,
                    "score": float(score),
                    "method": "multi-search+fuzzy+kw"
                }
                if uri and (uri not in merged_s or merged_s[uri]["score"] < cand["score"]):
                    merged_s[uri] = cand

        best_s = sorted(merged_s.values(), key=lambda x: x["score"], reverse=True)[:max(1, int(top_k_skills))]
        rows_s.extend(best_s)

        # ----- OCCUPATIONS -----
        merged_o: Dict[str, Dict[str, Any]] = {}
        for v in VARS:
            try:
                res = cli.search(v, type_="occupation", lang=language, limit=api_limit)
                metrics["calls_occs"] += 1
            except Exception:
                res = []
            if not res:
                try:
                    res = cli.search(v, type_="occupation", lang="en", limit=api_limit)
                    metrics["calls_occs"] += 1
                except Exception:
                    res = []

            for it in res:
                es = it.get("score", 0.0) or 0.0
                fz = fuzzy_sim(q, it.get("label", ""))
                bns = bonus_keywords(it.get("label",""), kws)
                score = alpha_fuzzy * fz + (1.0 - alpha_fuzzy) * es + bns
                uri = _nz(it.get("uri"))
                cand = {
                    "input_text": q,
                    "esco_occ_label": _nz(it.get("label")),
                    "esco_occ_uri": uri,
                    "score": float(score),
                    "method": "multi-search+fuzzy+kw"
                }
                if uri and (uri not in merged_o or merged_o[uri]["score"] < cand["score"]):
                    merged_o[uri] = cand

        best_o = sorted(merged_o.values(), key=lambda x: x["score"], reverse=True)[:max(1, int(top_k_occs))]
        rows_o.extend(best_o)

    df_s = pd.DataFrame(rows_s, columns=["input_text","esco_skill_label","esco_skill_uri","score","method"])
    df_o = pd.DataFrame(rows_o, columns=["input_text","esco_occ_label","esco_occ_uri","score","method"])
    metrics["elapsed_s"] = round(time.time() - t0, 3)
    return df_s, df_o, metrics


# ======== CLI simple de prueba (opcional) ========
if __name__ == "__main__":
    import sys, json
    textos = sys.argv[1:] or ["Aplicaciones básicas de ofimática", "Técnicas administrativas básicas"]
    df_s, df_o, m = map_competencias_a_esco_pro(textos, language="es", top_k_skills=6, top_k_occs=6, alpha_fuzzy=0.65)
    print("Metrics:", json.dumps(m, ensure_ascii=False, indent=2))
    print("\n== SKILLS ==")
    print(df_s.head(12).to_string(index=False))
    print("\n== OCCUPATIONS ==")
    print(df_o.head(12).to_string(index=False))
