"""
Universal data cleaner v2  (needs universal_anomaly_detector.py in the same folder)

Goal: output you can load straight into a database (DBeaver) with correct column types.

Principle: FIX what is deterministic, NULL what is unusable, FLAG what is ambiguous,
           DROP only exact duplicates (and those go to quarantine, never lost).
Originals are never modified.

Output (next to the input CSVs):
  cleaned/              <table>.csv  + schema.sql (CREATE TABLE, explicit types) + load_postgres.sql
  quarantine/           <table>.csv  rows removed from the clean table, with reason
  cleaning_report/      change_summary.csv, change_log_rows.csv, reconciliation.csv, validation.csv

Every cleaned table has a last column  dq_flag:
  'ok'                          nothing suspicious
  'outlier:duration'            one or more reasons joined by '|'
Strict analysis:  WHERE dq_flag = 'ok'      As-is analysis:  no filter.
"""
import math
import os
import re
import warnings

import numpy as np
import pandas as pd

from universal_anomaly_detector import infer_type, as_key, CFG as DCFG

warnings.filterwarnings("ignore")

CFG = {
    # ---- policies (change here, not in code)
    "exact_duplicates": "drop",       # drop (-> quarantine) | flag
    "dup_ignoring_id": "flag",        # flag | drop
    "orphan_fk": "flag",              # flag | null | drop
    "fd_conflict": "flag",            # flag | majority
    "out_of_range_date": "flag",      # flag | null
    "category_variants": "merge",     # merge (case/whitespace only) | flag
    "sql_quote": '"',                 # '"' postgres/redshift/sqlite   '`' mysql
    "strict_validation": True,        # raise if the exported files do not match schema.sql
    # ---- learned-rule thresholds
    "fk_min_overlap": DCFG["fk_min_overlap"],
    "order_learn_ratio": DCFG["order_learn_ratio"],
    "fd_learn_ratio": DCFG["fd_learn_ratio"],
    "iqr_mult": DCFG["iqr_mult"],
    "fixed_sample_rows": 200,         # row-level log of FIXED actions is sampled (summary counts are exact)
}

STRICT_ID = re.compile(r"(^id$|_id$|uuid|guid)")
ISO = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{1,2}:\d{2}(:\d{2}(\.\d+)?)?)?\s*(Z|[+-]\d{2}:?\d{2})?$")
DMY = re.compile(r"^(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{2,4})(?:[ T](\d{1,2}:\d{2}(?::\d{2})?))?$")

summary, rowlogs, flags, quarantine = [], [], {}, {}


# ------------------------------------------------------------------ logging
def log(table, idx, col, action, reason, old=None, new=None, code=None):
    idx = pd.Index(idx)
    if len(idx) == 0:
        return
    summary.append({"Table": table, "Column": col, "Action": action, "Reason": reason, "Count": len(idx)})
    sel = slice(0, CFG["fixed_sample_rows"]) if action == "FIXED" else slice(None)
    k = len(idx[sel])
    rowlogs.append(pd.DataFrame({
        "Table": table, "Row": idx[sel], "Column": col, "Action": action, "Reason": reason,
        "Old": np.asarray(old, dtype=object)[sel] if old is not None else [""] * k,
        "New": np.asarray(new, dtype=object)[sel] if new is not None else [""] * k,
    }))
    if code:
        flags.setdefault(table, {}).setdefault(code, []).append(idx)


def note(table, col, action, reason, count=0):
    summary.append({"Table": table, "Column": col, "Action": action, "Reason": reason, "Count": count})


def snake(name):
    n = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(name).strip())
    n = re.sub(r"[^0-9a-zA-Z]+", "_", n).strip("_").lower() or "col"
    return "c_" + n if n[0].isdigit() else n


def unique_names(names):
    seen, out = {}, []
    for n in map(snake, names):
        seen[n] = seen.get(n, 0) + 1
        out.append(n if seen[n] == 1 else f"{n}_{seen[n]}")
    return out


def infer(s, name):
    typ = infer_type(s, name)
    if typ == "id" and not STRICT_ID.search(name):   # 'paid', 'valid' are not IDs
        typ = infer_type(s, "x")
    return typ


# ------------------------------------------------------------------ step 1: text normalisation
def normalize_text(t, df):
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_numeric_dtype(s) or pd.api.types.is_datetime64_any_dtype(s):
            continue
        st = s.astype("string")
        stripped = st.str.strip()
        ws = (st.notna() & (st != stripped)).fillna(False).to_numpy(dtype=bool)
        log(t, df.index[ws], col, "FIXED", "trimmed whitespace", st[ws].map(repr).to_numpy(), stripped[ws].to_numpy())
        ph = (stripped.str.lower().isin(DCFG["placeholders"]) & stripped.notna()).fillna(False).to_numpy(dtype=bool)
        shown = ph & (stripped.fillna("").to_numpy() != "")
        log(t, df.index[shown], col, "FIXED", "placeholder string -> NULL", stripped[shown].to_numpy(),
            ["NULL"] * int(shown.sum()))
        s2 = stripped.mask(ph).astype(object)
        df[col] = s2.where(s2.notna(), np.nan)
    return df


# ------------------------------------------------------------------ step 2: type conversion
def convert_numeric(t, df, col):
    s = df[col]
    nn = s.dropna().astype("string")
    if nn.empty:
        return s
    if nn.str.match(r"^[+-]?0\d").any():
        note(t, col, "KEPT_AS_TEXT", "leading zeros -> identifier/code, not converted to number")
        return s
    num = pd.to_numeric(nn, errors="coerce")
    bad = (num.isna() | np.isinf(num.astype("float64"))).to_numpy(dtype=bool)
    if bad.any():
        log(t, nn.index[bad], col, "NULLED", "non-numeric value in numeric column", nn[bad].to_numpy(),
            ["NULL"] * int(bad.sum()), code=f"nulled:{col}")
    good = nn[~bad]
    stripped = good.str.replace(r"\.0+$", "", regex=True)
    is_int = bool(stripped.str.match(r"^[+-]?\d+$").all()) and int(stripped.str.len().max()) <= 18
    if is_int:
        out = pd.Series(pd.array([pd.NA] * len(s), dtype="Int64"), index=s.index)
        out.loc[good.index] = pd.to_numeric(stripped).astype("int64").to_numpy()
    else:
        out = pd.Series(np.nan, index=s.index, dtype="float64")
        out.loc[good.index] = num[~bad].astype("float64").to_numpy()
    return out


def to_ns(p):
    """Dates outside 1700-2200 are junk for any real dataset (and overflow datetime64[ns]) -> NaT."""
    p = pd.Series(p)
    p = p.mask((p < pd.Timestamp("1700-01-01")) | (p > pd.Timestamp("2200-01-01")))
    return p.astype("datetime64[ns]")


def parse_dates(s):
    """Returns (datetime64 series, ambiguous-mask). Never guesses silently:
    ISO parsed strictly; d/m/y vs m/d/y decided by evidence, else flagged."""
    sx = s.astype("string").fillna("")
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    amb = pd.Series(False, index=s.index)

    iso = sx.str.match(ISO).fillna(False).to_numpy(dtype=bool)
    if iso.any():
        p = pd.to_datetime(sx[iso], format="ISO8601", utc=True, errors="coerce")
        out.loc[sx.index[iso]] = to_ns(p.dt.tz_localize(None)).to_numpy()

    m = sx.str.extract(DMY)
    has = m[0].notna().to_numpy(dtype=bool) & ~iso
    if has.any():
        a, b = m.loc[has, 0].astype(int), m.loc[has, 1].astype(int)
        y = m.loc[has, 2]
        y = y.where(y.str.len() == 4, "20" + y.str.zfill(2)).astype(int)
        a_gt, b_gt = (a > 12), (b > 12)
        col_dayfirst = bool(a_gt.any()) and not bool(b_gt.any())
        col_monthfirst = bool(b_gt.any()) and not bool(a_gt.any())
        day = np.where(a_gt, a, np.where(b_gt, b, a if col_dayfirst else b))
        mon = np.where(a_gt, b, np.where(b_gt, a, b if col_dayfirst else a))
        ambiguous = (~a_gt) & (~b_gt) & (a != b) & ~(col_dayfirst or col_monthfirst)
        d = pd.to_datetime(pd.DataFrame({"year": y.to_numpy(), "month": mon, "day": day}), errors="coerce")
        tt = m.loc[has, 3].fillna("00:00:00")
        tt = tt.where(tt.str.count(":") == 2, tt + ":00")
        d = d + pd.to_timedelta(tt.to_numpy(), errors="coerce")
        out.loc[sx.index[has]] = to_ns(d).to_numpy()
        amb.loc[sx.index[has]] = np.asarray(ambiguous)

    rest = (sx != "").to_numpy(dtype=bool) & ~iso & ~has
    if rest.any():
        r = sx[rest]
        digits = r.str.fullmatch(r"\d+")
        ymd = digits & (r.str.len() == 8)
        if ymd.any():
            out.loc[r.index[ymd.to_numpy(dtype=bool)]] = to_ns(pd.to_datetime(
                r[ymd], format="%Y%m%d", errors="coerce")).to_numpy()
        txt = r[~digits.to_numpy(dtype=bool)]          # pure digits that are not yyyymmdd = junk, never epoch-guessed
        if len(txt):
            out.loc[txt.index] = to_ns(pd.to_datetime(txt, errors="coerce", utc=True).dt.tz_localize(None)).to_numpy()
    return out, amb


def convert_column(t, df, col, typ):
    s = df[col]
    if typ == "empty":
        return s

    if typ in ("numeric_text", "id"):
        nn = s.dropna().astype("string")
        if len(nn) and pd.to_numeric(nn, errors="coerce").notna().mean() > 0.9:
            s = convert_numeric(t, df, col)
        if typ == "id":
            return s

    if typ == "datetime":
        lo, hi = pd.Timestamp(DCFG["min_date"]), DCFG["max_date"]
        p, amb = parse_dates(s)
        bad = (s.notna() & p.isna()).to_numpy(dtype=bool)
        log(t, df.index[bad], col, "NULLED", "unparseable date", s[bad].astype(str).to_numpy(),
            ["NULL"] * int(bad.sum()), code=f"nulled:{col}")
        a = amb.to_numpy(dtype=bool)
        log(t, df.index[a], col, "FLAGGED", "ambiguous day/month order (parsed month-first)",
            s[a].astype(str).to_numpy(), code=f"ambiguous_date:{col}")
        rng = (p.notna() & ((p < lo) | (p > hi))).to_numpy(dtype=bool)
        if CFG["out_of_range_date"] == "null":
            log(t, df.index[rng], col, "NULLED", "date out of range", p[rng].astype(str).to_numpy(),
                ["NULL"] * int(rng.sum()), code=f"nulled:{col}")
            p = p.mask(rng)
        else:
            log(t, df.index[rng], col, "FLAGGED", "date out of range", p[rng].astype(str).to_numpy(),
                code=f"date_oor:{col}")
        return p

    if typ == "categorical" and s.dtype == object:
        nn = s.dropna().astype(str)
        key = nn.str.replace(r"\s+", " ", regex=True).str.lower()     # case + whitespace ONLY, never punctuation
        mapping = {}
        for _, g in nn.groupby(key):
            if g.nunique() > 1:
                canon = g.value_counts().idxmax()
                mapping.update({v: canon for v in g.unique() if v != canon})
        if mapping:
            m = s.isin(list(mapping)).to_numpy(dtype=bool)
            if CFG["category_variants"] == "merge":
                log(t, df.index[m], col, "FIXED", "category case/whitespace variant -> most frequent spelling",
                    s[m].to_numpy(), s[m].map(mapping).to_numpy())
                s = s.replace(mapping)
            else:
                log(t, df.index[m], col, "FLAGGED", "category case/whitespace variant", s[m].to_numpy(),
                    code=f"cat_variant:{col}")
    return s


def check_numeric_flags(t, df, col, typ):
    s = df[col]
    if typ == "id" or not pd.api.types.is_numeric_dtype(s):
        return
    x = s.dropna().astype("float64")
    neg = x[x < 0]
    if 0 < len(neg) < 0.05 * len(x):
        log(t, neg.index, col, "FLAGGED", "negative value in mostly non-negative column", neg.to_numpy(),
            code=f"negative:{col}")
    q1, q3 = x.quantile([0.25, 0.75])
    iqr = q3 - q1
    if iqr > 0:
        k = CFG["iqr_mult"]
        out = x[(x < q1 - k * iqr) | (x > q3 + k * iqr)]
        log(t, out.index, col, "FLAGGED", f"extreme outlier (>{k}xIQR)", out.to_numpy(), code=f"outlier:{col}")


# ------------------------------------------------------------------ step 3: table level
def clean_table_rows(t, df, types):
    d = df.duplicated()
    dm = d.to_numpy(dtype=bool)
    if CFG["exact_duplicates"] == "drop":
        q = df[dm].copy()
        quarantine.setdefault(t, []).append(q.assign(quarantine_reason="exact duplicate row (first copy kept)"))
        log(t, df.index[dm], "ALL", "DROPPED", "exact duplicate row (kept first) -> quarantine")
        df = df[~dm]
    else:
        log(t, df.index[dm], "ALL", "FLAGGED", "exact duplicate row", code="exact_duplicate")

    pk = [c for c in df.columns if c == "id"]
    if pk:
        rest = [c for c in df.columns if c not in pk]
        d2 = df.duplicated(subset=rest).to_numpy(dtype=bool)
        if CFG["dup_ignoring_id"] == "drop":
            quarantine.setdefault(t, []).append(
                df[d2].copy().assign(quarantine_reason="duplicate except surrogate id (first copy kept)"))
            log(t, df.index[d2], "ALL except id", "DROPPED", "duplicate except surrogate id -> quarantine")
            df = df[~d2]
        else:
            log(t, df.index[d2], "ALL except id", "FLAGGED", "duplicate except surrogate id", code="dup_ignoring_id")
        dpk = (df["id"].notna() & df["id"].duplicated()).to_numpy(dtype=bool)
        log(t, df.index[dpk], "id", "FLAGGED", "duplicate primary key", df.loc[dpk, "id"].astype(str).to_numpy(),
            code="dup_pk")

    dcols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
    for i, a in enumerate(dcols):
        for b in dcols[i + 1:]:
            m = df[a].notna() & df[b].notna()
            if m.sum() < 20:
                continue
            le, ge = (df.loc[m, a] <= df.loc[m, b]).mean(), (df.loc[m, a] >= df.loc[m, b]).mean()
            r = CFG["order_learn_ratio"]
            if r <= le < 1:
                bad = (m & (df[a] > df[b])).to_numpy(dtype=bool)
                log(t, df.index[bad], f"{a} vs {b}", "FLAGGED", f"{a} should be <= {b}",
                    (df.loc[bad, a].astype(str) + " > " + df.loc[bad, b].astype(str)).to_numpy(),
                    code=f"temporal:{a}_{b}")
            elif r <= ge < 1:
                bad = (m & (df[a] < df[b])).to_numpy(dtype=bool)
                log(t, df.index[bad], f"{a} vs {b}", "FLAGGED", f"{b} should be <= {a}",
                    (df.loc[bad, a].astype(str) + " < " + df.loc[bad, b].astype(str)).to_numpy(),
                    code=f"temporal:{b}_{a}")

    n = len(df)
    for k in df.columns:
        if types[(t, k)] != "id" or k == "id" or df[k].nunique() > 0.9 * n:
            continue
        g = df.groupby(k, dropna=True)
        for col in df.columns:
            if col == k or types[(t, col)] in ("id", "text", "empty"):
                continue
            nun = g[col].nunique(dropna=True)
            if not (CFG["fd_learn_ratio"] <= (nun <= 1).mean() < 1):
                continue
            sub = df[df[k].isin(nun[nun > 1].index)]
            mode = sub.groupby(k)[col].agg(lambda x: x.mode().iloc[0] if x.notna().any() else np.nan)
            expect = sub[k].map(mode)
            wrong = (sub[col].notna() & (sub[col] != expect)).to_numpy(dtype=bool)
            if CFG["fd_conflict"] == "majority":
                log(t, sub.index[wrong], col, "FIXED", f"overwritten with majority value per {k}",
                    sub[col][wrong].astype(str).to_numpy(), expect[wrong].astype(str).to_numpy(),
                    code=f"fd_fixed:{col}")
                df.loc[sub.index[wrong], col] = expect[wrong]
            else:
                log(t, sub.index[wrong], col, "FLAGGED", f"conflicts with majority value per {k}",
                    sub[col][wrong].astype(str).to_numpy(), expect[wrong].astype(str).to_numpy(),
                    code=f"fd_conflict:{col}")
    return df


def fix_orphans(dfs, types):
    pks = {}
    for t, df in dfs.items():
        for c in df.columns:
            if types[(t, c)] == "id" and df[c].notna().all() and df[c].is_unique and len(df) > 1:
                pks[(t, c)] = set(as_key(df[c]))
    drops = {t: set() for t in dfs}
    for ct, cdf in dfs.items():
        for cc in cdf.columns:
            if types[(ct, cc)] != "id":
                continue
            k = as_key(cdf[cc])
            nn = k.dropna()
            if nn.empty:
                continue
            for (pt, pc), ps in pks.items():
                if pt == ct or nn.isin(ps).mean() < CFG["fk_min_overlap"]:
                    continue
                orphan = (cdf[cc].notna() & ~k.isin(ps)).to_numpy(dtype=bool)
                reason, vals, pol = f"orphan FK, no match in {pt}.{pc}", cdf.loc[orphan, cc].astype(str).to_numpy(), CFG["orphan_fk"]
                if pol == "drop":
                    quarantine.setdefault(ct, []).append(cdf[orphan].copy().assign(quarantine_reason=reason))
                    log(ct, cdf.index[orphan], cc, "DROPPED", reason + " -> quarantine", vals)
                    drops[ct] |= set(cdf.index[orphan])
                elif pol == "null":
                    log(ct, cdf.index[orphan], cc, "NULLED", reason, vals, ["NULL"] * int(orphan.sum()),
                        code=f"nulled:{cc}")
                    cdf.loc[orphan, cc] = pd.NA
                else:
                    log(ct, cdf.index[orphan], cc, "FLAGGED", reason, vals, code=f"orphan_fk:{cc}")
    for t, ix in drops.items():
        if ix:
            dfs[t] = dfs[t].drop(index=list(ix))
    return dfs


def build_dq_flag(t, df):
    s = pd.Series(np.nan, index=df.index, dtype=object)
    for code, parts in sorted(flags.get(t, {}).items()):
        idx = parts[0].append(list(parts[1:])) if len(parts) > 1 else parts[0]
        m = df.index.isin(idx)
        s = s.where(~m, np.where(s.isna(), code, s.astype(str) + "|" + code))
    df["dq_flag"] = s.fillna("ok")
    return df


# ------------------------------------------------------------------ export: csv + DDL
def sql_type(s):
    if s.name == "dq_flag":
        return f"VARCHAR({max(50, int(math.ceil(s.str.len().max() * 1.5 / 10) * 10))})"
    if pd.api.types.is_integer_dtype(s):
        return "BIGINT"
    if pd.api.types.is_float_dtype(s):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(s):
        x = s.dropna()
        return "DATE" if len(x) == 0 or (x == x.dt.normalize()).all() else "TIMESTAMP"
    mx = int(s.dropna().astype(str).str.len().max()) if s.notna().any() else 0
    return "TEXT" if mx > 4000 else f"VARCHAR({max(50, int(math.ceil(mx * 1.5 / 10) * 10))})"


def to_export(df, specs):
    ex = df.copy()
    for col, typ, _ in specs:
        if typ == "DATE":
            ex[col] = ex[col].dt.strftime("%Y-%m-%d")
        elif typ == "TIMESTAMP":
            frac = (ex[col].dropna().dt.microsecond != 0).any()
            ex[col] = ex[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f" if frac else "%Y-%m-%d %H:%M:%S")
    return ex


def export_all(dfs, out_clean):
    q = CFG["sql_quote"]
    ddl, load, specs_all = [], [], {}
    for t, df in dfs.items():
        specs = []
        for col in df.columns:
            typ = sql_type(df[col])
            is_pk = col == "id" and df[col].notna().all() and df[col].is_unique and typ in ("BIGINT",) or \
                (col == "id" and typ.startswith("VARCHAR") and df[col].notna().all() and df[col].is_unique)
            specs.append((col, typ, "PRIMARY KEY" if is_pk else ("NOT NULL" if col == "dq_flag" else "")))
        specs_all[t] = specs
        body = ",\n".join(f"  {q}{c}{q} {ty} {ex}".rstrip() for c, ty, ex in specs)
        ddl.append(f"DROP TABLE IF EXISTS {q}{t}{q};\nCREATE TABLE {q}{t}{q} (\n{body}\n);\n")
        load.append(f"\\copy {q}{t}{q} FROM '{t}.csv' WITH (FORMAT csv, HEADER true)")
        to_export(df, specs).to_csv(os.path.join(out_clean, t + ".csv"), index=False, encoding="utf-8")
    with open(os.path.join(out_clean, "schema.sql"), "w", encoding="utf-8") as f:
        f.write("-- generated; explicit types, no PK/FK constraints except surrogate id\n\n" + "\n".join(ddl))
    with open(os.path.join(out_clean, "load_postgres.sql"), "w", encoding="utf-8") as f:
        f.write("-- run with psql from the cleaned/ folder after schema.sql:  psql -d <db> -f load_postgres.sql\n"
                + "\n".join(load) + "\n")
    return specs_all


def validate(dfs, specs_all, out_clean, raw_rows):
    rows = []

    def chk(table, check, ok, detail=""):
        rows.append({"Table": table, "Check": check, "Result": "PASS" if ok else "FAIL", "Detail": detail})

    for t, specs in specs_all.items():
        back = pd.read_csv(os.path.join(out_clean, t + ".csv"), dtype=str, keep_default_na=False, encoding="utf-8")
        chk(t, "header matches schema.sql", list(back.columns) == [c for c, _, _ in specs])
        chk(t, "row count matches", len(back) == len(dfs[t]), f"{len(back)} vs {len(dfs[t])}")
        for col, typ, ex in specs:
            v = back[col]
            v = v[v != ""]
            if typ == "BIGINT":
                ok = bool(v.str.fullmatch(r"-?\d{1,18}").all())
            elif typ == "DOUBLE PRECISION":
                ok = bool(pd.to_numeric(v, errors="coerce").notna().all())
            elif typ == "DATE":
                ok = bool(v.str.fullmatch(r"\d{4}-\d{2}-\d{2}").all()) and \
                    bool(pd.to_datetime(v, format="%Y-%m-%d", errors="coerce").notna().all())
            elif typ == "TIMESTAMP":
                ok = bool(v.str.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?").all()) and \
                    bool(pd.to_datetime(v, errors="coerce").notna().all())
            elif typ.startswith("VARCHAR"):
                ok = bool((v.str.len() <= int(re.search(r"\((\d+)\)", typ).group(1))).all())
            else:
                ok = True
            chk(t, f"{col}: values fit {typ}", ok)
            if ex == "PRIMARY KEY":
                chk(t, f"{col}: PRIMARY KEY unique & non-null", bool((back[col] != "").all() and back[col].is_unique))
            if ex == "NOT NULL":
                chk(t, f"{col}: NOT NULL", bool((back[col] != "").all()))
    rec = []
    for t, n in raw_rows.items():
        qn = sum(len(x) for x in quarantine.get(t, []))
        rec.append({"Table": t, "RawRows": n, "CleanedRows": len(dfs[t]), "Quarantined": qn,
                    "Reconciles": n == len(dfs[t]) + qn})
        chk(t, "raw rows = cleaned + quarantined", n == len(dfs[t]) + qn, f"{n} = {len(dfs[t])} + {qn}")
    return pd.DataFrame(rows), pd.DataFrame(rec)


# ------------------------------------------------------------------ main
def read_csv_safe(path, name):
    for enc in ("utf-8-sig", "latin-1"):
        try:
            df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""], low_memory=False, encoding=enc)
            if enc != "utf-8-sig":
                note(name, "ALL", "NOTE", "file was not UTF-8; decoded as latin-1")
            return df
        except UnicodeDecodeError:
            continue
    raise ValueError(f"cannot decode {path}")


def run(data_dir):
    for x in (summary, rowlogs, quarantine):
        x.clear()
    flags.clear()
    files = [f for f in os.listdir(data_dir) if f.lower().endswith(".csv")
             and not f.startswith("universal_anomaly_report")]
    dfs, raw_rows = {}, {}
    for f in files:
        t = snake(f[:-4])
        df = read_csv_safe(os.path.join(data_dir, f), t)
        new = unique_names(df.columns)
        for o, n in zip(df.columns, new):
            if o != n:
                note(t, o, "RENAMED", f"column renamed to '{n}'")
        df.columns = new
        dfs[t], raw_rows[t] = df, len(df)

    types = {}
    for t in dfs:
        dfs[t] = normalize_text(t, dfs[t])
        for c in dfs[t].columns:
            types[(t, c)] = infer(dfs[t][c], c)
    for t in dfs:
        for c in list(dfs[t].columns):
            dfs[t][c] = convert_column(t, dfs[t], c, types[(t, c)])
            check_numeric_flags(t, dfs[t], c, types[(t, c)])
        dfs[t] = clean_table_rows(t, dfs[t], types)
    dfs = fix_orphans(dfs, types)
    for t in dfs:
        dfs[t] = build_dq_flag(t, dfs[t])

    out_clean, out_q, out_rep = (os.path.join(data_dir, d) for d in ("cleaned", "quarantine", "cleaning_report"))
    for d in (out_clean, out_q, out_rep):
        os.makedirs(d, exist_ok=True)
    for t, parts in quarantine.items():
        q = pd.concat(parts)
        q.index.name = "orig_row"
        q.reset_index().to_csv(os.path.join(out_q, t + ".csv"), index=False)

    specs_all = export_all(dfs, out_clean)
    val, rec = validate(dfs, specs_all, out_clean, raw_rows)

    summ = pd.DataFrame(summary)
    if not summ.empty:
        summ = (summ.groupby(["Table", "Column", "Action", "Reason"], as_index=False)["Count"].sum()
                    .sort_values(["Table", "Action", "Count"], ascending=[True, True, False]))
    summ.to_csv(os.path.join(out_rep, "change_summary.csv"), index=False)
    if rowlogs:
        pd.concat(rowlogs, ignore_index=True).to_csv(os.path.join(out_rep, "change_log_rows.csv"), index=False)
    rec.to_csv(os.path.join(out_rep, "reconciliation.csv"), index=False)
    val.to_csv(os.path.join(out_rep, "validation.csv"), index=False)

    failed = val[val["Result"] == "FAIL"]
    if len(failed) and CFG["strict_validation"]:
        raise RuntimeError("Export validation FAILED:\n" + failed.to_string(index=False))
    return dfs, summ, rec, val


if __name__ == "__main__":
    DATA_DIR = r"C:\Users\BEN\Downloads\Eklipse\da_test_data_v2" ## CHANGE TO YOUR DATA FOLDER ##
    cleaned, summ, rec, val = run(DATA_DIR)
    pd.set_option("display.max_colwidth", 70, "display.width", 250, "display.max_rows", 500)
    print(summ.to_string(index=False))
    print("\nRECONCILIATION\n", rec.to_string(index=False))
    print(f"\nVALIDATION: {(val['Result'] == 'PASS').sum()} pass, {(val['Result'] == 'FAIL').sum()} fail")
    print("\nTables loaded with these dq_flag counts:")
    for t, df in cleaned.items():
        print(f"  {t}: {len(df):,} rows, {(df['dq_flag'] == 'ok').sum():,} ok")
