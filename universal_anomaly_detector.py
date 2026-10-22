"""
Universal anomaly detector.
Drop a folder of CSVs -> get one report. No hardcoded schema.

Layers:
  1. Column profiling   : type inferred from CONTENT (not column name only)
  2. Column checks      : missing, placeholders, whitespace, case variants, sentinels,
                          negatives, outliers, bad dates, out-of-range dates, constants
  3. Table checks       : duplicate rows, duplicate ignoring surrogate id, PK uniqueness,
                          date ordering (learned: if a<=b in 95%+ rows, flag the rest),
                          functional dependency (user_id -> join_at should be 1:1)
  4. Cross-table checks : FK inferred by VALUE overlap (not by name), orphans,
                          shared attributes that disagree across a relationship
"""
import os
import re
import warnings
from itertools import combinations

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

CFG = {
    "placeholders": {"n/a", "na", "none", "null", "nil", "?", "-", "--",
                     "unknown", "undefined", "nan", ""},
    "sentinels": {-1, 0, 99, 999, 9999, 99999, 999999, 9999999, 99999999,
                  -999, -9999, 1900, 1970},
    "iqr_mult": 3.0,              # 1.5 = noisy on skewed data, 3.0 = "extreme" only
    "min_date": "1990-01-01",
    "max_date": pd.Timestamp.now() + pd.Timedelta(days=1),
    "fk_min_overlap": 0.5,        # child col must overlap >=50% with a PK to count as FK
    "order_learn_ratio": 0.95,    # a<=b must hold in >=95% rows to be a "rule"
    "fd_learn_ratio": 0.95,       # fraction of groups that must be consistent
    "attr_agree_ratio": 0.90,     # shared attr must match in >=90% rows to be a rule
    "max_examples": 3,
}

report = []


def add(table, col, category, issue, count=None, total=None, severity="MEDIUM", examples=None):
    pct = (count / total) if (count is not None and total) else None
    report.append({
        "Severity": severity,
        "Table": table,
        "Column": col,
        "Category": category,
        "Count": count,
        "Pct": f"{pct:.2%}" if pct is not None else "",
        "Issue": issue,
        "Examples": "; ".join(map(str, list(examples)[: CFG["max_examples"]])) if examples is not None else "",
    })


def sev_by_pct(count, total, hi=0.05):
    """Rare = probably a real error (HIGH). Widespread = probably by design / systemic."""
    p = count / total if total else 0
    return "HIGH" if p < hi else "MEDIUM"


# ---------------------------------------------------------------- type inference
def is_text(s):
    """True for text columns in any pandas version (object in pandas 2, 'str' dtype in pandas 3)."""
    return not (pd.api.types.is_numeric_dtype(s) or pd.api.types.is_datetime64_any_dtype(s)
                or pd.api.types.is_bool_dtype(s))


def name_is_id(name):
    n = name.lower()
    return n == "id" or n.endswith("_id") or n.endswith("id") or "uuid" in n or "guid" in n


def infer_type(s, name):
    s = s.dropna()
    if s.empty:
        return "empty"
    if pd.api.types.is_bool_dtype(s):
        return "bool"
    if pd.api.types.is_numeric_dtype(s):
        return "id" if name_is_id(name) else "numeric"
    st = s.astype(str).str.strip()
    sample = st.sample(min(len(st), 3000), random_state=0)
    if pd.to_numeric(sample, errors="coerce").notna().mean() > 0.9:
        return "id" if name_is_id(name) else "numeric_text"
    parsed = pd.to_datetime(sample, errors="coerce")
    if parsed.notna().mean() > 0.8:
        return "datetime"
    if name_is_id(name):
        return "id"
    if st.nunique() / len(st) < 0.05 or st.nunique() <= 50:
        return "categorical"
    return "text"


def norm_key(v):
    return re.sub(r"[^a-z0-9]", "", str(v).lower())


# ---------------------------------------------------------------- 1+2. column checks
def scan_columns(tname, df, types, parsed):
    n = len(df)
    for col in df.columns:
        s_all = df[col]
        s = s_all.dropna()
        t = types[(tname, col)]

        if t == "empty":
            add(tname, col, "Empty Column", "Entire column is NULL", n, n, "HIGH")
            continue

        # missing: real nulls + placeholder strings
        null_cnt = int(s_all.isna().sum())
        ph_cnt = 0
        if is_text(s_all):
            ph_cnt = int(s.astype(str).str.strip().str.lower().isin(CFG["placeholders"]).sum())
        miss = null_cnt + ph_cnt
        if miss > 0:
            p = miss / n
            sev = "LOW" if p > 0.5 else sev_by_pct(miss, n)  # >50% null = likely optional field
            add(tname, col, "Missing Data",
                f"{null_cnt:,} NULL + {ph_cnt:,} placeholder strings", miss, n, sev)

        if s.nunique() == 1 and n > 1:
            add(tname, col, "Zero Variance", f"Single constant value: {s.iloc[0]!r}",
                len(s), n, "LOW")

        # ---- numeric
        if t in ("numeric", "numeric_text", "id") and (
                pd.api.types.is_numeric_dtype(s) or t != "id"):
            x = pd.to_numeric(s, errors="coerce").dropna()
            if len(x) == 0:
                continue

            if t == "numeric_text" and len(x) < len(s):
                bad = s[pd.to_numeric(s, errors="coerce").isna()]
                add(tname, col, "Type Corruption",
                    f"{len(bad):,} non-numeric values in numeric column", len(bad), n, "HIGH", bad.unique())

            negs = x[x < 0]
            if 0 < len(negs) and (len(negs) / len(x)) < 0.05 and t != "id":
                add(tname, col, "Numeric Bounds", "Negative values in a mostly non-negative column",
                    len(negs), n, "HIGH", negs.unique())

            if t != "id":
                q1, q3 = x.quantile([0.25, 0.75])
                iqr = q3 - q1
                if iqr > 0:
                    m = CFG["iqr_mult"]
                    out = x[(x < q1 - m * iqr) | (x > q3 + m * iqr)]
                    if len(out):
                        add(tname, col, "Statistical Outliers",
                            f"{len(out):,} beyond {m}xIQR [{q1 - m*iqr:,.2f}, {q3 + m*iqr:,.2f}]",
                            len(out), n, "MEDIUM", out.sort_values(ascending=False).unique())

                vc = x.value_counts(normalize=True)
                top, share = vc.index[0], vc.iloc[0]
                if share > 0.05 and top in CFG["sentinels"] and x.nunique() > 5:
                    add(tname, col, "Sentinel Value",
                        f"Value {top} = {share:.1%} of rows (possible default/placeholder)",
                        int((x == top).sum()), n, "MEDIUM")

        # ---- datetime
        elif t == "datetime":
            p = parsed[(tname, col)]
            corrupt = int(p.isna().sum() - s_all.isna().sum())
            if corrupt > 0:
                bad = s_all[p.isna() & s_all.notna()]
                add(tname, col, "Corrupt Date", f"{corrupt:,} values failed date parsing",
                    corrupt, n, "HIGH", bad.unique())
            lo, hi = pd.Timestamp(CFG["min_date"]), CFG["max_date"]
            rng = p[(p < lo) | (p > hi)]
            if len(rng):
                add(tname, col, "Date Out Of Range",
                    f"{len(rng):,} dates outside [{lo.date()}, {hi.date()}]",
                    len(rng), n, "HIGH", rng.dt.strftime("%Y-%m-%d").unique())

        # ---- text / categorical
        if t in ("categorical", "text", "id") and is_text(s_all):
            st = s.astype(str)
            uns = int((st.str.strip() != st).sum())
            if uns:
                add(tname, col, "Text Formatting", "Leading/trailing whitespace",
                    uns, n, "MEDIUM", st[st.str.strip() != st].map(repr).unique())

            if t == "categorical":
                clean = st.str.strip()
                groups = clean.groupby(clean.map(norm_key)).agg(lambda v: sorted(set(v)))
                variants = groups[groups.map(len) > 1]
                if len(variants):
                    add(tname, col, "Category Variants",
                        f"{len(variants)} categories spelled multiple ways",
                        len(variants), clean.nunique(), "HIGH", variants.map(lambda v: "/".join(v)))

            if t in ("categorical", "text"):
                num_like = pd.to_numeric(st, errors="coerce").notna()
                c = int(num_like.sum())
                if 0 < c < len(st) * 0.1:
                    add(tname, col, "Type Corruption",
                        f"{c:,} purely numeric values inside a text column", c, n, "MEDIUM",
                        st[num_like].unique())


# ---------------------------------------------------------------- 3. table checks
def scan_tables(tname, df, types, parsed):
    n = len(df)

    d = int(df.duplicated().sum())
    if d:
        add(tname, "ALL", "Duplicate Rows", "Exact duplicate rows", d, n, "HIGH")

    # duplicate ignoring surrogate PK ("id")
    pk = [c for c in df.columns if c.lower() == "id"]
    if pk:
        rest = [c for c in df.columns if c not in pk]
        d2 = int(df.duplicated(subset=rest).sum())
        if d2 and d2 != d:
            add(tname, "ALL except id", "Duplicate Rows",
                "Rows identical except surrogate id (silent re-inserts)", d2, n, "HIGH")
        c = pk[0]
        dup_pk = int(df[c].dropna().duplicated().sum())
        if dup_pk:
            add(tname, c, "Primary Key", "Duplicate values in 'id' column", dup_pk, n, "HIGH")

    # learned date-order rules
    dcols = [c for c in df.columns if types[(tname, c)] == "datetime"]
    for a, b in combinations(dcols, 2):
        pa, pb = parsed[(tname, a)], parsed[(tname, b)]
        m = pa.notna() & pb.notna()
        if m.sum() < 20:
            continue
        le = (pa[m] <= pb[m]).mean()
        ge = (pa[m] >= pb[m]).mean()
        r = CFG["order_learn_ratio"]
        if r <= le < 1:
            bad = m & (pa > pb)
            add(tname, f"{a} > {b}", "Temporal Logic",
                f"'{a}' should be <= '{b}' ({le:.1%} of rows comply)", int(bad.sum()), n, "HIGH",
                df.loc[bad, [a, b]].head(3).astype(str).agg(" > ".join, axis=1))
        elif r <= ge < 1:
            bad = m & (pa < pb)
            add(tname, f"{b} > {a}", "Temporal Logic",
                f"'{b}' should be <= '{a}' ({ge:.1%} of rows comply)", int(bad.sum()), n, "HIGH",
                df.loc[bad, [a, b]].head(3).astype(str).agg(" > ".join, axis=1))

    # learned functional dependency: key -> attribute should be constant per key
    for k in df.columns:
        if types[(tname, k)] != "id" or k.lower() == "id":
            continue
        if df[k].nunique() > 0.9 * n:
            continue
        g = df.groupby(k, dropna=True)
        for col in df.columns:
            if col == k or types[(tname, col)] in ("id", "text", "empty"):
                continue
            nun = g[col].nunique(dropna=True)
            ok = (nun <= 1).mean()
            if CFG["fd_learn_ratio"] <= ok < 1:
                bad = nun[nun > 1]
                add(tname, f"{k} -> {col}", "Functional Dependency",
                    f"'{col}' is constant per '{k}' in {ok:.1%} of groups; {len(bad):,} keys have >1 value",
                    len(bad), len(nun), "HIGH", bad.index)


# ---------------------------------------------------------------- 4. cross-table
def as_key(s):
    x = pd.to_numeric(s, errors="coerce")
    if x.notna().mean() > 0.9:
        return x
    return s.astype(str).str.strip()


def scan_relationships(dfs, types):
    # PK candidates: unique, non-null ID-like columns
    pks = {}
    for t, df in dfs.items():
        for c in df.columns:
            if types[(t, c)] == "id" and df[c].notna().all() and df[c].is_unique and len(df) > 1:
                pks[(t, c)] = set(as_key(df[c]))

    relations = []  # (child_tbl, child_col, parent_tbl, parent_col)
    for ct, cdf in dfs.items():
        for cc in cdf.columns:
            if types[(ct, cc)] != "id":
                continue
            vals = as_key(cdf[cc].dropna())
            if vals.empty:
                continue
            for (pt, pc), pset in pks.items():
                if pt == ct:
                    continue
                overlap = vals.isin(pset).mean()
                if overlap >= CFG["fk_min_overlap"]:
                    relations.append((ct, cc, pt, pc))
                    orphans = vals[~vals.isin(pset)]
                    if len(orphans):
                        add(ct, cc, "Referential Integrity",
                            f"{len(orphans):,} values not found in {pt}.{pc} "
                            f"(FK inferred, {overlap:.1%} overlap)",
                            len(orphans), len(vals), "HIGH", orphans.unique())

    # shared-attribute consistency across a relationship
    # include same-named key columns (e.g. user_id in 2 tables) even w/o a parent table
    names = {}
    for t, df in dfs.items():
        for c in df.columns:
            if types[(t, c)] == "id" and c.lower() != "id":
                names.setdefault(c.lower(), []).append((t, c))
    for lst in names.values():
        for (t1, c1), (t2, c2) in combinations(lst, 2):
            relations.append((t1, c1, t2, c2))

    seen = set()
    for ct, cc, pt, pc in relations:
        if (ct, cc, pt, pc) in seen:
            continue
        seen.add((ct, cc, pt, pc))
        shared = [c for c in dfs[ct].columns
                  if c in dfs[pt].columns and c not in (cc, pc)
                  and types[(ct, c)] not in ("id", "text", "empty")]
        if not shared:
            continue
        left = dfs[ct][[cc] + shared].copy()
        left["_k"] = as_key(left[cc])
        right = dfs[pt][[pc] + shared].copy()
        right["_k"] = as_key(right[pc])
        # dedupe right side to first value per key so we compare against ONE reference
        right = right.drop_duplicates("_k")
        mg = left.merge(right, on="_k", suffixes=("", "__ref"))
        for c in shared:
            a, b = mg[c], mg[c + "__ref"]
            both = a.notna() & b.notna()
            if both.sum() == 0:
                continue
            mism = both & (a.astype(str) != b.astype(str))
            agree = 1 - mism.sum() / both.sum()
            # learned rule: only an attribute that normally AGREES is a real shared attribute
            # (otherwise it is just two different things with the same column name)
            if mism.sum() and agree >= CFG["attr_agree_ratio"]:
                add(ct, c, "Cross-Table Mismatch",
                    f"{int(mism.sum()):,} rows disagree with {pt}.{c} via {cc}={pc} ({agree:.1%} agree)",
                    int(mism.sum()), int(both.sum()), "HIGH",
                    (a[mism].astype(str) + " vs " + b[mism].astype(str)).unique())


# ---------------------------------------------------------------- main
def run(data_dir, out_name="universal_anomaly_report.csv"):
    report.clear()
    files = [f for f in os.listdir(data_dir)
             if f.lower().endswith(".csv") and not f.startswith("universal_anomaly_report")]
    dfs = {f[:-4]: pd.read_csv(os.path.join(data_dir, f), low_memory=False) for f in files}

    types, parsed = {}, {}
    for t, df in dfs.items():
        for c in df.columns:
            types[(t, c)] = infer_type(df[c], c)
            if types[(t, c)] == "datetime":
                parsed[(t, c)] = pd.to_datetime(df[c], errors="coerce")

    for t, df in dfs.items():
        scan_columns(t, df, types, parsed)
        scan_tables(t, df, types, parsed)
    scan_relationships(dfs, types)

    out = pd.DataFrame(report)
    if not out.empty:
        order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        out = out.sort_values(["Severity", "Table", "Category"],
                              key=lambda s: s.map(order) if s.name == "Severity" else s)
    out.to_csv(os.path.join(data_dir, out_name), index=False)
    return out, types


if __name__ == "__main__":
    DATA_DIR = r"C:\Users\BEN\Downloads\Eklipse\da_test_data_v2" ##CHANGE TO YOU DATA SOURCE OR DATA FOLDER##
    df_report, _ = run(DATA_DIR)
    pd.set_option("display.max_colwidth", 80, "display.width", 250)
    print(df_report.to_string(index=False))
