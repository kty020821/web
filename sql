import pandas as pd

def parse_kv_string(s, kv_sep="=", item_sep=";", key_case="lower"):
    out = {}
    if s is None:
        return out
    if not isinstance(s, str):
        s = str(s)
    if not s:
        return out
    for tok in s.split(item_sep):
        tok = tok.strip()
        if not tok or kv_sep not in tok:
            continue
        k, v = tok.split(kv_sep, 1)
        k, v = k.strip(), v.strip()
        if key_case == "lower":
            k = k.lower()
        elif key_case == "upper":
            k = k.upper()
        out[k] = v
    return out

def expand_one_kv_col(df, kv_col, key_case="lower"):
    # kv_col만 확장 → 새 컬럼명은 'kv_col__key'
    dicts, all_keys = [], set()
    for _, row in df.iterrows():
        d = parse_kv_string(row.get(kv_col), key_case=key_case)
        dicts.append(d)
        all_keys.update(d.keys())
    out = df.copy()
    for k in sorted(all_keys):
        out[f"{kv_col}__{k}"] = [d.get(k) for d in dicts]
    return out

def expand_two_kv_cols_separately(df, col1, col2, key_case="lower", drop_original=False):
    # col1 확장 + col2 확장 → 병합 (키가 안겹친다는 전제)
    a = expand_one_kv_col(df, col1, key_case=key_case)
    b = expand_one_kv_col(df, col2, key_case=key_case)
    col2_expanded_only = [c for c in b.columns if c.startswith(f"{col2}__")]
    out = a.join(b[col2_expanded_only])
    if drop_original:
        out = out.drop(columns=[c for c in [col1, col2] if c in out.columns])
    return out
