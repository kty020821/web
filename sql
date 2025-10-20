import pandas as pd
import numpy as np
import re
from typing import List

def split_and_compare_by_fab_columns(
    df: pd.DataFrame,
    fab_col: str = "Fab",
    cond_cols: List[str] = ["FORMULA_GRP_NAME", "FORMULA", "OPERAND_NAME"],
    seq_col: str = "SEQ",
    value_col: str = "OPERAND_VALUE",
    case_sensitive: bool = True,
    strip_spaces: bool = True,
    mismatched_only: bool = False,
):
    """
    1) OPERAND_VALUE를 공백 기준으로 분리하여 OPERAND_VALUE_1..K 컬럼 생성
    2) (cond_cols + SEQ)별로 K(최대 토큰 수)를 맞춤
    3) (행) cond_cols + SEQ / (열) Fab__OPERAND_VALUE_i 로 피벗
    4) 위치별 동일성(__equal_OPERAND_VALUE_i)과 전체 동일성(__is_equal) 제공
    """

    # --- 필수 컬럼 확인
    need = set([fab_col, value_col, seq_col] + list(cond_cols))
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise ValueError(f"Missing columns: {miss}")

    work = df.copy()

    # --- Fab 표준화(오탈자/대소문자 혼선 방지)
    work[fab_col] = work[fab_col].astype(str).str.strip()

    # --- 값 정규화
    val = work[value_col].astype(str)
    if strip_spaces:
        val = val.str.strip()
        # 숨은 공백들을 일반 공백으로 치환
        val = (val
               .str.replace(r"[\u00A0\u2000-\u200B\u3000]", " ", regex=True)
               .str.replace(r"\s+", " ", regex=True))
    if not case_sensitive:
        val = val.str.lower()
    work["_VAL_NORM"] = val

    # --- 토큰 분리 (공백 기준)
    def _split_tokens(s: str):
        if s is None or s == "" or pd.isna(s):
            return []
        toks = re.split(r"\s+", s)
        toks = [t for t in toks if t != ""]
        return toks

    work["_TOKENS"] = work["_VAL_NORM"].apply(_split_tokens)

    key_cols = list(cond_cols) + [seq_col]

    # 같은 (조건+SEQ+Fab)에 여러 행이 있으면 첫 값을 사용(필요하면 규칙 바꿔)
    grp = (work.groupby(key_cols + [fab_col], dropna=False)["_TOKENS"]
                .agg(lambda s: s.iloc[0] if len(s) > 0 else [])
                .reset_index())

    # (조건+SEQ)별 최대 토큰 수
    max_tok = (grp.assign(_len=grp["_TOKENS"].apply(len))
                   .groupby(key_cols, dropna=False)["_len"]
                   .max()
                   .reset_index(name="MAX_K"))

    # (조건+SEQ+Fab) 레벨에서 OPERAND_VALUE_1..K 컬럼을 생성
    rows = []
    for _, r in grp.merge(max_tok, on=key_cols, how="left").iterrows():
        base = {c: r[c] for c in key_cols + [fab_col]}
        toks = r["_TOKENS"]
        K = int(r["MAX_K"]) if pd.notna(r["MAX_K"]) else 0
        for i in range(1, K + 1):
            base[f"OPERAND_VALUE_{i}"] = toks[i-1] if i-1 < len(toks) else np.nan
        rows.append(base)

    if not rows:
        # 데이터가 비어있다면 빈 DF 반환
        out = pd.DataFrame(columns=key_cols + ["__is_equal"])
        return out

    flat = pd.DataFrame(rows)

    # 피벗: (행) key_cols / (열) Fab__OPERAND_VALUE_i
    value_cols = sorted([c for c in flat.columns if c.startswith("OPERAND_VALUE_")],
                        key=lambda x: int(x.split("_")[-1]))
    flat["__Fab"] = flat[fab_col]  # 복사
    wide = flat.pivot_table(
        index=key_cols,
        columns="__Fab",
        values=value_cols,
        aggfunc="first"
    )

    # 컬럼 플랫하게: ('OPERAND_VALUE_1','M15') -> 'M15__OPERAND_VALUE_1'
    wide.columns = [f"{fab}__{col}" for col, fab in wide.columns]
    wide = wide.reset_index()

    # Fab 목록/포지션 목록 구하기
    fabs = sorted(list({c.split("__")[0] for c in wide.columns if "__OPERAND_VALUE_" in c}))
    positions = sorted(list({c.split("__")[1] for c in wide.columns if "__OPERAND_VALUE_" in c}),
                       key=lambda x: int(x.split("_")[-1]) if "_" in x else 0)

    # 위치별 동일성 플래그
    equal_flags = []
    for pos in positions:
        cols = [f"{f}__{pos}" for f in fabs if f"{f}__{pos}" in wide.columns]
        flag_col = f"__equal_{pos}"
        def _eq_row(row):
            vals = [row[c] for c in cols if pd.notna(row[c])]
            if len(vals) <= 1:
                return False
            return len(set(vals)) == 1
        wide[flag_col] = wide.apply(_eq_row, axis=1)
        equal_flags.append(flag_col)

    # 전체 동일성: 모든 위치가 True여야 함
    if equal_flags:
        wide["__is_equal"] = wide[equal_flags].all(axis=1)
    else:
        wide["__is_equal"] = False  # 비교할 포지션이 없으면 False

    if mismatched_only:
        wide = wide[~wide["__is_equal"]].copy()

    return wide
