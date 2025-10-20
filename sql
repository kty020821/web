import pandas as pd
import numpy as np
import re
from typing import List, Tuple

def compare_equal_by_fab_split(
    df: pd.DataFrame,
    fab_col: str = "Fab",
    cond_cols: List[str] = ["FORMULA_GRP_NAME", "FORMULA", "OPERAND_NAME"],
    seq_col: str = "SEQ",
    value_col: str = "OPERAND_VALUE",
    mismatched_only: bool = False,
    strip_spaces: bool = True,
    case_sensitive: bool = True,
    split_pattern: str = r"\s+",   # 공백 1개 이상 기준 분리
) -> pd.DataFrame:
    """
    동일 조건(cond_cols) + 같은 SEQ에서, OPERAND_VALUE를 공백 분리하여
    위치별(VALUE_IDX)로 Fab 간 값이 모두 같은지 비교.

    반환: index = cond_cols + [SEQ, VALUE_IDX]
         columns = 각 Fab
         values = 위치별 텍스트
         + '__is_equal' (행별 Fab 값 동일 여부)

    규칙:
    - Fab이 0~1개인 경우 → 비교군 없음 → False
    - 동일 (조건+SEQ+VALUE_IDX, Fab) 내에 값이 복수로 상이하면(불안정) → False
    - Fab 간 한 곳이라도 값이 비어있거나 다르면 → False
    """

    # --- 0) 필수 컬럼 체크
    need = set([fab_col, value_col, seq_col] + list(cond_cols))
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise ValueError(f"Missing columns: {miss}")

    # --- 1) 값 정규화
    val = df[value_col].astype(str)
    if strip_spaces:
        val = val.str.strip()
    if not case_sensitive:
        val = val.str.lower()
    df = df.copy()
    df["_VAL_NORM"] = val

    # --- 2) 토큰화(공백 분리) & 토큰 수
    def _split_or_empty(s: str) -> List[str]:
        if s == "" or pd.isna(s):
            return []
        tokens = re.split(split_pattern, s)
        tokens = [t for t in tokens if t != ""]
        return tokens

    df["_TOKENS"] = df["_VAL_NORM"].apply(_split_or_empty)
    df["_TOK_CNT"] = df["_TOKENS"].apply(len)

    # --- 3) explode: 위치 인덱스(VALUE_IDX: 1..n) 부여
    # 토큰이 0개인 경우도 비교에 포함하려면 빈값을 명시적으로 남길 수 있지만
    # 여기서는 실제 값이 있는 위치만 비교 대상으로 간주
    df_exploded = (
        df.loc[df["_TOK_CNT"] > 0, cond_cols + [seq_col, fab_col, "_TOKENS"]]
          .explode("_TOKENS", ignore_index=True)
    )
    # 위치 인덱스 부여(같은 그룹(cond+SEQ, Fab) 내에서 1부터)
    df_exploded["VALUE_IDX"] = (
        df_exploded
        .groupby(cond_cols + [seq_col, fab_col])
        .cumcount() + 1
    )
    df_exploded.rename(columns={"_TOKENS": "_VAL_TOKEN"}, inplace=True)

    # --- 4) (조건+SEQ+VALUE_IDX, Fab)별 고유 값 집계 → 한 Fab/위치에 여러 상이한 값이 있으면 불안정
    def _collapse(vals: List[str]):
        uniq = pd.unique(pd.Series(vals).dropna().astype(str)).tolist()
        if len(uniq) == 0:
            return pd.NA
        if len(uniq) == 1:
            return uniq[0]
        # 복수 상이 값 -> 튜플로 표기해 두고 나중에 False 처리
        return tuple(sorted(map(str, uniq)))

    agg = (
        df_exploded
        .groupby(cond_cols + [seq_col, "VALUE_IDX", fab_col], dropna=False)["_VAL_TOKEN"]
        .agg(_collapse)
        .reset_index(name="_VAL")
    )

    # --- 5) 가로 피벗(Fab 열)
    wide = agg.pivot_table(
        index=cond_cols + [seq_col, "VALUE_IDX"],
        columns=fab_col,
        values="_VAL",
        aggfunc="first"
    )

    # --- 6) 행별 동일성 판정
    def _is_equal_row(row) -> bool:
        # 존재하는 Fab 값만 수집
        vals = [v for v in row if pd.notna(v)]
        # Fab이 1개 이하면 비교군 없음
        if len(vals) <= 1:
            return False
        # 동일 Fab 내 복수 상이 값이 있었던 위치(튜플) → False
        if any(isinstance(v, tuple) for v in vals):
            return False
        # 모두 동일?
        return len(set(vals)) == 1

    out = wide.copy()
    out["__is_equal"] = out.apply(_is_equal_row, axis=1)

    if mismatched_only:
        out = out[~out["__is_equal"]]

    return out.reset_index()
