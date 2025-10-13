import pandas as pd
from typing import List

def compare_equal_by_fab(
    df: pd.DataFrame,
    fab_col: str = "Fab",
    cond_cols: List[str] = ["FORMULA_GRP_NAME", "FORMULA", "OPERAND_NAME"],
    value_col: str = "OPERAND_VALUE",
    mismatched_only: bool = False,
    strip_spaces: bool = True,      # 앞뒤 공백 제거
    case_sensitive: bool = True,    # 대소문자 구분 (False면 전부 소문자로 비교)
):
    """
    동일 조건(cond_cols)에서 Fab별 value_col(텍스트)을 비교해
    모두 같으면 True, 하나라도 다르면 False.
    - Fab이 1개뿐인 조건은 비교군 없음 → False 처리.
    - 동일 조건+동일 Fab 내에 값이 여러 개면(불안정) → False 처리.

    반환: 조건별로 Fab이 가로로 펼쳐진 DF + '__is_equal' 컬럼
    """

    # 필수 컬럼 체크
    need = set([fab_col, value_col] + list(cond_cols))
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise ValueError(f"Missing columns: {miss}")

    # 텍스트 정규화 함수
    def _norm_series(s: pd.Series) -> pd.Series:
        s = s.dropna().astype(str)
        if strip_spaces:
            s = s.str.strip()
        if not case_sensitive:
            s = s.str.lower()
        return s

    # (조건, Fab)별로 고유 텍스트 값 수집
    agg = (
        df.groupby(list(cond_cols) + [fab_col], dropna=False)[value_col]
          .agg(lambda s: pd.unique(_norm_series(s)).tolist())
          .reset_index(name="_vals")
    )

    # 동일 Fab 내에 값이 0/1/2+ 개인지 정리
    def _collapse(vals):
        if len(vals) == 0:
            return pd.NA
        if len(vals) == 1:
            return vals[0]                 # 단일 값
        return tuple(sorted(map(str, vals)))  # 복수 값 → 튜플(불안정 표시)

    agg["_val"] = agg["_vals"].apply(_collapse)

    # Fab을 가로로 피벗
    wide = agg.pivot_table(
        index=cond_cols,
        columns=fab_col,
        values="_val",
        aggfunc="first"
    )

    # 행별 동일성 판정
    def _is_equal_row(row):
        vals = [v for v in row if pd.notna(v)]
        # Fab이 0~1개만 존재 → 비교군 없음 → False
        if len(vals) <= 1:
            return False
        # 동일 Fab 내에 값이 여러 개인 경우(tuple) → False
        if any(isinstance(v, tuple) for v in vals):
            return False
        # 모두 같은 텍스트인가?
        return len(set(vals)) == 1

    out = wide.copy()
    out["__is_equal"] = out.apply(_is_equal_row, axis=1)

    if mismatched_only:
        out = out[~out["__is_equal"]]

    return out.reset_index()
