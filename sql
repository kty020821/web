import pandas as pd
from typing import List, Literal, Optional

def _setify(series: pd.Series, normalize_operand_value: bool = False):
    """
    그룹 내 값을 '집합'으로 정리.
    - SEQ: 값 그대로 집합
    - OPERAND_VALUE: normalize 옵션이 True면 공백 기준 토큰화 후 정렬하여 집합화
    """
    vals = series.dropna().astype(str).tolist()
    if not normalize_operand_value:
        return frozenset(vals)
    # 공백/연속공백 정리 & 토큰화 (예: "AB_CD EF" -> {"AB_CD","EF"})
    norm = []
    for v in vals:
        tokens = [t for t in " ".join(v.split()).split(" ") if t != ""]
        norm.append(" ".join(tokens))  # 원문 보존 버전
    # 토큰 단위 동등성 판정이 목적이면 아래 처리를 사용:
    # tokens_total = []
    # for v in vals:
    #     tokens_total.extend([t for t in " ".join(v.split()).split(" ") if t])
    # return frozenset(tokens_total)
    return frozenset(norm)

def attach_oper_from_B(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    key: str = "FORMULA_GRP_NAME",
    b_oper_col: str = "OPER",
) -> pd.DataFrame:
    """
    A에 B의 OPER를 key 기준으로 매칭해 A['OPER'] 컬럼을 생성한다.
    """
    if b_oper_col not in df_b.columns:
        raise KeyError(f"B 테이블에 '{b_oper_col}' 컬럼이 없습니다.")
    if key not in df_a.columns or key not in df_b.columns:
        raise KeyError(f"양쪽 테이블에 '{key}' 컬럼이 있어야 합니다.")

    out = df_a.merge(df_b[[key, b_oper_col]], on=key, how="left", validate="m:1")
    out = out.rename(columns={b_oper_col: "OPER"})
    return out

def compare_m15_vs_others(
    df: pd.DataFrame,
    fab_col: str = "FAB",
    keys_for_match: List[str] = ["OPER", "FORMULA_GRP_NAME", "FORMULA", "OPERAND_NAME"],
    seq_col: str = "SEQ",
    val_col: str = "OPERAND_VALUE",
    normalize_operand_value: bool = False,
    mismatched_only: bool = False,
    keep_examples: bool = True,
) -> pd.DataFrame:
    """
    M15와 '다른 각 FAB'를 1:1로 비교한다.
    - 같은 (OPER, FORMULA_GRP_NAME, FORMULA, OPERAND_NAME) 조합에서
      SEQ와 OPERAND_VALUE가 동일한지 판정.
    - 동일성 기준: 각 FAB 그룹 내부의 고유값 '집합' 비교 (여러 행 존재해도 OK).
    """
    required = set([fab_col, seq_col, val_col] + keys_for_match)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"필수 컬럼 누락: {missing}")

    # 분리
    m15 = df[df[fab_col] == "M15"].copy()
    others = df[df[fab_col] != "M15"].copy()

    # M15 집계
    gcols = keys_for_match
    m15_agg = m15.groupby(gcols).agg(
        M15_SEQ_values=(seq_col, lambda s: _setify(s, False)),
        M15_VAL_values=(val_col, lambda s: _setify(s, normalize_operand_value)),
    ).reset_index()

    # 각 FAB별 집계 후 M15와 조인 & 비교
    results = []
    for fab_name, g in others.groupby(fab_col):
        oth_agg = g.groupby(gcols).agg(
            OTHER_SEQ_values=(seq_col, lambda s: _setify(s, False)),
            OTHER_VAL_values=(val_col, lambda s: _setify(s, normalize_operand_value)),
        ).reset_index()
        merged = m15_agg.merge(oth_agg, on=gcols, how="outer", indicator=True)

        # 비교 플래그
        merged["Compare_Fab"] = fab_name
        merged["SEQ_same"] = merged["M15_SEQ_values"].eq(merged["OTHER_SEQ_values"])
        merged["VAL_same"] = merged["M15_VAL_values"].eq(merged["OTHER_VAL_values"])

        # 어느 한쪽에만 존재하는 경우(outer join으로 드러남)
        merged["exists_in_M15"] = merged["_merge"].isin(["both", "left_only"])
        merged["exists_in_Other"] = merged["_merge"].isin(["both", "right_only"])

        # 보기 좋게 정리
        cols = gcols + [
            "Compare_Fab",
            "exists_in_M15",
            "exists_in_Other",
            "M15_SEQ_values",
            "OTHER_SEQ_values",
            "SEQ_same",
            "M15_VAL_values",
            "OTHER_VAL_values",
            "VAL_same",
        ]

        if mismatched_only:
            view = merged[
                (~merged["SEQ_same"].fillna(False))
                | (~merged["VAL_same"].fillna(False))
                | (~merged["exists_in_M15"])
                | (~merged["exists_in_Other"])
            ][cols].copy()
        else:
            view = merged[cols].copy()

        # 예시용 원본 행 몇 개 남기기 (디버깅에 유용)
        if keep_examples:
            # 키 기준으로 M15/Other 예시 1~2개씩
            def sample_join(sub_keys):
                sk = dict(zip(gcols, sub_keys))
                ex_m15 = m15.query(" & ".join([f"`{k}` == @sk[k]" for k in gcols])).head(2)
                ex_oth = g.query(" & ".join([f"`{k}` == @sk[k]" for k in gcols])).head(2)
                return pd.Series({
                    "M15_examples": ex_m15[[fab_col, seq_col, val_col]].to_dict("records"),
                    "Other_examples": ex_oth[[fab_col, seq_col, val_col]].to_dict("records"),
                })

            examples = view[gcols].drop_duplicates().apply(
                lambda row: sample_join(tuple(row[k] for k in gcols)), axis=1
            )
            view = pd.concat([view.reset_index(drop=True), examples.reset_index(drop=True)], axis=1)

        results.append(view)

    if not results:
        # M15만 있거나 others가 없을 때
        return pd.DataFrame(columns=gcols + [
            "Compare_Fab","exists_in_M15","exists_in_Other",
            "M15_SEQ_values","OTHER_SEQ_values","SEQ_same",
            "M15_VAL_values","OTHER_VAL_values","VAL_same",
            "M15_examples","Other_examples"
        ])

    out = pd.concat(results, axis=0, ignore_index=True)

    # 정렬: 키 → 일치여부 → 비교대상 FAB
    out = out.sort_values(gcols + ["Compare_Fab"]).reset_index(drop=True)
    return out

# ========== 사용 예시 ==========
# df_a: [FAB, FORMULA_GRP_NAME, FORMULA, OPERAND_NAME, SEQ, OPERAND_VALUE, ...]
# df_b: [FORMULA_GRP_NAME, OPER, (PROC ...)]
# df_ab = attach_oper_from_B(df_a, df_b, key="FORMULA_GRP_NAME", b_oper_col="OPER")
# result = compare_m15_vs_others(df_ab,
#                                fab_col="FAB",
#                                keys_for_match=["OPER","FORMULA_GRP_NAME","FORMULA","OPERAND_NAME"],
#                                seq_col="SEQ",
#                                val_col="OPERAND_VALUE",
#                                normalize_operand_value=False,   # 공백 토큰화 동등성 필요하면 True
#                                mismatched_only=True,            # 차이만 보고 싶으면 True
#                                keep_examples=True)              # 원본 예시 몇 개 포함
# result.head()
