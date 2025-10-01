import pandas as pd

def preprocess_drop_rawid(df):
    # 'rawid' 포함 컬럼 삭제 (대소문자 무시)
    drop_cols = [c for c in df.columns if 'rawid' in c.lower()]
    return df.drop(columns=drop_cols) if drop_cols else df

def compare_by_recipe(df, id_col="eqp_id", recipe_id_col="recipe_id", recipe_para_col="recipe_para",
                      treat_missing_as_mismatch=True, missing_token="__MISSING__"):
    """
    (recipe_id, recipe_para) 별로 N대 장비의 나머지 컬럼 비교.
    결과: recipe_id, recipe_para, column, 각 장비 값, same, nunique, values
    """
    df = preprocess_drop_rawid(df).copy()
    exclude = {id_col, recipe_id_col, recipe_para_col}
    out_rows = []
    eqps = df[id_col].astype(str).unique().tolist()

    grouped = df.groupby([recipe_id_col, recipe_para_col], dropna=False)
    for (rid, rpara), g in grouped:
        g_sorted = g.sort_index()
        dedup = g_sorted.drop_duplicates(subset=[id_col], keep="first")
        value_cols = [c for c in dedup.columns if c not in exclude]

        for col in value_cols:
            # 장비 순서대로 값 수집
            series = pd.Series(
                [ (None if dedup[dedup[id_col].astype(str)==str(eq)].empty
                    else dedup[dedup[id_col].astype(str)==str(eq)].iloc[0][col])
                  for eq in eqps ],
                index=eqps
            )
            # 결측 처리 정책
            cmp_series = series.fillna(missing_token) if treat_missing_as_mismatch else series
            uniq = pd.unique(cmp_series.dropna() if not treat_missing_as_mismatch else cmp_series).tolist()
            nunique = len(uniq)
            same = (nunique <= 1)

            row = {
                recipe_id_col: rid,
                recipe_para_col: rpara,
                "column": col,
                "nunique": nunique,
                "values": uniq,
                "same": bool(same),
            }
            for eq in eqps:
                row[str(eq)] = series.get(eq, None)
            out_rows.append(row)

    result = pd.DataFrame(out_rows)
    if not result.empty:
        result = result.sort_values([recipe_id_col, recipe_para_col, "same", "column"], ascending=[True, True, True, True]).reset_index(drop=True)
    return result

# 사용 예
# df = pd.read_csv("step1_final_expanded.csv")  # 전처리 테이블
# compare_table = compare_by_recipe(df, id_col="eqp_id", recipe_id_col="recipe_id", recipe_para_col="recipe_para",
#                                   treat_missing_as_mismatch=True, missing_token="∅")
# compare_table.to_csv("compare_table.csv", index=False)
