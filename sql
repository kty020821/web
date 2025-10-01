import pandas as pd

def pick_single_row(df, id_col, recipe_col, eqp_id, recipe_val):
    sel = df[(df[id_col].astype(str) == str(eqp_id)) & (df[recipe_col].astype(str) == str(recipe_val))]
    if len(sel) == 0:
        raise ValueError(f"조건에 해당하는 행이 없습니다: {id_col}={eqp_id}, {recipe_col}={recipe_val}")
    if len(sel) > 1:
        raise ValueError(f"조건에 해당하는 행이 2개 이상입니다(1개만 허용): {id_col}={eqp_id}, {recipe_col}={recipe_val}, count={len(sel)}")
    return sel.iloc[0]

def compare_two_conditions(df, id_col="EQP_ID", recipe_col="recipe_para",
                           left_eqp="A", left_recipe="D",
                           right_eqp="B", right_recipe="D",
                           treat_missing_as_diff=True):
    """
    두 조건의 단일 행을 뽑아 (id_col, recipe_col) 제외 컬럼을 비교.
    - treat_missing_as_diff=True면 NaN vs 값은 '다름', NaN vs NaN은 '같음'
    """
    left_row  = pick_single_row(df, id_col, recipe_col, left_eqp, left_recipe)
    right_row = pick_single_row(df, id_col, recipe_col, right_eqp, right_recipe)

    exclude = {id_col, recipe_col}
    value_cols = [c for c in df.columns if c not in exclude]

    rows = []
    for c in value_cols:
        lv = left_row[c]
        rv = right_row[c]
        same = (pd.isna(lv) and pd.isna(rv)) or (lv == rv)
        if treat_missing_as_diff and ((pd.isna(lv) and not pd.isna(rv)) or (not pd.isna(lv) and pd.isna(rv))):
            same = False
        rows.append({"column": c, f"{left_eqp}:{left_recipe}": lv, f"{right_eqp}:{right_recipe}": rv, "same": bool(same)})
    full = pd.DataFrame(rows)
    diff_only = full[~full["same"]].reset_index(drop=True)
    return full, diff_only
