import pandas as pd

def compare_all_recipes(df, id_col="eqp_id", recipe_col="recipe_para"):
    """
    전체 장비에 대해 (id_col, recipe_col) 기준으로 비교 테이블 생성.
    - eqp_id가 두 개라는 전제.
    - recipe_para는 여러 개일 수 있음.
    - 결과: 각 recipe_para × 각 컬럼에 대해 두 eqp_id의 값과 차이 여부를 표시.
    """
    eqp_ids = df[id_col].unique().tolist()
    if len(eqp_ids) != 2:
        raise ValueError(f"{id_col} 고유값이 2개여야 합니다. 현재={eqp_ids}")
    eqp_left, eqp_right = eqp_ids

    results = []
    exclude = {id_col, recipe_col}
    value_cols = [c for c in df.columns if c not in exclude]

    for recipe in df[recipe_col].unique():
        left = df[(df[id_col] == eqp_left) & (df[recipe_col] == recipe)]
        right = df[(df[id_col] == eqp_right) & (df[recipe_col] == recipe)]
        if left.empty or right.empty:
            continue  # 한쪽 장비 데이터가 없으면 비교 불가
        # 단일행만 있다고 가정
        left_row, right_row = left.iloc[0], right.iloc[0]

        for col in value_cols:
            lv, rv = left_row[col], right_row[col]
            same = (pd.isna(lv) and pd.isna(rv)) or (lv == rv)
            results.append({
                recipe_col: recipe,
                "column": col,
                f"{eqp_left}": lv,
                f"{eqp_right}": rv,
                "same": same
            })

    return pd.DataFrame(results)
