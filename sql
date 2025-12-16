import pandas as pd
import numpy as np
import requests
from io import StringIO

def preprocess_drop_rawid(df):
    # 'rawid' 포함 컬럼 삭제 (대소문자 무시)
    drop_cols = [c for c in df.columns if 'rawid' in c.lower()]
    return df.drop(columns=drop_cols) if drop_cols else df

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
    df = df[['RAWID', kv_col]]
    # kv_col만 확장 → 새 컬럼명은 'kv_col__key'
    dicts, all_keys = [], set()
    for _, row in df.iterrows():
        d = parse_kv_string(row.get(kv_col), key_case=key_case)
        dicts.append(d)
        all_keys.update(d.keys())
    out = df.copy()
    for k in sorted(all_keys):
        out[f"{k}"] = [d.get(k) for d in dicts]
    out = out.drop(columns = kv_col, axis=1)
    return out

def expand_two_kv_cols_separately(df, col1, col2, key_case="lower", drop_original=False):
    # col1 확장 + col2 확장 → 병합 (키가 안겹친다는 전제)
    a = expand_one_kv_col(df, col1, key_case=key_case)
    b = expand_one_kv_col(df, col2, key_case=key_case)
    out = pd.merge(a, b, on = 'RAWID')
    if drop_original:
        out = out.drop(columns=[c for c in [col1, col2] if c in out.columns])
    return out


import pandas as pd
import numpy as np

def compare_and_detail_by_group(df, group_col, id_col, exclude_cols=None, missing_token="N/A"):
    """
    특정 그룹 열(group_col)을 기준으로 그룹화하여, 각 장비(id_col)의 값을 
    개별 컬럼으로 표시하며 일치 여부를 비교합니다.
    
    Args:
        df (pd.DataFrame): Key-Value 확장이 완료된 데이터프레임.
        group_col (str): 그룹화 기준 열 (예: 'recipe_para').
        id_col (str): 장비 ID 열 (예: 'eqp_id').
        exclude_cols (list): 비교에서 제외할 추가 열.
        missing_token (str): 결측치 비교 시 사용할 토큰.
    """
    
    if exclude_cols is None:
        exclude_cols = []
    
    # 1. 고유한 장비 호기 목록을 추출합니다. (예: ['6KCCF801', '5KCC8801'])
    eqps = df[id_col].astype(str).unique().tolist() 
    
    # 비교에서 제외할 모든 컬럼을 정의합니다.
    all_exclude = set(exclude_cols + [group_col, id_col])
    
    out_rows = []
    
    # 2. 그룹 키를 기준으로 데이터를 그룹화합니다.
    grouped = df.groupby(group_col, dropna=False)
    
    for group_val, g in grouped:
        # 실제 파라미터 값을 가진 컬럼 목록을 추출합니다.
        value_cols = [c for c in g.columns if c not in all_exclude]
        
        # 그룹 내에서 장비 ID가 중복된 경우 첫 번째 값만 사용합니다.
        dedup = g.drop_duplicates(subset=[id_col], keep="first")

        for col in value_cols:
            series = {}
            
            # 3. 각 장비 호기별로 현재 컬럼의 값을 수집합니다.
            for eq in eqps:
                eq_row = dedup[dedup[id_col].astype(str) == str(eq)]
                
                # 해당 장비가 이 그룹에 없으면 None, 있으면 해당 값을 할당합니다.
                value = eq_row.iloc[0][col] if not eq_row.empty else None
                series[str(eq)] = value

            # 4. 수집된 값을 기반으로 일치 여부를 판단합니다.
            value_series = pd.Series(series)
            
            # None/NaN을 'missing_token'으로 채워 결측치도 불일치로 간주하여 비교합니다.
            cmp_series = value_series.fillna(missing_token) 
            is_same = (cmp_series.nunique() <= 1)
            
            # 5. 결과 행을 구성합니다. (unique_count, values_list 제외)
            row = {
                group_col: group_val,      # 그룹 키 값 (예: recipe_para)
                "parameter_column": col,   # 파라미터 컬럼 이름 (예: 열A__WID)
                "is_same": bool(is_same),  # 일치 여부
            }
            
            # 6. 장비별 값을 결과 딕셔너리에 추가합니다.
            for eq in eqps:
                row[str(eq)] = series[str(eq)] # 실제 값 또는 None
            
            out_rows.append(row)

    # 7. 최종 DataFrame 생성 및 정렬
    result = pd.DataFrame(out_rows)
    if not result.empty:
        # 불일치(False) 항목이 먼저 오도록 정렬합니다.
        result = result.sort_values([group_col, "is_same", "parameter_column"], ascending=[True, True, True]).reset_index(drop=True)
        
    return result
    
ref_fab_id = refFabId.lower()
target_fab_id = targetFabId.lower()

ref_eqp_id = refEqpId
target_eqp_id = targetEqpId

ref_recipe_id = refRecipeId
target_recipe_id = targetRecipeId



compare_modeling_table = compare_and_detail_by_group(df=setup_df, group_col="recipe_para", id_col="eqp_id", exclude_cols=["RAWID"], missing_token="__NA__")

modeling_output = compare_modeling_table

modeling_df_2 = modeling_df[modeling_df['SETUP_KEY_RAWID'] == 437].copy()
setup_df2 = expand_two_kv_cols_separately(modeling_df_2, 'SETUP_KEY_VALUE', 'SETUP_DATA_VALUE', key_case="lower" )

compare_modeling_table_2 = compare_and_detail_by_group(df=setup_df2, group_col="recipe_para", id_col="eqp_id", exclude_cols=["RAWID"], missing_token="__NA__")

condition_output = compare_modeling_table_2

