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
