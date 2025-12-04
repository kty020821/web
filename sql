import pandas as pd
import numpy as np

def simple_expand_kv_cols(df, col_names, key_case="lower", prefix_sep='__'):
    """
    Key-Value 문자열 컬럼 목록을 파싱하고 확장하여 원본 DataFrame에 병합합니다.

    Args:
        df (pd.DataFrame): 원본 데이터프레임.
        col_names (list): 확장할 Key-Value 컬럼 이름 목록 (예: ['열A', '열B']).
        key_case (str): 키를 'lower' 또는 'upper'로 변환할지 지정.
        prefix_sep (str): 새 컬럼 이름에 붙일 접두사 구분 기호.
    """
    
    # 원본 DataFrame의 인덱스를 보존하면서 복사합니다.
    df_expanded = df.copy() 

    for col in col_names:
        # 1. parse_kv_string 함수를 모든 행에 적용하여 딕셔너리 리스트를 만듭니다.
        #    apply(lambda x: ...)를 사용하면 각 셀을 딕셔너리로 변환합니다.
        parsed_dicts = df[col].apply(
            lambda x: parse_kv_string(x, key_case=key_case)
        )
        
        # 2. 딕셔너리 리스트를 Pandas DataFrame으로 변환합니다.
        #    (index=df.index를 사용하여 원본 DataFrame의 행 인덱스를 유지합니다.)
        temp_df = pd.DataFrame(parsed_dicts.tolist(), index=df.index)
        
        # 3. 새로운 컬럼 이름에 접두사(예: '열A__')를 붙여 충돌을 방지합니다.
        new_cols = {k: f"{col}{prefix_sep}{k}" for k in temp_df.columns}
        temp_df = temp_df.rename(columns=new_cols)
        
        # 4. 원본 DataFrame에 병합합니다.
        df_expanded = pd.concat([df_expanded, temp_df], axis=1)

    # Key-Value 문자열이었던 원본 컬럼을 삭제합니다.
    df_expanded = df_expanded.drop(columns=col_names)
    
    return df_expanded
