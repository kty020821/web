import os 
import os
os.system('sudo pip install --index-url http://nexus.skhynix.com:8081/repository/pypi-proxy/simple asynclakesdk --trusted-host nexus.skhynix.com')
import pandas as pd
import numpy as np
# import happelinrunner
from datetime import datetime, timedelta, date
import uuid
import requests
import os
import time
import json

import cx_Oracle
import traceback
from day.auth.sdk import logon
from day.commc.cube import Cube_Connector
from io import StringIO
from datetime import datetime, timedelta, date
import happelinrunner
from typing import Dict, Optional, List

def goodDocsGetData(doc_id) :

    ROOT = 'http://good-docs.hcp01.skhynix.com/api/v2/gooddocs'
    DOC_ID = doc_id
    AUTH = {
            'USER_ID': '2057197', # 사번
            'TOKEN_SOURCE': 'NCNC',  # 발급 받은 TOKEN_SOURCE
            'TOKEN_KEY': 'NCNC-FEK4F4EE-472F-WDG1-BER3-9DGEWF3FDFGE'          # 발급 받은 TOKEN_KEY
    }
    data = AUTH.copy()
    url = f'{ROOT}/{DOC_ID}'
    
    response = requests.get(url, json=data)
    rows = response.json()
    df = pd.DataFrame(rows)  # 현재는 5000행까지 지원함 추후 업데이트 예정
    if doc_id == '20622721760576608613':
        df = df[['FAB', 'OPER', 'FORMULA_GRP_NAME']]
    else :
        df = df[['Fab','EQP_ID','OPER_DESC','Recipe_ID']]
    df.replace('', np.nan, inplace=True)
    df.dropna(axis=0, inplace=True)

    return df

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
                "result": bool(same),
            }
            for eq in eqps:
                row[str(eq)] = series.get(eq, None)
            out_rows.append(row)

    result = pd.DataFrame(out_rows)
    if not result.empty:
        result = result.sort_values([recipe_id_col, recipe_para_col, "result", "column"], ascending=[True, True, True, True]).reset_index(drop=True)
    return result

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
#########################################

doc_id = '20622721759214960919'
gds_df = goodDocsGetData(doc_id)
oper_list = list(gds_df['OPER_DESC'].unique())



for oper in oper_list :
    gds_df_1 = gds_df[gds_df['OPER_DESC'] == oper].copy()
    gds_df_1['LOT_CD'] = gds_df_1['Recipe_ID'].str.split('_').str[0]
    lot_cd_list = list(gds_df_1['Recipe_ID'].str.split('_').str[0].unique())
    rcp_list = list(gds_df_1['Recipe_ID'].unique())

    
    for rcp in rcp_list:
        gds_df_2 = gds_df_1[gds_df_1['Recipe_ID'] == rcp]
        modeling_df = pd.DataFrame()
        formula_df = pd.DataFrame()
        setup_df = pd.DataFrame()

        print(f'{oper}_{rcp} Start!!!')
    
        for row in gds_df_2.itertuples(index=False):

            if row.Fab == 'M10':
                setup_key_value = f'EQP_ID={row.EQP_ID};PROCESS_ID=*;OPERATION_ID=*;RECIPE_ID={row.Recipe_ID}.cas;%'
            elif (row.Fab != 'M10') & ('CU' in row.OPER_DESC):
               setup_key_value = f'EQP_ID={row.EQP_ID};PROCESS_ID=*;OPERATION_ID=*;RECIPE_ID={row.Recipe_ID}.CAS;%'
            else : 
                setup_key_value = f'EQP_ID={row.EQP_ID};PROCESS_ID=*;OPERATION_ID=*;RECIPE_ID={row.Recipe_ID};%'
            print (setup_key_value)
        
            project_name = 'm15x-apc-compare-table1'
            api_name = f'{row.Fab.lower()}-cmp-apc-modeling-table'
            print(api_name)
        
            access_url = f'http://dp.skhynix.com:8080/datahub/v1/api/{project_name}/{api_name}'
            
            api_key = '4f60cb40-4b6c-4240-94ad-8920fb1e8c50'
            
            headers = {'h-api-token':api_key, 
                       'Content-Type':'application/json'}
            
            data = {"bindParams": [setup_key_value]}
            
            resp = requests.post(access_url, headers=headers, json=data)
            df = pd.read_json(StringIO(resp.text))
        
            if df.empty == False:
            
                if modeling_df.empty == True :
                    modeling_df = df
                else :
                    modeling_df = pd.concat([modeling_df, df], axis=0)
                    
        if modeling_df.empty == False :
            
            modeling_df_1 = modeling_df[modeling_df['SETUP_KEY_RAWID'] == 434].copy()
            setup_df = expand_two_kv_cols_separately(modeling_df_1, 'SETUP_KEY_VALUE', 'SETUP_DATA_VALUE', key_case="lower" )
            setup_df['recipe_id'] = setup_df['recipe_id'].str.upper()

            modeling_df_2 = modeling_df[modeling_df['SETUP_KEY_RAWID'] == 437].copy()
            setup_df2 = expand_two_kv_cols_separately(modeling_df_2, 'SETUP_KEY_VALUE', 'SETUP_DATA_VALUE', key_case="lower" )
            setup_df2['recipe_id'] = setup_df2['recipe_id'].str.upper()

            setup_df_merge = pd.concat([setup_df, setup_df2], axis=0)

            compare_table = compare_by_recipe(setup_df_merge, id_col="eqp_id", recipe_id_col="recipe_id", recipe_para_col="recipe_para",
                                              treat_missing_as_mismatch=True, missing_token="∅")

            compare_table.to_csv(f'modeling_compare_table_{oper}_{rcp}.csv')
 

            #############################################################################
     

