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

ref_fab_id = refFabId.lower()
target_fab_id = targetFabId.lower()

ref_eqp_id = refEqpId
target_eqp_id = targetEqpId

ref_recipe_id = refRecipeId
target_recipe_id = targetRecipeId


project_name = 'm15x-apc-compare-table1'
api_key = '4f60cb40-4b6c-4240-94ad-8920fb1e8c50'
headers = {'h-api-token':api_key, 
           'Content-Type':'application/json'}

api_name = f'm16-cmp-apc-modeling-table'
access_url = f'http://dp.skhynix.com:8080/datahub/v1/api/{project_name}/{api_name}'
data = {"bindParams": [f'EQP_ID=6KCCF801;PROCESS_ID=*;OPERATION_ID=*;RECIPE_ID=E2_M1CU_R12_TSV.CAS;%']}
resp = requests.post(access_url, headers=headers, json=data)
df_ref = pd.read_json(StringIO(resp.text))


api_name_target = f'm15-cmp-apc-modeling-table'
access_url_target = f'http://dp.skhynix.com:8080/datahub/v1/api/{project_name}/{api_name_target}'
data_target = {"bindParams": [f'EQP_ID=5KCC8801;PROCESS_ID=*;OPERATION_ID=*;RECIPE_ID=E2_M1CU_R12_TSV.CAS;%']}
resp_target = requests.post(access_url_target, headers=headers, json=data_target)
df_target = pd.read_json(StringIO(resp_target.text))

modeling_df = pd.concat([df_ref, df_target], axis=0)

modeling_df_1 = modeling_df[modeling_df['SETUP_KEY_RAWID'] == 434].copy()
setup_df = expand_two_kv_cols_separately(modeling_df_1, 'SETUP_KEY_VALUE', 'SETUP_DATA_VALUE', key_case="lower" )
setup_df['recipe_id'] = target_recipe_id

compare_table = compare_by_recipe(setup_df, id_col="eqp_id", recipe_id_col="recipe_id", recipe_para_col="recipe_para",treat_missing_as_mismatch=True, missing_token="∅")
modeling_output = compare_table

modeling_df_2 = modeling_df[modeling_df['SETUP_KEY_RAWID'] == 437].copy()
setup_df2 = expand_two_kv_cols_separately(modeling_df_2, 'SETUP_KEY_VALUE', 'SETUP_DATA_VALUE', key_case="lower" )
setup_df2['recipe_id'] = target_recipe_id

compare_table_2 = compare_by_recipe(setup_df2, id_col="eqp_id", recipe_id_col="recipe_id", recipe_para_col="recipe_para", treat_missing_as_mismatch=True, missing_token="∅")
condition_output = compare_table_2
