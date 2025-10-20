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

doc_id = '20622721760576608613'
gds_df = goodDocsGetData(doc_id)

base_df = gds_df.copy()
test_df= base_df.copy()
test_df['FAB'] = 'M15'

setup_df = pd.concat([base_df, test_df], axis=0)
setup_df.reset_index(inplace=True)
print(setup_df)

oper_list = list(setup_df['PROC'].unique())
print(oper_list)





for oper in oper_list:

    setup_df_2 = setup_df[(setup_df['PROC'] == oper)].copy()
    formula_df = pd.DataFrame()

    Fab_list = list(setup_df_2['FAB'].unique())
    print(Fab_list)
    
    for fab in Fab_list :
        print(fab)
        print(oper)
        setup_df_3 = setup_df_2[(setup_df_2['FAB'] == fab)].copy()
        print(setup_df_3.head())
        sql_cond =list(setup_df_3['FORMULA GR'].unique())
        print(sql_cond)
        
        project_name_2 = 'm15x-apc-compare-table2'
        api_name_2 = f'{fab.lower()}-cmp-apc-formula-table'
    
        print(api_name_2)
    
        access_url_2 = f'http://dp.skhynix.com:8080/datahub/v1/api/{project_name_2}/{api_name_2}'
        
        api_key = '4f60cb40-4b6c-4240-94ad-8920fb1e8c50'
        
        headers = {'h-api-token':api_key, 
                   'Content-Type':'application/json'}
        
        data_2 = {"bindParams": [sql_cond]}
        
        resp_2 = requests.post(access_url_2, headers=headers, json=data_2)
        print(resp_2)
    
        try :
            df_2 = pd.read_json(StringIO(resp_2.text))
        
            df_2['Fab'] = fab
            
            if df_2.empty == False:
            
                if formula_df.empty == True :
                    formula_df = df_2
                else :
                    formula_df = pd.concat([formula_df, df_2], axis=0) 
                    
                    
        except Exception as e :
            print(f"Error : {e}")
    print(formula_df)

    formula_df['OPER']
    
    compare_formula_table = compare_equal_by_fab(formula_df, case_sensitive=False, strip_spaces=True)
    compare_formula_table.to_csv(f'compare_formula_table_{oper}.csv')   

