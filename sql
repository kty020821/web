import pandas as pd
import numpy as np

def build_apc_compare_report(
    wide_df: pd.DataFrame,
    fabs: list,
    focus_fab: str = "M15",
    out_xlsx_path: str = "apc_compare_report.xlsx",
    sheet_prefix: str = ""
):
    """
    wide_df: compare_equal_by_fab_split()의 반환값
             (index cols 포함된 DF; 내부에서 다시 설정)
    fabs    : 비교 대상 Fab 목록(list). 예: ["M11","M14","M15","M16"]
    focus_fab: 결측 집중 체크할 Fab (예: "M15")
    out_xlsx_path: 저장할 엑셀 파일 경로
    sheet_prefix : 시트명 앞에 붙일 접두사(oper 구분 시 유용)
    """

    # 원본 보존
    df = wide_df.copy()

    # 식별자 컬럼 추출
    id_cols = [c for c in ["FORMULA_GRP_NAME","FORMULA","OPERAND_NAME","SEQ","VALUE_IDX"] if c in df.columns]

    # ===== (A) 파생 진단 컬럼 만들기 =====
    # 1) Fab 중 존재하는 열만 사용 (혹시 일부 Fab이 없을 수 있음)
    present_fabs = [f for f in fabs if f in df.columns]

    # 2) Fab 값 중 하나라도 존재?
    df["__any_value"] = df[present_fabs].apply(lambda r: any(pd.notna(r)), axis=1)

    # 3) 동일 Fab 내 불안정(튜플) 존재 여부
    def _has_unstable(row):
        for f in present_fabs:
            v = row.get(f, np.nan)
            if isinstance(v, tuple):
                return True
        return False
    df["__has_unstable"] = df.apply(_has_unstable, axis=1)

    # 4) 결측 Fab 리스트(다른 Fab엔 값 있는데 해당 Fab만 NaN)
    def _missing_fabs(row):
        vals = {f: row.get(f) for f in present_fabs}
        # 값 있는 Fab이 1개 미만이면 '비교군 없음'이라 Missing 판단 무의미 → 빈 리스트
        non_na = [f for f,v in vals.items() if pd.notna(v)]
        if len(non_na) <= 1:
            return []
        return [f for f,v in vals.items() if pd.isna(v)]
    df["__missing_fabs"] = df.apply(_missing_fabs, axis=1)

    # 5) 포커스 Fab(M15)이 비어있고, 타 Fab은 하나 이상 값이 있는가?
    def _focus_missing(row):
        if focus_fab not in present_fabs:
            return False
        others = [f for f in present_fabs if f != focus_fab]
        others_have = any(pd.notna(row.get(f)) for f in others)
        return pd.isna(row.get(focus_fab)) and others_have
    df["__focus_missing"] = df.apply(_focus_missing, axis=1)

    # 6) __is_equal이 이미 있음(함수 결과). 없으면 계산(보수)
    if "__is_equal" not in df.columns:
        def _is_equal_row(row):
            vals = [row.get(f) for f in present_fabs if pd.notna(row.get(f))]
            if len(vals) <= 1:
                return False
            if any(isinstance(v, tuple) for v in vals):
                return False
            return len(set(vals)) == 1
        df["__is_equal"] = df.apply(_is_equal_row, axis=1)

    # ===== (B) 요약 테이블들 =====
    mismatched = df.loc[~df["__is_equal"]].copy()
    focus_missing = df.loc[df["__focus_missing"]].copy()

    # Fab별 결측·커버리지 요약
    def _coverage_for(fab):
        # 값 존재 행 수
        have = df[fab].notna().sum() if fab in df.columns else 0
        # 비교 의미 있는 행(타 Fab 값 있는 행)
        # -> __any_value가 True인 행 기준
        total_ctx = df["__any_value"].sum()
        # 그 중에서 해당 fab도 값 있는 비율
        cov = have / total_ctx if total_ctx > 0 else np.nan
        return have, total_ctx, cov

    coverage_rows = []
    for f in present_fabs:
        have, total_ctx, cov = _coverage_for(f)
        coverage_rows.append({"Fab": f, "rows_with_value": have, "rows_context": total_ctx, "coverage_ratio": cov})
    coverage = pd.DataFrame(coverage_rows)

    # 조건별(식별자 기준)로, Fab 중 누가 결측인지 요약(집계 문자열)
    summary_by_key = (
        df[id_cols + ["__missing_fabs","__focus_missing","__is_equal","__has_unstable"]]
        .assign(missing_str=lambda x: x["__missing_fabs"].apply(lambda L: ", ".join(L) if L else ""))
        .drop(columns=["__missing_fabs"])
    )

    # Fab별 토큰 수 분포(동일 조건+SEQ에서 VALUE_IDX 최대값을 토큰 수로 간주)
    token_counts = (
        df[id_cols]
        .groupby(id_cols[:-1], dropna=False)["VALUE_IDX"]
        .max()
        .reset_index()
        .rename(columns={"VALUE_IDX":"MAX_VALUE_IDX_PER_COND"})
        .sort_values(by=id_cols[:-1])
    )

    # ===== (C) 엑셀로 내보내기(색으로 직관 강화) =====
    with pd.ExcelWriter(out_xlsx_path, engine="xlsxwriter") as writer:
        def _sheet(name):  # 시트명 길면 잘릴 수 있어 접두사와 함께 안전 처리
            nm = f"{sheet_prefix}{name}".strip()
            return nm[:31]

        # 1) 전체
        df[id_cols + present_fabs + ["__is_equal","__has_unstable","__focus_missing","__any_value"]].to_excel(
            writer, sheet_name=_sheet("ALL"), index=False
        )
        # 2) 불일치만
        mismatched[id_cols + present_fabs + ["__is_equal","__has_unstable","__focus_missing"]].to_excel(
            writer, sheet_name=_sheet("MISMATCH_ONLY"), index=False
        )
        # 3) 포커스 Fab(M15) 결측만
        focus_missing[id_cols + present_fabs + ["__is_equal","__has_unstable"]].to_excel(
            writer, sheet_name=_sheet(f"{focus_fab}_MISSING"), index=False
        )
        # 4) 커버리지
        coverage.to_excel(writer, sheet_name=_sheet("COVERAGE"), index=False)
        # 5) 조건요약
        summary_by_key.to_excel(writer, sheet_name=_sheet("KEY_SUMMARY"), index=False)
        # 6) 토큰개수 분포
        token_counts.to_excel(writer, sheet_name=_sheet("TOKEN_COUNTS"), index=False)

        # --- 조건부 서식(색)
        wb = writer.book

        # 포맷
        fmt_red   = wb.add_format({"bg_color": "#FFC7CE"})  # 불일치
        fmt_yellow= wb.add_format({"bg_color": "#FFEB9C"})  # 결측(특히 focus)
        fmt_orange= wb.add_format({"bg_color": "#F4B084"})  # 불안정(튜플)
        fmt_gray  = wb.add_format({"font_color": "#666666"})

        # 헬퍼: 특정 시트에 조건부 서식 적용
        def colorize_sheet(sheet_name, df_like):
            ws = writer.sheets[sheet_name]
            nrows, ncols = df_like.shape
            if nrows == 0:
                return
            # 컬럼 인덱스 매핑
            columns = list(df_like.columns)
            col_idx = {c:i for i,c in enumerate(columns)}

            # 1) __is_equal == FALSE → 빨강
            if "__is_equal" in col_idx:
                col_letter = col_idx["__is_equal"]
                ws.conditional_format(1, 0, nrows, ncols-1, {
                    "type": "formula",
                    "criteria": f'=${xlsx_col(col_letter)}2=FALSE',
                    "format": fmt_red
                })

            # 2) __has_unstable == TRUE → 주황
            if "__has_unstable" in col_idx:
                col_letter = col_idx["__has_unstable"]
                ws.conditional_format(1, 0, nrows, ncols-1, {
                    "type": "formula",
                    "criteria": f'=${xlsx_col(col_letter)}2=TRUE',
                    "format": fmt_orange
                })

            # 3) 포커스 Fab 결측 하이라이트(노랑): focus_fab 셀만 노랑
            if focus_fab in col_idx:
                fcol = col_idx[focus_fab]
                # 셀 자체가 비어있고, 다른 Fab 중 하나라도 값이 있는 경우
                # 간단화: __focus_missing TRUE면 전체 행 노랑 → 가독성↑
                if "__focus_missing" in col_idx:
                    fmcol = col_idx["__focus_missing"]
                    ws.conditional_format(1, 0, nrows, ncols-1, {
                        "type": "formula",
                        "criteria": f'=${xlsx_col(fmcol)}2=TRUE',
                        "format": fmt_yellow
                    })

            # 4) 값이 튜플 문자열(불안정)인 Fab 셀 색칠(주황)
            for f in present_fabs:
                if f in col_idx:
                    c = col_idx[f]
                    # 텍스트가 '(' 로 시작하면 튜플 문자열로 간주
                    ws.conditional_format(1, c, nrows, c, {
                        "type": "text",
                        "criteria": "begins with",
                        "value": "(",
                        "format": fmt_orange
                    })

        # 엑셀 열 인덱스 -> A1 표기용 헬퍼
        def xlsx_col(i):
            # 0->A, 1->B ...
            s = ""
            i0 = i
            while True:
                i, r = divmod(i, 26)
                s = chr(65 + r) + s
                if i == 0:
                    break
                i -= 1
            return s

        # 적용
        colorize_sheet(_sheet("ALL"), df[id_cols + present_fabs + ["__is_equal","__has_unstable","__focus_missing","__any_value"]])
        colorize_sheet(_sheet("MISMATCH_ONLY"), mismatched[id_cols + present_fabs + ["__is_equal","__has_unstable","__focus_missing"]])
        colorize_sheet(_sheet(f"{focus_fab}_MISSING"), focus_missing[id_cols + present_fabs + ["__is_equal","__has_unstable"]])

    # 터미널 요약 출력(빠른 감)
    print("=== APC Compare Quick Summary ===")
    print(f"Fabs in report: {present_fabs}")
    print("Coverage:")
    print(coverage)
    print(f"\nMismatched rows: {len(mismatched)}")
    print(f"{focus_fab} missing rows (while others have): {len(focus_missing)}")

    return {
        "df_all": df,
        "df_mismatched": mismatched,
        f"df_{focus_fab}_missing": focus_missing,
        "coverage": coverage,
        "token_counts": token_counts
    }
