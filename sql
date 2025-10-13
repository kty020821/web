def compare_by_fab(df, fab_col='Fab',
                   cond_cols = ["FORMULA_GRP_NAME", 'FORMULA', 'OPERAND_NAME'], 
                   target_cols = ['SEQ', 'OPERAND_VALUE']):

    group_keys = [fab_col] + cond_cols

    results = []
    grouped = df.groupby(group_keys, dropna=False)

    for keys, g in grouped:
        row = dict(zip(group_keys, keys))

        for tcol in target_cols:
            unique_vals = g[tcol].dropna().unique().tolist()
            row[f"{tcol}_values"] = unique_vals
            row[f"{tcol}_nunique"] = len(unique_vals)
            row[f"{tcol}_same"] = (len(unique_vals) == 1)

        results.append(row)
        
    return pd.DataFrame(results)
