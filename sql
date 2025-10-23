[APC_Compare_Result] =
If( Not([Exists_Base]), "NO_BASE",
    If( Not([Exists_Target]), "NO_TARGET",
        If( Not([B_SEQ_IsSingle]), "BASE_SEQ_MULTI",
            If( Not([T_SEQ_IsSingle]), "TARGET_SEQ_MULTI",
                If( Not([B_VAL_IsSingle]), "BASE_VAL_MULTI",
                    If( Not([T_VAL_IsSingle]), "TARGET_VAL_MULTI",
                        If( ([B_SEQ_Max]=[T_SEQ_Max]) AND ([B_VAL_Max]=[T_VAL_Max]),
                            "SAME",
                            "NO_SAME"
                        )
                    )
                )
            )
        )
    )
)

[FinalFlag] = If([APC_Compare_Result]="SAME","SAME","NO_SAME")
