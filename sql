select *
                    from ( SELECT A.lot_id, A.wf_id, A.main_eqp_id, A.param_nm, A.oper_id, A.oper_det_desc, A.meas_val as THK_VALUE, A.end_tm, B.eqp_id as pre_eqp_id, C.Recipe_rank,
                                    B.module_id as pre_eqp_ch, B.last_update_dtts as pre_oper_time, RANK() over(partition by A.lot_id, A.wf_id, A.param_nm order by A.end_tm DESC ) r2r_rank
                            FROM tas.tas_src_wf_metr_inf A
                            left join ( select lot_id, slot_id, wf_id, eqp_id, module_id, last_update_dtts
                                        from apc.apc_sk_wafer_hst_r2r_all_m10
                                        where 1=1
                                        and dt between '20250618' and '20250718'
                                        and operation_id like '-%'
                                        and resource_type = 'INDEPENDENT'
                                        
                                        union
                                        select lot_id, slot_id, wf_id, eqp_id, module_id, last_update_dtts
                                        from apc.apc_sk_wafer_hst_r2r_all_m11
                                        where 1=1
                                        and dt between '20250618' and '20250718'
                                        and operation_id like '-%'
                                        and resource_type = 'INDEPENDENT'
                                        
                                        union
                                        select lot_id, slot_id, wf_id, eqp_id, module_id, last_update_dtts
                                        from apc.apc_sk_wafer_hst_r2r_all_m14
                                        where 1=1
                                        and dt between '20250618' and '20250718'
                                        and operation_id like '-%'
                                        and resource_type = 'INDEPENDENT'
                                        
                                        union
                                        select lot_id, slot_id, wf_id, eqp_id, module_id, last_update_dtts
                                        from apc.apc_sk_wafer_hst_r2r_all_m15
                                        where 1=1
                                        and dt between '20250618' and '20250718'
                                        and operation_id like '-%'
                                        and resource_type = 'INDEPENDENT'
                                        
                                        union
                                        select lot_id, slot_id, wf_id, eqp_id, module_id, last_update_dtts
                                        from apc.apc_sk_wafer_hst_r2r_m16
                                        where 1=1
                                        and mt between '202506' and '202507'
                                        and operation_id like '-%'
                                        and resource_type = 'INDEPENDENT'
                                        
                                        ) B on CONCAT(left(A.lot_id, 7), '.', A.wf_id) = CONCAT(left(B.lot_id, 7), '.', B.slot_id)
                                                
                        left join (  select distinct D.lot_id, D.eqp_recipe_id, D.Recipe_rank
                                        from ( 
                                        select lot_id, crt_tm, eqp_recipe_id, RANK() over (partition by lot_id order by crt_tm desc) Recipe_rank
                                        from DCP.DCP_DCP_DCOLDATA_INF_M15
                                        where 1=1
                                        and SUBSTRING(lot_id,2,2) = '9C'
                                        and oper_id = 'A4097000A'
                                        and dt between '20250618' and '20250718'
                                        ) D
                                    where 1=1
                                    and D.Recipe_rank = 1
                                    ) C on A.lot_id = C.lot_id
                            
                    where A.mt between '202506' and '202507'
                    and A.end_tm >= '2025-06-18'
                    and A.end_tm <= '2025-07-18'
                    and A.oper_id = 'A4097000A'
                    and right(A.lot_cd, 2) = '9C'
                    and C.eqp_recipe_id like '9C_SLIMOX%'
                    and ( 
                        (A.param_nm like '%POST_THK%' and A.param_nm like '%_AVG' and A.param_nm not like '%GOF%')    
                        or (A.param_nm like '%POST_THK%' and A.param_nm like '%_AVG_A%' and A.param_nm not like '%GOF%')    
                        or (A.param_nm like '%POST_THK%' and A.param_nm like '%THK2_A%' and A.param_nm not like '%GOF%')   
                        or (A.param_nm like '%PRE_THK%' and A.param_nm like '%_AVG' and A.param_nm not like '%GOF%')   
                        or (A.param_nm like '%POST_THK%' and A.param_nm like '%_RAN' and A.param_nm not like '%GOF%') 
                        or (A.param_nm like '%REV%' and A.param_nm like '%_AVG' )
                        or (A.param_nm like '%REV%' and A.param_nm like '%_Z%' )
                        or (A.param_nm like '%REV%' and A.param_nm like '%_RAN' )
                        or A.param_nm like 'EBARA_PAD_%'
                        or A.param_nm like 'EBARA_HEAD_%'
                        or A.param_nm like 'EBARA_DISK_%'
                        or A.param_nm like 'DISK_TIME_%'
                        or A.param_nm like 'HEAD_TIME_%'
                        or A.param_nm like 'PAD_TIME_%'
                    )
                ) B
                where B.r2r_rank = 1
                and B.lot_id = '59C0602'
