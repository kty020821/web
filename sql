import pandas as pd

# 입력: input_data (조인된 Lot 테이블)
# 출력: output_table (예측 결과 테이블)

def predict_cmp_arrival(input_data):
    
    # 1. 데이터 복사 및 목표 CMP 공정 식별
    df = input_data.copy()
    
    # '공정 이름'에 'CMP'가 포함된 모든 공정과 해당 TAT를 추출합니다.
    # (이들이 잠재적인 목표 공정입니다.)
    cmp_targets_df = df[df['공정 이름'].str.contains('CMP')].drop_duplicates(subset=['공정 이름', '공정TAT'])
    
    # TAT를 키로, 공정 이름을 값으로 하는 맵을 생성합니다.
    cmp_tat_map = cmp_targets_df.set_index('공정TAT')['공정 이름'].to_dict()
    # TAT를 내림차순으로 정렬합니다. (숫자가 작을수록 Fab Out에 가까운 공정)
    cmp_tats_sorted = sorted(cmp_tat_map.keys(), reverse=True)

    prediction_results = []

    # 2. 각 Lot을 순회하며 도착 시간 계산
    for index, row in df.iterrows():
        current_tat = row['공정TAT']
        lot_id = row['Lot_ID']
        current_process = row['공정 이름']
        
        # 목표 공정(Target CMP) 찾기: 현재 Lot의 TAT보다 작은 TAT 중 가장 큰 TAT를 가진 CMP를 찾습니다.
        target_tat = None
        target_cmp_name = None
        
        for tat_val in cmp_tats_sorted:
            if tat_val < current_tat:
                # 현재 Lot이 지나갈 다음 CMP 공정입니다.
                target_tat = tat_val
                target_cmp_name = cmp_tat_map[tat_val]
                break
        
        # 3. 도착 일수 계산 및 필터링 (7일 이내)
        if target_tat is not None:
            # 도착까지 남은 일수 = 현재 TAT - 목표 TAT
            days_to_target = current_tat - target_tat
            
            if days_to_target <= 7 and days_to_target >= 0:
                # 7일 이내 도착이 예상되는 Lot 저장
                prediction_results.append({
                    'Lot_ID': lot_id,
                    'Current_Process': current_process,
                    'Days_To_Target': days_to_target,
                    'Target_CMP_Process': target_cmp_name,
                })

    # 4. 결과 DataFrame 생성 및 반환
    output_df = pd.DataFrame(prediction_results)
    
    # 결과가 없을 경우 빈 DataFrame 반환 (Spotfire 오류 방지)
    if output_df.empty:
         output_df = pd.DataFrame(columns=[
            'Lot_ID', 'Current_Process', 'Days_To_Target', 'Target_CMP_Process'
        ])

    return output_df

# Spotfire 데이터 함수에서는 이 함수를 호출하고 결과를 output_table 매개변수에 할당합니다.
# output_table = predict_cmp_arrival(input_data) 
