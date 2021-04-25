# -*- coding: utf-8 -*-
import pandas as pd
from src.local.etl_util import Extract

"""
### filter_b

從 df_all 做出
1. 計算 總件數
2. 計算 總車位數（透過交易筆棟數）
3. 計算 平均總價元
4. 計算 平均車位總價元
"""

if __name__ == '__main__':
    local_extractor = Extract()
    df_a = local_extractor.read(file_path='../data/a_lvr_land_a.csv', col_mapping=None)
    df_b = local_extractor.read(file_path='../data/b_lvr_land_a.csv', col_mapping=None)
    df_e = local_extractor.read(file_path='../data/e_lvr_land_a.csv', col_mapping=None)
    df_f = local_extractor.read(file_path='../data/f_lvr_land_a.csv', col_mapping=None)
    df_h = local_extractor.read(file_path='../data/h_lvr_land_a.csv', col_mapping=None)

    pd_list = [df_a, df_b, df_e, df_f, df_h]  # List of your dataframes
    df_all = pd.concat(pd_list, axis=0)  # concatenate along {0/’index’, 1/’columns’}
    print("df_all.shape:", df_all.shape)
    df_all = df_all.loc[df_all['鄉鎮市區'] != 'The villages and towns urban district', :]
    print("df_all.shape:", df_all.shape)

    # 總件數, 總車位數（透過交易筆棟數）, 平均總價元, 平均車位總價元
    # df_filter_b = df_all[['建物型態', '交易標的', '總價元', '車位類別', '車位移轉總面積(平方公尺)', '車位總價元']]
    df_filter_b = df_all

    ans_b = df_filter_b
    ans_b['交易筆棟數_車位'] = ans_b['交易筆棟數'].apply(lambda x: int(x.split("車位")[1]))
    ans_b = ans_b.astype({"總價元": "int", "車位移轉總面積(平方公尺)": 'float', "車位總價元": 'int', '交易筆棟數_車位': 'int'})
    ans_b = ans_b[(ans_b['交易筆棟數_車位'] > 0) & (ans_b['交易筆棟數_車位'].notnull()) &
                  (ans_b['車位移轉總面積(平方公尺)'] > 0) & (ans_b['車位移轉總面積(平方公尺)'].notnull()) &
                  (ans_b['車位總價元'] > 0) & (ans_b['車位總價元'].notnull())
    ]

    print('ans_b:', ans_b)
    print('ans_b.shape:', ans_b.shape)

    total_sum = ans_b['總價元'].sum()
    total_count = ans_b['總價元'].count()
    total_mean = total_sum / total_count
    print('總件數: {}'.format(total_count))
    print('平均總價元: {}'.format(total_mean))
    print("-")

    total_parking_space_dollars = ans_b['車位總價元'].sum()
    total_parking_spaces = ans_b['交易筆棟數_車位'].sum()
    parking_space_mean_dollars = total_parking_space_dollars / total_parking_spaces
    print('總車位數(透過交易筆棟數): {}'.format(total_parking_spaces))
    print('平均車位總價元: {}'.format(parking_space_mean_dollars))

    df_filter_b_ans = pd.DataFrame({'總件數': [total_count],
                                    '平均總價元': [total_mean],
                                    '總車位數(透過交易筆棟數)': [total_parking_spaces],
                                    '平均車位總價元': [parking_space_mean_dollars]})

    # output answer file
    df_filter_b_ans.to_csv('../answers/filter_b_2.csv', index=False)

    # validation
    df_1 = pd.read_csv('../answers/filter_b.csv')
    df_2 = pd.read_csv('../answers/filter_b_2.csv')
    print('df_1 == df_2:', df_1 == df_2)
