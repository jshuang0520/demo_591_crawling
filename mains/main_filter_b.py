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

    print(df_all.shape)
    df_all = df_all.loc[df_all['鄉鎮市區'] != 'The villages and towns urban district', :]
    print(df_all.shape)

    # 總件數, 總車位數（透過交易筆棟數）, 平均總價元, 平均車位總價元
    # df_filter_b = df_all[['建物型態', '交易標的', '總價元', '車位類別', '車位移轉總面積(平方公尺)', '車位總價元']]
    df_filter_b = df_all[['交易筆棟數', '建物型態', '交易標的', '總價元', '車位類別', '車位移轉總面積(平方公尺)', '車位總價元']]

    ans_b = df_filter_b
    ans_b['交易筆棟數_車位'] = ans_b['交易筆棟數'].apply(lambda x: int(x.split("車位")[1]))
    ans_b = ans_b.astype({"總價元": "int", "車位移轉總面積(平方公尺)": 'float', "車位總價元": 'int'})
    ans_b = ans_b[(ans_b['交易筆棟數_車位'] > 0) &
                  (ans_b['車位移轉總面積(平方公尺)'] > 0) &
                  (ans_b['車位總價元'] > 0)]

    print('ans_b:', ans_b)
    print(ans_b.shape)

    print(ans_b['總價元'].sum())
    print(ans_b['總價元'].count())
    print('平均總價元: {}'.format(ans_b['總價元'].sum() / ans_b['總價元'].count()))
    print("-")

    print(ans_b['車位總價元'].sum())
    print(ans_b['交易筆棟數_車位'].sum())
    print('平均車位總價元: {}'.format(ans_b['車位總價元'].sum() / ans_b['交易筆棟數_車位'].sum()))
