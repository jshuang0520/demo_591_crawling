# -*- coding: utf-8 -*-
import pandas as pd
from src.local.etl_util import Extract


"""
### filter_a

從 df_all 做出
1. 主要用途 為 住家用
2. 建物型態 為 住宅大樓
3. 總樓層數 為 大於等於十三層
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

    df_filter_a = df_all
    df_filter_a = df_filter_a[(df_filter_a['主要用途'].str.contains("住家用", na=False)) &
                              (df_filter_a['建物型態'].str.contains("住宅大樓", na=False))
                              ]
    # print("sorted(list(set(df_filter_a['總樓層數']))):", sorted(list(set(df_filter_a['總樓層數']))))
    # print(df_filter_a.head(3))

    """
    ![img1](https://upload-images.jianshu.io/upload_images/256855-312b19ff4fcaae34.png)
    """
    import re
    # import string

    common_used_numerals_tmp = {'零': 0,
                                '一': 1,
                                '二': 2, '兩': 2,
                                '三': 3,
                                '四': 4,
                                '五': 5,
                                '六': 6,
                                '七': 7,
                                '八': 8,
                                '九': 9,
                                '十': 10,
                                '百': 100, '千': 1000, '萬': 10000, '億': 100000000}

    def simple_case_ch_char_to_int(han_char):
        total_sum = 0
        power = len(han_char) - 1  # 2
        for char in han_char:
            total_sum += common_used_numerals_tmp[char] * (10 ** power)
            power -= 1
        return total_sum


    def ch_char_to_int(uchar):
        """
        purpose: we want to recognize the floor of the building,
        we do a mapping from Chinese character to digital integer

        """
        # print("===***=== original input:", uchar)  # e.g. '一百零一層'

        if '層' in uchar:
            uchar = uchar.split('層')[0]

        # for the exception case: 一零一層 (there's no '百'); 一二一層
        if (len(uchar) == 3) and ('零' in uchar):
            if uchar.index('零') == 1:
                in_series = uchar.split('零')
                floor_100 = common_used_numerals_tmp[in_series[0]]
                floor_1 = common_used_numerals_tmp[in_series[1]]
                total_sum = 100 * floor_100 + floor_1
            else:
                total_sum = simple_case_ch_char_to_int(uchar)
        elif (len(uchar) == 3) and ('十' not in uchar):  # for: 一二一層 here, and exclude 九十一層
            total_sum = simple_case_ch_char_to_int(uchar)

        # for the exception case: 二十層
        elif (len(uchar) == 2) and (uchar.startswith('十')):
            floor_1 = common_used_numerals_tmp[uchar.split('十')[1]]
            total_sum = 10 * 1 + floor_1

        else:
            # 1. split unit: '百'
            sep_char = re.split(r'百', uchar)
            total_sum = 0
            # print("sep_char:", sep_char)  # ['一', '零一']
            # loop for this list
            for idx, char in enumerate(sep_char):
                # print("-level 1: 'idx, char' = {}, {}".format(idx, char))
                # turn characters into integers in Decimal(十進位)
                int_series = char.replace('百', '100').replace('十', '10')
                # print("level 1 int_series:", int_series)
                int_series = re.split(r'(\d+)', int_series)
                int_series.append("")
                # print("int_series:", int_series)
                # to combine chinese with decimal e.g. 100, 10, 1
                char_in_decimal = ["".join(i) for i in zip(int_series[0::2], int_series[1::2])]
                char_in_decimal = ['零' if i == '' else i for i in char_in_decimal]
                # print("int_series:", int_series)
                num = 0
                # char_in_decimal：["三1000", "二100", "四10", "二"]
                # 3. 求和加總 int_series
                for idx2, char2 in enumerate(char_in_decimal):
                    char2 = re.sub('零', '', char2) if char2 != '零' else char2
                    # print("--level 2: 'idx2, char2' = {}, {}".format(idx2, char2))
                    temp = common_used_numerals_tmp[char2[0]] * int(char2[1:]) if len(char2) > 1 \
                        else common_used_numerals_tmp[char2[0]]
                    num += temp
                    # print("transformed part sum %s"%str(num))
                total_sum += num * (10 ** (2 * (len(sep_char) - idx - 1)))
        return int(total_sum)


    def test(ch):
        print("{}:{}".format(ch, ch_char_to_int(ch)))


    test("一零一層")
    test("一百零一層")
    test("九十一層")
    test("九十層")
    test("五層")

    test("三百五十四層")
    test("二十層")
    test("十二層")
    test("一五零層")
    test("一二一層")

    test_list = sorted(list(set(df_filter_a['總樓層數'])))
    for x in test_list:
        test(x)

    df_filter_a['總樓層數_int'] = df_filter_a['總樓層數'].apply(ch_char_to_int)
    df_filter_a = df_filter_a[df_filter_a['總樓層數_int'] >= 13]
    print(df_filter_a, df_filter_a.shape, sorted(list(set(df_filter_a['總樓層數']))))

    # output file
    df_filter_a.to_csv('../answers/filter_a_2.csv', index=False)

    # validation
    df_1 = pd.read_csv('../answers/filter_a.csv')
    df_2 = pd.read_csv('../answers/filter_a_2.csv')
    print('df_1 == df_2:', df_1 == df_2)

    """ we can accelerate the process through using swifter
    swifter
    --
    
    df.groupby(['columns_1','column_2'])[['column_3']].swifter.apply(lambda g: g.values.tolist()).to_dict()
    --

    gb = input_dataframe.groupby(['columns_1'])
    self.logger.info('fitting using swifter.apply')
    splitted_dfs = [my_function_1(gb.get_group(x), 'column_3').swifter.progress_bar(False).apply(
        lambda row: my_function_2(row['column_4'], my_constant),
        axis=1)
        for x in gb.groups]
    series = pd.concat(splitted_dfs, axis=0)  # concat vertically
    index_to_keep = list(series.loc[series == 0].index)  # loc - label/absolute location
    input_dataframe = input_dataframe.loc[index_to_keep].reset_index(drop=True)
    --

    models = df_user_activity.swifter.progress_bar(False).apply(
            fit_model, model_creator=get_model_creator(fit_method),
            returned_data_preparation=return_data_prepared, axis=1, **kwargs)

    """
