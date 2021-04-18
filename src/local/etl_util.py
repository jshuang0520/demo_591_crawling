# -*- coding: utf-8 -*-
# import missingno as msno
import numpy as np
import pandas as pd
import seaborn as sns
from src.utility.utils import Logger


class Extract:
    def __init__(self):
        self.logger = Logger().get_logger('Extract')

    def read(self, file_path, col_mapping=None):
        """
        read_local_csv_to_pd
        """
        df = pd.read_csv(file_path, na_values=["?", "NaN", "nan", None])  # we set some common na_values as default
        if col_mapping:
            df.rename(columns=col_mapping, inplace=True)

        self.logger.info("shape: {}".format(df.shape))
        self.logger.info("columns: {}".format(df.columns))
        self.logger.info(df.info())

        # missing values
        missing_values_count = df.isnull().sum()
        total_cells = np.product(df.shape)
        total_missing = missing_values_count.sum()
        percent_missing = (total_missing / total_cells) * 100
        self.logger.info("missing percentage: {:.{prec}f}%".format(percent_missing, prec=2))

        # sns.heatmap(df.isnull(), cbar=False)
        # msno.matrix(df)

        # sns.set_style("darkgrid", {"font.sans-serif": ['simhei', 'Droid Sans Fallback']})
        # msno.heatmap(df)

        print(df.head(3))

        return df


class Transform:
    def __init__(self):
        self.logger = Logger().get_logger('Transform')

    def gender(self):
        """
        {'id': 10741776,
        'user_id': 14581,
        'address': '民權東路五段民生社..',
        'type': '1',
        'post_id': 10741776,
        'regionid': 1,
        'sectionid': 4,
        'streetid': 26168,
        'room': 0,
        'area': 6,
        'price': '8,800',
        'storeprice': 0,
        'comment_total': 1,
        'comment_unread': 1,
        'comment_ltime': 1617867405,
        'hasimg': 1,
        'kind': 3,
        'shape': 1,
        'houseage': 0,
        'posttime': '2小時內',
        'updatetime': 1617805112,
        'refreshtime': 1618639202,
        'checkstatus': 1,
        'status': '',
        'closed': 0,
        'living': 'advstore,market,night,park,school',
        'condition': 'tv,icebox,cold,washer,hotwater,four,broadband,girl,balcony_0,bed,wardrobe,bookTable,chair',
        'isvip': 1,
        'mvip': 1,
        'is_combine': 1,
        'cover': 'https://hp2.591.com.tw/house/active/2010/11/15/128981415391868403_210x158.crop.jpg',
        'browsenum': 97,
        'browsenum_all': 1249,
        'floor2': 0,
        'floor': 2,
        'ltime': '2021-04-17 14:02:39',
        'cases_id': 0,
        'social_house': 0,
        'distance': 0,
        'search_name': '',
        'mainarea': None,
        'balcony_area': None,
        'groundarea': None,
        'linkman': '蔡先生',
        'housetype': 2,
        'street_name': '民權東路五段',
        'alley_name': '',
        'lane_name': '',
        'addr_number_name': '',
        'kind_name_img': '分租套房',
        'address_img': '民生社區便宜二樓套房，二分鐘到車站，超市',
        'cases_name': '',
        'layout': '',
        'layout_str': '',
        'allfloor': 4,
        'floorInfo': '樓層：2/4',
        'house_img': '205602721,205602722,205602723,205602724,205602725,205602726,205602727,',
        'houseimg': None,
        'cartplace': '',
        'space_type_str': '',
        'photo_alt': '台北租屋,松山租屋,分租套房出租,民生社區便宜二樓套房，二分鐘到車站，超市',
        'addition4': 1,
        'addition2': 0,
        'addition3': 0,
        'vipimg': '',
        'vipstyle': 'isvip',
        'vipBorder': 'vipStyle',
        'new_list_comment_total': 1,
        'comment_class': '',
        'price_hide': 'price-hide',
        'kind_name': '分租套房',
        'photoNum': '7',
        'filename': 'https://hp2.591.com.tw/house/active/2010/11/15/128981415391868403_210x158.crop.jpg',
        'nick_name': '代理人 蔡先生',
        'new_img': '',
        'regionname': '台北市',
        'sectionname': '松山區',
        'icon_name': '出租',
        'icon_class': 'rent',
        'fulladdress': '民權東路五段民生社區便宜二樓套房，二分鐘到車站，超市',
        'address_img_title': '民生社區便宜二樓套房，二分鐘到車站，超市',
        'browsenum_name': '昨日瀏覽',
        'unit': '元/月',
        'houseid': 10741776,
        'region_name': '台北市',
        'section_name': '松山區',
        'addInfo': '',
        'onepxImg': ''}

        :return:
        """