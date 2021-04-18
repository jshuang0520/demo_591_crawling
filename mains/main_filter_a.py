# -*- coding: utf-8 -*-
from src.local.etl_util import Extract

if __name__ == '__main__':
    local_extractor = Extract()
    df_a = local_extractor.read(file_path='../data/a_lvr_land_a.csv', col_mapping=None)
    df_b = local_extractor.read(file_path='../data/b_lvr_land_a.csv', col_mapping=None)
    df_e = local_extractor.read(file_path='../data/e_lvr_land_a.csv', col_mapping=None)
    df_f = local_extractor.read(file_path='../data/f_lvr_land_a.csv', col_mapping=None)
    df_h = local_extractor.read(file_path='../data/h_lvr_land_a.csv', col_mapping=None)
