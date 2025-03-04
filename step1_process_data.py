#import public codes
import datetime
import os, sys

import pandas as pd

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
public_code_dir = parent_dir + '/public_codes'
public_code_common_dir = public_code_dir + '/common'
sys.path.append(public_code_common_dir)

from common_functions import func_df_elem_is_nan, func_str_is_contain_substr, func_date_add_days
from common_functions import func_df_col_unique_values, func_list_sort, func_df_merge_by_col, func_df_col_dt_diff, func_df_col_str2dt, func_df_col_dt2str, func_df_col_drop

from config import DATA_DIR

if __name__ == '__main__':
    data_raw = pd.read_excel(DATA_DIR + "/city_product.xlsx")
    data_raw = data_raw.to_dict('records')
    last_notna_卸货日期=None; last_notna_起点=None; last_notna_终点=None; last_notna_货物=None; last_notna_合计金额=None

    updated = []
    for each_row in data_raw:
        卸货日期 = each_row['卸货日期']; 起点 = each_row['起点']; 终点 = each_row['终点']; 货物=each_row['货物']; 合计金额=each_row['合计金额']

        if func_df_elem_is_nan(卸货日期): 卸货日期_update=last_notna_卸货日期
        else: 卸货日期_update=卸货日期
        if func_df_elem_is_nan(起点): 起点_update = last_notna_起点
        else: 起点_update = 起点
        if func_df_elem_is_nan(终点): 终点_update = last_notna_终点
        else: 终点_update = 终点
        if func_df_elem_is_nan(货物): 货物_update = last_notna_货物
        else: 货物_update = 货物
        if func_df_elem_is_nan(合计金额): 合计金额_update = last_notna_合计金额
        else: 合计金额_update = 合计金额

        起点_update_省份 = 起点_update.split('，')[0]
        起点_update_城市 = 起点_update.split('，')[1] if func_str_is_contain_substr(起点_update,'，') else 起点_update.split('，')[0]
        终点_update_省份 = 终点_update.split('，')[0]
        终点_update_城市 = 终点_update.split('，')[1] if func_str_is_contain_substr(终点_update, '，') else 终点_update.split('，')[0]
        updated.append({'卸货日期':卸货日期_update,
                        '起点_省份':起点_update_省份, '起点_城市':起点_update_城市,
                        '终点_省份':终点_update_省份, '终点_城市':终点_update_城市,
                        '货物': 货物_update,
                        '合计金额':合计金额_update})

        if not func_df_elem_is_nan(卸货日期): last_notna_卸货日期 = 卸货日期
        if not func_df_elem_is_nan(起点): last_notna_起点 = 起点
        if not func_df_elem_is_nan(终点): last_notna_终点 = 终点
        if not func_df_elem_is_nan(货物): last_notna_货物 = 货物
        if not func_df_elem_is_nan(合计金额): last_notna_合计金额 = 合计金额

    updated = pd.DataFrame(updated)

    product_category = pd.read_excel(DATA_DIR + "/货物.xlsx")
    updated = func_df_merge_by_col(updated, product_category[['货物','类别']], by_cols_x=['货物'], by_cols_y=['货物'], join_type='all.x')

    #adding week_i
    min_卸货日期 = min(updated['卸货日期'])
    updated['min_卸货日期'] = min_卸货日期
    updated = func_df_col_str2dt(updated,str_col='min_卸货日期',dt_col='min_卸货日期'); updated = func_df_col_str2dt(updated,str_col='卸货日期',dt_col='卸货日期')
    updated = func_df_col_dt_diff(updated,dt_col_1='卸货日期',dt_col_2='min_卸货日期',des_col='day_i',unit='day')

    updated = func_df_col_dt2str(updated,dt_col='卸货日期',str_col='卸货日期',str_format='%Y-%m-%d'); updated = func_df_col_drop(updated,cols=['min_卸货日期'])

    WEEK_SIZE = 7
    updated['week_i'] = updated['day_i']//WEEK_SIZE

    unique_week_is = func_df_col_unique_values(updated,col='week_i')
    week_dates = []
    for each_week_i in unique_week_is:
        start_date = func_date_add_days(min_卸货日期, each_week_i*WEEK_SIZE)
        end_date = func_date_add_days(min_卸货日期, each_week_i*WEEK_SIZE+WEEK_SIZE-1)
        week_dates.append(f"{start_date}~{end_date}")
    week_infor = pd.DataFrame({'week_i':unique_week_is, 'week_date':week_dates})
    updated = func_df_merge_by_col(updated, week_infor, by_cols_x=['week_i'], by_cols_y=['week_i'], join_type='all.x')

    updated.to_excel(DATA_DIR+'/city_product_clean.xlsx',index=False)


