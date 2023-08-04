'''
File Description:
    将NOAA CMORPH原始降水数据转为每月的nc数据，
    一个月的数据中包含每天6小时间隔数据（00、06、12、18），直接从CMORPH对应时间提取
    水平分辨率仍为8km

Created Date: 2023-8-4 wwji
Last Modified Date: 2023-8-4
'''

import sys
import os
import xarray as xr
import re

class GenMonthData:
    def __init__(self,input_dir,output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.year = '2021'
    
    def process(self):
        # months = ['01','02','03','04','05','06','07','08','09','10','11','12']
        months = ['%02d' % m for m in range(1, 13)]
        for month in months:
            print(f'Processing {month} month data')
            self.daily2monthly(month)


    def daily2monthly(self, month):
        # 月文件位置，及列出一个月所有文件名称（逐小时，每个小时文件time=2，最终分辨率30分钟）
        month_path  = os.path.join(self.input_dir,month)
        month_files = sorted(os.listdir(month_path))
        month_data = []
        # 编译正则表达式以提取时间  
        regex = re.compile(r'(\d{8})(\d{2})')  # 提取时间名称
        hour_times = ['00','06','12','18']

        # 合并每天00、06、12、18整点的数据
        # file_prefix = 'CMORPH_V1.0_ADJ_8km-30min_'
        for f in month_files:
            time_match = regex.search(f)
            hour_match = time_match.group(2) # 第二个group即 小时
            if hour_match in hour_times:
                ds = xr.open_dataset(os.path.join(month_path, f))
                # 提取变量
                da = ds['cmorph'].isel(time=0)
                # 添加到数据中
                month_data.append(da)

        # 拼接一个月的数据
        combined = xr.concat(month_data, dim='time')

        # 输出到指定位置
        month_output_path = os.path.join(self.output_dir,self.year)
        if os.path.exists(month_output_path):
            print(f'{month_output_path} exists! Start to generate monthly data')
        else:
            print(f'create new {month_output_path}! Start to generate monthly data')
            os.mkdir(month_output_path)
        # 将每个月的nc输出到output文件夹
        month_data_path = os.path.join(month_output_path,f'CMORPH_8km_Monthly_6hourDaily_{self.year}{month}.nc')
        combined.to_netcdf(month_data_path)




if __name__ == '__main__':
    input_dir  = '/home/zjt/jiweiwen/NOAA_CMORPH/2021/'
    output_dir = '/home/zjt/jiweiwen/NOAA_CMORPH/output/' 
    NOAA_month_output =  GenMonthData(input_dir,output_dir)
    NOAA_month_output.process()
