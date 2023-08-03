'''
File Description:
  从ERA5提取后的nc文件中将同一种要素、一个月的数据整合起来。
  最终每种变量一个文件夹。
  要素包括为：
    [1] plevels: z, q, t, u, v
        hPa = 1000,925,850,700,600,500,400,300,250,200,150,100,50
    [2] surface: msl(mean sea level pressure), u10, v10, t2m
  
Author: Weiwen Ji (wwji@pku.edu.cn)
Created Date: 2023-8-2
Last Modified Date:2023-8-3
'''

import xarray as xr
import os

class NCProcessor:

    def __init__(self, in_dir, out_dir, month_out_dir):
        self.in_dir = in_dir
        self.out_dir = out_dir
        self.month_out_dir = month_out_dir

    def process(self):
        variables = ['z', 'q', 't', 'u', 'v', 'msl', 'u10', 'v10', 't2m']
        for var in variables:
            print(f'Processing {var}')
            if var in ['z', 'q', 't', 'u', 'v']:
                self._process_plevels(var)
            else:
                self._process_surface(var)

    def _process_plevels(self, var): # 将每天四个时次的ERA5文件合并为每天一个nc文件，同时将每个变量提取为一个nc文件
        files = sorted(os.listdir(self.in_dir)) # files列出所有日期文件夹
        # print(files)
        # 将每日4个文件合并
        for daily_file in files:
            output_file = os.path.join(os.path.join(self.out_dir,daily_file), f"daily_plevels_{var}_{daily_file}.nc")
            if os.path.exists(output_file):
                print(f'{output_file} exists!')
                continue
            else:
                plevels_files = [f for f in os.listdir(os.path.join(self.in_dir,daily_file)) if f.startswith("plevel")] # plevels_files 为每天的4歌plevel的nc文件
                daily_datasets = [xr.open_dataset((os.path.join(os.path.join(self.in_dir,daily_file), f)),decode_times=False)[var] for f in plevels_files] # 读取所有指定var的nc文件
                merged_daily_data = xr.concat(daily_datasets, dim='time') #合并指定var的文件
                # Save the merged data to a new daily nc file
                
                merged_daily_data.to_netcdf(output_file)

        # 开始生成月数据
        date_folders = sorted(os.listdir(self.in_dir))
        # print(date_folders)
        # 获取唯一月份  
        months = sorted(set([d[:6] for d in date_folders]))  #!!! 需要排序
        # print(months)
        for m in months:
    
            # 过滤属于该月的文件
            month_days = [f for f in date_folders if m in f]
            print(month_days)
            month_files = []
            for day in month_days:
                # data_dir = os.path.join(self.in_dir,)
                daily_file_var = os.path.join(os.path.join(self.in_dir, day),f"daily_plevels_{var}_{day}.nc")

                month_files.append(daily_file_var)
            
            # 打开和合并月份文件
            print(month_files)
            data = xr.open_mfdataset(month_files, concat_dim='time' ,combine='nested') #,decode_times=False) 
            
            # 输出月份NetCDF
            data.to_netcdf(os.path.join(month_out_dir, m+f'_plevels_{var}.nc'))


    def _process_surface(self, var):
        files = sorted(os.listdir(self.in_dir)) # files列出所有日期文件夹
        
        # 将每日4个文件合并
        for daily_file in files:
            output_file = os.path.join(os.path.join(self.out_dir,daily_file), f"daily_surface_{var}_{daily_file}.nc")
            if os.path.exists(output_file):
                print(f'{output_file} exists!')
                continue
            else:
                plevels_files = [f for f in os.listdir(os.path.join(self.in_dir,daily_file)) if f.startswith("surface")] # plevels_files 为每天的4歌plevel的nc文件
                daily_datasets = [xr.open_dataset((os.path.join(os.path.join(self.in_dir,daily_file), f)),decode_times=False)[var] for f in plevels_files] # 读取所有指定var的nc文件
                merged_daily_data = xr.concat(daily_datasets, dim='time') #合并指定var的文件
                # Save the merged data to a new daily nc file
                # output_file = os.path.join(os.path.join(self.out_dir,daily_file), f"daily_surface_{var}_{daily_file}.nc")
                merged_daily_data.to_netcdf(output_file)

        # 开始生成月数据
        date_folders = sorted(os.listdir(self.in_dir))
        # 获取唯一月份  
        months = sorted(set([d[:6] for d in date_folders]))
        for m in months:
    
            # 过滤属于该月的文件
            month_days = [f for f in date_folders if m in f]
            month_files = []
            for day in month_days:
                daily_file_var = os.path.join(os.path.join(self.in_dir, day),f"daily_surface_{var}_{day}.nc")

                month_files.append(daily_file_var)
            
            # 打开和合并月份文件
            data = xr.open_mfdataset(month_files, concat_dim='time',combine='nested')#decode_times=False) 
            
            # 输出月份NetCDF
            data.to_netcdf(os.path.join(month_out_dir, m+f'_surface_{var}.nc'))
    
    def merge_monthly_data(self, var):
        date_folders = sorted(os.listdir(self.in_dir))
        # 获取唯一月份  
        months = set([d[:6] for d in date_folders])    
        for m in months:
    
            # 过滤属于该月的文件
            month_days = [f for f in date_folders if m in f]
            month_files = []
            for day in month_days:
                daily_file_var = os.path.join(os.path.join(data_dir, day),f"daily_*_{var}_{day}.nc")

                month_files.append(daily_file_var)         
            # 打开和合并月份文件
            data = xr.open_mfdataset(month_files, concat_dim='time') 
            # 输出月份NetCDF
            data.to_netcdf(os.path.join(month_out_dir, m+f'_{var}.nc'))


if __name__ == '__main__':
    input_dir  = '/home/zjt/jiweiwen/ERA5/Extracted_data/2021/'
    output_dir = '/home/zjt/jiweiwen/ERA5/Extracted_data/2021/' 
    month_out_dir = '/home/zjt/jiweiwen/ERA5/Extracted_data/2021_monthly/'
    processor = NCProcessor(input_dir, output_dir, month_out_dir)
    processor.process()

