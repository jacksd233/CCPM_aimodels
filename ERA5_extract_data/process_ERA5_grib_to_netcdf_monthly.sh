#!/bin/bash

'''
File Description:
  使用grib_to_netcdf来从ERA5的grib文件中提取plevel和地面的部分要素，提取的要素和Pangu的输出要素一致。
  注意：surface的数据使用reduced GG，需要首先转换到regular Gaussian网格
  使用Extract_nc_monthly.sh来提交
  提取要素为：
    [1] plevels: z, q, t, u, v
        hPa = 1000,925,850,700,600,500,400,300,250,200,150,100,50
    [2] surface: msl(mean sea level pressure), u10, v10, t2m
  
Author: Weiwen Ji (wwji@pku.edu.cn)
Created Date: 2023-8-1
Last Modified Date:2023-8-2
'''

source ~/.bashrc
conda activate py39
# 参数设置
# plevels=(1000 925 850 700 600 500 400 300 250 200 150 100 50)
# plevel_vars=(z q t u v)  
# surface_vars=(msl u10 v10 t2m)

start_date=$1  #20211113
end_date=$2    #20211231
data_dir=/global/home/aimodels/data/ERA5/2022
output_dir=/global/home/aimodels/data/ERA5/ERA5_extracted/2022


# 处理单日数据
process_day() {
  local date=$1
  
  for time in 00 06 12 18; do

    # 判断文件夹是否存在
    if [ -d $output_dir/$date ]; then
        echo "文件夹已存在，继续操作。"
    else
        # 如果文件夹不存在则创建文件夹
        mkdir $output_dir/$date
        echo "文件夹已创建。"
    fi
    # mkdir $output_dir/$date

    grib_plevel_file=$data_dir/${date}/ERA5-plevels-${date}${time}.grib
    grib_surface_file=$data_dir/${date}/ERA5-surface-${date}${time}.grib
    grib_surface_file_normal=$data_dir/${date}/ERA5-surface-${date}${time}_normal.grib # 坐标转换后的surface文件
    nc_file=$data_dir/ERA5-plevels-${date}${time}.nc
    surface_nc_file=$data_dir/ERA5-surface-${date}${time}.nc

    # GRIB转NC 
    grib_to_netcdf -o $nc_file $grib_plevel_file 
    
    # 提取高空数据
    cdo -sellevel,1000,925,850,700,600,500,400,300,250,200,150,100,50 -selname,z,q,t,u,v $nc_file $output_dir/$date/plevel_${date}_${time}.nc
    
    # 转换地面reduced GG
    cdo -R copy $grib_surface_file $grib_surface_file_normal

    # 转换地面grib to nc
    grib_to_netcdf -o $surface_nc_file $grib_surface_file_normal

    # 提取地面
    cdo -selname,msl,u10,v10,t2m $surface_nc_file $output_dir/$date/surface_${date}_${time}.nc

    # 删除NC
    rm $nc_file
    rm $surface_nc_file
    rm $grib_surface_file_normal

  done
}

# 批处理所有日期
date=$start_date
while [ $date -le $end_date ]; do
  
  process_day $date
  date=$(date -d "$date + 1 day" +%Y%m%d)

done
