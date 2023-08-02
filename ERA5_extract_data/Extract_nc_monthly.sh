#!/bin/bash
source ~/.bashrc
startYear=2022
endYear=2022

for ((year=$startYear; year<=$endYear; year++))
do
  for ((month=2; month<=10; month++))
  do
    
    # 构造每个月的开始和结束日期
    start=$(date -d "${year}-${month}-01" +%Y%m%d)  
    end=$(date -d "${year}-${month}-01 next month -1 day" +%Y%m%d)
    
    # echo $start
    # echo $end
    # 调用脚本1,传递月初和月末日期
    bsub -n 1 -W 48:00 -q research -J wwjiERA5 -o %J_ERA5_extract.out -e %J_ERA5_extract.err \
    /bin/bash /global/home/aimodels/data/ERA5/Data_Analysis_ERA5/process_ERA5_grib_to_netcdf_monthly.sh $start $end \
    > /global/home/aimodels/data/ERA5/Data_Analysis_ERA5/log_01_extract_from_ERA5_grib_${start}_to_${end}
    
  done
done
