# -*- coding:utf-8 -*-

from requests_html import HTMLSession
import json
import pandas as pd
import os
import math
import utils

# 存放文件
def write_to_csv(data):
    df = pd.DataFrame(data)
    folder_name = '-'
    folder_name_full = 'data' + os.sep + folder_name + os.sep
    if os.path.exists(folder_name_full) is False:
        os.makedirs(folder_name_full)
    file_name = 'poi-gaode-new_add3' + ".csv"
    file_path = folder_name_full + file_name
    df.to_csv(file_path, index=False, encoding='utf_8_sig')
    print(file_path)
    print('写入成功')
    return folder_name_full, file_name


# 高德接口
keys = []

# 读取文件并将84坐标改为高德坐标
data = pd.read_csv('D:\\BaiduNetdiskWorkspace\\挖掘运输热点数据\\wwy (11).csv')
coor_left = data.apply(utils.wgs84togaode_arr, axis=1, args=('left_up_longitude', 'left_up_latitude'))
coor_right = data.apply(utils.wgs84togaode_arr, axis=1, args=('right_down_longitude', 'right_down_latitude'))
coor_left = pd.DataFrame(coor_left)
coor_right = pd.DataFrame(coor_right)
data['gaode_left_up_longitude'] = coor_left[0].str[0]
data['gaode_left_up_latitude'] = coor_left[0].str[1]
data['gaode_right_down_longitude'] = coor_right[0].str[0]
data['gaode_right_down_latitude'] = coor_right[0].str[1]
data['cen_longitude'] = (coor_left[0].str[0]+coor_right[0].str[0])/2
data['cen_latitude'] = (coor_left[0].str[1]+coor_right[0].str[1])/2


ans = {}
radius_list = [500]# [100, 200, 500]
tmp = 0 #+51 设置每轮爬取的总次数
for idx, center_point in enumerate(data.itertuples()):
    for num in radius_list:
        # 分类 火锅店 上海菜 安徽菜(徽菜) 四川菜(川菜) 东北菜
        # PoiTypes = ['050117', '050107', '050109', '050102', '050113']
        # 测试PoiTypes = ['010100']
        PoiTypes = ['010100', '010300', '010400', '010500', '010600', '010700', '010800', '030000', \
        '050000', '060600', '060601', '060603', '070501', '071200', '100000', '170000', '180300']
        key_index = keys[0]

        if idx-tmp > 1:
            key_index = keys[1]
        if idx-tmp > 2:
            key_index = keys[2]
        if idx-tmp > 3:
            key_index = keys[3]
        if idx-tmp > 4:
            key_index = keys[4]

        for PoiType in PoiTypes:
            key = key_index
            params = {
                "key": key,
                "location": [str(round(getattr(center_point, 'cen_longitude'), 6))+','+\
                             str(round(getattr(center_point, 'cen_latitude'), 6))],
                "types": PoiType,
                "radius": num,
                "output": "json",
            }
            # 这里设置ans里存放列表
            if idx == 0:
                ans[str(PoiType) + '_' + str(num)] = ['']
                url = 'https://restapi.amap.com/v3/place/around'
                session = HTMLSession()
                rq = session.get(url, params=params)
                result = json.loads(rq.html.html)
                total_page = result['count']
                ans[str(PoiType) + '_' + str(num)]=[total_page]
                continue
            if idx < tmp:
                ans[str(PoiType) + '_' + str(num)].append('')
                continue

            url = 'https://restapi.amap.com/v3/place/around'
            session = HTMLSession()
            # 尝试修改ip
            # proxie = {
            #     # "http": "140.255.139.52"
            #     # "http": "101.28.93.84"
            #     # "http": "122.4.40.59"
            # }
            #
            # rq = session.get(url, params=params, proxies=proxie)
            rq = session.get(url, params=params)
            result = json.loads(rq.html.html)
            # 控制时间反爬虫
            # time.sleep(random.randint(3, 4))
            total_page = result['count']
            ans[str(PoiType) + '_' + str(num)].append(total_page)

    if idx >= tmp:
        print('第'+str(idx+1)+'个点爬取完成')
    # 每次爬取50个
    if idx-tmp >= 1:
        break
    # if idx == 15:
    #     break
ans = pd.DataFrame(ans)
data = pd.concat([data, ans], axis = 1)

PoiTypes = ['010100', '010300', '010400', '010500', '010600', '010700', '010800', '030000', \
         '050000', '060600', '060601', '060603', '070501', '071200', '100000', '170000', '180300']
names = ['汽车服务_加油站','汽车服务_加气站', '汽车服务_汽车养护/装饰', '汽车服务_洗车场', '汽车服务_汽车俱乐部' , '汽车服务_汽车救援', '汽车服务_汽车配件销售',\
        '汽车维修','餐饮服务','购物服务_家居建材市场_家居建材市场','购物服务_家居建材市场_家居建材综合市场','购物服务_家居建材市场_建材五金市场',\
         '生活服务_物流速递_物流仓储地','生活服务_维修站点_维修站点','住宿服务','公司企业','道路附属设施_服务区']
radius = [100, 200, 500]
columns = {}
for r in radius:
    for name, PoiType in zip(names, PoiTypes):
        columns[PoiType+'_'+str(r)] = name+'_'+str(r)
# 添加tag标签
def tag_map(x):
    tag = 0
    if x == "运输终点":
        tag = 0
    elif x == "采购地点":
        tag = 1
    elif x == "休息区":
        tag = 2
    elif x == "加油站":
        tag = 3
    elif x == "维修点":
        tag = 4
    else:
        pass
    return tag
# data["tag"] = data["type(运输终点/采购地点等)"].map(tag_map)


data.rename(columns=columns, inplace=True)
print(data.head())
write_to_csv(data)

