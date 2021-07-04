from requests_html import HTMLSession
import json
import pandas as pd
import os
import time
import random

def write_to_csv(data):
    df = pd.DataFrame(data)
    folder_name = '-'
    folder_name_full = 'data' + os.sep + folder_name + os.sep
    if os.path.exists(folder_name_full) is False:
        os.makedirs(folder_name_full)
    file_name = 'poi-gaode-new_add1' + ".csv"
    file_path = folder_name_full + file_name
    df.to_csv(file_path, index=False, encoding='utf_8_sig')
    print(file_path)
    print('写入成功')
    return folder_name_full, file_name

# 高德接口
data = pd.read_excel('D:/data/挖掘运输热点数据/6.29新的分类数据/人工标注整合0702.xlsx')
keys = [...]


ans = {}
radius_list = [500]# [100, 200, 500]

tmp = 459 #+51
for idx, center_point in enumerate(data.itertuples()):
    for num in radius_list:
        # 分类 火锅店 上海菜 安徽菜(徽菜) 四川菜(川菜) 东北菜
        # PoiTypes = ['050117', '050107', '050109', '050102', '050113']
        # 测试PoiTypes = ['010100']
        PoiTypes = ['010100', '010300', '010400', '010500', '010600', '010700', '010800', '030000', \
        '050000', '060600', '060601', '060603', '070501', '071200', '100000', '170000', '180300']
        key_index = keys[0]

        if idx-tmp > 10:
            key_index = keys[1]
        if idx-tmp > 20:
            key_index = keys[2]
        if idx-tmp > 30:
            key_index = keys[3]
        if idx-tmp > 40:
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
            #     # "http": "218.88.205.69"
            #     # "http": "101.28.93.84"
            #     "http": "122.4.40.59"
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
    if idx-tmp >= 50:
        break
    # if idx == 15:
    #     break
ans = pd.DataFrame(ans)
data = pd.concat([data, ans], axis = 1)

codes = ['010100', '010300', '010400', '010500', '010600', '010700', '010800', '030000', \
         '050000', '060600', '060601', '060603', '070501', '071200', '100000', '170000', '180300']
names = ['汽车服务_加油站','汽车服务_加气站', '汽车服务_汽车养护/装饰', '汽车服务_洗车场', '汽车服务_汽车俱乐部' , '汽车服务_汽车救援', '汽车服务_汽车配件销售',\
        '汽车维修','餐饮服务','购物服务_家居建材市场_家居建材市场','购物服务_家居建材市场_家居建材综合市场','购物服务_家居建材市场_建材五金市场',\
         '生活服务_物流速递_物流仓储地','生活服务_维修站点_维修站点','住宿服务','公司企业','道路附属设施_服务区']
radius = [100, 200, 500]
columns = {}
for r in radius:
    for name, code in zip(names, codes):
        columns[code+'_'+str(r)] = name+'_'+str(r)
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
data["tag"] = data["type(运输终点/采购地点等)"].map(tag_map)


data.rename(columns=columns, inplace=True)
print(data.head())
write_to_csv(data)




# 百度接口
# center_points = open('中心点列表.txt', 'r', encoding='utf-8').readlines()
#
# ans = {}
# radius_list = [500, 1000, 1500, 2000, 2500]
# for num in radius_list:
#     for center_point in center_points[:2]:
#         # 一级分类
#         PoiClass = '酒店'
#         # 二级分类
#         PoiTypes = ['星级酒店', '快捷酒店', '公寓式酒店', '民宿', '其他']
#         for PoiType in PoiTypes:
#             token = "vuC40T7VAeKzQ1Njk4eooj4kKWzGpa5D"
#             params = {
#                 "query": PoiClass,
#                 "tag": PoiType,
#                 "location": center_point.split(),
#                 "radius": num,
#                 "output": "json",
#                 "page_size": 20,
#                 "page_num": 1,
#                 'scope': 2,
#                 "ak": token,
#             }
#             url = 'http://api.map.baidu.com/place/v2/search'
#             session = HTMLSession()
#             rq = session.get(url, params=params)
#             result = json.loads(rq.html.html)
#             time.sleep(random.randint(2, 4))
#             total_page = result['total']
#             if num == 500:
#                 ans[str(PoiType)] = []
#                 ans[str(PoiType)].append(total_page)
#             else:
#                 ans[str(PoiType)].append(total_page)
#
#