import pandas as pd

pd= pd.read_csv("region_template/qk_cn.csv",encoding="utf-8",low_memory=False)

place = "132100103322203"

region_N=[]
quadkey_N=[]
for j in range(len(place)+1):
    for i in range(len(pd["quadkey"])):
        if place[0:j] == str(pd.quadkey[i]):
            region_N.append(pd.region[i])
            quadkey_N.append(pd.quadkey[i])
print(region_N)
print(quadkey_N)
#['中国区', '北京市', '北京西游', '北京后海簋街', '北京市西城区', 'T1-北京五星级酒店']
#[13210, 132100103, 132100103322, 13210010332220, 13210010332220, 132100103322203]