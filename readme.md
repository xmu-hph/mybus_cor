# 重要文件
`station_distance.ipynb`:从站点坐标文件中统计各个站点之间的距离，得到的文件有：
1. 站点名字顺序列表:`station_sequence_ndarray.txt`
2. 站点的数量
3. 每个站点到其他所有站点的距离表:`station_pair_distance.csv`
4. 每个站点到其他站点按照距离进行排序后的表:`station_pair_distance_rank_index.csv`
5. 站点名字到`id`的映射表:`station_2_id.json`
6. `id`到站点名字的映射表:`id_2_station.json`

问题：这里面的站点名字，跟别的文件中的站点名字可能有细微的不同。
`od_pairs.ipynb`:从用户刷卡数据中获取可用的od对。

# 进度
1.公交站之间的距离处理完成，可以根据下一趟车的上车位置来推断这趟车的下车位置。（默认选择当前线路距离下一旅程上车点最近的下车站）
2.对于没有后续旅程的，下车点设置为距离第一趟旅程最近的站点为下车点。

# 计划
1. 识别每个用户的od对，进而统计出各个路段承载的客运量


# 预期成果


# 命令

```sh
git init
git remote add origin git@github.com:xmu-hph/mybus_cor.git
git config --global user.email "huph210950@gmail.com"
git config --global user.name "huph210950"
git branch -M main
git add .
git commit -m "initial"
git push -u origin main
git push --set-upstream origin master
```
