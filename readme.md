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
