import matplotlib.pyplot as plt
import numpy as np
from matplotlib.font_manager import FontProperties

# 指定自定义字体文件路径
custom_font = FontProperties(fname='/root/mnt/bus_corr/STSong.ttf')

# 路段的端点坐标
segment_coordinates = {
    '1': (30, 29),
    '2': (31, 31),
    '3': (25, 25),
    '5': (15, 40)
}

# 路段上的人数
segment_population = {
    '1_2': 30,
    '3_5': 30
}

# 计算每条路段的中心坐标和人数
segment_centers = {}
segment_populations = []
for key, value in segment_population.items():
    seg_start, seg_end = key.split('_')
    x1, y1 = segment_coordinates[seg_start]
    x2, y2 = segment_coordinates[seg_end]
    center_x = (x1 + x2) / 2
    center_y = (y1 + y2) / 2
    segment_centers[key] = (center_x, center_y)
    segment_populations.append(value)

# 提取坐标和人数
x_coords, y_coords = zip(*segment_centers.values())
populations = np.array(segment_populations)

# 绘制热力图
plt.figure(figsize=(10, 8))
plt.scatter(x_coords, y_coords, s=populations*10, c=populations, cmap='hot', alpha=0.6)
plt.colorbar(label='人数')
plt.xlabel('X 坐标')
plt.ylabel('Y 坐标')
plt.title('路段热力图')
plt.grid(True)
plt.tight_layout()
plt.show()