import json

import numpy as np
from matplotlib import pyplot as plt
from matplotlib.ticker import MultipleLocator

distrib = json.load(open('../distribution-kernel-1000.json', 'r'))
data = np.array(distrib)
data = data[data[:, 0].argsort()]
# fig, axs = plt.subplots(2, 1)

# uncomment to limit
# data = data[data[:, 0] <= 40000]

fig, ax = plt.subplots()
x, y = data.T
plt.bar(x, y, 1000)
plt.xticks(np.arange(0, max(x), 7500))
ax.xaxis.set_minor_locator(MultipleLocator(1000))
# plt.grid(True)

# data1, data2 = data[data[:, 0] < 32000], data[data[:, 0] >= 32000]
# x1, y1 = data1.T
# x2, y2 = data2.T
# axs[0].bar(x1, y1, 500)
# axs[1].bar(x2, y2, 500)
# # plt.yticks(np.arange(0, 6000, 400))
# # axs[0].xaxis.ticks(np.arange(0, 32000, 7000))
# # axs[1].xticks(np.arange(32000, 64000, 7000))
# axs[0].xaxis.set_minor_locator(MultipleLocator(1000))
# axs[1].xaxis.set_minor_locator(MultipleLocator(1000))


# axs[0].grid(True)
# axs[1].grid(True)
plt.show()
