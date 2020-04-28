import matplotlib.pyplot as plt
import numpy as np
from latency_lists import *

flask_get = np.array(flask_get) * 1000
flask_post = np.array(flask_post) * 1000
flask_delete = np.array(flask_delete) * 1000
cherry_get = np.array(cherry_get) * 1000
cherry_post = np.array(cherry_post) * 1000
cherry_delete = np.array(cherry_delete) * 1000

flask_get_avg = np.average(flask_get)
flask_post_avg = np.average(flask_post)
flask_delete_avg = np.average(flask_delete)
cherry_get_avg = np.average(cherry_get)
cherry_post_avg = np.average(cherry_post)
cherry_delete_avg = np.average(cherry_delete)

print(flask_get_avg)
print(flask_post_avg)
print(flask_delete_avg)
print(cherry_get_avg)
print(cherry_post_avg)
print(cherry_delete_avg)


labels = ['GET', 'POST', 'DELETE']

flask_means = [round(flask_get_avg, 3), round(flask_post_avg, 3), round(flask_delete_avg, 3)]
cherry_means = [round(cherry_get_avg, 3), round(cherry_post_avg, 3), round(cherry_delete_avg, 3)]

x = np.arange(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, flask_means, width, label='Flask')
rects2 = ax.bar(x + width/2, cherry_means, width, label='Cherry')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Latency [ms]')
ax.set_title('Latency by method and server')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()


def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


autolabel(rects1)
autolabel(rects2)

fig.tight_layout()

plt.show()
plt.savefig('multi_tutti.png')
