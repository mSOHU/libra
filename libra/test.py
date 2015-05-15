#coding: utf-8

""" 负载测试脚本

"""
import random
import time
from libra.manager import AveragePointVisit, WeightNodes


class Server():

    def __init__(self, name, p=0.05):
        """
        Args:
            p：服务器失败的概率

        """
        self._point = name
        self._probability = p
        self._LAST_FAIL_NUM = 15
        self._fail_num = self._LAST_FAIL_NUM  # 一旦失败，就持续失败次数
        self._is_fail = False

    def request(self):
        # time.sleep(0.1)

        if self._is_fail:
            print self._point, 'fail'
            self._fail_num -= 1
            if self._fail_num > 0:
                return False
            else:
                self._is_fail = False
                self._fail_num = self._LAST_FAIL_NUM
                return True

        if random.random() < self._probability:
            self._is_fail = True
            return False
        return True

    @property
    def name(self):
        return self._point


def main():
    TableServer = {
        Server('A', 0.5): 20,
        Server('B', 0): 20,
        Server('C', 0.5): 20,
        Server('D', 0.2): 20,
        Server('E', 0.3): 20,
    }

    TableManger = WeightNodes(TableServer)

    node = TableManger.get_node()
    TableManger.release_node(node, time_cost=1)
    TableManger.dead_node(node,time_cost=2)
    pass







if __name__ == "__main__":
    main()





