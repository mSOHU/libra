#coding: utf-8

""" 负载测试脚本

"""
import random
import time
from libra.manager import AveragePointVisit



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
        Server('A', 0.5): 60,
        Server('B', 0): 10,
        Server('C', 0.5): 10,
        Server('D', 0.2): 5,
        Server('E', 0.3): 15,
    }

    TableManger = AveragePointVisit(TableServer, bt=2, cc=10)

    for i in range(10000000):
        if i % 100 == 0:
            time.sleep(1)
        serv, status = TableManger.get_points()

        print "当前选择服务器："
        print serv.name, status,

        if serv.name == "B":
            pass
        # 模拟使用
        TableManger.acquire(serv)
        if serv.request():
            TableManger.release(serv)
            print 'True',
        else:
            print 'False'







if __name__ == "__main__":
    main()





