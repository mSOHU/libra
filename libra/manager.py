# coding: utf-8

import logging
import random
import time

logger = logging.getLogger(__name__)

class AveragePointException(Exception):
    pass


class AveragePointVisit(object):
    """ 平均权重光临花名册管理
    功能：
        1. 按照规定的权重，返回相应的名单
        2. 记录元素状态
        3. 判断元素状态，返回次数+处理间隔
        4. 合理的分配和剔
        5. 问题节点的剔除和恢复
    """

    def __init__(self, points, vl=100, el=1, wn=4, bt=2, ct=None, cc=None, rn=1):
        """ 初始化必要的数据

        管理所有的节点point，一个管理类的节点一次性创建，后期不提供删减point，
        活着的point记录于_live中，失败的节点记录于_fail中；

        每一个point的访问状态记录于_point中
        在_point中，每一个节点，记录vl个访问时间vt，记录el个访问返回时间et，并依次判断是否节点有效，
        每次访问最大时间bt(s)，作为此次访问是否失败标准,wn是节点失败最大个数；

        返回point时会依据权重依次返回不同的point
        对于失败的point可以根据时间ct(s)，或者次数cc来check是否恢复 ,ct优先级高于cc

        Args：
            points： 权重表，字典类型
            vl： 访问深度
            el： 返回深度
            bt： 最大间隔时间
            ct： 重试时间
            cc： 重试次数
            rn： 节点安全恢复深度
        """
        self._point = {}  # 权重字典
        self._points_visit = {}
        self._points_back = {}
        self._live = set()
        self._live_points = []
        self._live_num = []
        self._fail = set()
        self._step = 0
        self._MAX_STEP = 1000000000

        self._vl = vl
        self._el = el
        self._bt = bt
        self._ct = ct
        self._cc = cc
        self._wn = wn
        self._rn = rn if rn < el else el  # 所有检测是以返回时间为基准的，所以恢复时，检测的次数不能大于

        self._recovery_by_time = bool(self._ct)

        livenum = 0
        for k, v in points.iteritems():
            self._point[k] = v
            self._points_visit[k] = []
            self._points_back[k] = []
            self._live.add(k)
            livenum += v
            self._live_points.append(k)
            self._live_num.append(livenum)
        self._live_len = self._live_num[-1]
        if self._live_len > self._MAX_STEP:
            raise AveragePointException("权重总数需要小于 %d" % self._MAX_STEP)

    def get_points(self, only_live=False):
        """返回可以使用的point

        按照权重返回可以使用的point

        Args:
            only_live: 为True时，仅返回正常的point，否则按恢复条件，可以返回失败的point供恢复使用

        Return：
            tuple类型，(point, live_status)
            live_status : 0 OK, 1 NO
        """

        self._step = (self._step + 1) % self._MAX_STEP
        print ''
        print "Step:%s" % self._step

        step = self._step % self._live_len
        temp = [i for i in self._live_num]
        temp.append(step)
        temp.sort()
        live_index = temp.index(step)
        point = self._live_points[live_index]

        self.check_point(point)

        if only_live:
            return point, 0

        if self.is_check_fail():
            fail_point = self.get_fail_points()
            if fail_point:
                print"尝试恢复%s" % fail_point.name
                if self.is_point_fail(fail_point):
                    self.recover_fail(fail_point)
                    return fail_point, 0
                else:
                    return fail_point, 1
        return point, 0

    def is_check_fail(self):
        """判断是否检测fail的point

        检测失败节点的规则可以是时间，也可以是次数，
        默认如果时间存在，优先时间判断
        如果时间和次数都不存在，则始终返回False

        """
        if self._ct:
            now = int(time.time())
            return bool(now % (self._ct + 1) == 0)
        elif self._cc:
            return bool(self._step % self._cc == 0)
        else:
            return False

    def get_fail_points(self):
        """返回失败的节点
        """
        if not self._fail:
            return None
        fail_points = list(self._fail)
        i = self._step % len(fail_points)
        return fail_points[i]


    def check_point(self, point):
        """ 检测point是否失败

        检测point是否失败，如果失败，则fail这个point
        """
        if self.is_point_fail(point):
            self.fail_point(point)

    def is_point_fail(self, point, num=1):
        """ 判断节点是否失败
        每一个节点可以消费最多bt的时间，否则为失败
        以返回时间作为判断标准，

        Args：
            num: 测试返回时间的个数
        """
        _num = num if 0 < num < self._el else self._el

        for index in range(_num):
            i = 1 + index
            if self.reback_point_fail(point, i):
                return True
        return False

    def reback_point_fail(self, point, index=1):
        """ 检测节点是否失败

        根据指定位置的返回时间，检测节点是否失败
        return 失败为True， 没失败为False
        """
        point_visits = self._points_visit[point]
        if not point_visits:
            return False
        end_point_visit = point_visits[-1]
        begin_point_visit = point_visits[0]

        point_back = self._points_back[point][-index] if self._points_back[point] else int(time.time())

        if point_back > end_point_visit:
            return (point_back-self._bt) >= end_point_visit

        if point_back < begin_point_visit:
            return (begin_point_visit + self._bt) >= end_point_visit

        best_last_visit_time = None
        for vt in point_visits[::-1]:
            if vt <= point_back:
                best_last_visit_time = vt  # 第一个小于point_back的时间假设为找到最可能的visit_time
                break
        return (best_last_visit_time + self._bt) < end_point_visit

    def fail_point(self, point):
        print "剔除point%s" % point.name
        print self._points_visit[point]
        print self._points_back[point]

        self._fail.add(point)
        self._live.remove(point)
        self._live_points.remove(point)
        self._live_num = []
        temp = 0
        for p in self._live_points:
            temp = temp + self._point[p]
            self._live_num.append(temp)

        if not self._live_num:
            raise AveragePointException("所有的节点均不可用")
        self._live_len = self._live_num[-1]
        if self._live_len > self._MAX_STEP:
            raise AveragePointException("权重总数需要小于 %d" % self._MAX_STEP)

    def recover_fail(self, point):
        """恢复point
        """
        print "恢复：%s" % point.name
        self._fail.remove(point)
        self._live.add(point)
        self._live_points.append(point)
        self._live_num = []
        temp = 0
        for p in self._live_points:
            temp = temp + self._point[p]
            self._live_num.append(temp)
        self._live_len = self._live_num[-1]
        if self._live_len > self._MAX_STEP:
            raise AveragePointException("权重总数需要小于 %d" % self._MAX_STEP)

    def acquire(self, point):
        """标记使用point
        记录point的使用时间
        """
        self._points_visit[point].append(int(time.time()))
        if len(self._points_visit[point]) > self._vl:
            del self._points_visit[point][0]

    def release(self, point):
        """标记返回point

        记录point的返回时间
        """
        self._points_back[point].append(int(time.time()))
        if len(self._points_back[point]) > self._el:
            del self._points_back[point][0]


class WeightNodes(object):
    """按权重返回node
    """

    def __init__(self, weight_table, recovery_num=1000):
        """
        Args:
            weight_table: 字典类型，权重对应表
        """
        self._step = 0
        self._MAX_STEP = 1000000000
        self._weight_node = {}
        self._live_nodes = []
        self._live = set()
        self._fail = set()
        self._recovery_num = recovery_num

        self._node_counter = {}

        for k, v in weight_table.iteritems():
            self._weight_node[k] = v
            self._live.add(k)
            self._live_nodes += [k] * v
            self._node_counter[k] = {
                'get': 0,
                'release': 0,
                'dead': 0,
            }

        self._live_len = len(self._live_nodes)
        random.shuffle(self._live_nodes)

    def get_node(self):
        self._step = (self._step + 1) % self._MAX_STEP

        # 重试机制
        if self._fail and self._step % self._recovery_num == 0:
            return random.choice(list(self._fail))

        if not self._live_nodes:
            return random.choice(list(self._fail))  # 如果self._fail为空会失败哦，应该不会失败
        step = self._step % self._live_len
        node = self._live_nodes[step]
        self._node_counter[node]['get'] += 1
        return node

    def release_node(self, node):
        if node in self._fail:
            self._fail.remove(node)
            self._live.add(node)
            v = self._weight_node[node]
            self._live_nodes += [node] * v
            self._live_len = len(self._live_nodes)
            random.shuffle(self._live_nodes)
            logger.info('恢复node:%s' % node)
        self._node_counter[node]['release'] += 1

    def dead_node(self, node):
        self._fail.add(node)
        self._live.remove(node)
        self._live_nodes = filter(lambda x: x != node, self._live_nodes)
        self._live_len = len(self._live_nodes)
        random.shuffle(self._live_nodes)
        self._node_counter[node]['dead'] += 1
        logger.info('剔除node:%s' % node)

        if not self._live_nodes:
            logger.warning("所有节点都失败了，请立刻检测")

    def get_node_counter(self):
        return self._node_counter







