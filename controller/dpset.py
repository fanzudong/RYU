# Copyright (C) 2012, 2013 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2012 Isaku Yamahata <yamahata at valinux co jp>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Manage switches.

计划由ryu/topology替代
"""

import logging
import warnings

from ryu.base import app_manager
from ryu.controller import event
from ryu.controller import handler
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls
import ryu.exception as ryu_exc

from ryu.lib.dpid import dpid_to_str

LOG = logging.getLogger('ryu.controller.dpset')

DPSET_EV_DISPATCHER = "dpset"

#事件datapath基类
class EventDPBase(event.EventBase):
    def __init__(self, dp):
        super(EventDPBase, self).__init__()
        self.dp = dp

#事件datapath类
class EventDP(EventDPBase):
    """
    用于通知交换机的连接/断开的事件类。

    对于OpenFlow交换机，可以通过观察获得相同的通知
    ryu.controller.ofp_event.EventOFPStateChange。
    实例至少具有以下属性。

    ========= =================================================================
    属性      描述
    ========= =================================================================
    dp        一个 ryu.controller.controller.Datapath instance of the switch
    enter     当交换机连接到我们的控制器时为真。 
              假的时候断开。
    ports     端口实例化列表.
    ========= =================================================================
    """
#初始化
    def __init__(self, dp, enter_leave):
        # 进入_离开
        # True: dp 进入
        # False: dp 离开
        super(EventDP, self).__init__(dp)
        self.enter = enter_leave
        self.ports = []  # 何时进入和离开的端口列表

#事件datapath重连接
class EventDPReconnected(EventDPBase):
    def __init__(self, dp):
        super(EventDPReconnected, self).__init__(dp)
        # port list, which should not change across reconnects
        self.ports = []

#事件端口基类
class EventPortBase(EventDPBase):
    def __init__(self, dp, port):
        super(EventPortBase, self).__init__(dp)
        self.port = port

#事件新增端口
class EventPortAdd(EventPortBase):
    """
    交换机端口状态“ADD”通知的事件类.

    当新端口添加到交换机时，会生成此事件.
    对于OpenFlow交换机，可以获得相同的通知，通过观察
    ryu.controller.ofp_event.EventOFPPortStatus.
    实例至少具有以下属性.

    ========= =================================================================
    属性      描述
    ========= =================================================================
    dp        A ryu.controller.controller.Datapath instance of the switch
    port      端口数量
    ========= =================================================================
    """
#初始化
    def __init__(self, dp, port):
        super(EventPortAdd, self).__init__(dp, port)

#事件端口删除类
class EventPortDelete(EventPortBase):
    """
    用于切换端口状态“DELETE”通知的事件类.

    当端口从交换机中删除时，会生成此事件。
    对于OpenFlow交换机，可以获得相同的通知通过观察
    ryu.controller.ofp_event.EventOFPPortStatus.
    实例至少具有以下属性.

    ========= =================================================================
    属性      描述
    ========= =================================================================
    dp        A ryu.controller.controller.Datapath instance of the switch
    port      端口数量
    ========= =================================================================
    """

    def __init__(self, dp, port):
        super(EventPortDelete, self).__init__(dp, port)

#事件端口修改类
class EventPortModify(EventPortBase):
    """
    切换端口状态“MODIFY”通知的事件类.

    当端口的某些属性更改时，会生成此事件.
    对于OpenFlow交换机，可以获得相同的通知通过观察
    ryu.controller.ofp_event.EventOFPPortStatus.
    实例至少具有以下属性.

    ========= ====================================================================
    属性      描述
    ========= ====================================================================
    dp        A ryu.controller.controller.Datapath instance of the switch
    port      port number
    ========= ====================================================================
    """

    def __init__(self, dp, new_port):
        super(EventPortModify, self).__init__(dp, new_port)

#端口状态类
class PortState(dict):
    def __init__(self):
        super(PortState, self).__init__()
#增加
    def add(self, port_no, port):
        self[port_no] = port
#移除
    def remove(self, port_no):
        del self[port_no]
#修改
    def modify(self, port_no, port):
        self[port_no] = port


# 这取决于controller :: Datapath和调度程序在处理程序中
class DPSet(app_manager.RyuApp):
    """
    DPSet应用程序管理一组交换机（datapaths）
    连接到这个控制器。
    """
#初始化
    def __init__(self, *args, **kwargs):
        super(DPSet, self).__init__()
        self.name = 'dpset'

        self.dps = {}   # datapath_id => class Datapath
        self.port_state = {}  # datapath_id => ports
#注册者
    def _register(self, dp):
        LOG.debug('DPSET: register datapath %s', dp)
        assert dp.id is not None

        # 不同于dpid是唯一的, 我们需要在这里重复处理
        # 因为在我们注意到上一次连接的丢弃之前，
        # 交换机完全可以重新连接。
        # 在这种情况下,
        # - 忘记旧的连接，因为它很可能会消失
        # - 不要发送EventDP leave/enter events
        # -保持dpid的端口状态
        send_dp_reconnected = False
        if dp.id in self.dps:
            self.logger.warning('DPSET: Multiple connections from %s',
                                dpid_to_str(dp.id))
            self.logger.debug('DPSET: Forgetting datapath %s', self.dps[dp.id])
            (self.dps[dp.id]).close()
            self.logger.debug('DPSET: New datapath %s', dp)
            send_dp_reconnected = True
        self.dps[dp.id] = dp
        if dp.id not in self.port_state:
            self.port_state[dp.id] = PortState()
            ev = EventDP(dp, True)
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                for port in dp.ports.values():
                    self._port_added(dp, port)
                    ev.ports.append(port)
            self.send_event_to_observers(ev)
        if send_dp_reconnected:
            ev = EventDPReconnected(dp)
            ev.ports = self.port_state.get(dp.id, {}).values()
            self.send_event_to_observers(ev)
#未注册者
    def _unregister(self, dp):
        # see the comment in _register().
        if dp not in self.dps.values():
            return
        LOG.debug('DPSET: unregister datapath %s', dp)
        assert self.dps[dp.id] == dp

        # 现在datapath已经dead，所以端口状态改变事件不会干扰我们.
        ev = EventDP(dp, False)
        for port in list(self.port_state.get(dp.id, {}).values()):
            self._port_deleted(dp, port)
            ev.ports.append(port)

        self.send_event_to_observers(ev)

        del self.dps[dp.id]
        del self.port_state[dp.id]
#获取
    def get(self, dp_id):
        """
        该方法返回 the ryu.controller.controller.Datapath
        instance for the given Datapath ID.
        """
        return self.dps.get(dp_id)

    def get_all(self):
        """
        此方法返回代表的元组列表
        连接到该控制器的交换机的实例。
        元组由数据路径标识和实例组成
        ryu.controller.controller.Datapath。
        返回值如下所示:

            [ (dpid_A, Datapath_A), (dpid_B, Datapath_B), ... ]
        """
        return list(self.dps.items())
#已增加的端口
    def _port_added(self, datapath, port):
        self.port_state[datapath.id].add(port.port_no, port)
#已删除的端口
    def _port_deleted(self, datapath, port):
        self.port_state[datapath.id].remove(port.port_no)
#调度者改变
    @set_ev_cls(ofp_event.EventOFPStateChange,
                [handler.MAIN_DISPATCHER, handler.DEAD_DISPATCHER])
    def dispatcher_change(self, ev):
        datapath = ev.datapath
        assert datapath is not None
        if ev.state == handler.MAIN_DISPATCHER:
            self._register(datapath)
        elif ev.state == handler.DEAD_DISPATCHER:
            self._unregister(datapath)
#交换机特征处理者
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, handler.CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        # ofp_handler.py does the following so we could remove...
        if datapath.ofproto.OFP_VERSION < 0x04:
            datapath.ports = msg.ports
#端口状态处理者
    @set_ev_cls(ofp_event.EventOFPPortStatus, handler.MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        msg = ev.msg
        reason = msg.reason
        datapath = msg.datapath
        port = msg.desc
        ofproto = datapath.ofproto

        if reason == ofproto.OFPPR_ADD:
            LOG.debug('DPSET: A port was added.' +
                      '(datapath id = %s, port number = %s)',
                      dpid_to_str(datapath.id), port.port_no)
            self._port_added(datapath, port)
            self.send_event_to_observers(EventPortAdd(datapath, port))
        elif reason == ofproto.OFPPR_DELETE:
            LOG.debug('DPSET: A port was deleted.' +
                      '(datapath id = %s, port number = %s)',
                      dpid_to_str(datapath.id), port.port_no)
            self._port_deleted(datapath, port)
            self.send_event_to_observers(EventPortDelete(datapath, port))
        else:
            assert reason == ofproto.OFPPR_MODIFY
            LOG.debug('DPSET: A port was modified.' +
                      '(datapath id = %s, port number = %s)',
                      dpid_to_str(datapath.id), port.port_no)
            self.port_state[datapath.id].modify(port.port_no, port)
            self.send_event_to_observers(EventPortModify(datapath, port))
#获取端口
    def get_port(self, dpid, port_no):
        """
        此方法返回ryu.controller.dpset.PortState
        给定的Datapath ID和端口号的实例。
        如果没有这样的数据路径连接到ryu_exc.PortNotFound
        该控制器或没有这样的端口存在。
        """
        try:
            return self.port_state[dpid][port_no]
        except KeyError:
            raise ryu_exc.PortNotFound(dpid=dpid, port=port_no,
                                       network_id=None)

    def get_ports(self, dpid):
        """
        此方法返回ryu.controller.dpset.PortState的列表
        给定Datapath ID的实例。
        如果没有这样的数据路径连接到此控制器，则引发KeyError。
        """
        return list(self.port_state[dpid].values())


handler.register_service('ryu.controller.dpset')
