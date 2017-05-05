# Copyright (C) 2012 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2012 Isaku Yamahata <yamahata at private email ne jp>
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

import collections
import logging

import ryu.exception as ryu_exc
from ryu.base import app_manager
from ryu.controller import event

#移除dpid已经存在类
class RemoteDPIDAlreadyExist(ryu_exc.RyuException):
    message = ('port (%(dpid)s, %(port)s) has already '
               'remote dpid %(remote_dpid)s')

#隧道密钥已经存在类
class TunnelKeyAlreadyExist(ryu_exc.RyuException):
    message = 'tunnel key %(tunnel_key)s already exists'

#隧道密钥未找到类
class TunnelKeyNotFound(ryu_exc.RyuException):
    message = 'no tunnel key for network %(network_id)s'

#事件隧道密钥基类
class EventTunnelKeyBase(event.EventBase):
    def __init__(self, network_id, tunnel_key):
        super(EventTunnelKeyBase, self).__init__()
        self.network_id = network_id
        self.tunnel_key = tunnel_key

#事件隧道密钥增加
class EventTunnelKeyAdd(EventTunnelKeyBase):
    """
    隧道密钥注册的事件类。

    当通过REST API注册或更新隧道密钥时，会生成此事件。
    
    实例至少具有以下属性。

    =========== ===============================================================
    属性        描述
    =========== ===============================================================
    network_id  Network ID
    tunnel_key  Tunnel Key
    =========== ===============================================================
    """

    def __init__(self, network_id, tunnel_key):
        super(EventTunnelKeyAdd, self).__init__(network_id, tunnel_key)

#隧道密钥删除事件类
class EventTunnelKeyDel(EventTunnelKeyBase):
    """
    隧道密钥注册的事件类。

    当REST API删除隧道密钥时，会生成此事件。
    实例至少具有以下属性。

    =========== ===============================================================
    属性        描述
    =========== ===============================================================
    network_id  Network ID
    tunnel_key  Tunnel Key
    =========== ===============================================================
    """

    def __init__(self, network_id, tunnel_key):
        super(EventTunnelKeyDel, self).__init__(network_id, tunnel_key)

#隧道端口事件类
class EventTunnelPort(event.EventBase):
    """
    隧道港口注册事件类。

    当通过REST API添加或删除隧道端口时，会生成此事件。
     
    实例至少具有以下属性。

    =========== ===============================================================
    属性        描述
    =========== ===============================================================
    dpid        OpenFlow Datapath ID
    port_no     OpenFlow 端口号
    remote_dpid OpenFlow 隧道对端的端口号
    add_del     True for adding a tunnel.  False for removal.
    =========== ===============================================================
    """
    def __init__(self, dpid, port_no, remote_dpid, add_del):
        super(EventTunnelPort, self).__init__()
        self.dpid = dpid
        self.port_no = port_no
        self.remote_dpid = remote_dpid
        self.add_del = add_del

#隧道密钥类
class TunnelKeys(dict):
    """network id(uuid) <-> tunnel key(32bit unsigned int)"""
    def __init__(self, f):
        super(TunnelKeys, self).__init__()
        self.send_event = f
#获取密钥
    def get_key(self, network_id):
        try:
            return self[network_id]
        except KeyError:
            raise TunnelKeyNotFound(network_id=network_id)
#设置密钥
    def _set_key(self, network_id, tunnel_key):
        self[network_id] = tunnel_key
        self.send_event(EventTunnelKeyAdd(network_id, tunnel_key))
#密钥注册
    def register_key(self, network_id, tunnel_key):
        if network_id in self:
            raise ryu_exc.NetworkAlreadyExist(network_id=network_id)
        if tunnel_key in self.values():
            raise TunnelKeyAlreadyExist(tunnel_key=tunnel_key)
        self._set_key(network_id, tunnel_key)
#密钥更新
    def update_key(self, network_id, tunnel_key):
        if network_id not in self and tunnel_key in self.values():
            raise TunnelKeyAlreadyExist(key=tunnel_key)

        key = self.get(network_id)
        if key is None:
            self._set_key(network_id, tunnel_key)
            return
        if key != tunnel_key:
            raise ryu_exc.NetworkAlreadyExist(network_id=network_id)
#密钥删除
    def delete_key(self, network_id):
        try:
            tunnel_key = self[network_id]
            self.send_event(EventTunnelKeyDel(network_id, tunnel_key))
            del self[network_id]
        except KeyError:
            raise ryu_exc.NetworkNotFound(network_id=network_id)

#dpids类
class DPIDs(object):
    """dpid -> port_no -> remote_dpid"""
    def __init__(self, f):
        super(DPIDs, self).__init__()
        self.dpids = collections.defaultdict(dict)
        self.send_event = f
#端口列表
    def list_ports(self, dpid):
        return self.dpids[dpid]
#增加移除dpid
    def _add_remote_dpid(self, dpid, port_no, remote_dpid):
        self.dpids[dpid][port_no] = remote_dpid
        self.send_event(EventTunnelPort(dpid, port_no, remote_dpid, True))
#增加移除dpid
    def add_remote_dpid(self, dpid, port_no, remote_dpid):
        if port_no in self.dpids[dpid]:
            raise ryu_exc.PortAlreadyExist(dpid=dpid, port=port_no,
                                           network_id=None)
        self._add_remote_dpid(dpid, port_no, remote_dpid)
#更新移除dpid
    def update_remote_dpid(self, dpid, port_no, remote_dpid):
        remote_dpid_ = self.dpids[dpid].get(port_no)
        if remote_dpid_ is None:
            self._add_remote_dpid(dpid, port_no, remote_dpid)
        elif remote_dpid_ != remote_dpid:
            raise ryu_exc.RemoteDPIDAlreadyExist(dpid=dpid, port=port_no,
                                                 remote_dpid=remote_dpid)
#获取移除dpid
    def get_remote_dpid(self, dpid, port_no):
        try:
            return self.dpids[dpid][port_no]
        except KeyError:
            raise ryu_exc.PortNotFound(dpid=dpid, port=port_no)
#端口删除
    def delete_port(self, dpid, port_no):
        try:
            remote_dpid = self.dpids[dpid][port_no]
            self.send_event(EventTunnelPort(dpid, port_no, remote_dpid, False))
            del self.dpids[dpid][port_no]
        except KeyError:
            raise ryu_exc.PortNotFound(dpid=dpid, port=port_no)
#端口获取
    def get_port(self, dpid, remote_dpid):
        try:
            dp = self.dpids[dpid]
        except KeyError:
            raise ryu_exc.PortNotFound(dpid=dpid, port=None, network_id=None)

        res = [port_no for (port_no, remote_dpid_) in dp.items()
               if remote_dpid_ == remote_dpid]
        assert len(res) <= 1
        if len(res) == 0:
            raise ryu_exc.PortNotFound(dpid=dpid, port=None, network_id=None)
        return res[0]

#隧道类
class Tunnels(app_manager.RyuApp):
    def __init__(self):
        super(Tunnels, self).__init__()
        self.name = 'tunnels'
        self.tunnel_keys = TunnelKeys(self.send_event_to_observers)
        self.dpids = DPIDs(self.send_event_to_observers)
#密钥获取
    def get_key(self, network_id):
        return self.tunnel_keys.get_key(network_id)
#密钥注册
    def register_key(self, network_id, tunnel_key):
        self.tunnel_keys.register_key(network_id, tunnel_key)
#密钥更新
    def update_key(self, network_id, tunnel_key):
        self.tunnel_keys.update_key(network_id, tunnel_key)
#密钥删除
    def delete_key(self, network_id):
        self.tunnel_keys.delete_key(network_id)
#端口列表
    def list_ports(self, dpid):
        return self.dpids.list_ports(dpid).keys()
#端口注册
    def register_port(self, dpid, port_no, remote_dpid):
        self.dpids.add_remote_dpid(dpid, port_no, remote_dpid)
#端口更新
    def update_port(self, dpid, port_no, remote_dpid):
        self.dpids.update_remote_dpid(dpid, port_no, remote_dpid)
#获取移除dpid
    def get_remote_dpid(self, dpid, port_no):
        return self.dpids.get_remote_dpid(dpid, port_no)
#端口删除
    def delete_port(self, dpid, port_no):
        self.dpids.delete_port(dpid, port_no)

    #
    # gre隧道的方法
    #
    def get_port(self, dpid, remote_dpid):
        return self.dpids.get_port(dpid, remote_dpid)
