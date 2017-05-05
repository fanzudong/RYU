# Copyright (C) 2011 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2011 Isaku Yamahata <yamahata at valinux co jp>
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

from ryu.base import app_manager
import ryu.exception as ryu_exc
from ryu.controller import event
from ryu.exception import NetworkNotFound, NetworkAlreadyExist
from ryu.exception import PortAlreadyExist, PortNotFound, PortUnknown


NW_ID_UNKNOWN = '__NW_ID_UNKNOWN__'

#Mac地址已经存在类
class MacAddressAlreadyExist(ryu_exc.RyuException):
    message = 'port (%(dpid)s, %(port)s) has already mac %(mac_address)s'

#事件网络删除类
class EventNetworkDel(event.EventBase):
    """
    网络删除的事件类.

    当REST API删除网络时，会生成此事件。
    实例至少具有以下属性.

    ========== ===================================================================
    属性       描述
    ========== ===================================================================
    network_id Network ID
    ========== ===================================================================
    """
#初始化
    def __init__(self, network_id):
        super(EventNetworkDel, self).__init__()
        self.network_id = network_id

#事件网络端口类
class EventNetworkPort(event.EventBase):
    """
    通知事件类端口到达和离开

    当通过REST API引入或从网络中删除端口时，会生成此事件。
    
    实例至少具有以下属性.

    ========== ================================================================
    属性       描述
    ========== ================================================================
    network_id Network ID
    dpid       OpenFlow Datapath ID of the switch to which the port belongs.
    port_no    OpenFlow port number of the port
    add_del    True for 增加一个端口.  False for 移除一个端口.
    ========== ================================================================
    """
#初始化
    def __init__(self, network_id, dpid, port_no, add_del):
        super(EventNetworkPort, self).__init__()
        self.network_id = network_id
        self.dpid = dpid
        self.port_no = port_no
        self.add_del = add_del

#事件Mac地址类
class EventMacAddress(event.EventBase):
    """
    端点MAC地址注册的事件类。

    当REST API更新端点MAC地址时，会生成此事件。
    
    实例至少具有以下属性.

    =========== ===============================================================
    属性        描述
    =========== ===============================================================
    network_id  Network ID
    dpid        OpenFlow Datapath ID of the switch to which the port belongs.
    port_no     OpenFlow port number of the port
    mac_address The old MAC address of the port if add_del is False.  Otherwise
                the new MAC address.
    add_del     False 如果此事件是端口删除的结果.  
                Otherwise True.
    =========== ===============================================================
    """
#初始化
    def __init__(self, dpid, port_no, network_id, mac_address, add_del):
        super(EventMacAddress, self).__init__()
        assert network_id is not None
        assert mac_address is not None
        self.dpid = dpid
        self.port_no = port_no
        self.network_id = network_id
        self.mac_address = mac_address
        self.add_del = add_del

#网络类
class Networks(dict):
    "network_id -> set of (dpid, port_no)"
    def __init__(self, f):
        super(Networks, self).__init__()
        self.send_event = f
#网络列表
    def list_networks(self):
        return list(self.keys())
#已有网络
    def has_network(self, network_id):
        return network_id in self
#网络更新
    def update_network(self, network_id):
        self.setdefault(network_id, set())
#创建网络
    def create_network(self, network_id):
        if network_id in self:
            raise NetworkAlreadyExist(network_id=network_id)

        self[network_id] = set()
#移除网络
    def remove_network(self, network_id):
        try:
            ports = self[network_id]
        except KeyError:
            raise NetworkNotFound(network_id=network_id)

        while ports:
            (dpid, port_no) = ports.pop()
            self._remove_event(network_id, dpid, port_no)
        if self.pop(network_id, None) is not None:
            self.send_event(EventNetworkDel(network_id))
#端口列表
    def list_ports(self, network_id):
        try:
            # 使用list（）来保持输出的兼容性
            # set() isn't json serializable  set（）不是json可序列化的
            return list(self[network_id])
        except KeyError:
            raise NetworkNotFound(network_id=network_id)
#增加raw
    def add_raw(self, network_id, dpid, port_no):
        self[network_id].add((dpid, port_no))
#增加事件
    def add_event(self, network_id, dpid, port_no):
        self.send_event(
            EventNetworkPort(network_id, dpid, port_no, True))

    # def add(self, network_id, dpid, port_no):
    #     self.add_raw(network_id, dpid, port_no)
    #     self.add_event(network_id, dpid, port_no)
#移除事件
    def _remove_event(self, network_id, dpid, port_no):
        self.send_event(EventNetworkPort(network_id, dpid, port_no, False))
#移除raw
    def remove_raw(self, network_id, dpid, port_no):
        ports = self[network_id]
        if (dpid, port_no) in ports:
            ports.remove((dpid, port_no))
            self._remove_event(network_id, dpid, port_no)
#移除
    def remove(self, network_id, dpid, port_no):
        try:
            self.remove_raw(network_id, dpid, port_no)
        except KeyError:
            raise NetworkNotFound(network_id=network_id)
        except ValueError:
            raise PortNotFound(network_id=network_id, dpid=dpid, port=port_no)
#已有端口
    def has_port(self, network_id, dpid, port):
        return (dpid, port) in self[network_id]
#获取dpids
    def get_dpids(self, network_id):
        try:
            ports = self[network_id]
        except KeyError:
            return set()

        # python 2.6 doesn't support set comprehension
        # port = (dpid, port_no)
        return set([port[0] for port in ports])

#端口类
class Port(object):
    def __init__(self, port_no, network_id, mac_address=None):
        super(Port, self).__init__()
        self.port_no = port_no
        self.network_id = network_id
        self.mac_address = mac_address

#dpid类
class DPIDs(dict):
    """dpid -> port_no -> Port(port_no, network_id, mac_address)"""
    def __init__(self, f, nw_id_unknown):
        super(DPIDs, self).__init__()
        self.send_event = f
        self.nw_id_unknown = nw_id_unknown
#设置缺省dpid
    def setdefault_dpid(self, dpid):
        return self.setdefault(dpid, {})
#设置缺省网络
    def _setdefault_network(self, dpid, port_no, default_network_id):
        dp = self.setdefault_dpid(dpid)
        return dp.setdefault(port_no, Port(port_no=port_no,
                                           network_id=default_network_id))
#设置缺省网络
    def setdefault_network(self, dpid, port_no):
        self._setdefault_network(dpid, port_no, self.nw_id_unknown)
#端口更新
    def update_port(self, dpid, port_no, network_id):
        port = self._setdefault_network(dpid, port_no, network_id)
        port.network_id = network_id
#端口移除
    def remove_port(self, dpid, port_no):
        try:
            # self.dpids[dpid][port_no] 已被port_deleted()删除
            # 
            port = self[dpid].pop(port_no, None)
            if port and port.network_id and port.mac_address:
                self.send_event(EventMacAddress(dpid, port_no,
                                                port.network_id,
                                                port.mac_address,
                                                False))
        except KeyError:
            raise PortNotFound(dpid=dpid, port=port_no, network_id=None)
#获取端口
    def get_ports(self, dpid, network_id=None, mac_address=None):
        if network_id is None:
            return list(self.get(dpid, {}).values())
        if mac_address is None:
            return [p for p in self.get(dpid, {}).values()
                    if p.network_id == network_id]

        # 实时迁移：可以有两个端口具有相同的MAC地址.
        return [p for p in self.get(dpid, {}).values()
                if p.network_id == network_id and p.mac_address == mac_address]
#获取一个端口
    def get_port(self, dpid, port_no):
        try:
            return self[dpid][port_no]
        except KeyError:
            raise PortNotFound(dpid=dpid, port=port_no, network_id=None)
#获取一个网络
    def get_network(self, dpid, port_no):
        try:
            return self[dpid][port_no].network_id
        except KeyError:
            raise PortUnknown(dpid=dpid, port=port_no)
#获取多个网络
    def get_networks(self, dpid):
        return set(self[dpid].values())
#获取一个安全网络
    def get_network_safe(self, dpid, port_no):
        port = self.get(dpid, {}).get(port_no)
        if port is None:
            return self.nw_id_unknown
        return port.network_id
#获取Mac
    def get_mac(self, dpid, port_no):
        port = self.get_port(dpid, port_no)
        return port.mac_address
#设置Mac
    def _set_mac(self, network_id, dpid, port_no, port, mac_address):
        if not (port.network_id is None or
                port.network_id == network_id or
                port.network_id == self.nw_id_unknown):
            raise PortNotFound(network_id=network_id, dpid=dpid, port=port_no)
            
        port.network_id = network_id
        port.mac_address = mac_address
        if port.network_id and port.mac_address:
            self.send_event(EventMacAddress(
                            dpid, port_no, port.network_id, port.mac_address,
                            True))
#设置Mac
    def set_mac(self, network_id, dpid, port_no, mac_address):
        port = self.get_port(dpid, port_no)
        if port.mac_address is not None:
            raise MacAddressAlreadyExist(dpid=dpid, port=port_no,
                                         mac_address=mac_address)
        self._set_mac(network_id, dpid, port_no, port, mac_address)
#Mac更新
    def update_mac(self, network_id, dpid, port_no, mac_address):
        port = self.get_port(dpid, port_no)
        if port.mac_address is None:
            self._set_mac(network_id, dpid, port_no, port, mac_address)
            return

        # 现在，我们不允许改变mac地址。
        if port.mac_address != mac_address:
            raise MacAddressAlreadyExist(dpid=dpid, port=port_no,
                                         mac_address=port.mac_address)


MacPort = collections.namedtuple('MacPort', ('dpid', 'port_no'))

#Mac到端口类
class MacToPort(collections.defaultdict):
    """mac_address -> set of MacPort(dpid, port_no)"""
    def __init__(self):
        super(MacToPort, self).__init__(set)
#增加端口
    def add_port(self, dpid, port_no, mac_address):
        self[mac_address].add(MacPort(dpid, port_no))
#移除端口
    def remove_port(self, dpid, port_no, mac_address):
        ports = self[mac_address]
        ports.discard(MacPort(dpid, port_no))
        if not ports:
            del self[mac_address]
#获取多个端口
    def get_ports(self, mac_address):
        return self[mac_address]

#Mac地址类
class MacAddresses(dict):
    """network_id -> mac_address -> set of (dpid, port_no)"""
    def add_port(self, network_id, dpid, port_no, mac_address):
        mac2port = self.setdefault(network_id, MacToPort())
        mac2port.add_port(dpid, port_no, mac_address)
#端口移除
    def remove_port(self, network_id, dpid, port_no, mac_address):
        mac2port = self.get(network_id)
        if mac2port is None:
            return
        mac2port.remove_port(dpid, port_no, mac_address)
        if not mac2port:
            del self[network_id]
#获取端口
    def get_ports(self, network_id, mac_address):
        mac2port = self.get(network_id)
        if not mac2port:
            return set()
        return mac2port.get_ports(mac_address)

#网络类
class Network(app_manager.RyuApp):
    def __init__(self, nw_id_unknown=NW_ID_UNKNOWN):
        super(Network, self).__init__()
        self.name = 'network'
        self.nw_id_unknown = nw_id_unknown
        self.networks = Networks(self.send_event_to_observers)
        self.dpids = DPIDs(self.send_event_to_observers, nw_id_unknown)
        self.mac_addresses = MacAddresses()
#检测网络ID是否未知
    def _check_nw_id_unknown(self, network_id):
        if network_id == self.nw_id_unknown:
            raise NetworkAlreadyExist(network_id=network_id)
#网络列表
    def list_networks(self):
        return self.networks.list_networks()
#网络更新
    def update_network(self, network_id):
        self._check_nw_id_unknown(network_id)
        self.networks.update_network(network_id)
#创建网络
    def create_network(self, network_id):
        self._check_nw_id_unknown(network_id)
        self.networks.create_network(network_id)
#移除网络
    def remove_network(self, network_id):
        self.networks.remove_network(network_id)
#端口列表
    def list_ports(self, network_id):
        return self.networks.list_ports(network_id)
#端口号唤起列表
    def list_ports_noraise(self, network_id):
        try:
            return self.list_ports(network_id)
        except NetworkNotFound:
            return []
#端口更新
    def _update_port(self, network_id, dpid, port, port_may_exist):
        def _known_nw_id(nw_id):
            return nw_id is not None and nw_id != self.nw_id_unknown

        queue_add_event = False
        self._check_nw_id_unknown(network_id)
        try:
            old_network_id = self.dpids.get_network_safe(dpid, port)
            if (self.networks.has_port(network_id, dpid, port) or
                    _known_nw_id(old_network_id)):
                if not port_may_exist:
                    raise PortAlreadyExist(network_id=network_id,
                                           dpid=dpid, port=port)

            if old_network_id != network_id:
                queue_add_event = True
                self.networks.add_raw(network_id, dpid, port)
                if _known_nw_id(old_network_id):
                    self.networks.remove_raw(old_network_id, dpid, port)
        except KeyError:
            raise NetworkNotFound(network_id=network_id)

        self.dpids.update_port(dpid, port, network_id)
        if queue_add_event:
            self.networks.add_event(network_id, dpid, port)
#创建端口
    def create_port(self, network_id, dpid, port):
        self._update_port(network_id, dpid, port, False)
#更新端口
    def update_port(self, network_id, dpid, port):
        self._update_port(network_id, dpid, port, True)
#获取旧Mac
    def _get_old_mac(self, network_id, dpid, port_no):
        try:
            port = self.dpids.get_port(dpid, port_no)
        except PortNotFound:
            pass
        else:
            if port.network_id == network_id:
                return port.mac_address
        return None
#端口移除
    def remove_port(self, network_id, dpid, port_no):
        # 先产生事件, 然后执行实际任务
        old_mac_address = self._get_old_mac(network_id, dpid, port_no)

        self.dpids.remove_port(dpid, port_no)
        try:
            self.networks.remove(network_id, dpid, port_no)
        except NetworkNotFound:
            # 由于Openstack自动删除端口，网络删除后可以调用端口删除（dhcp / router port）
            pass
        if old_mac_address is not None:
            self.mac_addresses.remove_port(network_id, dpid, port_no,
                                           old_mac_address)

    #
    # gre隧道的方法
    #
#获取dpids
    def get_dpids(self, network_id):
        return self.networks.get_dpids(network_id)
#已有网络
    def has_network(self, network_id):
        return self.networks.has_network(network_id)
#获取多个网络
    def get_networks(self, dpid):
        return self.dpids.get_networks(dpid)
#创建Mac
    def create_mac(self, network_id, dpid, port_no, mac_address):
        self.mac_addresses.add_port(network_id, dpid, port_no, mac_address)
        self.dpids.set_mac(network_id, dpid, port_no, mac_address)
#更新Mac
    def update_mac(self, network_id, dpid, port_no, mac_address):
        old_mac_address = self._get_old_mac(network_id, dpid, port_no)

        self.dpids.update_mac(network_id, dpid, port_no, mac_address)
        if old_mac_address is not None:
            self.mac_addresses.remove_port(network_id, dpid, port_no,
                                           old_mac_address)
        self.mac_addresses.add_port(network_id, dpid, port_no, mac_address)
#获取Mac
    def get_mac(self, dpid, port_no):
        return self.dpids.get_mac(dpid, port_no)
#Mac列表
    def list_mac(self, dpid, port_no):
        mac_address = self.dpids.get_mac(dpid, port_no)
        if mac_address is None:
            return []
        return [mac_address]
#获取多个端口
    def get_ports(self, dpid, network_id=None, mac_address=None):
        return self.dpids.get_ports(dpid, network_id, mac_address)
#获取一个端口
    def get_port(self, dpid, port_no):
        return self.dpids.get_port(dpid, port_no)
#根据Mac获取多个端口
    def get_ports_with_mac(self, network_id, mac_address):
        return self.mac_addresses.get_ports(network_id, mac_address)

    #
    # 简单隔离的方法
    #
#相同网络
    def same_network(self, dpid, nw_id, out_port, allow_nw_id_external=None):
        assert nw_id != self.nw_id_unknown
        out_nw = self.dpids.get_network_safe(dpid, out_port)

        if nw_id == out_nw:
            return True

        if (allow_nw_id_external is not None and
                (allow_nw_id_external == nw_id or
                    allow_nw_id_external == out_nw)):
            # 允许外部网络 -> known 网络ID
            return True

        self.logger.debug('blocked dpid %s nw_id %s out_port %d out_nw %s'
                          'external %s',
                          dpid, nw_id, out_port, out_nw, allow_nw_id_external)
        return False
#获取网络
    def get_network(self, dpid, port):
        return self.dpids.get_network(dpid, port)
#增加datapath
    def add_datapath(self, ofp_switch_features):
        datapath = ofp_switch_features.datapath
        dpid = ofp_switch_features.datapath_id
        ports = ofp_switch_features.ports
        self.dpids.setdefault_dpid(dpid)
        for port_no in ports:
            self.port_added(datapath, port_no)
#已增加的端口
    def port_added(self, datapath, port_no):
        if port_no == 0 or port_no >= datapath.ofproto.OFPP_MAX:
            # 跳过假输出端口
            return

        self.dpids.setdefault_network(datapath.id, port_no)
#已删除端口
    def port_deleted(self, dpid, port_no):
        self.dpids.remove_port(dpid, port_no)
#端口过滤器
    def filter_ports(self, dpid, in_port, nw_id, allow_nw_id_external=None):
        assert nw_id != self.nw_id_unknown
        ret = []

        for port in self.get_ports(dpid):
            nw_id_ = port.network_id
            if port.port_no == in_port:
                continue

            if nw_id_ == nw_id:
                ret.append(port.port_no)
            elif (allow_nw_id_external is not None and
                  nw_id_ == allow_nw_id_external):
                ret.append(port.port_no)

        return ret
