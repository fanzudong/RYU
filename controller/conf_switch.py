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

import logging

from ryu.controller import event
from ryu.lib.dpid import dpid_to_str
from ryu.base import app_manager

LOG = logging.getLogger(__name__)

#事件配置交换机删除数据路径标识的类
class EventConfSwitchDelDPID(event.EventBase):
    def __init__(self, dpid):
        super(EventConfSwitchDelDPID, self).__init__()
        self.dpid = dpid
#数据路径标识字符串
    def __str__(self):
        return 'EventConfSwitchDelDPID<%s>' % dpid_to_str(self.dpid)

#事件配置交换机的设置
class EventConfSwitchSet(event.EventBase):
    def __init__(self, dpid, key, value):
        super(EventConfSwitchSet, self).__init__()
        self.dpid = dpid
        self.key = key
        self.value = value
#字符串
    def __str__(self):
        return 'EventConfSwitchSet<%s, %s, %s>' % (
            dpid_to_str(self.dpid), self.key, self.value)

#事件配置交换机删除的类
class EventConfSwitchDel(event.EventBase):
    def __init__(self, dpid, key):
        super(EventConfSwitchDel, self).__init__()
        self.dpid = dpid
        self.key = key
#字符串
    def __str__(self):
        return 'EventConfSwitchDel<%s, %s>' % (dpid_to_str(self.dpid),
                                               self.key)

#配置交换机设置
class ConfSwitchSet(app_manager.RyuApp):
    def __init__(self):
        super(ConfSwitchSet, self).__init__()
        self.name = 'conf_switch'
        self.confs = {}
#数据路径标识
    def dpids(self):
        return list(self.confs.keys())
#删除数据路径标识
    def del_dpid(self, dpid):
        del self.confs[dpid]
        self.send_event_to_observers(EventConfSwitchDelDPID(dpid))
#数据路径的关键属性
    def keys(self, dpid):
        return list(self.confs[dpid].keys())
#设置关键属性
    def set_key(self, dpid, key, value):
        conf = self.confs.setdefault(dpid, {})
        conf[key] = value
        self.send_event_to_observers(EventConfSwitchSet(dpid, key, value))#事件分发给所有监听者
#获取关键属性
    def get_key(self, dpid, key):
        return self.confs[dpid][key]
#删除关键属性
    def del_key(self, dpid, key):
        del self.confs[dpid][key]
        self.send_event_to_observers(EventConfSwitchDel(dpid, key))

# 隧道更新程序的方法
    def __contains__(self, item):
        """(dpid, key) in <配置交换机设置实例>"""
        (dpid, key) = item
        return dpid in self.confs and key in self.confs[dpid]
#发现数据路径标识
    def find_dpid(self, key, value):
        for dpid, conf in self.confs.items():
            if key in conf:
                if conf[key] == value:
                    return dpid

        return None
