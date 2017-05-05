# Copyright (C) 2012 Nippon Telegraph and Telephone Corporation.
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

"""
OpenFlow事件定义.
"""

import inspect

from ryu.controller import handler
from ryu import ofproto
from ryu import utils
from . import event

#事件OpenFlow消息基类
class EventOFPMsgBase(event.EventBase):
    """
    OpenFlow事件类的基类。

    OpenFlow事件类至少具有以下属性。

    .. 表格列:: |l|L|

    ============ ==============================================================
    属性         描述
    ============ ==============================================================
    msg          描述对应的OpenFlow消息的对象。
    msg.datapath 一个ryu.controller.controller.Datapath实例
                 它描述了一个OpenFlow交换机，我们从中接收到
                 此OpenFlow消息。
    ============ ==============================================================

    msg对象具有一些其他成员，其值从原始OpenFlow消息中提取.
    """
    def __init__(self, msg):
        super(EventOFPMsgBase, self).__init__()
        self.msg = msg


#
# 创建符合OFP消息的ofp_event类型
#
#创建一个OpenFlow协议的消息事件组
_OFP_MSG_EVENTS = {}

#ofp下消息名转化为事件名
def _ofp_msg_name_to_ev_name(msg_name):
    return 'Event' + msg_name

#ofp消息转化为事件
def ofp_msg_to_ev(msg):
    return ofp_msg_to_ev_cls(msg.__class__)(msg)

#ofp消息到事件类
def ofp_msg_to_ev_cls(msg_cls):
    name = _ofp_msg_name_to_ev_name(msg_cls.__name__)
    return _OFP_MSG_EVENTS[name]

#创建ofp消息事件类
def _create_ofp_msg_ev_class(msg_cls):
    name = _ofp_msg_name_to_ev_name(msg_cls.__name__)
    # 打印 'creating ofp_event %s' % name

    if name in _OFP_MSG_EVENTS:
        return

    cls = type(name, (EventOFPMsgBase,),
               dict(__init__=lambda self, msg:
                    super(self.__class__, self).__init__(msg)))
    globals()[name] = cls
    _OFP_MSG_EVENTS[name] = cls

#从模块创建ofp消息事件
def _create_ofp_msg_ev_from_module(ofp_parser):
    # 打印模块
    for _k, cls in inspect.getmembers(ofp_parser, inspect.isclass):
        if not hasattr(cls, 'cls_msg_type'):
            continue
        _create_ofp_msg_ev_class(cls)


for ofp_mods in ofproto.get_ofp_modules().values():
    ofp_parser = ofp_mods[1]
    # 打印 '模块正在加载 %s' % ofp_parser
    _create_ofp_msg_ev_from_module(ofp_parser)

#事件ofp状态转换
class EventOFPStateChange(event.EventBase):
    """
    用于协商阶段更改通知的事件类。

    更改协商阶段后，此类的实例将发送给观察者。
    实例至少具有以下属性。

    ========= =================================================================
    属性      描述
    ========= =================================================================
    datapath  ryu.controller.controller.Datapath instance of the switch
    ========= =================================================================
    """
    def __init__(self, dp):
        super(EventOFPStateChange, self).__init__()
        self.datapath = dp

#事件ofp端口状态转换
class EventOFPPortStateChange(event.EventBase):
    """
    一个事件类来通知端口状态Dtatapath实例的变化。

    这个事件执行像EventOFPPortStatus，但Ryu会
    在更新Datapath实例的“ports”命令后发送此事件。
    实例至少具有以下属性。

    ========= =================================================================
    属性      描述
    ========= =================================================================
    datapath  ryu.controller.controller.Datapath instance of the switch
    reason    one of OFPPR_*
    port_no   状态改变时的端口号
    ========= =================================================================
    """
    def __init__(self, dp, reason, port_no):
        super(EventOFPPortStateChange, self).__init__()
        self.datapath = dp
        self.reason = reason
        self.port_no = port_no

#处理注册者服务
handler.register_service('ryu.controller.ofp_handler')
