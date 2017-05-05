# Copyright (C) 2011-2014 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2011, 2012 Isaku Yamahata <yamahata at valinux co jp>
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

import inspect
import logging
import sys

LOG = logging.getLogger('ryu.controller.handler')

# 仅代表数据通路状态。 datapath具体所以应该移动。
HANDSHAKE_DISPATCHER = "handshake"
CONFIG_DISPATCHER = "config"
MAIN_DISPATCHER = "main"
DEAD_DISPATCHER = "dead"

#调用函数类
class _Caller(object):
    """Describe how to handle an event class.
    """

    def __init__(self, dispatchers, ev_source):
        """初始化 _Caller.

        :param dispatchers: 一个生效的状态列表
                            None and [] mean all states.
                            
        :param ev_source: 产生事件的模块.
                          ev_cls.__module__ for set_ev_cls.
                          None for set_ev_handler.
        """
        self.dispatchers = dispatchers
        self.ev_source = ev_source


# 应该被命名为 '监听事件'
def set_ev_cls(ev_cls, dispatchers=None):
    """
    Ryu应用程序的装饰器声明一个事件处理程序。

    装饰的方法将成为一个事件处理程序。
    ev_cls是这个RyuApp想要接收的实例的事件类。
    dispatchers参数指定以下协商阶段之一
   （或它们的列表），为此处理程序应为其生成事件。
    请注意，如果事件改变阶段，则更改前的阶段
    用于检查兴趣。

    .. 表格列:: |l|L|

    =========================================== ===============================
    协商阶段                                    描述
    =========================================== ===============================
    ryu.controller.handler.HANDSHAKE_DISPATCHER 发送和等待hello消息
    ryu.controller.handler.CONFIG_DISPATCHER    版本协商 and sent
                                                features-request message
    ryu.controller.handler.MAIN_DISPATCHER      Switch-features message
                                                接收和发送设置配置信息
    ryu.controller.handler.DEAD_DISPATCHER      断开与对端的连接。 要么
                                                断开由于某些不可恢复
                                                的错误的连接
    =========================================== ===============================
    """
#设置事件类调度者
    def _set_ev_cls_dec(handler):
        if 'callers' not in dir(handler):
            handler.callers = {}
        for e in _listify(ev_cls):
            handler.callers[e] = _Caller(_listify(dispatchers), e.__module__)
        return handler
    return _set_ev_cls_dec

#设置事件处理
def set_ev_handler(ev_cls, dispatchers=None):
    def _set_ev_cls_dec(handler):
        if 'callers' not in dir(handler):
            handler.callers = {}
        for e in _listify(ev_cls):
            handler.callers[e] = _Caller(_listify(dispatchers), None)
        return handler
    return _set_ev_cls_dec

#有调用者
def _has_caller(meth):
    return hasattr(meth, 'callers')

#列表化
def _listify(may_list):
    if may_list is None:
        may_list = []
    if not isinstance(may_list, list):
        may_list = [may_list]
    return may_list

#注册实例
def register_instance(i):
    for _k, m in inspect.getmembers(i, inspect.ismethod):
        # LOG.debug('instance %s k %s m %s', i, _k, m)
        if _has_caller(m):
            for ev_cls, c in m.callers.items():
                i.register_handler(ev_cls, m)

#方法
def _is_method(f):
    return inspect.isfunction(f) or inspect.ismethod(f)

#获取依赖服务
def get_dependent_services(cls):
    services = []
    for _k, m in inspect.getmembers(cls, _is_method):
        if _has_caller(m):
            for ev_cls, c in m.callers.items():
                service = getattr(sys.modules[ev_cls.__module__],
                                  '_SERVICE_NAME', None)
                if service:
                    # 避免注册自己的事件的类
                    # 像ofp_handler
                    if cls.__module__ != service:
                        services.append(service)

    m = sys.modules[cls.__module__]
    services.extend(getattr(m, '_REQUIRED_APP', []))
    services = list(set(services))
    return services

#注册服务
def register_service(service):
    """
    将“service”指定的ryu应用程序注册为
    在调用模块中定义的事件提供者。

    如果正在加载的应用程序消耗事件（在某种意义上）
    set_ev_cls）由'service'应用程序提供，后者
    应用程序将自动加载。

    该机制用于例如 如果自动启动p_handler
    有消耗OFP事件的应用程序。
    """
    frame = inspect.currentframe()
    m_name = frame.f_back.f_globals['__name__']
    m = sys.modules[m_name]
    m._SERVICE_NAME = service
