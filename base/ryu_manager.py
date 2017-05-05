# Copyright (C) 2011-2014 Nippon Telegraph and Telephone Corporation.
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
Ryu应用的中央管理。

- 加载Ryu应用程序
- 向Ryu应用程序提供“上下文”
- 在Ryu应用程序之间路由消息

"""

import inspect
import itertools
import logging
import sys
import os
import gc

from ryu import cfg
from ryu import utils
from ryu.app import wsgi
from ryu.controller.handler import register_instance, get_dependent_services
from ryu.controller.controller import Datapath
from ryu.controller import event
from ryu.controller.event import EventRequestBase, EventReplyBase
from ryu.lib import hub
from ryu.ofproto import ofproto_protocol

LOG = logging.getLogger('ryu.base.app_manager')

SERVICE_BRICKS = {}

#查找服务链
def lookup_service_brick(name):
    return SERVICE_BRICKS.get(name)

#通过事件类查找服务链
def _lookup_service_brick_by_ev_cls(ev_cls):
    return _lookup_service_brick_by_mod_name(ev_cls.__module__)

#通过模块名查找服务链
def _lookup_service_brick_by_mod_name(mod_name):
    return lookup_service_brick(mod_name.split('.')[-1])

#应用注册
def register_app(app):
    assert isinstance(app, RyuApp)
    assert app.name not in SERVICE_BRICKS
    SERVICE_BRICKS[app.name] = app
    register_instance(app)

#未注册应用
def unregister_app(app):
    SERVICE_BRICKS.pop(app.name)

#需要的应用
def require_app(app_name, api_style=False):
    """
     请求应用程序自动加载。

     如果这用于由客户端导入的“api”样式模块
     application，set api_style = True。

     如果这是用于客户端应用程序模块，请设置api_style = False。
    """
    iterable = (inspect.getmodule(frame[0]) for frame in inspect.stack())
    modules = [module for module in iterable if module is not None]
    if api_style:
        m = modules[2]  # 跳过“api”模块的框架
    else:
        m = modules[1]
    m._REQUIRED_APP = getattr(m, '_REQUIRED_APP', [])
    m._REQUIRED_APP.append(app_name)
    LOG.debug('require_app: %s is required by %s', app_name, m.__name__)


class RyuApp(object):
    """
    Ryu应用程序的基础类。

     RyuApp子类在ryu-manager加载所有请求的
     Ryu应用程序模块之后实例化。
     __init__应该用相同的参数调用RyuApp .__ init__。
     在__init__中发送任何事件是非法的。

     实例属性“name”是Ryu应用程序中用于消息路由
     的类的名称。 （Cf. send_event）
     它被设置为__class __.__ name__由RyuApp .__ init__。
     对于子类来说，这是不鼓励的。
    """

    _CONTEXTS = {}
    """
     用于指定此Ryu应用程序要使用的上下文的字典。
     它的关键是上下文的名称，它的价值是一个普通的类
     用于实现上下文。 该类由app_manager实例化
     并且实例在具有_CONTEXTS的RyuApp子类之间共享
     会员具有相同的密钥。 一个RyuApp子类可以获得一个引用
     该实例通过其__init __的kwargs如下。

    Example::

        _CONTEXTS = {
            'network': network.Network
        }

        def __init__(self, *args, *kwargs):
            self.network = kwargs['network']
    """

    _EVENTS = []
    """
    这个RyuApp子类将生成的事件类的列表。
    当且仅当在与RyuApp子类不同的python模块中
    定义了事件类时才应指定这一点。
    """

    OFP_VERSIONS = None
    """
    此Ryu应用程序支持的OpenFlow版本列表。
    默认是框架支持的所有版本。

    Examples::

        OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION,
                        ofproto_v1_2.OFP_VERSION]

    如果系统中加载了多个Ryu应用程序，
    使用它们的OFP_VERSIONS的交集。
    """

    @classmethod
    def context_iteritems(cls):
        """
        通过应用程序上下文的（键，上下文类）返回迭代器
        """
        return iter(cls._CONTEXTS.items())
#初始化函数
    def __init__(self, *_args, **_kwargs):
        super(RyuApp, self).__init__()
        self.name = self.__class__.__name__
        self.event_handlers = {}        # ev_cls -> handlers:list       事件_类 --> 处理:列表
        self.observers = {}     # ev_cls -> observer-name -> states:set  处理:列表-->监听函数名-->状态:设置
        self.threads = []
        self.main_thread = None
        self.events = hub.Queue(128)
        self._events_sem = hub.BoundedSemaphore(self.events.maxsize)
        if hasattr(self.__class__, 'LOGGER_NAME'):
            self.logger = logging.getLogger(self.__class__.LOGGER_NAME)
        else:
            self.logger = logging.getLogger(self.name)
        self.CONF = cfg.CONF

# 防止在RyuApp之外意外创建这个类(事件线程停止)的实例
        class _EventThreadStop(event.EventBase):
            pass
        self._event_stop = _EventThreadStop()
        self.is_active = True
#开始函数
    def start(self):
        """
        Hook that is called after startup initialization is done.启动初始化完成后调用Hook。
        """
        self.threads.append(hub.spawn(self._event_loop))
#停止函数
    def stop(self):
        if self.main_thread:
            hub.kill(self.main_thread)
        self.is_active = False
        self._send_event(self._event_stop, None)
        hub.joinall(self.threads)
#设置主函数线程
    def set_main_thread(self, thread):
        """
        设置self.main_thread（主函数_线程），以使stop（）可以终止它。

        只有AppManager.instantiate_apps（应用管理.实例化_应用）才能调用此函数。
        """
        self.main_thread = thread
#注册应用处理
    def register_handler(self, ev_cls, handler):
        assert callable(handler)
        self.event_handlers.setdefault(ev_cls, [])
        self.event_handlers[ev_cls].append(handler)
#未注册应用处理
    def unregister_handler(self, ev_cls, handler):
        assert callable(handler)
        self.event_handlers[ev_cls].remove(handler)
        if not self.event_handlers[ev_cls]:
            del self.event_handlers[ev_cls]
#注册应用监听者
    def register_observer(self, ev_cls, name, states=None):
        states = states or set()
        ev_cls_observers = self.observers.setdefault(ev_cls, {})
        ev_cls_observers.setdefault(name, set()).update(states)
#未注册应用监听者
    def unregister_observer(self, ev_cls, name):
        observers = self.observers.get(ev_cls, {})
        observers.pop(name)
#未注册应用监听者-所有事件
    def unregister_observer_all_event(self, name):
        for observers in self.observers.values():
            observers.pop(name, None)
#监听事件
    def observe_event(self, ev_cls, states=None):
        brick = _lookup_service_brick_by_ev_cls(ev_cls)
        if brick is not None:
            brick.register_observer(ev_cls, self.name, states)

    def unobserve_event(self, ev_cls):#未监听事件
        brick = _lookup_service_brick_by_ev_cls(ev_cls)
        if brick is not None:
            brick.unregister_observer(ev_cls, self.name)
#获得处理程序列表
    def get_handlers(self, ev, state=None):
        """返回特定事件的处理程序列表。

         ：param ev：处理事件。
         ：param state：当前状态。（ “调度员”）
                       如果没有给出，返回事件的所有处理程序。
                       否则，只返回有兴趣的处理程序
                       处于指定状态。
                       默认值为无。
        """
        ev_cls = ev.__class__
        handlers = self.event_handlers.get(ev_cls, [])
        if state is None:
            return handlers

        def test(h):
            if not hasattr(h, 'callers') or ev_cls not in h.callers:
                # 动态注册的处理程序没有事件
                # h.callers元素
                return True
            states = h.callers[ev_cls].dispatchers
            if not states:
                # 空态是指所有状态
                return True
            return state in states

        return filter(test, handlers)
#获取监听者
    def get_observers(self, ev, state):
        observers = []
        for k, v in self.observers.get(ev.__class__, {}).items():
            if not state or not v or state in v:
                observers.append(k)

        return observers
#发送请求
    def send_request(self, req):
        """
        进行同步请求
        将req.sync设置为True，将其发送到指定的Ryu应用程序
        req.dst，并阻止直到收到回复。
        返回收到的回复。
        参数应该是EventRequestBase的一个实例。
        """

        assert isinstance(req, EventRequestBase)
        req.sync = True
        req.reply_q = hub.Queue()
        self.send_event(req.dst, req)
        # going to sleep for the reply 回复休眠
        return req.reply_q.get()
#事件循环
    def _event_loop(self):
        while self.is_active or not self.events.empty():
            ev, state = self.events.get()
            self._events_sem.release()
            if ev == self._event_stop:
                continue
            handlers = self.get_handlers(ev, state)
            for handler in handlers:
                try:
                    handler(ev)
                except hub.TaskExit:
                    # 正常退出.
                    # 向上传播，所以我们离开事件循环.
                    raise
                except:
                    LOG.exception('%s: Exception occurred during handler processing. '
                                  'Backtrace from offending handler '
                                  '[%s] servicing event [%s] follows.',
                                  self.name, handler.__name__, ev.__class__.__name__)
#发送事件
    def _send_event(self, ev, state):
        self._events_sem.acquire()
        self.events.put((ev, state))
#发送事件
    def send_event(self, name, ev, state=None):
        """
        将指定的事件发送到由名称指定的RyuApp实例。
        """

        if name in SERVICE_BRICKS:
            if isinstance(ev, EventRequestBase):
                ev.src = self.name
            LOG.debug("EVENT %s->%s %s",
                      self.name, name, ev.__class__.__name__)
            SERVICE_BRICKS[name]._send_event(ev, state)
        else:
            LOG.debug("EVENT LOST %s->%s %s",
                      self.name, name, ev.__class__.__name__)
#事件分发给所有监听者
    def send_event_to_observers(self, ev, state=None):
        """
        将指定的事件发送给该RyuApp的所有监听者.
        """

        for observer in self.get_observers(ev, state):
            self.send_event(observer, ev, state)
#回复请求
    def reply_to_request(self, req, rep):
        """
        发送send_request发送的同步请求的回复。
        第一个参数应该是EventRequestBase的一个实例。
        第二个参数应该是EventReplyBase的一个实例。
        """

        assert isinstance(req, EventRequestBase)
        assert isinstance(rep, EventReplyBase)
        rep.dst = req.src
        if req.sync:
            req.reply_q.put(rep)
        else:
            self.send_event(rep.dst, rep)
#关闭函数
    def close(self):
        """
        拆卸方法。
        python上下文管理器选择方法名称close
        """
        pass

#应用管理类
class AppManager(object):
    # singleton 独生子
    _instance = None

    @staticmethod
    def run_apps(app_lists):
        """
        运行一套Ryu应用程序
        一种方便的方法来加载和实例化应用程序。
        这将阻止所有相关应用程序停止。
        """
        app_mgr = AppManager.get_instance()
        app_mgr.load_apps(app_lists)
        contexts = app_mgr.create_contexts()
        services = app_mgr.instantiate_apps(**contexts)
        webapp = wsgi.start_service(app_mgr)
        if webapp:
            services.append(hub.spawn(webapp))
        try:
            hub.joinall(services)
        finally:
            app_mgr.close()
            for t in services:
                t.kill()
            hub.joinall(services)
            gc.collect()
#获取实例
    @staticmethod
    def get_instance():
        if not AppManager._instance:
            AppManager._instance = AppManager()
        return AppManager._instance
#初始化
    def __init__(self):
        self.applications_cls = {}
        self.applications = {}
        self.contexts_cls = {}
        self.contexts = {}
        self.close_sem = hub.Semaphore()
#加载应用
    def load_app(self, name):
        mod = utils.import_module(name)
        clses = inspect.getmembers(mod,
                                   lambda cls: (inspect.isclass(cls) and
                                                issubclass(cls, RyuApp) and
                                                mod.__name__ ==
                                                cls.__module__))
        if clses:
            return clses[0][1]
        return None
#加载应用列表
    def load_apps(self, app_lists):
        app_lists = [app for app
                     in itertools.chain.from_iterable(app.split(',')
                                                      for app in app_lists)]
        while len(app_lists) > 0:
            app_cls_name = app_lists.pop(0)

            context_modules = [x.__module__ for x in self.contexts_cls.values()]
            if app_cls_name in context_modules:
                continue

            LOG.info('loading app %s', app_cls_name)

            cls = self.load_app(app_cls_name)
            if cls is None:
                continue

            self.applications_cls[app_cls_name] = cls

            services = []
            for key, context_cls in cls.context_iteritems():
                v = self.contexts_cls.setdefault(key, context_cls)
                assert v == context_cls
                context_modules.append(context_cls.__module__)

                if issubclass(context_cls, RyuApp):
                    services.extend(get_dependent_services(context_cls))

            # we can't load an app that will be initiataed for
            # contexts.我们无法加载将为上下文启动的应用程序。
            for i in get_dependent_services(cls):
                if i not in context_modules:
                    services.append(i)
            if services:
                app_lists.extend([s for s in set(services)
                                  if s not in app_lists])
#创建上下文环境
    def create_contexts(self):
        for key, cls in self.contexts_cls.items():
            if issubclass(cls, RyuApp):
                # hack for dpset 黑客攻击
                context = self._instantiate(None, cls)
            else:
                context = cls()
            LOG.info('creating context %s', key)
            assert key not in self.contexts
            self.contexts[key] = context
        return self.contexts
#更新服务链
    def _update_bricks(self):
        for i in SERVICE_BRICKS.values():
            for _k, m in inspect.getmembers(i, inspect.ismethod):
                if not hasattr(m, 'callers'):
                    continue
                for ev_cls, c in m.callers.items():
                    if not c.ev_source:
                        continue

                    brick = _lookup_service_brick_by_mod_name(c.ev_source)
                    if brick:
                        brick.register_observer(ev_cls, i.name,
                                                c.dispatchers)

                    # 允许RyuApp和Event类在不同的模块中
                    for brick in SERVICE_BRICKS.values():
                        if ev_cls in brick._EVENTS:
                            brick.register_observer(ev_cls, i.name,
                                                    c.dispatchers)
#报告服务链
    @staticmethod
    def _report_brick(name, app):
        LOG.debug("BRICK %s", name)
        for ev_cls, list_ in app.observers.items():
            LOG.debug("  PROVIDES %s TO %s", ev_cls.__name__, list_)
        for ev_cls in app.event_handlers.keys():
            LOG.debug("  CONSUMES %s", ev_cls.__name__)
#报告服务链
    @staticmethod
    def report_bricks():
        for brick, i in SERVICE_BRICKS.items():
            AppManager._report_brick(brick, i)
#实例化
    def _instantiate(self, app_name, cls, *args, **kwargs):
        # 现在只有一个给定模块的单一实例
        # 我们需要支持多个实例吗？
        # 是的，也许是切片.
        LOG.info('instantiating app %s of %s', app_name, cls.__name__)

        if hasattr(cls, 'OFP_VERSIONS') and cls.OFP_VERSIONS is not None:
            ofproto_protocol.set_app_supported_versions(cls.OFP_VERSIONS)

        if app_name is not None:
            assert app_name not in self.applications
        app = cls(*args, **kwargs)
        register_app(app)
        assert app.name not in self.applications
        self.applications[app.name] = app
        return app
#实例化
    def instantiate(self, cls, *args, **kwargs):
        app = self._instantiate(None, cls, *args, **kwargs)
        self._update_bricks()
        self._report_brick(app.name, app)
        return app
#应用实例化
    def instantiate_apps(self, *args, **kwargs):
        for app_name, cls in self.applications_cls.items():
            self._instantiate(app_name, cls, *args, **kwargs)

        self._update_bricks()
        self.report_bricks()

        threads = []
        for app in self.applications.values():
            t = app.start()
            if t is not None:
                app.set_main_thread(t)
                threads.append(t)
        return threads
#关闭应用
    @staticmethod
    def _close(app):
        close_method = getattr(app, 'close', None)
        if callable(close_method):
            close_method()
#未实例化
    def uninstantiate(self, name):
        app = self.applications.pop(name)
        unregister_app(app)
        for app_ in SERVICE_BRICKS.values():
            app_.unregister_observer_all_event(name)
        app.stop()
        self._close(app)
        events = app.events
        if not events.empty():
            app.logger.debug('%s events remains %d', app.name, events.qsize())
#关闭函数
    def close(self):
        def close_all(close_dict):
            for app in close_dict.values():
                self._close(app)
            close_dict.clear()

        # 此信号量防止并行执行此功能，
        # 因为run_apps的finally子句启动另一个close（）调用.
        with self.close_sem:
            for app_name in list(self.applications.keys()):
                self.uninstantiate(app_name)
            assert not self.applications
            close_all(self.contexts)
