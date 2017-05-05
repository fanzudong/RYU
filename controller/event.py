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

#事件基类
class EventBase(object):
    """
    所有事件类的基础。

    Ryu应用程序可以通过创建一个子类来定义自己的事件类型。
    """
#初始化函数
    def __init__(self):
        super(EventBase, self).__init__()

#事件请求基类
class EventRequestBase(EventBase):
    """
    RyuApp.send_request同步请求的基类。
    """
    def __init__(self):
        super(EventRequestBase, self).__init__()
        self.dst = None  # 提供事件的应用名.
        self.src = None
        self.sync = False
        self.reply_q = None

#事件回复基类
class EventReplyBase(EventBase):
    """
    RyuApp.send_reply的同步请求回复的基类。
    """
    def __init__(self, dst):
        super(EventReplyBase, self).__init__()
        self.dst = dst
