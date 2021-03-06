# Copyright (C) 2013 Nippon Telegraph and Telephone Corporation.
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

from ryu.base import app_manager
from ryu.topology import event

#获取交换机
def get_switch(app, dpid=None):
    rep = app.send_request(event.EventSwitchRequest(dpid))
    return rep.switches

#获取所有交换机
def get_all_switch(app):
    return get_switch(app)

#获取链路
def get_link(app, dpid=None):
    rep = app.send_request(event.EventLinkRequest(dpid))
    return rep.links

#获取所有链路
def get_all_link(app):
    return get_link(app)

#获取主机
def get_host(app, dpid=None):
    rep = app.send_request(event.EventHostRequest(dpid))
    return rep.hosts

#获取所有主机
def get_all_host(app):
    return get_host(app)

app_manager.require_app('ryu.topology.switches', api_style=True)
