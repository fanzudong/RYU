# Copyright (C) 2011, 2012 Nippon Telegraph and Telephone Corporation.
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

"""
基本的OpenFlow处理包括协商
"""

import itertools
import logging
import warnings

import ryu.base.app_manager

from ryu.lib import hub
from ryu import utils
from ryu.controller import ofp_event
from ryu.controller.controller import OpenFlowController
from ryu.controller.handler import set_ev_handler
from ryu.controller.handler import HANDSHAKE_DISPATCHER, CONFIG_DISPATCHER,\
    MAIN_DISPATCHER
from ryu.ofproto import ofproto_parser


# 状态过渡: HANDSHAKE -> CONFIG -> MAIN
#
# HANDSHAKE: 如果它收到可用的OFP版本的HELLO消息,
#发送特征请求消息, 然后去CONFIG.
#
# CONFIG: 收到Feature Reply消息并切换到MAIN
#
# MAIN: 什么也不做. 应用要注册它们自己的处理者
# 
#
# 请注意，在任何状态下，当我们收到回应请求消息时，返回回复消息。

#ofp处理者类
class OFPHandler(ryu.base.app_manager.RyuApp):
    def __init__(self, *args, **kwargs):
        super(OFPHandler, self).__init__(*args, **kwargs)
        self.name = 'ofp_event'
#开始
    def start(self):
        super(OFPHandler, self).start()
        return hub.spawn(OpenFlowController())
#hello失败
    def _hello_failed(self, datapath, error_desc):
        self.logger.error(error_desc)
        error_msg = datapath.ofproto_parser.OFPErrorMsg(datapath)
        error_msg.type = datapath.ofproto.OFPET_HELLO_FAILED
        error_msg.code = datapath.ofproto.OFPHFC_INCOMPATIBLE
        error_msg.data = error_desc
        datapath.send_msg(error_msg)
#设置事件处理者
    @set_ev_handler(ofp_event.EventOFPHello, HANDSHAKE_DISPATCHER)
    def hello_handler(self, ev):
        self.logger.debug('hello ev %s', ev)
        msg = ev.msg
        datapath = msg.datapath

        # 检测收到的版本是否支持.
        # 1.0之前的版本不支持
        elements = getattr(msg, 'elements', None)
        if elements:
            switch_versions = set()
            for version in itertools.chain.from_iterable(
                    element.versions for element in elements):
                switch_versions.add(version)
            usable_versions = switch_versions & set(
                datapath.supported_ofp_version)

            # 我们没有发送我们支持的版本的互操作性，
            #因为大多数交换机目前不了解组成。
            #所以交换机会认为协商的版本是
            #max（negotiiated_versions），但实际的
            #可用版本是max（available_versions）。
            negotiated_versions = set(
                version for version in switch_versions
                if version <= max(datapath.supported_ofp_version))
            if negotiated_versions and not usable_versions:
                # e.g.
                # 交换机的版本是1.0和1.1
                # ryu支持1.2，1.1, 1.0
                # 协商版本 = 1.1
                # 可用版本 = None
                error_desc = (
                    'no compatible version found: '
                    'switch versions %s controller version 0x%x, '
                    'the negotiated version is 0x%x, '
                    'but no usable version found. '
                    'If possible, set the switch to use one of OF version %s'
                    % (switch_versions, max(datapath.supported_ofp_version),
                       max(negotiated_versions),
                       sorted(datapath.supported_ofp_version)))
                self._hello_failed(datapath, error_desc)
                return
            if (negotiated_versions and usable_versions and
                    max(negotiated_versions) != max(usable_versions)):
                # e.g.
                # 交换机的版本是1.0和1.1
                # ryu支持1.2， 1.0
                # negotiated version = 1.0
                # usable version = 1.0
                #
                # TODO: 为了获得版本1.0, Ryu需要发送
                # 支持版本
                error_desc = (
                    'no compatible version found: '
                    'switch versions 0x%x controller version 0x%x, '
                    'the negotiated version is %s but found usable %s. '
                    'If possible, '
                    'set the switch to use one of OF version %s' % (
                        max(switch_versions),
                        max(datapath.supported_ofp_version),
                        sorted(negotiated_versions),
                        sorted(usable_versions), sorted(usable_versions)))
                self._hello_failed(datapath, error_desc)
                return
        else:
            usable_versions = set(version for version
                                  in datapath.supported_ofp_version
                                  if version <= msg.version)
            if (usable_versions and
                max(usable_versions) != min(msg.version,
                                            datapath.ofproto.OFP_VERSION)):
                # 版本min(msg.version, datapath.ofproto.OFP_VERSION)
                # 根据规范应该被用. 但我们不能.
                # So log it and 用max(usable_versions) 希望
                # 交换机可以解析低版本.
                # e.g.
                # OF 1.1 from switch
                # OF 1.2 from Ryu and supported_ofp_version = (1.0, 1.2)
                # 这种情况下, 根据规范1.1应该可以用,
                # 但1.1 不能用
                #
                # OF1.3.1 6.3.1
                # 收到此消息后，收件人必须
                #计算要使用的OpenFlow协议版本。如果
                #发送Hello消息和Hello消息
                #收到包含一个OFPHET_VERSIONBITMAP hello元素，
                #，如果这些位图设置了一些常用位，那么
                #negotiationiated必须是最高版本
                #两个位图。否则，协商版本必须是
                #发送的版本号越小越好
                #在版本字段中收到的。如果
                #negotiated版本由收件人支持
                #连接进行。否则，收件人必须
                #回复一个OFPT_ERROR消息，其类型为
                #OFPET_HELLO_FAILED，一个OFPHFC_INCOMPATIBLE的代码字段，
                #和可选的ASCII字符串解释情况
                #在数据中，然后终止连接。
                version = max(usable_versions)
                error_desc = (
                    'no compatible version found: '
                    'switch 0x%x controller 0x%x, but found usable 0x%x. '
                    'If possible, set the switch to use OF version 0x%x' % (
                        msg.version, datapath.ofproto.OFP_VERSION,
                        version, version))
                self._hello_failed(datapath, error_desc)
                return
#如果没有可用版本
        if not usable_versions:
            error_desc = (
                'unsupported version 0x%x. '
                'If possible, set the switch to use one of the versions %s' % (
                    msg.version, sorted(datapath.supported_ofp_version)))
            self._hello_failed(datapath, error_desc)
            return
        datapath.set_version(max(usable_versions))

#切换到配置状态
        self.logger.debug('move onto config mode')
        datapath.set_state(CONFIG_DISPATCHER)

#最终，发送特征请求
        features_request = datapath.ofproto_parser.OFPFeaturesRequest(datapath)
        datapath.send_msg(features_request)
#设置事件处理者
    @set_ev_handler(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        self.logger.debug('switch features ev %s', msg)

        datapath.id = msg.datapath_id

        # 恶意的解决方法，将被删除。 OF1.3 doesn't have ports.
        # 应用程序不应该依赖它们.
        # 但是可能会有这样糟糕的应用程序，
        # 所以保持这个解决方法。
        if datapath.ofproto.OFP_VERSION < 0x04:
            datapath.ports = msg.ports
        else:
            datapath.ports = {}

        if datapath.ofproto.OFP_VERSION < 0x04:
            self.logger.debug('move onto main mode')
            ev.msg.datapath.set_state(MAIN_DISPATCHER)
        else:
            port_desc = datapath.ofproto_parser.OFPPortDescStatsRequest(
                datapath, 0)
            datapath.send_msg(port_desc)
#多回复处理者
    @set_ev_handler(ofp_event.EventOFPPortDescStatsReply, CONFIG_DISPATCHER)
    def multipart_reply_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            for port in msg.body:
                datapath.ports[port.port_no] = port

        if msg.flags & datapath.ofproto.OFPMPF_REPLY_MORE:
            return
        self.logger.debug('move onto main mode')
        ev.msg.datapath.set_state(MAIN_DISPATCHER)
#echo请求处理者
    @set_ev_handler(ofp_event.EventOFPEchoRequest,
                    [HANDSHAKE_DISPATCHER, CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def echo_request_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        echo_reply = datapath.ofproto_parser.OFPEchoReply(datapath)
        echo_reply.xid = msg.xid
        echo_reply.data = msg.data
        datapath.send_msg(echo_reply)
#echo回复处理者
    @set_ev_handler(ofp_event.EventOFPEchoReply,
                    [HANDSHAKE_DISPATCHER, CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def echo_reply_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        datapath.acknowledge_echo_reply(msg.xid)
#端口状态处理者
    @set_ev_handler(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
#
        if msg.reason in [ofproto.OFPPR_ADD, ofproto.OFPPR_MODIFY]:
            datapath.ports[msg.desc.port_no] = msg.desc
        elif msg.reason == ofproto.OFPPR_DELETE:
            datapath.ports.pop(msg.desc.port_no, None)
        else:
            return

        self.send_event_to_observers(
            ofp_event.EventOFPPortStateChange(
                datapath, msg.reason, msg.desc.port_no),
            datapath.state)
#错误消息处理者
    @set_ev_handler(ofp_event.EventOFPErrorMsg,
                    [HANDSHAKE_DISPATCHER, CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def error_msg_handler(self, ev):
        msg = ev.msg
        ofp = msg.datapath.ofproto
        self.logger.debug(
            "EventOFPErrorMsg received.\n"
            "version=%s, msg_type=%s, msg_len=%s, xid=%s\n"
            " `-- msg_type: %s\n"
            "OFPErrorMsg(type=%s, code=%s, data=b'%s')\n"
            " |-- type: %s\n"
            " |-- code: %s",
            hex(msg.version), hex(msg.msg_type), hex(msg.msg_len),
            hex(msg.xid), ofp.ofp_msg_type_to_str(msg.msg_type),
            hex(msg.type), hex(msg.code), utils.binary_str(msg.data),
            ofp.ofp_error_type_to_str(msg.type),
            ofp.ofp_error_code_to_str(msg.type, msg.code))
        if msg.type == ofp.OFPET_HELLO_FAILED:
            self.logger.debug(
                " `-- data: %s", msg.data.decode('ascii'))
        elif len(msg.data) >= ofp.OFP_HEADER_SIZE:
            (version, msg_type, msg_len, xid) = ofproto_parser.header(msg.data)
            self.logger.debug(
                " `-- data: version=%s, msg_type=%s, msg_len=%s, xid=%s\n"
                "     `-- msg_type: %s",
                hex(version), hex(msg_type), hex(msg_len), hex(xid),
                ofp.ofp_msg_type_to_str(msg_type))
        else:
            self.logger.warning(
                "The data field sent from the switch is too short: "
                "len(msg.data) < OFP_HEADER_SIZE\n"
                "The OpenFlow Spec says that the data field should contain "
                "at least 64 bytes of the failed request.\n"
                "Please check the settings or implementation of your switch.")
