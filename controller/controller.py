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
OpenFlow控制器的主要组件。

- 处理交换机的连接
- 生成并将事件路由到适当的实体，如Ryu应用程序

"""

import contextlib
from ryu import cfg
import logging
from ryu.lib import hub
from ryu.lib.hub import StreamServer
import traceback
import random
import ssl
from socket import IPPROTO_TCP, TCP_NODELAY, SHUT_RDWR, timeout as SocketTimeout
import warnings

import ryu.base.app_manager

from ryu.ofproto import ofproto_common
from ryu.ofproto import ofproto_parser
from ryu.ofproto import ofproto_protocol
from ryu.ofproto import ofproto_v1_0
from ryu.ofproto import nx_match

from ryu.controller import ofp_event
from ryu.controller.handler import HANDSHAKE_DISPATCHER, DEAD_DISPATCHER

from ryu.lib.dpid import dpid_to_str

LOG = logging.getLogger('ryu.controller.controller')
#缺省ofp主机
DEFAULT_OFP_HOST = '0.0.0.0'

CONF = cfg.CONF
CONF.register_cli_opts([
    cfg.StrOpt('ofp-listen-host', default=DEFAULT_OFP_HOST,
               help='openflow listen host (default %s)' % DEFAULT_OFP_HOST),  #of监听主机
    cfg.IntOpt('ofp-tcp-listen-port', default=None,
               help='openflow tcp listen port '                               #of tcp监听端口
                    '(default: %d)' % ofproto_common.OFP_TCP_PORT),
    cfg.IntOpt('ofp-ssl-listen-port', default=None,
               help='openflow ssl listen port '                               #of ssl监听端口
                    '(default: %d)' % ofproto_common.OFP_SSL_PORT),
    cfg.StrOpt('ctl-privkey', default=None, help='controller private key'),   #控制器私有密钥
    cfg.StrOpt('ctl-cert', default=None, help='controller certificate'),      #控制器证书
    cfg.StrOpt('ca-certs', default=None, help='CA certificates')              #CA证书
])
CONF.register_opts([
    cfg.FloatOpt('socket-timeout',
                 default=5.0,
                 help='Time, in seconds, to await completion of socket operations.'), #时间，用秒计算，等待完成套接字操作
    cfg.FloatOpt('echo-request-interval',
                 default=15.0,
                 help='Time, in seconds, between sending echo requests to a datapath.'), #给datapath发送echo消息请求的时间间隔
    cfg.IntOpt('maximum-unreplied-echo-requests',
               default=0,
               min=0,
               help='Maximum number of unreplied echo requests before datapath is disconnected.') #datapath未连接之前，echo请求消息最大未回复时间
])

#of控制器类
class OpenFlowController(object):
    def __init__(self):
        super(OpenFlowController, self).__init__()
        if not CONF.ofp_tcp_listen_port and not CONF.ofp_ssl_listen_port:
            self.ofp_tcp_listen_port = ofproto_common.OFP_TCP_PORT
            self.ofp_ssl_listen_port = ofproto_common.OFP_SSL_PORT
            # 为了向后兼容，我们产生一个服务器循环
            # 监听旧的of监听端口6633.
            hub.spawn(self.server_loop,
                      ofproto_common.OFP_TCP_PORT_OLD,
                      ofproto_common.OFP_SSL_PORT_OLD)
        else:
            self.ofp_tcp_listen_port = CONF.ofp_tcp_listen_port
            self.ofp_ssl_listen_port = CONF.ofp_ssl_listen_port

# 入口点
    def __call__(self):
        # LOG.debug('call') 
        self.server_loop(self.ofp_tcp_listen_port,
                         self.ofp_ssl_listen_port)
#服务循环
    def server_loop(self, ofp_tcp_listen_port, ofp_ssl_listen_port):
        if CONF.ctl_privkey is not None and CONF.ctl_cert is not None:
            if CONF.ca_certs is not None:
                server = StreamServer((CONF.ofp_listen_host,
                                       ofp_ssl_listen_port),
                                      datapath_connection_factory,
                                      keyfile=CONF.ctl_privkey,
                                      certfile=CONF.ctl_cert,
                                      cert_reqs=ssl.CERT_REQUIRED,
                                      ca_certs=CONF.ca_certs,
                                      ssl_version=ssl.PROTOCOL_TLSv1)
            else:
                server = StreamServer((CONF.ofp_listen_host,
                                       ofp_ssl_listen_port),
                                      datapath_connection_factory,
                                      keyfile=CONF.ctl_privkey,
                                      certfile=CONF.ctl_cert,
                                      ssl_version=ssl.PROTOCOL_TLSv1)
        else:
            server = StreamServer((CONF.ofp_listen_host,
                                   ofp_tcp_listen_port),
                                  datapath_connection_factory)

        # LOG.debug('loop')
        server.serve_forever()

#关闭函数
def _deactivate(method):
    def deactivate(self):
        try:
            method(self)
        finally:
            try:
                self.socket.shutdown(SHUT_RDWR)
            except (EOFError, IOError):
                pass

            if not self.is_active:
                self.socket.close()
    return deactivate

#数据路径类
class Datapath(ofproto_protocol.ProtocolDesc):
    """
    描述连接到该控制器的OpenFlow开关的类。

    实例具有以下属性。

    .. 表格列:: |l|L|

    ==================================== ======================================
    属性                                 描述
    ==================================== ======================================
    id                                   64-bit OpenFlow Datapath ID.
                                         只能应用于
                                         ryu.controller.handler.MAIN_DISPATCHER
                                         phase.
                                         
    ofproto                              导出OpenFlow定义的模块，主要是常量
                                         出现在规范中，用于协商的OpenFlow版本。
                                         例如，对于OpenFlow 1.0，ryu.ofproto.ofproto_v1_0。
                                         
    ofproto_parser                       导出OpenFlow定义的解析模块，主要是常量
                                         出现在规范中，用于协商的OpenFlow版本。
                                         例如,ryu.ofproto.ofproto_v1_0_parser
                                         for OpenFlow 1.0.
                                         
    ofproto_parser.OFPxxxx(datapath,...) 可以准备一个OpenFlow
                                         给定交换机的消息。它可以
                                         请稍候再与Datapath.send_msg一起发送。
                                         xxxx是消息的名称。对于
                                         FlowPF的OFPFlowMod示例
                                         信息。 Arguemnts依赖于
                                         信息。
    set_xid(self, msg)                   生成一个OpenFlow XID 并把它放在msg.xid.
    
    send_msg(self, msg)                  队列一个要发送的OpenFlow消息
                                         相应的开关。如果msg.xid
                                         是None，set_xid是自动的
                                         在排队前呼叫消息。
    send_packet_out                      弃用
    send_flow_mod                        弃用
    send_flow_del                        弃用
    send_delete_all_flows                弃用
    send_barrier                         将OpenFlow屏障消息排队
                                         发送到交换机.
    send_nxt_set_flow_format             弃用
    is_reserved_port                     弃用
    ==================================== ======================================
    """
#初始化
    def __init__(self, socket, address):
        super(Datapath, self).__init__()

        self.socket = socket
        self.socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        self.socket.settimeout(CONF.socket_timeout)
        self.address = address
        self.is_active = True

        #限制是任意的。 我们需要限制队列大小，以防止其记忆。
        
        self.send_q = hub.Queue(16)
        self._send_q_sem = hub.BoundedSemaphore(self.send_q.maxsize)

        self.echo_request_interval = CONF.echo_request_interval
        self.max_unreplied_echo_requests = CONF.maximum_unreplied_echo_requests
        self.unreplied_echo_requests = []

        self.xid = random.randint(0, self.ofproto.MAX_XID)
        self.id = None  # datapath_id 暂时还不知道
        self._ports = None
        self.flow_format = ofproto_v1_0.NXFF_OPENFLOW10
        self.ofp_brick = ryu.base.app_manager.lookup_service_brick('ofp_event')
        self.set_state(HANDSHAKE_DISPATCHER)
#关闭函数
    @_deactivate
    def close(self):
        if self.state != DEAD_DISPATCHER:
            self.set_state(DEAD_DISPATCHER)
#设置状态
    def set_state(self, state):
        self.state = state
        ev = ofp_event.EventOFPStateChange(self)
        ev.state = state
        self.ofp_brick.send_event_to_observers(ev, state)

    # 低级套接字处理层
    @_deactivate
    def _recv_loop(self):   #接收循环
        buf = bytearray()
        count = 0
        min_read_len = remaining_read_len = ofproto_common.OFP_HEADER_SIZE

        while self.state != DEAD_DISPATCHER:
            try:
                read_len = min_read_len
                if (remaining_read_len > min_read_len):
                    read_len = remaining_read_len
                ret = self.socket.recv(read_len)
            except SocketTimeout:
                continue
            except ssl.SSLError:
                # eventlet抛出SSL错误(which is a subclass of IOError)
                # on SSL socket read timeout; 在这个情况下重新尝试循环.
                continue
            except (EOFError, IOError):
                break

            if len(ret) == 0:
                break

            buf += ret
            buf_len = len(buf)
            while buf_len >= min_read_len:
                (version, msg_type, msg_len, xid) = ofproto_parser.header(buf)
                if (msg_len < min_read_len):
                    # Someone isn't playing nicely; log it, and try something sane.（理性的东西）
                    LOG.debug("Message with invalid length %s received from switch at address %s",
                              msg_len, self.address)
                    msg_len = min_read_len
                if buf_len < msg_len:
                    remaining_read_len = (msg_len - buf_len)
                    break

                msg = ofproto_parser.msg(
                    self, version, msg_type, msg_len, xid, buf[:msg_len])
                # LOG.debug('queue msg %s cls %s', msg, msg.__class__)
                if msg:
                    ev = ofp_event.ofp_msg_to_ev(msg)
                    self.ofp_brick.send_event_to_observers(ev, self.state)

                    dispatchers = lambda x: x.callers[ev.__class__].dispatchers
                    handlers = [handler for handler in
                                self.ofp_brick.get_handlers(ev) if
                                self.state in dispatchers(handler)]
                    for handler in handlers:
                        handler(ev)

                buf = buf[msg_len:]
                buf_len = len(buf)
                remaining_read_len = min_read_len

                # We need to schedule other greenlets. Otherwise, ryu
                # 不能接受新的交换机or 处理存在的
                # 交换机. 限制是任意的. 在未来我们需要
                # 更好的方法.
                count += 1
                if count > 2048:
                    count = 0
                    hub.sleep(0)
#发送循环
    def _send_loop(self):
        try:
            while self.state != DEAD_DISPATCHER:
                buf = self.send_q.get()
                self._send_q_sem.release()
                self.socket.sendall(buf)
        except SocketTimeout:
            LOG.debug("Socket timed out while sending data to switch at address %s", #Socket在发送数据时时间到了就切换到某地址
                      self.address)
        except IOError as ioe:
            # 将ioe.errno转换为字符串，以防其以某种方式设置为无。
            errno = "%s" % ioe.errno
            LOG.debug("Socket error while sending data to switch at address %s: [%s] %s",
                      self.address, errno, ioe.strerror)
        finally:
            q = self.send_q
            # 首先，清除self.send_q以防止新引用.
            self.send_q = None
            # 现在，排除send_q，为每个条目释放相关的信号量.
            # 这应该释放所有线程等待获取信号量.
            try:
                while q.get(block=False):
                    self._send_q_sem.release()
            except hub.QueueEmpty:
                pass
            # 最后，确保_recv_loop终止。
            self.close()
#发送函数
    def send(self, buf):
        msg_enqueued = False
        self._send_q_sem.acquire()
        if self.send_q:
            self.send_q.put(buf)
            msg_enqueued = True
        else:
            self._send_q_sem.release()
        if not msg_enqueued:
            LOG.debug('Datapath in process of terminating; send() to %s discarded.',
                      self.address)
        return msg_enqueued
#设置交换机标识
    def set_xid(self, msg):
        self.xid += 1
        self.xid &= self.ofproto.MAX_XID
        msg.set_xid(self.xid)
        return self.xid
#发送信息
    def send_msg(self, msg):
        assert isinstance(msg, self.ofproto_parser.MsgBase)
        if msg.xid is None:
            self.set_xid(msg)
        msg.serialize()
        # LOG.debug('send_msg %s', msg)
        return self.send(msg.buf)
#echo请求循环
    def _echo_request_loop(self):
        if not self.max_unreplied_echo_requests:
            return
        while (self.send_q and
               (len(self.unreplied_echo_requests) <= self.max_unreplied_echo_requests)):
            echo_req = self.ofproto_parser.OFPEchoRequest(self)
            self.unreplied_echo_requests.append(self.set_xid(echo_req))
            self.send_msg(echo_req)
            hub.sleep(self.echo_request_interval)
        self.close()
#告知已收到echo回复
    def acknowledge_echo_reply(self, xid):
        try:
            self.unreplied_echo_requests.remove(xid)
        except:
            pass
#服务
    def serve(self):
        send_thr = hub.spawn(self._send_loop)

        # 立即发送hello消息
        hello = self.ofproto_parser.OFPHello(self)
        self.send_msg(hello)

        echo_thr = hub.spawn(self._echo_request_loop)

        try:
            self._recv_loop()
        finally:
            hub.kill(send_thr)
            hub.kill(echo_thr)
            hub.joinall([send_thr, echo_thr])
            self.is_active = False

#
# 实用方法 for convenience
#发出数据包
    def send_packet_out(self, buffer_id=0xffffffff, in_port=None,
                        actions=None, data=None):
        if in_port is None:
            in_port = self.ofproto.OFPP_NONE
        packet_out = self.ofproto_parser.OFPPacketOut(
            self, buffer_id, in_port, actions, data)
        self.send_msg(packet_out)
#流发送模块
    def send_flow_mod(self, rule, cookie, command, idle_timeout, hard_timeout,
                      priority=None, buffer_id=0xffffffff,
                      out_port=None, flags=0, actions=None):
        if priority is None:
            priority = self.ofproto.OFP_DEFAULT_PRIORITY
        if out_port is None:
            out_port = self.ofproto.OFPP_NONE
        flow_format = rule.flow_format()
        assert (flow_format == ofproto_v1_0.NXFF_OPENFLOW10 or
                flow_format == ofproto_v1_0.NXFF_NXM)
        if self.flow_format < flow_format:
            self.send_nxt_set_flow_format(flow_format)
        if flow_format == ofproto_v1_0.NXFF_OPENFLOW10:
            match_tuple = rule.match_tuple()
            match = self.ofproto_parser.OFPMatch(*match_tuple)
            flow_mod = self.ofproto_parser.OFPFlowMod(
                self, match, cookie, command, idle_timeout, hard_timeout,
                priority, buffer_id, out_port, flags, actions)
        else:
            flow_mod = self.ofproto_parser.NXTFlowMod(
                self, cookie, command, idle_timeout, hard_timeout,
                priority, buffer_id, out_port, flags, rule, actions)
        self.send_msg(flow_mod)
#流发送删除
    def send_flow_del(self, rule, cookie, out_port=None):
        self.send_flow_mod(rule=rule, cookie=cookie,
                           command=self.ofproto.OFPFC_DELETE,
                           idle_timeout=0, hard_timeout=0, priority=0,
                           out_port=out_port)
#发出删除所有流
    def send_delete_all_flows(self):
        rule = nx_match.ClsRule()
        self.send_flow_mod(
            rule=rule, cookie=0, command=self.ofproto.OFPFC_DELETE,
            idle_timeout=0, hard_timeout=0, priority=0, buffer_id=0,
            out_port=self.ofproto.OFPP_NONE, flags=0, actions=None)
#发送屏障
    def send_barrier(self):
        barrier_request = self.ofproto_parser.OFPBarrierRequest(self)
        return self.send_msg(barrier_request)
#发送下一个流设置格式
    def send_nxt_set_flow_format(self, flow_format):
        assert (flow_format == ofproto_v1_0.NXFF_OPENFLOW10 or
                flow_format == ofproto_v1_0.NXFF_NXM)
        if self.flow_format == flow_format:
            # 什么也不做
            return
        self.flow_format = flow_format
        set_format = self.ofproto_parser.NXTSetFlowFormat(self, flow_format)
        # FIXME: If NXT_SET_FLOW_FORMAT or NXFF_NXM is not supported by
        # the switch then 收到一个错误消息. 它可能是
        # handled by setting self.flow_format to
        # ofproto_v1_0.NXFF_OPENFLOW10 but currently isn't.
        self.send_msg(set_format)
        self.send_barrier()
#保留端口
    def is_reserved_port(self, port_no):
        return port_no > self.ofproto.OFPP_MAX

#数据路径连接factory
def datapath_connection_factory(socket, address):
    LOG.debug('connected socket:%s address:%s', socket, address)
    with contextlib.closing(Datapath(socket, address)) as datapath:
        try:
            datapath.serve()
        except:
            # 出了些问题.
            # 特别是恶意切换可能会发送格式错误的数据包,
            # 解析器引发异常.
            # 我们可以做任何更优雅的事情吗?
            if datapath.id is None:
                dpid_str = "%s" % datapath.id
            else:
                dpid_str = dpid_to_str(datapath.id)
            LOG.error("Error in the datapath %s from %s", dpid_str, address)
            raise
