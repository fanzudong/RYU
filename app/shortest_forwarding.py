# Copyright (C) 2016 Li Cheng at Beijing University of Posts
# and Telecommunications. www.muzixing.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# conding=utf-8
import logging
import struct
import networkx as nx
from operator import attrgetter
from ryu import cfg
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ipv4
from ryu.lib.packet import arp

from ryu.topology import event, switches
from ryu.topology.api import get_switch, get_link

import network_awareness
import network_monitor
import network_delay_detector


CONF = cfg.CONF

#最短路径转发类
class ShortestForwarding(app_manager.RyuApp):
    """
        最短的转发是用于在最短路径中转发数据包的Ryu应用程序，此应用程序未定义路径计算方法。
        要获得最短路径，该模块取决于网络感知，网络监控和网络延迟检测模块。
    """
#openflow1.3
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
            #_CONTEXTS中的内容将作为当前模块的服务在模块初始化时得到加载。在模块启动时，首先会将_CONTEXTS中的模块先启动，
            #在模块的初始化函数中可以通过self.network_aware = kwargs["Network_Aware"]的形式获得该服务模块的实例，
            #从而获取到该模块的数据，并具有完全的读写能力。这种模式很清晰地体现了模块之间的关系。
    _CONTEXTS = {                                                          
        "network_awareness": network_awareness.NetworkAwareness,    #提前加载的模块
        "network_monitor": network_monitor.NetworkMonitor,
        "network_delay_detector": network_delay_detector.NetworkDelayDetector}
#权重模型字典，跳数，权值，延迟，带宽
    WEIGHT_MODEL = {'hop': 'weight', 'delay': "delay", "bw": "bw"}
#初始化
    def __init__(self, *args, **kwargs):
        super(ShortestForwarding, self).__init__(*args, **kwargs)  #自动找到基类的方法，并传入self参数
        self.name = 'shortest_forwarding'                      #名字是最短路径转发
        self.awareness = kwargs["network_awareness"]           #获得网络感知服务模块的实例
        self.monitor = kwargs["network_monitor"]               #获得网络监控服务模块的实例
        self.delay_detector = kwargs["network_delay_detector"] #获得网络延迟检测服务模块的实例
        self.datapaths = {}                                    #交换机的空字典
        self.weight = self.WEIGHT_MODEL[CONF.weight]           #权重
#设置权重模块
    def set_weight_mode(self, weight):
        """
            设置路径计算的权重模式
        """
        self.weight = weight   #权重
        if self.weight == self.WEIGHT_MODEL['hop']:   #如果跳数是权重
            self.awareness.get_shortest_paths(weight=self.weight) #就根据跳数获取最短路径
        return True
#状态改变处理
    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        """
            收集datapath交换机的信息.
        """
        datapath = ev.datapath #事件相关的交换机
        if ev.state == MAIN_DISPATCHER: #如果是一般状态
            if not datapath.id in self.datapaths:  #交换机不在那群交换机里，就记录日志，并将其加入
                self.logger.debug('register datapath: %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:  #若事件处于解除状态
            if datapath.id in self.datapaths: #若交换机在那群交换机里，则打印解除注册的交换机，并删除它
                self.logger.debug('unregister datapath: %016x', datapath.id)
                del self.datapaths[datapath.id]
#增加流表
    def add_flow(self, dp, p, match, actions, idle_timeout=0, hard_timeout=0):
        """
           给datapath交换机发送一个流表.
        """
        ofproto = dp.ofproto  #协议
        parser = dp.ofproto_parser #协议解析

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,  #指令是 解析器解析出的 ofp采取 执行 actions 动作
                                             actions)]
#流表修改  datapath，优先权，软超时，硬超时，匹配，指令
        mod = parser.OFPFlowMod(datapath=dp, priority=p,
                                idle_timeout=idle_timeout,
                                hard_timeout=hard_timeout,
                                match=match, instructions=inst)
        dp.send_msg(mod)  #下发流表
#发送流表修改 datapath，流表信息，源端口，目的端口
    def send_flow_mod(self, datapath, flow_info, src_port, dst_port):
        """
           构建流入口，并将其发送到datapath
        """
        parser = datapath.ofproto_parser  #解析器
        actions = []  #空动作数组
        actions.append(parser.OFPActionOutput(dst_port)) #动作是在actions后插入 解析出的 目的端口
#匹配是 入端口，以太网类型，ipv4 源地址，ipv4 目的地址
        match = parser.OFPMatch(  
            in_port=src_port, eth_type=flow_info[0],
            ipv4_src=flow_info[1], ipv4_dst=flow_info[2])
#增加流表 datapath，匹配，动作，软超时，硬超时
        self.add_flow(datapath, 1, match, actions,
                      idle_timeout=15, hard_timeout=60)
#构建数据包输出
    def _build_packet_out(self, datapath, buffer_id, src_port, dst_port, data):
        """
            构建数据包输出对象
        """
        actions = []  #空的动作表
        if dst_port: #动作后添加 交换机解析出的动作输出的 目的端口
            actions.append(datapath.ofproto_parser.OFPActionOutput(dst_port))

        msg_data = None #消息的数据为空
        if buffer_id == datapath.ofproto.OFP_NO_BUFFER:  #如果缓存id是交换机的无缓存
            if data is None: #数据没有，就返回None
                return None
            msg_data = data 
#输出为 datapath，缓存id，消息数据，源端口，动作
        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=buffer_id,
            data=msg_data, in_port=src_port, actions=actions)
        return out
#发出数据包  datapath，缓存id，源端口，目的端口，data
    def send_packet_out(self, datapath, buffer_id, src_port, dst_port, data):
        """
            发送数据包到分配的datapath
        """
        out = self._build_packet_out(datapath, buffer_id,
                                     src_port, dst_port, data)
        if out:
            datapath.send_msg(out)
#获取端口
    def get_port(self, dst_ip, access_table):
        """
            获取接入端口 if dst host.
            access_table: {(sw,port) :(ip, mac)}
        """
        if access_table:
            if isinstance(access_table.values()[0], tuple):
                for key in access_table.keys():
                    if dst_ip == access_table[key][0]:
                        dst_port = key[1]
                        return dst_port
        return None
#从链路获取端口对
    def get_port_pair_from_link(self, link_to_port, src_dpid, dst_dpid):
        """
            获取端口对的链接，使控制器可以安装流入
        """
        if (src_dpid, dst_dpid) in link_to_port:
            return link_to_port[(src_dpid, dst_dpid)]
        else:
            self.logger.info("dpid:%s->dpid:%s is not in links" % (
                             src_dpid, dst_dpid))
            return None
#流
    def flood(self, msg):
        """
            流ARP数据包到没有记录的接入端口
        """
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        for dpid in self.awareness.access_ports:
            for port in self.awareness.access_ports[dpid]:
                if (dpid, port) not in self.awareness.access_table.keys():
                    datapath = self.datapaths[dpid]
                    out = self._build_packet_out(
                        datapath, ofproto.OFP_NO_BUFFER,
                        ofproto.OFPP_CONTROLLER, port, msg.data)
                    datapath.send_msg(out)
        self.logger.debug("Flooding msg")
#地址解析协议转发
    def arp_forwarding(self, msg, src_ip, dst_ip):
        """
        将ARP数据包发送到目标主机，如果存在dst主机记录，否则将其传送到未知访问端口。
        """
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        result = self.awareness.get_host_location(dst_ip)
        if result:  # host record in access table.
            datapath_dst, out_port = result[0], result[1]
            datapath = self.datapaths[datapath_dst]
            out = self._build_packet_out(datapath, ofproto.OFP_NO_BUFFER,
                                         ofproto.OFPP_CONTROLLER,
                                         out_port, msg.data)
            datapath.send_msg(out)
            self.logger.debug("Reply ARP to knew host")
        else:
            self.flood(msg)
#获取路径
    def get_path(self, src, dst, weight):
        """
            从网络感知模块获取最短路径
        """
        shortest_paths = self.awareness.shortest_paths
        graph = self.awareness.graph

        if weight == self.WEIGHT_MODEL['hop']:
            return shortest_paths.get(src).get(dst)[0]
        elif weight == self.WEIGHT_MODEL['delay']:
            # 如果存在路径，返回它，否则计算并保存
            try:
                paths = shortest_paths.get(src).get(dst)
                return paths[0]
            except:
                paths = self.awareness.k_shortest_paths(graph, src, dst,
                                                        weight=weight)

                shortest_paths.setdefault(src, {})
                shortest_paths[src].setdefault(dst, paths)
                return paths[0]
        elif weight == self.WEIGHT_MODEL['bw']:
            # 因为所有的路径都会计算在调用self.monitor.get_best_path_by_bw
            # 所以我们只需要在一段时间内调用一次，然后我们可以直接获取路径。
            try:
                # 如果路径存在，返回它
                path = self.monitor.best_paths.get(src).get(dst)
                return path
            except:
                # 否则，计算它，并返回它
                result = self.monitor.get_best_path_by_bw(graph,
                                                          shortest_paths)
                paths = result[1]
                best_path = paths.get(src).get(dst)
                return best_path
#获取交换机
    def get_sw(self, dpid, in_port, src, dst):
        """
            获取一对源和目的地交换机
        """
        src_sw = dpid
        dst_sw = None

        src_location = self.awareness.get_host_location(src)
        if in_port in self.awareness.access_ports[dpid]:
            if (dpid,  in_port) == src_location:
                src_sw = src_location[0]
            else:
                return None

        dst_location = self.awareness.get_host_location(dst)
        if dst_location:
            dst_sw = dst_location[0]

        return src_sw, dst_sw
#安装流
    def install_flow(self, datapaths, link_to_port, access_table, path,
                     flow_info, buffer_id, data=None):
        ''' 
            安装往返的流条目：往返
            @parameter: path=[dpid1, dpid2...]
                        flow_info=(eth_type, src_ip, dst_ip, in_port)
        '''
        if path is None or len(path) == 0:
            self.logger.info("Path error!")
            return
        in_port = flow_info[3]
        first_dp = datapaths[path[0]]
        out_port = first_dp.ofproto.OFPP_LOCAL
        back_info = (flow_info[0], flow_info[2], flow_info[1])

        # inter_link
        if len(path) > 2:
            for i in xrange(1, len(path)-1):
                port = self.get_port_pair_from_link(link_to_port,
                                                    path[i-1], path[i])
                port_next = self.get_port_pair_from_link(link_to_port,
                                                         path[i], path[i+1])
                if port and port_next:
                    src_port, dst_port = port[1], port_next[0]
                    datapath = datapaths[path[i]]
                    self.send_flow_mod(datapath, flow_info, src_port, dst_port)
                    self.send_flow_mod(datapath, back_info, dst_port, src_port)
                    self.logger.debug("inter_link flow install")
        if len(path) > 1:
            # the last flow entry: tor -> host
            port_pair = self.get_port_pair_from_link(link_to_port,
                                                     path[-2], path[-1])
            if port_pair is None:
                self.logger.info("Port is not found")
                return
            src_port = port_pair[1]

            dst_port = self.get_port(flow_info[2], access_table)
            if dst_port is None:
                self.logger.info("Last port is not found.")
                return

            last_dp = datapaths[path[-1]]
            self.send_flow_mod(last_dp, flow_info, src_port, dst_port)
            self.send_flow_mod(last_dp, back_info, dst_port, src_port)

            # 第一个流进入
            port_pair = self.get_port_pair_from_link(link_to_port,
                                                     path[0], path[1])
            if port_pair is None:
                self.logger.info("Port not found in first hop.")
                return
            out_port = port_pair[0]
            self.send_flow_mod(first_dp, flow_info, in_port, out_port)
            self.send_flow_mod(first_dp, back_info, out_port, in_port)
            self.send_packet_out(first_dp, buffer_id, in_port, out_port, data)

        # 源和目的在同一路径
        else:
            out_port = self.get_port(flow_info[2], access_table)
            if out_port is None:
                self.logger.info("Out_port is None in same dp")
                return
            self.send_flow_mod(first_dp, flow_info, in_port, out_port)
            self.send_flow_mod(first_dp, back_info, out_port, in_port)
            self.send_packet_out(first_dp, buffer_id, in_port, out_port, data)
#最短路径转发
    def shortest_forwarding(self, msg, eth_type, ip_src, ip_dst):
        """
            计算最短转发路径并将其安装到数据路径中
        """
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        result = self.get_sw(datapath.id, in_port, ip_src, ip_dst)
        if result:
            src_sw, dst_sw = result[0], result[1]
            if dst_sw:
                # 路径已经计算好了，仅需得到它.
                path = self.get_path(src_sw, dst_sw, weight=self.weight)
                self.logger.info("[PATH]%s<-->%s: %s" % (ip_src, ip_dst, path))
                flow_info = (eth_type, ip_src, ip_dst, in_port)
                # 将流条目安装到路径旁的数据路径
                self.install_flow(self.datapaths,
                                  self.awareness.link_to_port,
                                  self.awareness.access_table, path,
                                  flow_info, msg.buffer_id, msg.data)
        return
#数据包进入处理
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        '''
           在packet_in处理程序中，我们需要通过ARP学习access_table。 因此，未知主机的第一个包必须是ARP。
        '''
        msg = ev.msg
        datapath = msg.datapath
        in_port = msg.match['in_port']
        pkt = packet.Packet(msg.data)
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)

        if isinstance(arp_pkt, arp.arp):
            self.logger.debug("ARP processing")
            self.arp_forwarding(msg, arp_pkt.src_ip, arp_pkt.dst_ip)

        if isinstance(ip_pkt, ipv4.ipv4):
            self.logger.debug("IPV4 processing")
            if len(pkt.get_protocols(ethernet.ethernet)):
                eth_type = pkt.get_protocols(ethernet.ethernet)[0].ethertype
                self.shortest_forwarding(msg, eth_type, ip_pkt.src, ip_pkt.dst)
