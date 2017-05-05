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
import logging   #记录
import struct    #结构
import copy      #复制
import networkx as nx    #networkx网络拓扑仿真工具
from operator import attrgetter   #
from ryu import cfg      #配置
from ryu.base import app_manager  #应用管理
from ryu.controller import ofp_event  #ofp事件
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER  #主函数调度，数据调度
from ryu.controller.handler import CONFIG_DISPATCHER    #配置调度
from ryu.controller.handler import set_ev_cls   #设置事件类
from ryu.ofproto import ofproto_v1_3  #ofp1.3
from ryu.lib.packet import packet     #数据包
from ryu.lib.packet import ethernet   #以太网
from ryu.lib.packet import ipv4       #ipv4
from ryu.lib.packet import arp        #地址解析协议
from ryu.lib import hub               #

from ryu.topology import event, switches   #事件，交换机
from ryu.topology.api import get_switch, get_link   #获取交换机，获取链路
import setting   #设置


CONF = cfg.CONF

#网络感知类
class NetworkAwareness(app_manager.RyuApp):
    """
        NetworkAwareness是用于发现拓扑信息的Ryu应用程序。
        该应用程序可以为其他应用程序提供许多数据服务，如
        link_to_port，access_table，switch_port_table，access_ports，
        interior_ports，拓扑图和最短路径。

    """
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
#初始化
    def __init__(self, *args, **kwargs):
        super(NetworkAwareness, self).__init__(*args, **kwargs)
        self.topology_api_app = self
        self.name = "awareness"
        self.link_to_port = {}       # (src_dpid,dst_dpid)->(src_port,dst_port)
        self.access_table = {}       # {(sw,port) :[host1_ip]}
        self.switch_port_table = {}  # dpip->port_num
        self.access_ports = {}       # dpid->port_num
        self.interior_ports = {}     # dpid->port_num

        self.graph = nx.DiGraph()
        self.pre_graph = nx.DiGraph()
        self.pre_access_table = {}
        self.pre_link_to_port = {}
        self.shortest_paths = None

        # 启动一个绿色线程来发现网络资源。
        self.discover_thread = hub.spawn(self._discover)

    def _discover(self):
        i = 0
        while True:
            self.show_topology()
            if i == 5:
                self.get_topology(None)
                i = 0
            hub.sleep(setting.DISCOVERY_PERIOD)
            i = i + 1
#装饰交换机特征处理
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        """
            初始化操作，将table-miss流条目发送到数据路径。
        """
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        msg = ev.msg
        self.logger.info("switch:%s connected", datapath.id)

        # 安装 table-miss flow entry
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
#增加流
    def add_flow(self, dp, p, match, actions, idle_timeout=0, hard_timeout=0):
        ofproto = dp.ofproto
        parser = dp.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]

        mod = parser.OFPFlowMod(datapath=dp, priority=p,
                                idle_timeout=idle_timeout,
                                hard_timeout=hard_timeout,
                                match=match, instructions=inst)
        dp.send_msg(mod)
#获取主机位置
    def get_host_location(self, host_ip):
        """
            获取主机位置信息：（datapath，port）根据主机ip
        """
        for key in self.access_table.keys():
            if self.access_table[key][0] == host_ip:
                return key
        self.logger.info("%s location is not found." % host_ip)
        return None
#获取交换机
    def get_switches(self):
        return self.switches
#获取链路
    def get_links(self):
        return self.link_to_port

#获取图
    def get_graph(self, link_list):
        """
           从link_to_port获取邻接矩阵
        """
        for src in self.switches:
            for dst in self.switches:
                if src == dst:
                    self.graph.add_edge(src, dst, weight=0)
                elif (src, dst) in link_list:
                    self.graph.add_edge(src, dst, weight=1)
        return self.graph
#创建端口地图
    def create_port_map(self, switch_list):
        """
            创建interior_port表和access_port表. 
        """
        for sw in switch_list:
            dpid = sw.dp.id
            self.switch_port_table.setdefault(dpid, set())
            self.interior_ports.setdefault(dpid, set())
            self.access_ports.setdefault(dpid, set())

            for p in sw.ports:
                self.switch_port_table[dpid].add(p.port_no)
#创建内部链接
    def create_interior_links(self, link_list):
        """
            从link_list获取源端口到目的端口的链接,
            link_to_port:(src_dpid,dst_dpid)->(src_port,dst_port)
        """
        for link in link_list:
            src = link.src
            dst = link.dst
            self.link_to_port[
                (src.dpid, dst.dpid)] = (src.port_no, dst.port_no)

            # 查找访问端口和内部端口
            if link.src.dpid in self.switches:
                self.interior_ports[link.src.dpid].add(link.src.port_no)
            if link.dst.dpid in self.switches:
                self.interior_ports[link.dst.dpid].add(link.dst.port_no)
#创建访问端口
    def create_access_ports(self):
        """
            Get ports without link into access_ports 获取端口，无链接到access_ports
        """
        for sw in self.switch_port_table:
            all_port_table = self.switch_port_table[sw]
            interior_port = self.interior_ports[sw]
            self.access_ports[sw] = all_port_table - interior_port
# K最短路径
    def k_shortest_paths(self, graph, src, dst, weight='weight', k=1):
        """
            Creat K shortest paths of src to dst.创建源到目的K个最短路径
        """
        generator = nx.shortest_simple_paths(graph, source=src,
                                             target=dst, weight=weight)
        shortest_paths = []
        try:
            for path in generator:
                if k <= 0:
                    break
                shortest_paths.append(path)
                k -= 1
            return shortest_paths
        except:
            self.logger.debug("No path between %s and %s" % (src, dst))
#所有K个最短路径
    def all_k_shortest_paths(self, graph, weight='weight', k=1):
        """
            创建数据路径之间的所有K个最短路径。
        """
        _graph = copy.deepcopy(graph)
        paths = {}

        # 在图中查找ksp k shortest paths.
        for src in _graph.nodes():
            paths.setdefault(src, {src: [[src] for i in xrange(k)]})
            for dst in _graph.nodes():
                if src == dst:
                    continue
                paths[src].setdefault(dst, [])
                paths[src][dst] = self.k_shortest_paths(_graph, src, dst,
                                                        weight=weight, k=k)
        return paths

    # 列出应该被监听的事件列表
    events = [event.EventSwitchEnter,
              event.EventSwitchLeave, event.EventPortAdd,
              event.EventPortDelete, event.EventPortModify,
              event.EventLinkAdd, event.EventLinkDelete]
#装饰拓扑获取
    @set_ev_cls(events)
    def get_topology(self, ev):
        """
            获取拓扑信息并计算最短路径.
        """
        switch_list = get_switch(self.topology_api_app, None)
        self.create_port_map(switch_list)
        self.switches = self.switch_port_table.keys()
        links = get_link(self.topology_api_app, None)
        self.create_interior_links(links)
        self.create_access_ports()
        self.get_graph(self.link_to_port.keys())
        self.shortest_paths = self.all_k_shortest_paths(
            self.graph, weight='weight', k=CONF.k_paths)
#注册接入信息
    def register_access_info(self, dpid, in_port, ip, mac):
        """
            将访问主机信息注册到访问表中
        """
        if in_port in self.access_ports[dpid]:
            if (dpid, in_port) in self.access_table:
                if self.access_table[(dpid, in_port)] == (ip, mac):
                    return
                else:
                    self.access_table[(dpid, in_port)] = (ip, mac)
                    return
            else:
                self.access_table.setdefault((dpid, in_port), None)
                self.access_table[(dpid, in_port)] = (ip, mac)
                return
#装饰数据包正在处理
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """
            处理包中的数据包，并注册访问信息。
        """
        msg = ev.msg
        datapath = msg.datapath

        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        pkt = packet.Packet(msg.data)

        eth_type = pkt.get_protocols(ethernet.ethernet)[0].ethertype
        arp_pkt = pkt.get_protocol(arp.arp)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)

        if arp_pkt:
            arp_src_ip = arp_pkt.src_ip
            arp_dst_ip = arp_pkt.dst_ip
            mac = arp_pkt.src_mac

            # 记录接入信息
            self.register_access_info(datapath.id, in_port, arp_src_ip, mac)
#展示拓扑
    def show_topology(self):
        switch_num = len(list(self.graph.nodes()))
        if self.pre_graph != self.graph and setting.TOSHOW:
            print "---------------------Topo Link---------------------"
            print '%10s' % ("switch"),
            for i in self.graph.nodes():
                print '%10d' % i,
            print ""
            for i in self.graph.nodes():
                print '%10d' % i,
                for j in self.graph[i].values():
                    print '%10.0f' % j['weight'],
                print ""
            self.pre_graph = copy.deepcopy(self.graph)

        if self.pre_link_to_port != self.link_to_port and setting.TOSHOW:
            print "---------------------Link Port---------------------"
            print '%10s' % ("switch"),
            for i in self.graph.nodes():
                print '%10d' % i,
            print ""
            for i in self.graph.nodes():
                print '%10d' % i,
                for j in self.graph.nodes():
                    if (i, j) in self.link_to_port.keys():
                        print '%10s' % str(self.link_to_port[(i, j)]),
                    else:
                        print '%10s' % "No-link",
                print ""
            self.pre_link_to_port = copy.deepcopy(self.link_to_port)

        if self.pre_access_table != self.access_table and setting.TOSHOW:
            print "----------------Access Host-------------------"
            print '%10s' % ("switch"), '%12s' % "Host"
            if not self.access_table.keys():
                print "    NO found host"
            else:
                for tup in self.access_table:
                    print '%10d:    ' % tup[0], self.access_table[tup]
            self.pre_access_table = copy.deepcopy(self.access_table)
