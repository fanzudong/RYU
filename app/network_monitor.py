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

from __future__ import division
import copy #复制
from operator import attrgetter #属性获取
from ryu import cfg #配置
from ryu.base import app_manager #应用管理
from ryu.base.app_manager import lookup_service_brick #查找服务链
from ryu.controller import ofp_event #ofp事件
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER #一般状态，解除状态
from ryu.controller.handler import CONFIG_DISPATCHER #配置状态
from ryu.controller.handler import set_ev_cls #设置事件类
from ryu.ofproto import ofproto_v1_3 #openflow 1.3
from ryu.lib import hub 
from ryu.lib.packet import packet #数据包
import setting #设置


CONF = cfg.CONF #配置

#网络监控类
class NetworkMonitor(app_manager.RyuApp):
    """
        网络监控是ryu的应用，用来收集网络流量信息
    """
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]    #openflow1.3
#初始化
    def __init__(self, *args, **kwargs):
        super(NetworkMonitor, self).__init__(*args, **kwargs)
        self.name = 'monitor'
        self.datapaths = {}  #记录与控制器连接的datapath交换机;
        self.port_stats = {} #保存端口的统计信息；
        self.port_speed = {} #保存端口的速率信息；
        self.flow_stats = {} #保存流的统计信息；
        self.flow_speed = {} #保存流的速率信息；
        self.stats = {}      #保存所有的统计信息；
        self.port_features = {} #保存端口特性
        self.free_bandwidth = {} #保存空闲带宽
        self.awareness = lookup_service_brick('awareness') #查找网络感知服务
        self.graph = None  #图
        self.capabilities = None #容量
        self.best_paths = None #最佳路径
        # 开始绿线程分别监控流量和计算链路的有效带宽。
        self.monitor_thread = hub.spawn(self._monitor) #监控线程
        self.save_freebandwidth_thread = hub.spawn(self._save_bw_graph) #保存空闲带宽线程
#监控状态改变并进行处理
    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        """
            记录datapath的信息
        """
        datapath = ev.datapath #交换机
        if ev.state == MAIN_DISPATCHER: #事件处于一般状态
            if not datapath.id in self.datapaths: #交换机不在那里面，就打印它，并加进去
                self.logger.debug('register datapath: %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER: #事件处于解除状态
            if datapath.id in self.datapaths: #交换机在里面，就打印它，并删除之
                self.logger.debug('unregister datapath: %016x', datapath.id)
                del self.datapaths[datapath.id]
#监控
    def _monitor(self):
        """
            监控流量的主要方法
        """
        while CONF.weight == 'bw':  #配置权重为带宽
            self.stats['flow'] = {} #统计流
            self.stats['port'] = {} #统计端口
            for dp in self.datapaths.values(): #若交换机在里面
                self.port_features.setdefault(dp.id, {}) #端口特性设置
                self._request_stats(dp) #交换机请求统计
                # 刷新数据
                self.capabilities = None #容量
                self.best_paths = None #最佳路径
            hub.sleep(setting.MONITOR_PERIOD) #设置监控周期
            if self.stats['flow'] or self.stats['port']: #如果是流统计或端口统计
                self.show_stat('flow') #显示流统计
                self.show_stat('port') #显示端口统计
                hub.sleep(1) #间隔1s
#保存带宽图
    def _save_bw_graph(self):
        """
            将带宽数据保存到networkx图形对象中
        """
        while CONF.weight == 'bw': #权重配置为 带宽
            self.graph = self.create_bw_graph(self.free_bandwidth) #创建带宽图
            self.logger.debug("save_freebandwidth") 
            hub.sleep(setting.MONITOR_PERIOD) #设置监控间隔
#请求统计
    def _request_stats(self, datapath):
        """
            发送请求msg到datapath
        """
        self.logger.debug('send stats request: %016x', datapath.id)#发送统计请求
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPPortDescStatsRequest(datapath, 0) #datapath端口描述统计请求
        datapath.send_msg(req) #发送请求

        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY) #端口统计请求
        datapath.send_msg(req)

        req = parser.OFPFlowStatsRequest(datapath) #流统计请求
        datapath.send_msg(req)
#获取最小带宽链路
    def get_min_bw_of_links(self, graph, path, min_bw):
        """
            获得路径带宽。 实际上，链路的最小带宽是带宽，因为它是路径的颈瓶。
        """
        _len = len(path) #路径长度
        if _len > 1: #长度>1，最小带宽
            minimal_band_width = min_bw
            for i in xrange(_len-1):
                pre, curr = path[i], path[i+1] #前一个，当前
                if 'bandwidth' in graph[pre][curr]: 
                    bw = graph[pre][curr]['bandwidth']
                    minimal_band_width = min(bw, minimal_band_width) #两者带宽取最小
                else:
                    continue
            return minimal_band_width #返回最小带宽
        return min_bw #返回最小带宽
#获取带宽最佳路径
    def get_best_path_by_bw(self, graph, paths):
        """
            通过比较路径获得最佳路径。
        """
        capabilities = {} #容量
        best_paths = copy.deepcopy(paths) #最佳路径

        for src in paths: 
            for dst in paths[src]:
                if src == dst:
                    best_paths[src][src] = [src]
                    capabilities.setdefault(src, {src: setting.MAX_CAPACITY})
                    capabilities[src][src] = setting.MAX_CAPACITY
                    continue
                max_bw_of_paths = 0
                best_path = paths[src][dst][0]
                for path in paths[src][dst]:
                    min_bw = setting.MAX_CAPACITY
                    min_bw = self.get_min_bw_of_links(graph, path, min_bw)
                    if min_bw > max_bw_of_paths:
                        max_bw_of_paths = min_bw
                        best_path = path

                best_paths[src][dst] = best_path
                capabilities.setdefault(src, {dst: max_bw_of_paths})
                capabilities[src][dst] = max_bw_of_paths
        self.capabilities = capabilities
        self.best_paths = best_paths
        return capabilities, best_paths
#创建带宽图
    def create_bw_graph(self, bw_dict):
        """
           将带宽数据保存到networkx图形对象中
        """
        try:
            graph = self.awareness.graph
            link_to_port = self.awareness.link_to_port
            for link in link_to_port:
                (src_dpid, dst_dpid) = link
                (src_port, dst_port) = link_to_port[link]
                if src_dpid in bw_dict and dst_dpid in bw_dict:
                    bw_src = bw_dict[src_dpid][src_port]
                    bw_dst = bw_dict[dst_dpid][dst_port]
                    bandwidth = min(bw_src, bw_dst)
                    # add key:value of bandwidth into graph. 在图中增加 键:带宽值 
                    graph[src_dpid][dst_dpid]['bandwidth'] = bandwidth
                else:
                    graph[src_dpid][dst_dpid]['bandwidth'] = 0
            return graph
        except:
            self.logger.info("Create bw graph exception")
            if self.awareness is None:
                self.awareness = lookup_service_brick('awareness')
            return self.awareness.graph
#保存空闲带宽
    def _save_freebandwidth(self, dpid, port_no, speed):
        # 计算端口的可用带宽并保存
        port_state = self.port_features.get(dpid).get(port_no)
        if port_state:
            capacity = port_state[2]
            curr_bw = self._get_free_bw(capacity, speed)
            self.free_bandwidth[dpid].setdefault(port_no, None)
            self.free_bandwidth[dpid][port_no] = curr_bw
        else:
            self.logger.info("Fail in getting port state")
#保存统计数据
    def _save_stats(self, _dict, key, value, length):
        if key not in _dict:
            _dict[key] = []
        _dict[key].append(value)

        if len(_dict[key]) > length:
            _dict[key].pop(0)
#获取速率
    def _get_speed(self, now, pre, period):
        if period:
            return (now - pre) / (period)
        else:
            return 0
#获取空闲带宽
    def _get_free_bw(self, capacity, speed):
        # BW:Mbit/s
        return max(capacity / 10**3 - speed * 8, 0)
#获取时间
    def _get_time(self, sec, nsec):
        return sec + nsec / (10 ** 9)
#获取周期
    def _get_period(self, n_sec, n_nsec, p_sec, p_nsec):
        return self._get_time(n_sec, n_nsec) - self._get_time(p_sec, p_nsec)
#流统计回复处理
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """
            将流统计信息保存到self.flow_stats中，计算流速并保存。
        """
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        self.stats['flow'][dpid] = body
        self.flow_stats.setdefault(dpid, {})
        self.flow_speed.setdefault(dpid, {})
        for stat in sorted([flow for flow in body if flow.priority == 1],
                           key=lambda flow: (flow.match.get('in_port'),
                                             flow.match.get('ipv4_dst'))):
            key = (stat.match['in_port'],  stat.match.get('ipv4_dst'),
                   stat.instructions[0].actions[0].port)
            value = (stat.packet_count, stat.byte_count,
                     stat.duration_sec, stat.duration_nsec)
            self._save_stats(self.flow_stats[dpid], key, value, 5)

            # 获得流的速率
            pre = 0
            period = setting.MONITOR_PERIOD
            tmp = self.flow_stats[dpid][key]
            if len(tmp) > 1:
                pre = tmp[-2][1]
                period = self._get_period(tmp[-1][2], tmp[-1][3],
                                          tmp[-2][2], tmp[-2][3])

            speed = self._get_speed(self.flow_stats[dpid][key][-1][1],
                                    pre, period)

            self._save_stats(self.flow_speed[dpid], key, speed, 5)
#端口状态回复处理
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        """
           保存端口的统计信息，计算端口的速度并保存。
        """
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        self.stats['port'][dpid] = body
        self.free_bandwidth.setdefault(dpid, {})

        for stat in sorted(body, key=attrgetter('port_no')):
            port_no = stat.port_no
            if port_no != ofproto_v1_3.OFPP_LOCAL:
                key = (dpid, port_no)
                value = (stat.tx_bytes, stat.rx_bytes, stat.rx_errors,
                         stat.duration_sec, stat.duration_nsec)

                self._save_stats(self.port_stats, key, value, 5)

                # 获取端口速率
                pre = 0
                period = setting.MONITOR_PERIOD
                tmp = self.port_stats[key]
                if len(tmp) > 1:
                    pre = tmp[-2][0] + tmp[-2][1]
                    period = self._get_period(tmp[-1][3], tmp[-1][4],
                                              tmp[-2][3], tmp[-2][4])

                speed = self._get_speed(
                    self.port_stats[key][-1][0] + self.port_stats[key][-1][1],
                    pre, period)

                self._save_stats(self.port_speed, key, speed, 5)
                self._save_freebandwidth(dpid, port_no, speed)
#端口描述统计回复处理
    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, MAIN_DISPATCHER)
    def port_desc_stats_reply_handler(self, ev):
        """
            保存端口描述信息
        """
        msg = ev.msg
        dpid = msg.datapath.id
        ofproto = msg.datapath.ofproto

        config_dict = {ofproto.OFPPC_PORT_DOWN: "Down",
                       ofproto.OFPPC_NO_RECV: "No Recv",
                       ofproto.OFPPC_NO_FWD: "No Farward",
                       ofproto.OFPPC_NO_PACKET_IN: "No Packet-in"}

        state_dict = {ofproto.OFPPS_LINK_DOWN: "Down",
                      ofproto.OFPPS_BLOCKED: "Blocked",
                      ofproto.OFPPS_LIVE: "Live"}

        ports = []
        for p in ev.msg.body:
            ports.append('port_no=%d hw_addr=%s name=%s config=0x%08x '
                         'state=0x%08x curr=0x%08x advertised=0x%08x '
                         'supported=0x%08x peer=0x%08x curr_speed=%d '
                         'max_speed=%d' %
                         (p.port_no, p.hw_addr,
                          p.name, p.config,
                          p.state, p.curr, p.advertised,
                          p.supported, p.peer, p.curr_speed,
                          p.max_speed))

            if p.config in config_dict:
                config = config_dict[p.config]
            else:
                config = "up"

            if p.state in state_dict:
                state = state_dict[p.state]
            else:
                state = "up"

            port_feature = (config, state, p.curr_speed)
            self.port_features[dpid][p.port_no] = port_feature
#端口状态处理
    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def _port_status_handler(self, ev):
        """
            处理端口状态改变事件
        """
        msg = ev.msg
        reason = msg.reason
        port_no = msg.desc.port_no
        dpid = msg.datapath.id
        ofproto = msg.datapath.ofproto

        reason_dict = {ofproto.OFPPR_ADD: "added",
                       ofproto.OFPPR_DELETE: "deleted",
                       ofproto.OFPPR_MODIFY: "modified", }

        if reason in reason_dict:

            print "switch%d: port %s %s" % (dpid, reason_dict[reason], port_no)
        else:
            print "switch%d: Illeagal port state %s %s" % (port_no, reason)
#显示统计
    def show_stat(self, type):
        '''
            根据数据类型显示统计信息 键入：'port'，'flow'
        '''
        if setting.TOSHOW is False:
            return

        bodys = self.stats[type]
        if(type == 'flow'):
            print('datapath         ''   in-port        ip-dst      '
                  'out-port packets  bytes  flow-speed(B/s)')
            print('---------------- ''  -------- ----------------- '
                  '-------- -------- -------- -----------')
            for dpid in bodys.keys():
                for stat in sorted(
                    [flow for flow in bodys[dpid] if flow.priority == 1],
                    key=lambda flow: (flow.match.get('in_port'),
                                      flow.match.get('ipv4_dst'))):
                    print('%016x %8x %17s %8x %8d %8d %8.1f' % (
                        dpid,
                        stat.match['in_port'], stat.match['ipv4_dst'],
                        stat.instructions[0].actions[0].port,
                        stat.packet_count, stat.byte_count,
                        abs(self.flow_speed[dpid][
                            (stat.match.get('in_port'),
                            stat.match.get('ipv4_dst'),
                            stat.instructions[0].actions[0].port)][-1])))
            print '\n'

        if(type == 'port'):
            print('datapath             port   ''rx-pkts  rx-bytes rx-error '
                  'tx-pkts  tx-bytes tx-error  port-speed(B/s)'
                  ' current-capacity(Kbps)  '
                  'port-stat   link-stat')
            print('----------------   -------- ''-------- -------- -------- '
                  '-------- -------- -------- '
                  '----------------  ----------------   '
                  '   -----------    -----------')
            format = '%016x %8x %8d %8d %8d %8d %8d %8d %8.1f %16d %16s %16s'
            for dpid in bodys.keys():
                for stat in sorted(bodys[dpid], key=attrgetter('port_no')):
                    if stat.port_no != ofproto_v1_3.OFPP_LOCAL:
                        print(format % (
                            dpid, stat.port_no,
                            stat.rx_packets, stat.rx_bytes, stat.rx_errors,
                            stat.tx_packets, stat.tx_bytes, stat.tx_errors,
                            abs(self.port_speed[(dpid, stat.port_no)][-1]),
                            self.port_features[dpid][stat.port_no][2],
                            self.port_features[dpid][stat.port_no][0],
                            self.port_features[dpid][stat.port_no][1]))
            print '\n'
