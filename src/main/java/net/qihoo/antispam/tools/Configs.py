# -*- coding: utf-8 -*-
"""
Created on Mon Feb  1 19:16:50 2021

@author: wangchengkun
"""

class Configs():
    
    def __init__(self):
        
        # 单列需要的配置
        self.columnName = "CGapBinRatioStat" # 列名
        self.distanceWay = "entropy" # 距离计算方式
        self.threshold = 0.047 # 距离阈值
        self.visualNum = 10 # 输出和图中簇的数量
        # 特别关注的渠道
        self.specialChannel = []
        
        
        
        # 多列需要的配置
        # 输入路径

        self.inputPath = "/tmp/pc/all/stat.csv"
        # self.inputPath = "/tmp/sslm/model/20210124_channel_group/part-00000-6e5eaa72-5235-496e-b338-e70cba0c4693-c000.csv"
        # 输出路径
        self.outputPath = "/tmp/pc/output.csv"
        # 最终产出结果数量
        self.pathNum = 10
        # 需要处理的统计列
        self.columns = {
                        # "CGapBinRatioStat": 0.067,
                        # "ClickIndexRatioStat":0.067
                        # "CHourRatioStat": 0.1,
                        # "CIPBinRatioStat": self.threshold,
                        # "CIPSceneRatioStat": self.threshold,
                        # "CUaRatioStat": self.threshold,
                        # "ReqHourRatioStat": self.threshold,
                        # "reqipBinRatioStat": self.threshold,
                        # "RIPSceneRatioStat": self.threshold,
                        # "SIPSceneRatioStat": 0.067,
                        # "ipBinRatioStat": 0.037,
                        "qBinRatioStat": 0.037,
                        # "SipBinRatioStat": 0.067,
                        }
        # 共同配置
        self.groupByKeys = ["srcg"]
        self.limitColumn = "SPV"
        self.limitNum = 10000
        # 配置文件输出路径
        self.txtName = "/tmp/pc/output_standard.txt"
        # 编码格式
        self.encoding = "utf-8"
        self.compareWay = "pv" # ls代表渠道数量，pv代表新增量, avg代表pv/ls
 
        
