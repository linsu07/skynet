# -*- coding: utf-8 -*-
"""
Created on Mon Feb  1 19:30:40 2021

@author: wangchengkun
"""

import pandas as pd
import numpy as np
from ClusterMethod import ClusterMethod
import os,sys
from Configs import Configs

from channels import get_excluded_channels

# 把当前路径加到path中，避免导入包的时候出错。


sys.path.append(os.getcwd())

class FindDistribution():
    def __init__(self, data, configs):
        
        self.configs = configs
        self.columns = configs["columns"]
        self.limitColumn = configs["limitColumn"]
        self.limitNum = configs["limitNum"]
        self.groupByKeys = configs["groupByKeys"]
        self.pathNum = configs["pathNum"]
        self.compareWay = configs["compareWay"]
        self.outputPath = configs["outputPath"]
        
        if "encoding" in configs:
            self.encoding = configs["encoding"]
        else:
            self.encoding = "utf-8"
            print("encoding default is 'utf-8'")
        
        if data:
            self.data = data
        elif "inputPath" in configs:
            self.inputPath = configs["inputPath"]
            self.data = pd.read_csv(self.inputPath, encoding=self.encoding, low_memory=False)
        else:
            raise KeyError("please set data or config 'inputPath'")
        self.data = self.data.sort_values(self.limitColumn, ascending=False)
        
        self.txtName = configs["txtName"]
        
    
    def keepOneInSamePage(self):
        """
        同一个site，保留请求数量最多，点击数量最多的的ls.

        Returns
        -------
        None.

        """
        sites = set(self.data["site"])
    
        channels = list()
        
        for site in sites:
            if site in ['unk']:
                continue
            temp = self.data.loc[self.data['site'] == site].sort_values([self.limitColumn ], ascending=False)
            temp = np.array(temp[['srcg', self.limitColumn]])
            
            reqNum = 0
            # maxClkNum = 0
            recodeChannel = 0
            for i in range(temp.shape[0]):
                diff = (temp[i][1]-reqNum)/(temp[i][1]+reqNum)
    
                # 如果差别较小，
                if abs(diff)<0.01:
                    # if temp[i][1] > maxClkNum:
                    #     maxClkNum = temp[i][1]
                    recodeChannel = temp[i][0]
                else:
                    if recodeChannel != 0:
                        channels.append(recodeChannel)
                    recodeChannel = temp[i][0]
                    # maxClkNum = temp[i][1]
                    reqNum = temp[i][1]
            # 保存最后一个
            channels.append(recodeChannel)
        self.data = pd.concat([self.data[self.data["srcg"].isin(channels)],self.data[self.data['site']=='unk']])
    
    
    def recurrentFind(self, tempData, columns, bestPath, currentPath):
        """
        递归寻找最优的分类序列.

        Parameters
        ----------
        tempData : DataFrame
            filter之后的数据.
        columns : List
            统计列名.
        bestPath : List
            保存最优结果的数组.
        currentPath : String
            当前路径.

        Raises
        ------
        KeyError
            compareWay参数配置出错.

        Returns
        -------
        None.

        """
        num = 0
        index = 0
        ls = len(tempData)
        pv = sum(list(tempData[self.limitColumn]))
        # 排序数据所在的位置
        if self.compareWay == "ls":
            num = ls
            index = 1
        elif self.compareWay == "pv":
            num = pv
            index = 2
        elif self.compareWay == "avg":
            num = pv/(ls if ls else 1)
            index = 3
        else:
            raise KeyError("wrong compareWay!")
        
        # 剪枝处理
        if num <= bestPath[0][index]:
            return
        
        currentPath = "%s\t%d\t%d" % (currentPath, ls, int(pv))
        
        if not columns:
            i = 0
            while i<len(bestPath):
                if num < bestPath[i][index]:
                    break
                i += 1
            site_col = self.groupByKeys[0]
            sorted_result = tempData.groupby(site_col).sum().sort_values(self.limitColumn, ascending=False)[self.limitColumn]
            site_info =  ["{}:{}".format(name,count) for name,count in list(zip(sorted_result.index,sorted_result.values))]
            bestPath.insert(i, (currentPath, ls, pv, pv/ls , site_info))
            
            # 只保存排名靠前的结果
            if len(bestPath)>self.pathNum:
                bestPath.pop(0)
            return
        
        column = columns[0]+"_tag"
        tags = list(set(tempData[column]))
        
        for tag in tags:
            newData = tempData.loc[tempData[column]==tag].copy()
            # 调整输出格式
            newPath = currentPath + "|" + str(tag)
            # 递归
            self.recurrentFind(newData, columns[1:], bestPath, newPath)
    
    
    def findBestPath(self):
        """
        寻找最优路径启动函数.

        Returns
        -------
        None.

        """
        # 初始化递归函数需要的参数
        tempData = self.data
        columns = list(self.columns.keys())
        currentPath = ""
        bestPath = [["", 0, 0, 0, ""]]
        self.recurrentFind(tempData, columns, bestPath, currentPath)
        # 打印结果
        bestPath = reversed(bestPath)
        print("\n")
        for path, ls, pv, avg, channels in bestPath:
            print("----------------------"*3)
            print("channels: %d \t pv: %d \t average: %.2f" % (ls, int(pv), avg))
            print("include channels: " + ",".join(channels[:self.pathNum]))
            if(len(path)==0):
                break
            path = path.split("|")[1:]
            for i in range(len(columns)):
                t = path[i].split("\t")
                print("\t%s\t tag:%s\t channels:%s\t pv:%s" % (columns[i], t[0], t[1], t[2]))
        return
        
    
    def customFilter(self):
        # 自定义filter
        self.data = self.data.loc[self.data[self.limitColumn]>=self.limitNum]
        channel_list = get_excluded_channels()
        self.data = self.data.loc[~self.data["site"].isin(channel_list)]
    
    
    def multiColumnStart(self):
        # 对输出文件预处理
        if os.path.exists(self.txtName):
            os.remove(self.txtName)
        # 自定义过滤
        self.customFilter()
        # 同一页面保留一个渠道
        self.keepOneInSamePage()
        
        print("total %s is %d" % (self.limitColumn,sum(self.data[self.limitColumn])))
        print("channels reserved is %d" % self.data.shape[0])
        print("\n")
        for column in self.columns.keys():
            print("----------------------"*3)
            print("\n")
            # 定义配置文件
            conf = Configs()
            conf.columnName = column
            conf.threshold = self.columns[column]
            # 聚类
            result = ClusterMethod(self.data, conf.__dict__).start()
            self.data = pd.merge(self.data, result, how="left", on=self.groupByKeys)
        self.findBestPath()
        
        # 结果存储
        saveList = self.groupByKeys+[column+"_tag" for column in self.columns.keys()]
        outputResult = self.data[saveList]

        outputResult.to_csv(self.outputPath, index=False, encoding=self.encoding)
        return self.data
        
        
 
def functionTest():
    # 函数测试入口
    inputPath = "./data/search_union_channel_group.csv" # 输出路径
    # columns = ["adindustryRatioStat", "totalActionRatioStat", "ipBinRatioStat",\
    #            "actionTimeGapsRatioStat", "adClickHourRatioStat", "posClickRatioStat",\
    #            "adClickPosYRatioStat", "adClickPosXRatioStat", "ADClickViewGapRatioStat"]
    data = pd.read_csv(inputPath, encoding='ansi')
    data = data.loc[~data["CGapBinRatioStat"].isnull()].head(30).copy()
    configs = Configs().__dict__
    
    c = ClusterMethod(data, configs)
    
    c.start()
    
    
    
if __name__ == "__main__":
    conf = Configs().__dict__
    f = FindDistribution(None, conf)
    f.multiColumnStart()
    # functionTest()
    
        