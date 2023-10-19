# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 18:11:31 2021

@author: wangchengkun
"""
import math
import pandas as pd
import numpy as np
from multiprocessing import Pool, Manager
from scipy.stats import entropy
from sklearn.cluster import AgglomerativeClustering
from sklearn.manifold import TSNE
import matplotlib
import matplotlib.pyplot as plt
import os

matplotlib.rcParams['font.sans-serif'] = ['FangSong']
matplotlib.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题

class ClusterMethod():
    def __init__(self, data, configs):
        self.colors = ['black', 'gray', 'darkorange', 'blue', 'purple', 'red', 'gold', 'yellowgreen', 'green', 'cyan'
                       ,'black', 'gray', 'darkorange', 'blue', 'purple', 'red', 'gold', 'yellowgreen', 'green', 'cyan']
        
        self.keys = list()
        
        self.columnName = configs["columnName"]
        self.columnTagName = self.columnName+"_tag"
        self.columnVecName = self.columnName+"_vec"
        
        self.distanceWay = configs["distanceWay"]
        self.threshold = configs["threshold"]
        self.visualNum = configs["visualNum"]
        self.groupByKeys = configs["groupByKeys"]
        self.limitColumn = configs["limitColumn"]
        self.txtName = configs["txtName"]
        self.compareWay = configs["compareWay"]
        
        self.data = data.loc[~data[self.columnName].isnull()].copy()
        
        # 可选参数
        if "poolNum" in configs:
            self.poolNum = configs["poolNum"]
        else:
            self.poolNum = 8
        
        if "specialChannel" in configs:
            self.specialChannel = configs["specialChannel"]
        else:
            self.specialChannel = []
        
    
    def init_keys(self):
        """
        初始化keys

        Returns
        -------
        None.

        """
        temp = set()
        stat_demo = list(self.data[self.columnName])
        for stat in stat_demo:
            for key in stat.split("|"):
                temp.add(key.split(":")[0])
                
        # 当KEY全部是数字时，按照数字大小排序，否则按照字符排序
        t = True
        for k in temp:
            try:
                int(k)
            except ValueError:
                t = False
                break
        if t:
            temp = [int(i) for i in temp]
        else:
            temp = list(temp)
        temp.sort()
        
        # 方便后续使用join
        self.keys = [str(i) for i in temp]
        print("load keys complete.....")
        
        
    def stat2Vector(self, stat):
        """
        统计结果转换成向量

        Parameters
        ----------
        stat : String
            统计结果. Demo: 1:0.3|2:0.2|3:0.2|4:0.2|5:0.1

        Returns
        -------
        result : List[float]
            对应的向量. Demo: [0.3, 0.2, 0.2, 0.2, 0.1]

        """
        result = [0]*len(self.keys)
        for s in stat.split("|"):
            temp = s.split(":")
            result[self.keys.index(temp[0])] = float(temp[1])
        return result
    
    
    def calDistance(self):
        """
        计算距离矩阵

        Raises
        ------
        KeyError
            计算距离的方式不支持.

        Returns
        -------
        distanceMatrix : List[List]
            距离矩阵.

        """
        distanceMatrix = [0] * len(self.data)
        distanceMethod = None
        if self.distanceWay == "entropy":
            distanceMethod = self.entropyWithMultiProcess
        else:
            raise KeyError("only support entropy...")
        with Manager() as manager:
            p = Pool(self.poolNum)
            result = manager.list()
            
            for index in range(len(distanceMatrix)):
                p.apply_async(distanceMethod, args=(result, index,))
            p.close()
            p.join()

            for r in result:
                distanceMatrix[r[0]] = r[1:]
        return distanceMatrix
        
    
    def entropyWithMultiProcess(self, result, index):
        """
        多进程计算交叉熵，每一个index渠道与其他渠道的距离

        Parameters
        ----------
        result : Manager.list()
            共享内存中的数组，多进程交互用.
        index : int
            行索引.

        Returns
        -------
        None.

        """
        vectors = [np.array(i)+0.0001 for i in list(self.data[self.columnVecName])]
        origin_vector = vectors[index]
        temp = [index]
        for vec in vectors:
            temp.append(entropy(origin_vector, vec))
        result.append(temp)
    
    
    def dimensionReduce(self, n_components=2):
        """
        对数据进行降维统计.

        Parameters
        ----------
        n_components : int, optional
            降维后的维度. The default is 2.

        Returns
        -------
        None.

        """
        trainData = np.array(list(self.data[self.columnVecName]), dtype=np.float32)
        return TSNE(n_components=n_components, init="pca", method='exact').fit_transform(trainData)
        
        
        
    def plotClassFigure(self, plotData):
        """
        降降维后的结果展示

        Parameters
        ----------
        plotData : np.array
            降维后的结果.

        Returns
        -------
        fig : Figure
            绘制好的图片.

        """
        headTag = self.headTag[:self.visualNum]
        label = list(self.data[self.columnTagName])
        channels = list(self.data[self.groupByKeys[-1]])

        # 归一化处理
        x_min, x_max = np.min(plotData), np.max(plotData)
        plotData = (plotData - x_min) / (x_max - x_min)
        
        fig = plt.figure()
        for i in range(plotData.shape[0]):
            # 绘制特殊关注点
            if channels[i] in self.specialChannel:
                plt.text(plotData[i, 0], plotData[i, 1], channels[i], fontsize=10, color='olive')
            # 绘制分类点
            if label[i] in headTag:
                plt.scatter(plotData[i, 0], plotData[i, 1], c=self.colors[headTag.index(label[i])])
        plt.xticks([])
        plt.yticks([])
        show_number = min(self.visualNum,len(headTag))
        labelContet = ["%d: %s" % (headTag[i], self.colors[i]) for i in range(show_number)]
        plt.xlabel(" ".join(labelContet))
        plt.title("%s cluster result" % self.columnName)
        return fig
    
    
    def calClassEntropy(self, classData):
        """
        计算簇内密度

        Parameters
        ----------
        classData : List[list[]]
            每个类的向量.
    
        Returns
        -------
        Float
            簇内密度.
    
        """
        output = 0
        count = len(classData)
        
        for i in range(count):
            for j in range(i):
                output += entropy(np.array(classData[i])+0.0001, np.array(classData[j])+0.0001)
        return output/(count*(count-1)/2 if count>1 else 1)
    
    
    def getHeadClass(self):
        """
        根据参数compareWay,选择排名靠前的分类标签

        Raises
        ------
        KeyError
            compareWay配置错误.

        Returns
        -------
        None.

        """
        if self.compareWay == "ls":
            self.headTag = list(self.data[self.columnTagName].value_counts().index)
        elif self.compareWay == "pv":
            temp = self.data[[self.columnTagName, self.limitColumn]]
            temp = temp.groupby(self.columnTagName).sum().sort_values(self.limitColumn, ascending=False)
            self.headTag = list(temp.index)
        elif self.compareWay == "avg":
            temp = self.data[[self.columnTagName, self.limitColumn]]
            temp = temp.groupby(self.columnTagName).mean().sort_values(self.limitColumn, ascending=False)
            self.headTag = list(temp.index)
        else:
            raise KeyError("Wrong compareWay!")
    
    
    def getClusterStat(self):
        """
        计算包含渠道数量最多的前几个簇的统计数据。

        Returns
        -------
        output : dict
            每个簇的统计数据.

        """
        headTag = self.headTag[:self.visualNum]
        output = list()
        for index in range(len(headTag)):
            tagData = self.data.loc[self.data[self.columnTagName]==headTag[index]]
            vectors = list(tagData[self.columnVecName])
            # 计算密度
            density = self.calClassEntropy(vectors)
            statData = list(tagData[self.limitColumn])

            # 计算方差
            stdValue = list()
            tempArray = np.array([vec for vec in vectors])
            for i in range(tempArray.shape[1]):
                stdValue.append(np.std(tempArray[:,i]))
                
            # 计算均值，这里因为要加权，所以需要自定义计算
            # fir = np.array(vectors[0])*statData[0]
            # for i in range(1, len(vectors)):
            #     fir += (np.array(vectors[i])*statData[i])
            # vector = list(fir/sum(statData))
            fir = np.array(vectors[0])
            for i in range(1, len(vectors)):
                fir += (np.array(vectors[i]))
            vector = list(fir/len(statData))
            # 产出结果
            output.append( {
                              "tag":headTag[index],
                              "color": self.colors[index],
                              "nodes": len(vectors),
                              "stdValue": stdValue,
                              self.limitColumn: sum(statData),
                              "density": density,
                              "vector": vector}
                        )
        return output
    
    
    def showOutput(self, output):
        """
        产出结果的形式，如果要更改产出，在此处。

        Parameters
        ----------
        output : dict
            产出的结果，每个簇对应的统计信息.

        Returns
        -------
        fig : Figure
            统计折线图.

        """
        
        fig = plt.figure()
        f = open(self.txtName, 'a', encoding='utf-8')
        for values in output:
            print("----------------------"*3)
            plt.plot(self.keys, values["vector"], color=values["color"], marker='o')
            stdPrint =  "column: %s, tag: %d, color: %s, nodes: %d, %s: %d, average: %.4f density: %.4f \n" \
                    % (self.columnName, values["tag"], values["color"], values["nodes"], self.limitColumn,
                       values[self.limitColumn], values[self.limitColumn]/values["nodes"], values["density"])
            print(stdPrint.replace("column: %s, "%self.columnName, ""), end="")
            f.write(stdPrint)
            for i in range(len(self.keys)):
                s = "\t\"%s\" : [%.4f, %.4f] \n" % (self.keys[i], values["vector"][i], values["stdValue"][i])
                f.write(s)
            vec = ["%s: %.4f"%(self.keys[i], values["vector"][i]) for i in range(len(self.keys))]
            print("|".join(vec))
        plt.title(self.columnName)
        f.close()
        return fig
        
    
    def start(self):
        self.init_keys()
        print("%s \t channel number: %d" %(self.columnName, len(self.data)))
        self.data[self.columnVecName] = self.data[self.columnName].apply(self.stat2Vector)
        
        distanceMatrix = self.calDistance()
        print("calculate distance completed...")
        self.data[self.columnTagName] = AgglomerativeClustering(n_clusters=None, 
                                            affinity="precomputed", 
                                            distance_threshold=self.threshold, 
                                            linkage="average").fit_predict(distanceMatrix)
        self.getHeadClass()
        tsneResult = self.dimensionReduce()
        fig1 = self.plotClassFigure(tsneResult)
        plt.show(fig1)
        output = self.getClusterStat()
        fig2 = self.showOutput(output)
        plt.show(fig2)
        return self.data[self.groupByKeys+[self.columnTagName]]
