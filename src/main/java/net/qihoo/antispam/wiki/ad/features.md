## 分组维度
##### 目前的分组维度
* channel
* channel，age
* channel, lifestage(young0-3,old(>=4)
* ip
* ipseg
* _guid_ 与其它维度不同


##### 智能分组
    所谓智能分组，考虑的是不同渠道在不同维度暴露出来得问题可能不一样
    如果ip聚集，首先选ip 维度
    其次 如果ipseg聚集，选 ipseg维度
    其次如果lifestage聚集，选lifestage
    再次 选channel
    每个用户的分组是之上的一种，一个分组下特征是一样的；
    广告主维度？
+ ctr
+ adctr
+ suv/cuv
+ ActionEntropy
+ UserActionTimeGap
+ UserActionCounts
+ SecondSearchProp
+ SearchtimeEntropy
+ ClicktimeEntropy
+ ClickPosEntropy
+ sugDisptooLow
+ 

#### 单独的分组 guid
    guid 与其它维度看的特征不一样
+ QueryChangeTooQuick
+ ActivitiesTooLong
+ BusyTimeZone
+ Guid2MoreBrowser
+ SearchClickTooQuick
+ SearchCrawler
+ CityChangeTooQuick
    

#### 规划
    以上所有的特征如果特别异常，spam，异常  anomaly
    以上所有特征尽量转换为特征值，进入算法判断
    
