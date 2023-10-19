## 分组维度
##### 目前的分组维度
* channel
* channel，age
* channel, lifestage(young0-3,old(>=4)
* ip
* ipseg
* _guid_ 与其它维度不同


#### 单独的分组 guid
    guid 与其它维度看的特征不一样
特征|含义|均值|标准差
----|---- |----|--------
spv|请求量|2.72|49.90
cpv|点击量|3.587|36.78
adshow|广告曝光量|9.51|28.24
adclick|广告点击量|1.50|2.42
scrgCount |渠道个数|1|0.000
srcgCountMax|渠道最大请求量|OK
srcgCountMin|渠道最小请求量|OK
adshowIndustryCount|广告曝光行业数|1.31|0.70
adclickIndustryCount|广告点击行业数|1.046|0.242
adshowSubIndustryCount|广告曝光二级行业数|1.394|0.895
adclickSubIndustryCount|广告点击二级行业数|1.061|0.301
ipCount|ip个数|1.12|18.72
cityCount|城市个数|1.014|0.60
hourCount|活跃小时数|1.604|1.18
actionSpeed|行为速度|ok
actionList|行为链|ok
actionTimeList|行为时间链|ok
actionIntervalList|行为时间差链|ok
browserCount|浏览器个数 |1.01|0.23
browserNameDistribut|浏览器名称 |ok
firstSearchCount|一次搜索个数 |1.71|5.86
secondSearchCount|二次搜索个数|2.57|65
sugShowCount|联想词出现个数 |1.899|2.00
sugInCount|从联想词开始的查询|1.29|0.989
queryCount|搜索词个数（需要max）|2.42|20.96
pageCount|翻页个数 |1.08|0.53


#### 单独的分组 其他
    统计意义在5000以上
特征|含义|均值|标准差
----|---- |----|--------
CTR|点击率|0.83|0.028
SPV|请求量|OK
CPV|点击量|OK
SUV|请求UV|OK
CUV|点击UV|OK
SPV/SUV|人均搜索个数|2.01|0.1627
CPV/CUV|人均点击个数|2.434|0.115
CUV/SUV|点击人比例|0.689|0.00627
ADCTR|广告点击率|
ADSPV|广告曝光数|OK
ADCPV| 广告点击数|OK
ADSUV|广告曝光人数|OK
ADCUV| 广告点击人数|OK
ADCPV/ADSPV|曝光的广告里被点击的比例|0.0309|0.00235
ADSPV/ADSUV|出现广告人中人均曝光广告个数|6.831|0.294
ADCPV/ADCUV|广告人均点击个数|1.499|0.103
ADCUV/ADSUV|广告点击人比例|0.1405|0.00702
ADCUV/CUV|有点击的人中点广告的比例|0.0864|0.0046
secondSearchRate |二次搜索比例|0.4729|0.02159
sugSearchRate|联想词搜索比例|0.17|0.00963
lifeStageRate|年龄段比例|OK
age0Ratio|年龄是0的用户比例|0.0905|0.005
clickPosMaxRatio|点击最大位置的数量占比|0.495|0.0126
hourInfo|小时分布|OK
adClickHourInfo|广告点击小时分布|22.19|1.62
adClickHourMaxRatio|广告最多的那个小时占比|0.111|0.016
adClickPosInfo|广告点击位置分布|OK
adShowIndustryInfo|广告曝光的一级行业分布|36.21|0.956
adShowIndustryMaxRatio|广告曝光最多的行业占比|0.136|0.0168
adShowSubIndustryInfo|一级行业下二级行业分布
adClickIndustryInfo|广告点击一级行业分布|30.48|3.43
adClickSubIndustryInfo|广告点击一级行业下二级行业分布
ipInfo|ip分布情况（包含Info信息以及统计分bin信息）|ok
adClickIpInfo|广告点击ip分布情况（包含Info信息以及统计分bin信息）|ok
queryMaxRatio|搜索最多的词频占比|0.064|0.0139
adClickQueryInfo|广告点击搜索词分布情况（包含Info信息以及统计分bin信息）|ok
actionTimeGapInfo|思考时间分布
actionCountInfo|行为个数分布
actionEntropy|行为熵
cityinfo|城市分布（特殊排除IP投放类）|ok
adCityInfo|广告点击城市分布|ok
browserInfo|浏览器分布|ok
srcInfo|搜索src分布|ok
adClickAreaInfo|广告点击区域分布
adShowToClickTimeGapInfo|广告曝光到点击的时间间隔分布
SUV/ipInfoCount|一个IP对应n个用户|0.952|0.0285
SUV/QueryInfoSum|一个Query对应n个用户|ok



备注：以上分布需要UV分布

#### 规划
    以上所有的特征如果特别异常，spam，异常  anomaly
    以上所有特征尽量转换为特征值，进入算法判断
    
