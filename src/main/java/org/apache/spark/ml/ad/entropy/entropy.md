###步骤
+ 在guid组，做出个人的分布列（个性化操作，行为间隔时间直接分好bin）
[(分布key:Count)]， 比如分布key是 ip，word，行为间隔时间，点击位置
+ 在guid组，对ip，word等做统计后的频率分布（统一操作）
[(频率区间key:Count)]
+ 在guid组，统计出全局分布空间，
    + 全局分布
    + 算出相对熵
    + 算出分布key对应的异常值， 0 为不异常
    + transform local，得到每个guid的异常分(每个分布key的异常值的平均）
    + 得到模型DistributeModel
+ 使用得到模型DistributeModel transform 统计组，加列
    + 全局分布
    + 相对熵
    + 算出分布key对应的异常值
    
    