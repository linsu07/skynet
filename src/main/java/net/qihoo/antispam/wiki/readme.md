## MdEditor的功能列表演示

# 标题H1  
## 标题H2  
###### 标题H5
~~abcd~~   
<s>删除线（开启识别HTML标签时）</s>  
**ab**  
_abc_  
上标：X<sub>i,j</sub>  
下标：O<sup>x,k</sup>  
> 即更长的单词或短语的缩写形式，前提是开启识别HTML标签时，已默认开启   

如果想要插入空白换行`即<br />标签`，在插入处先键入两个以上的空格然后回车即可  
[普通链接](https://www.mdeditor.com/)  
即缩进四个空格，也做为实现类似 `<pre>` 预格式化文本 ( Preformatted Text ) 的功能。

    <?php
        echo "Hello world!";
    ?>
预格式化文本：

    | First Header  | Second Header |
    | ------------- | ------------- |
    | Content Cell  | Content Cell  |
    | Content Cell  | Content Cell  |
    
    ab
    cd


### 列表 Lists

#### 无序列表（减号）Unordered Lists (-)

- 列表一
- 列表二
- 列表三

#### 无序列表（星号）Unordered Lists (*)

* 列表一
* 列表二
* 列表三

#### 无序列表（加号和嵌套）Unordered Lists (+)
+ 列表一
+ 列表二
    + 列表二-1
    + 列表二-2
    + 列表二-3
+ 列表三
    * 列表一
    * 列表二
    * 列表三

#### 有序列表 Ordered Lists (-)

1. 第一行
2. 第二行
3. 第三行  


First Header  | Second Header
------------- | -------------
Content Cell  | Content Cell
Content Cell  | Content Cell

| Function name | Description                    |
| ------------- | ------------------------------ |
| `help()`      | Display the help window.       |
| `destroy()`   | **Destroy your computer!**     |