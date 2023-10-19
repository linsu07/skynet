# skynet



## overview
Spark is widely used for large-scale data processing, but it can be challenging when multiple models need to work together to generate desired outputs. I encountered such a challenge while managing a web search anti-cheating project. The challenges I faced are listed below:

  + Three data resources with 10 billion rows.
  + Over 60 models responsible for producing features, with some high-order models relying on the output of low-order ones.
  + More than 50 models dedicated to cheat-hunting, consuming all the features.
  + Seven reports to be generated within a three-hour timeframe.
  To address these challenges, I designed an architecture called "skynet" that resolves all the mentioned issues. I believe skynet can be a general solution for many Spark tasks, so I am sharing it here with sensitive content deleted.
 
## features
#### concept
  The concept behind skynet is similar to "middleware" in software development, where each developer develops functions in their own "beans" with loose coupling.
#### beans
    class MarkovChain(override val uid:String) extends Estimator[MarkovChainModel] with AntiSpamAlgorithm with MarkovChainParam {
      def this() = this(Identifiable.randomUID("MarkovChain"))
      val signItem = "guid_search_num"
      val signItem_click = "guid_click_num"
      var voc:Array[String] = null
      override def setConfig(c: Config): this.type = {
        super.setConfig(c)  
  "Beans" in this context refer to models that extend the estimator or model interface in the Spark ML library, allowing easy organization and collaboration.
#### configuration file
    include "reference.conf"
    app_name = "pcsearchbox 2.6.x antispam"
    local=false
    rules_group = [
      feature_group
      guid_group
      srcg_guid_group
      srcg_group
      ip_group
      srcg_lifestage_group
      check_judge_group
    ]
    feature_group = {
      algorithm_list: [
        ${?HourModel}{ isJudgeModel = false, isFeatureModel = true,outputCols:[hour]}
        ${?IPsegModel}{ isJudgeModel = false, isFeatureModel = true,outputCols:[IPSeg]} 
  A configuration file defines the entire workflow, including the occurrence of models, model parameters, and their dependencies.
  
#### control program
    trait PcSearchBoxShadow extends GeneralApp {
      val executor = Executors.newFixedThreadPool(10)
      val interModels = new mutable.HashMap[String,ListBuffer[Model[_]]]()
      def init(args: Array[String], configFile: String = null) = {
        param = ParamFactory.getParams(args)
        if (configFile != null)
          config = ConfigFactory.load(configFile)
        context = new Context().setCurDay(param.day).setProduct(param.product).setModelPath(param.modelPath)
      }
    
      def transformModel(model: Model[_], data: DataFrame,saved:Boolean = false): DataFrame = {
        executor.submit(new Runnable {
          override def run(): Unit = {
            val am= model.asInstanceOf[AntiSpamAlgorithm]
  A control program reads the configuration file, initializes everything, and starts running until all the reports are generated.
#### feature group
    srcg_group = {
      groupkey:{statDimension:[site,srcg]}
    
      algorithm_list:[
        ${NumericFeatureAdd}{name:SecondSearchCount,inputCols:["srcType|srcType==search2"],outputCols:[secondSearchCount], isFeatureModel=true,isJudgeModel=false,transformTarget=["local"]}
        ${FeaBasicOperationFeatureADD}{name: SugFinalCount,expression: "secondSearchCount/SPV",outputCols:[secondSearchRate],isFeatureModel=true,isJudgeModel=false,transformTarget=["local"]}
        ${NumericFeatureAdd}{name:Age0Count,inputCols:["age|age==0"],outputCols:[age0Count], isFeatureModel=true,isJudgeModel=false,transformTarget=["local"]}
        ${FeaBasicOperationFeatureADD}{name:Age0Rate,expression:"age0Count/SPV",outputCols:[age0Rate], isFeatureModel=true,isJudgeModel=false,transformTarget=["local"]}
        ${FeaBasicOperationFeatureADD}{name:ADCPV2ADSPV,expression:"ADCPV/ADSPV",outputCols:[ADCPV2ADSPV],isFeatureModel=true,isJudgeModel=false,trainSource="global",transformTarget=["local"]}
      ]
    }  
  Estimators based on the same dimensions are grouped together, allowing for statistical analysis in a single data scan and saving CPU time.
   
   
## Requirements
   * spark>=2.4
   * scala     
