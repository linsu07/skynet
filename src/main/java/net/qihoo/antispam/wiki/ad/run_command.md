## 项目相关运行命令
##### 数值求均值和方差命令
机器:cvdev2v
路径:/home/hdp-cvdev-w/qinsha/pcsearchbox_release
命令:sh -x run_srcg_mean_dev.sh -d "20200720,20200721,20200722,20200723,20200724,20200725,20200726" -f "ADClickPosStat,ADClickViewGapStat,adClickPosXStat,adClickPosYStat" -i "/home/hdp-saonline-w/pcad/v2.5.1_nature_srcg_all/" -g "/model/srcg_guid_group_featureFrame"  -o /home/hdp-cvdev-w/zhubaowen/guid_score_numric -e "net.qihoo.antispam.application.pc.ad.SrcgNumricMeanStddev" -j ./pcsearchbox2.5.0/anti-spam-skynet-2.5.0.jar
##### 分布求均值和方差命令 
机器:cvdev2v
路径:/home/hdp-cvdev-w/qinsha/pcsearchbox_release
命令:sh -x run_srcg_mean_dev.sh -d "20200720,20200721,20200722,20200723,20200724,20200725,20200726" -f "ADClickPosStat,ADClickViewGapStat,adClickPosXStat,adClickPosYStat" -i "/home/hdp-saonline-w/pcad/v2.5.1_nature_srcg_all/" -g "/model/srcg_group_featureFrame"  -o /home/hdp-cvdev-w/zhubaowen/srcg_distrbute -e "net.qihoo.antispam.application.pc.ad.SrcgDistributeMeanStddev" -j ./pcsearchbox2.5.0/anti-spam-skynet-2.5.0.jar
