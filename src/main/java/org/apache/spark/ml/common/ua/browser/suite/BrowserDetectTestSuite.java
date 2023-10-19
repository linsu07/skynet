package org.apache.spark.ml.common.ua.browser.suite;

import org.apache.spark.ml.common.ua.browser.*;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
		BlackberryParameterizedTest.class, 
		BotParameterizedTest.class,
		CaminoParameterizedTest.class, 
		CFNetworkParameterizedTest.class,
		ChromeParameterizedTest.class,
		ChromeMobileParameterizedTest.class, 
		EdgeParameterizedTest.class,
		FirefoxParameterizedTest.class,
		InternetExplorerMobileParameterizedTest.class,
		InternetExplorerParameterizedTest.class,
		InternetExplorerXboxParameterizedTest.class,
		KonquerorParameterizedTest.class, 
		LotusParameterizedTest.class,
		LynxParameterizedTest.class, 
		OperaParameterizedTest.class,
		OthersParameterizedTest.class, 
		OutlookParameterizedTest.class,
		SafariParameterizedTest.class, 
		ThunderbirdParameterizedTest.class,
		ToolParameterizedTest.class })
public class BrowserDetectTestSuite {

}
