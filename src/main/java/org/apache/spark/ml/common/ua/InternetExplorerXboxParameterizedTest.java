package org.apache.spark.ml.common.ua;

import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static java.util.Arrays.asList;

public class InternetExplorerXboxParameterizedTest extends AbstractUserAgentParameterizedTest {

	public InternetExplorerXboxParameterizedTest(String userAgentValue,
                                                 Browser expectedBrowser, String browserVersionExpected,
                                                 OperatingSystem osExpected) {
		super(userAgentValue, expectedBrowser, browserVersionExpected,
				osExpected);
	}

	@Parameters
	public static Collection<Object[]> testData() {
		return asList(new Object[][] {

				{
						"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0; Xbox; Xbox One)",
						Browser.IE_XBOX, "10.0", OperatingSystem.XBOX_OS },
				{
						"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; Xbox)",
						Browser.IE_XBOX, "9.0", OperatingSystem.XBOX_OS }
		});
	}
}
