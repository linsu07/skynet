package org.apache.spark.ml.common.ua;

import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static java.util.Arrays.asList;

public class CFNetworkParameterizedTest extends
        AbstractUserAgentParameterizedTest {

	public CFNetworkParameterizedTest(String userAgentValue,
			Browser expectedBrowser, String browserVersionExpected,
			OperatingSystem osExpected) {
		super(userAgentValue, expectedBrowser, browserVersionExpected,
				osExpected);
	}

	@Parameters
	public static Collection<Object[]> testData() {
		return asList(new Object[][] {

				// CFNetworks
				{ "MobileSafari/9537.53 CFNetwork/672.0.8 Darwin/14.0.0",
						Browser.CFNETWORK, "672.0.8", OperatingSystem.MAC_OS_X },
				{ "MobileSafari/9537.53 CFNetwork/128 Darwin/14.0.0",
						Browser.CFNETWORK, "128", OperatingSystem.MAC_OS_X },
				{ "MobileSafari/9537.53 CFNetwork/129.9 Darwin/14.0.0",
						Browser.CFNETWORK, "129.9", OperatingSystem.MAC_OS_X },

		});
	}
}
