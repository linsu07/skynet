package org.apache.spark.ml.common.ua;

import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static java.util.Arrays.asList;

public class ToolParameterizedTest extends AbstractUserAgentParameterizedTest {

	public ToolParameterizedTest(String userAgentValue,
                                 Browser expectedBrowser, String browserVersionExpected,
                                 OperatingSystem osExpected) {
		super(userAgentValue, expectedBrowser, browserVersionExpected,
				osExpected);
	}

	@Parameters
	public static Collection<Object[]> testData() {
		return asList(new Object[][] {

				// tools
				{
						"curl/7.19.5 (i586-pc-mingw32msvc) libcurl/7.19.5 OpenSSL/0.9.8l zlib/1.2.3",
						Browser.DOWNLOAD, null, OperatingSystem.UNKNOWN },
				{ "Wget/1.8.1", Browser.DOWNLOAD, null, OperatingSystem.UNKNOWN }

		});
	}
}
