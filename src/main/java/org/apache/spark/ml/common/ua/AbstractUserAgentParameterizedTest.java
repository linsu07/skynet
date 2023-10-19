package org.apache.spark.ml.common.ua;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

/**
 * @author pcollaog
 *
 */
@RunWith(Parameterized.class)
public abstract class AbstractUserAgentParameterizedTest {

	private final String userAgentValue;

	private final Browser expectedBrowser;

	private final OperatingSystem expectedOS;

	private final String expectedBrowserVersion;

	/**
	 * @param userAgentValue
	 * @param browserExpected
	 */
	public AbstractUserAgentParameterizedTest(String userAgentValue,
			Browser expectedBrowser, String expectedBrowserVersion,
			OperatingSystem expectedOS) {
		this.userAgentValue = userAgentValue;
		this.expectedBrowser = expectedBrowser;
		this.expectedBrowserVersion = expectedBrowserVersion;
		this.expectedOS = expectedOS;
	}

	@Test
	public void shouldParseUserAgent() throws Exception {
		UserAgent userAgent = UserAgent
				.parseUserAgentString(this.userAgentValue);
		assertEquals(this.expectedBrowser, userAgent.getBrowser());

		Version browserVersion = userAgent.getBrowserVersion();
		if (null != browserVersion) {
			assertEquals(this.userAgentValue, this.expectedBrowserVersion, browserVersion.toString());
		} else {
			assertEquals(this.userAgentValue, this.expectedBrowserVersion, browserVersion);
		}

		OperatingSystem os = userAgent.getOperatingSystem();
		assertEquals(this.userAgentValue, expectedOS, os);
	}

}
