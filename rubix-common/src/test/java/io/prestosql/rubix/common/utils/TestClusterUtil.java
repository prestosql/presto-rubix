/**
 * Copyright (c) 2019. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package io.prestosql.rubix.common.utils;

import io.prestosql.rubix.common.metrics.MetricsReporterType;
import io.prestosql.rubix.spi.CacheConfig;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.prestosql.rubix.common.utils.ClusterUtil.getMetricsReporters;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestClusterUtil
{
  private static final String TEST_DEFAULT_MASTER_HOSTNAME = "localhost";
  private static final String TEST_MASTER_HOSTNAME = "123.456.789.0";
  private static final String TEST_YARN_RESOURCEMANAGER_ADDRESS = "255.255.255.1:1234";
  private static final String TEST_YARN_RESOURCEMANAGER_HOSTNAME = "255.255.255.1";

  private final Configuration conf = new Configuration();

  private String workingDirectory = System.getProperty("user.dir");
  private String rubixSiteXmlName;

  @AfterMethod
  public void clearConfiguration()
  {
    conf.clear();
  }

  /**
   * Verify that the <code>master.hostname</code> configuration value is returned if it is available.
   */
  @Test
  public void testGetMasterHostname_masterHostnameConf()
  {
    CacheConfig.setCoordinatorHostName(conf, TEST_MASTER_HOSTNAME);
    CacheConfig.setResourceManagerAddress(conf, TEST_YARN_RESOURCEMANAGER_ADDRESS);

    final String hostname = ClusterUtil.getMasterHostname(conf);
    assertEquals(hostname, TEST_MASTER_HOSTNAME, "Unexpected hostname!");
  }

  /**
   * Verify that the <code>yarn.resourcemanager.address</code> configuration value is returned if <code>master.hostname</code> is not available.
   */
  @Test
  public void testGetMasterHostname_yarnResourceManagerConf()
  {
    CacheConfig.setResourceManagerAddress(conf, TEST_YARN_RESOURCEMANAGER_ADDRESS);

    final String hostname = ClusterUtil.getMasterHostname(conf);
    assertEquals(hostname, TEST_YARN_RESOURCEMANAGER_HOSTNAME, "Unexpected hostname!");
  }

  /**
   * Verify that the default hostname is returned if alternative options are not available.
   */
  @Test
  public void testGetMasterHostname_noConfFound()
  {
    final String hostname = ClusterUtil.getMasterHostname(conf);
    assertEquals(hostname, TEST_DEFAULT_MASTER_HOSTNAME, "Unexpected hostname!");
  }

  @Test
  public void testPickLastConfigInRubixSiteXML()
  {
    Configuration configuration = new Configuration();
    ClusterUtil.rubixSiteExists = new AtomicReference<>();
    rubixSiteXmlName = workingDirectory + "/../rubix-common/src/test/resources/rubix-site-duplicate-key.xml";
    CacheConfig.setRubixSiteLocation(configuration, rubixSiteXmlName);
    configuration = ClusterUtil.applyRubixSiteConfig(configuration);
    assertEquals(configuration.get("rubix.cache.parallel.warmup"), "false", "Configuration is returning wrong value");
    assertNull(configuration.get("qubole.team"), "Configuration is returning wrong value");
    assertNotNull(ClusterUtil.rubixSiteExists.get(), "ClusterUtil.rubixSiteExists should not be null after applyingRubixConfig");
  }

  @Test
  public void testRubixConfigOnReapplying()
  {
    Configuration configuration = new Configuration();
    ClusterUtil.rubixSiteExists = new AtomicReference<>();
    rubixSiteXmlName = workingDirectory + "/../rubix-common/src/test/resources/rubix-site.xml";
    CacheConfig.setRubixSiteLocation(configuration, rubixSiteXmlName);
    configuration = ClusterUtil.applyRubixSiteConfig(configuration);
    assertEquals(configuration.get("rubix.cache.parallel.warmup"), "true", "Configuration is returning wrong value");
    assertEquals(configuration.get("rubix.team.example"), "true", "Configuration is returning wrong value");
    assertNull(configuration.get("qubole.team"), "Configuration is returning wrong value");

    rubixSiteXmlName = workingDirectory + "/../rubix-common/src/test/resources/rubix-site-duplicate-key.xml";
    CacheConfig.setRubixSiteLocation(configuration, rubixSiteXmlName);
    configuration = ClusterUtil.applyRubixSiteConfig(configuration);
    assertEquals(configuration.get("rubix.cache.parallel.warmup"), "true", "Configuration is returning wrong value");
    assertEquals(configuration.get("rubix.team.example"), "true", "Configuration is returning wrong value");
    assertNull(configuration.get("qubole.team"), "Configuration is returning wrong value");
  }

  @Test
  public void testConfigUnchanged()
  {
    Configuration configuration = new Configuration();
    ClusterUtil.rubixSiteExists = new AtomicReference<>();
    Configuration configuration2 = ClusterUtil.applyRubixSiteConfig(configuration);
    assertEquals(configuration, configuration2, "Returned config in no config should be unchanged");
  }

  @Test
  public void testEmptyRubixSite()
  {
    Configuration configuration = new Configuration();
    ClusterUtil.rubixSiteExists = new AtomicReference<>();
    rubixSiteXmlName = workingDirectory + "/../rubix-common/src/test/resources/faulty-rubix-site.xml";
    CacheConfig.setRubixSiteLocation(configuration, rubixSiteXmlName);
    Assert.assertThrows(Exception.class, () -> ClusterUtil.applyRubixSiteConfig(configuration));
  }

  @Test
  public void testGetMetricReportors()
  {
    Configuration conf = new Configuration();
    CacheConfig.setMetricsReporters(conf, "JmX, , ganglia   , , ,,");
    Set<MetricsReporterType> reporterSet = getMetricsReporters(conf);
    assertEquals(reporterSet.size(), 2, "Number of reporter not correct");
    assertTrue(reporterSet.contains(MetricsReporterType.JMX), "Metrics reporters not resolved correctly");
    assertTrue(reporterSet.contains(MetricsReporterType.GANGLIA), "Metrics reporters not resolved correctly");
  }
}