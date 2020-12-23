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
package io.prestosql.rubix.prestosql;

import io.prestosql.rubix.spi.ClusterManager;

import org.apache.hadoop.conf.Configuration;

public class TestSyncClusterManager extends TestClusterManager {
  @Override
  protected ClusterManager newPrestoClusterManager(Configuration conf, String hostAddress)
  {
    PrestoClusterManager.setNodeManager(new StandaloneNodeManager(conf, hostAddress));
    return new SyncPrestoClusterManager();
  }
}
