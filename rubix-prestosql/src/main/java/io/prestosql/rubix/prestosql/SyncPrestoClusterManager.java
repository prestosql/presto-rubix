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

import io.prestosql.rubix.spi.ClusterType;
import io.prestosql.rubix.spi.SyncClusterManager;
import io.prestosql.spi.Node;

import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SyncPrestoClusterManager extends SyncClusterManager
{
  private static Log log = LogFactory.getLog(SyncPrestoClusterManager.class);
  private volatile Set<Node> workerNodes;

  @Override
  protected boolean hasStateChanged() {
    requireNonNull(PrestoClusterManager.NODE_MANAGER, "nodeManager is null");
    Set<Node> workerNodes = PrestoClusterManager.NODE_MANAGER.getWorkerNodes();
    boolean hasChanged = !workerNodes.equals(this.workerNodes);
    this.workerNodes = workerNodes;
    return hasChanged;
  }

  @Override
  public Set<String> getNodesInternal() {
    return ClusterManagerNodeGetter.getNodesInternal(PrestoClusterManager.NODE_MANAGER);
  }

  @Override
  protected String getCurrentNodeHostname() {
    return ClusterManagerNodeGetter.getCurrentNodeHostname(PrestoClusterManager.NODE_MANAGER);
  }

  @Override
  protected String getCurrentNodeHostAddress() {
    try {
      return ClusterManagerNodeGetter.getCurrentNodeHostAddress(PrestoClusterManager.NODE_MANAGER);
    }
    catch (UnknownHostException e) {
      log.warn("Could not get HostAddress from NodeManager", e);
      // fallback
    }
    return super.getCurrentNodeHostAddress();
  }

  @Override
  public ClusterType getClusterType()
  {
    return ClusterType.PRESTOSQL_CLUSTER_MANAGER;
  }
}
