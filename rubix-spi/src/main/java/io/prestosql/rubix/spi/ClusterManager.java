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
package io.prestosql.rubix.spi;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.SimpleNode;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

/**
 * Created by stagra on 14/1/16.
 */

/*
 * This class should be implemented for each engine.
 * The implementation should return the nodes in a form which the scheduler of that engine can recognize and route the splits to
 */
public abstract class ClusterManager
{
  private static Log log = LogFactory.getLog(ClusterManager.class);

  protected String currentNodeName;
  private String nodeHostname;
  private String nodeHostAddress;
  // Concluded from testing that Metro Hash results in better load distribution across the nodes in cluster.
  private final ConsistentHash<SimpleNode> consistentHashRing = HashRing.<SimpleNode>newBuilder()
          .hasher(DefaultHasher.METRO_HASH)
          .build();

  public abstract ClusterType getClusterType();

  /*
   * gets the nodes as per the engine
   * returns null in case node list cannot be fetched
   * returns empty in case of master-only setup
   */
  protected abstract Set<String> getNodesInternal();

  // Returns sorted list of nodes in the cluster
  public abstract Set<String> getNodes();

  protected String getCurrentNodeHostname()
  {
    return nodeHostname;
  }

  protected String getCurrentNodeHostAddress()
  {
    return nodeHostAddress;
  }

  protected synchronized Set<String> getNodesAndUpdateState()
  {
    Set<String> nodes = getNodesInternal();
    if (nodes == null) {
      nodes = ImmutableSet.of();
    } else if (nodes.isEmpty()) {
      // Empty result set => server up and only master node running, return localhost has the only node
      // Do not need to consider failed nodes list as 1node cluster and server is up since it replied to allNodesRequest
      nodes = ImmutableSet.of(getCurrentNodeHostAddress());
    }

    // remove stale nodes from consistent hash ring
    for (SimpleNode ringNode : consistentHashRing.getNodes()) {
      if (!nodes.contains(ringNode.getKey()))
      {
        log.debug("Removing node: " + ringNode.getKey() + " from consistent hash ring, Total nodes: " + consistentHashRing.getNodes());
        consistentHashRing.remove(ringNode);
      }
    }

    // add new nodes to consistent hash ring
    for (String node : nodes) {
      SimpleNode ringNode = SimpleNode.of(node);
      if (!consistentHashRing.contains(ringNode)) {
        log.debug("Adding node: " + ringNode.getKey() + " to consistent hash ring, Total nodes: " + consistentHashRing.getNodes());
        consistentHashRing.add(ringNode);
      }
    }

    if (currentNodeName == null) {
      if (consistentHashRing.contains(SimpleNode.of(getCurrentNodeHostname()))) {
        currentNodeName = getCurrentNodeHostname();
      }
      else if (consistentHashRing.contains(SimpleNode.of(getCurrentNodeHostAddress()))) {
        currentNodeName = getCurrentNodeHostAddress();
      }
      else {
        log.error(String.format("Could not initialize cluster nodes=%s nodeHostName=%s nodeHostAddress=%s " +
                "currentNodeIndex=%s", nodes, getCurrentNodeHostname(), getCurrentNodeHostAddress(), currentNodeName));
      }
    }
    return nodes;
  }

  public void initialize(Configuration conf)
          throws UnknownHostException
  {
    if (nodeHostname == null) {
      synchronized (this) {
        if (nodeHostname == null) {
          nodeHostname = InetAddress.getLocalHost().getCanonicalHostName();
          nodeHostAddress = InetAddress.getLocalHost().getHostAddress();
        }
      }
    }
  }

  public String locateKey(String key)
  {
    return consistentHashRing.locate(key).orElseThrow(() -> new RuntimeException("Unable to locate key: " + key)).getKey();
  }

  public String getCurrentNodeName()
  {
    // refresh cluster nodes first, which updates currentNodeName if it is not set.
    refreshClusterNodes();
    return currentNodeName;
  }

  private void refreshClusterNodes()
  {
    // getNodes() updates the currentNodeName
    Set<String> nodes = getNodes();
    if (nodes == null) {
      log.error("Initialization not done for Cluster Type: " + getClusterType());
      throw new RuntimeException("Unable to find current node name");
    }
  }
}
