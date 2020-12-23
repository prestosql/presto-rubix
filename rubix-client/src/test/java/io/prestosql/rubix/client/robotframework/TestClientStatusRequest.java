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
package io.prestosql.rubix.client.robotframework;

import io.prestosql.rubix.spi.ClusterType;

public class TestClientStatusRequest
{
  private final String remotePath;
  private final long fileLength;
  private final long lastModified;
  private final long startBlock;
  private final long endBlock;
  private final int clusterType;

  public TestClientStatusRequest(String remotePath,
                                 long fileLength,
                                 long lastModified,
                                 long startBlock,
                                 long endBlock,
                                 int clusterType)
  {
    this.remotePath = remotePath;
    this.fileLength = fileLength;
    this.lastModified = lastModified;
    this.startBlock = startBlock;
    this.endBlock = endBlock;
    this.clusterType = clusterType;
  }

  public String getRemotePath()
  {
    return remotePath;
  }

  public long getStartBlock()
  {
    return startBlock;
  }

  public long getEndBlock()
  {
    return endBlock;
  }

  public long getFileLength()
  {
    return fileLength;
  }

  public long getLastModified()
  {
    return lastModified;
  }

  public int getClusterType()
  {
    return clusterType;
  }

  @Override
  public String toString()
  {
    return String.format(
        "Status request for file %s [%s-%s] (%sB / LM: %s) {Cluster type: %s}",
        remotePath,
        startBlock,
        endBlock,
        fileLength,
        lastModified,
        ClusterType.findByValue(clusterType));
  }
}
