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

package io.prestosql.rubix.hadoop2;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import io.prestosql.rubix.core.CachingFileSystem;
import io.prestosql.rubix.spi.ClusterType;

import java.io.IOException;

/**
 * Created by Kamesh Vankayala 06/19/2019
 */
public class CachingGoogleHadoopFileSystem extends CachingFileSystem<GoogleHadoopFileSystem>
{
  private static final String SCHEME = "gs";

  public CachingGoogleHadoopFileSystem() throws IOException
  {
    super();
  }

  public String getScheme()
  {
    return SCHEME;
  }

  @Override
  public ClusterType getClusterType() {
    return ClusterType.HADOOP2_CLUSTER_MANAGER;
  }
}