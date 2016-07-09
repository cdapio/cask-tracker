/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tracker.utils;

import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A helper to use metadata client.
 */
public class MetadataClientHelper {
  private MetadataClient mdc;

  public MetadataClientHelper(String hostname, Integer port) {
    ConnectionConfig connectionConfig = ConnectionConfig.builder()
      .setHostname(hostname)
      .setPort(port)
      .build();
    ClientConfig config = ClientConfig.builder().setConnectionConfig(connectionConfig).build();
    this.mdc = new MetadataClient(config);
  }

  public MetadataClientHelper() {
    this.mdc = new MetadataClient(ClientConfig.getDefault());
  }

  public Set<String> getTags(NamespaceId namespace) throws IOException, UnauthenticatedException,
    NotFoundException, BadRequestException {
    Set<MetadataSearchResultRecord> metadataSet =
      mdc.searchMetadata(namespace.toId(), "*",
                         ImmutableSet.<MetadataSearchTargetType>of(MetadataSearchTargetType.DATASET,
                                                                                   MetadataSearchTargetType.STREAM));
    Set<String> tagSet = new HashSet<>();
    for (MetadataSearchResultRecord mdsr: metadataSet) {
      Set<String> set = mdc.getTags(mdsr.getEntityId(), MetadataScope.USER);
      tagSet.addAll(set);
    }
    return tagSet;
  }

  public int getEntityNum(String tag, NamespaceId namespace) throws IOException, UnauthenticatedException,
    NotFoundException, BadRequestException {
    Set<MetadataSearchResultRecord> metadataSet =
      mdc.searchMetadata(namespace.toId(), tag,
                         ImmutableSet.<MetadataSearchTargetType>of(MetadataSearchTargetType.DATASET,
                                                                                   MetadataSearchTargetType.STREAM));
    return metadataSet.size();
  }

}
