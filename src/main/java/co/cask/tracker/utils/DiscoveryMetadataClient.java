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

import co.cask.cdap.proto.Id.Namespace;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

/**
 * extends MetadataClient to interact with CDAP Metadata
 */
public class DiscoveryMetadataClient {

  private MetadataClient mdc;
  private MetadataClient defaultMdc;

  public DiscoveryMetadataClient() {
    ConnectionConfig connectionConfig = ConnectionConfig.builder()
      .setHostname("127.0.0.1")
      .setPort(10000)
      .build();
    ClientConfig config = ClientConfig.builder().setConnectionConfig(connectionConfig).build();
    this.mdc = new MetadataClient(config);
    this.defaultMdc = new MetadataClient(ClientConfig.getDefault());
  }

  public Set<String> getTags(Namespace namespace) throws Exception {
    Set<MetadataSearchResultRecord> metadataSet =
          mdc.searchMetadata(namespace, "*", ImmutableSet.<MetadataSearchTargetType>of());
    Set<String> tagSet = new HashSet<>();
    for (MetadataSearchResultRecord mdsr: metadataSet) {
        Set<String> set = mdc.getTags(mdsr.getEntityId(), MetadataScope.USER);
        tagSet.addAll(set);
    }
    return tagSet;
  }

  public int getEntityNum(String tag, Namespace namespace) throws Exception {
    Set<MetadataSearchResultRecord> metadataSet =
      mdc.searchMetadata(namespace, tag, ImmutableSet.<MetadataSearchTargetType>of());
    return metadataSet.size();
  }

}