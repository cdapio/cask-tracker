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



/**
 * extends MetadataClient to interact with CDAP Metadata
 */
public class DiscoveryMetadataClient {
  private MetadataClient mdc;
  private MetadataClient default_mdc;
  public DiscoveryMetadataClient() {
    ConnectionConfig connectionConfig = ConnectionConfig.builder()
      .setHostname("127.0.0.1")
      .setPort(2181)
      .build();
    ClientConfig config = ClientConfig.builder().setConnectionConfig(connectionConfig).build();
    this.mdc = new MetadataClient(config);
    this.default_mdc = new MetadataClient(ClientConfig.getDefault());
  }
  public MetadataClient getMdc(){
    return this.mdc;
  }
  public MetadataClient getDefault_mdc(){
    return this.default_mdc;
  }
}
