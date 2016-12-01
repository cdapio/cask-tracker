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


import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.metadata.AbstractMetadataClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.tracker.DataDictionaryHandler;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Singleton class that extends AbstractMetadataClient, interact with CDAP (security)
 */
@ThreadSafe
public class DiscoveryMetadataClient extends AbstractMetadataClient {

  private static final String SCHEMA = "schema";
  private static final int ROUTER = 0;
  private static final int DISCOVERY = 1;
  private static final int BASE_DELAY = 500;
  private static final int MAX_DELAY = 2000;
  private static final Logger LOG = LoggerFactory.getLogger(DiscoveryMetadataClient.class);

  private static volatile DiscoveryMetadataClient client;

  private final int mode;
  private final Supplier<EndpointStrategy> endpointStrategySupplier;
  private final ClientConfig clientConfig;

  private DiscoveryMetadataClient(final DiscoveryServiceClient discoveryClient) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.METADATA_SERVICE));
      }
    });
    this.clientConfig = null;
    this.mode = DISCOVERY;
  }

  private DiscoveryMetadataClient(ClientConfig clientConfig) {
    this.endpointStrategySupplier = null;
    // simply make a copy, to ensure that the ClientConfig instance we use is never modified
    this.clientConfig = new ClientConfig.Builder(clientConfig).build();
    this.mode = ROUTER;
  }

  public static DiscoveryMetadataClient getInstance(HttpServiceRequest request,
                                                    String zookeeperQuorum) throws UnauthorizedException {
    try {
      String hostport = Objects.firstNonNull(request.getHeader("host"), request.getHeader("Host"));
      LOG.info("Creating ConnectionConfig using host and port {}", hostport);
      String hostName = hostport.split(":")[0];
      int port = Integer.parseInt(hostport.split(":")[1]);
      ConnectionConfig connectionConfig = ConnectionConfig.builder()
        .setHostname(hostName)
        .setPort(port)
        .build();
      ClientConfig config = ClientConfig.builder().setConnectionConfig(connectionConfig).build();
      try {
        new MetaClient(config).ping();
      } catch (IOException e) {
        config = ClientConfig.getDefault();
        LOG.debug("Got error while pinging router. Falling back to default client config: " + config, e);
      }
      return new DiscoveryMetadataClient(config);

      // create it based upon ClientConfig if you don't get an exception
    } catch (UnauthenticatedException e) {
      if (client != null) {
        return client;
      }
      synchronized (DiscoveryMetadataClient.class) {
        if (client != null) {
          return client;
        }
        // Authentication is enabled, so we can't go through router. Have to use discovery via zookeeper.
        // Note that we can't use zookeeper discovery in CDAP standalone.
        LOG.debug("Got error while pinging router. Falling back to DiscoveryMetadataClient.", e);
        LOG.info("Using discovery with zookeeper quorum {}", zookeeperQuorum);
        //delete "kafka" to make "/cdap/kafka" to "/cdap"
        ZKClientService zkClient = createZKClient(zookeeperQuorum.replace("/kafka", ""));
        zkClient.startAndWait();
        ZKDiscoveryService zkDiscoveryService = new ZKDiscoveryService(zkClient);
        client = new DiscoveryMetadataClient(zkDiscoveryService);
        return client;
      }
    }
  }

  private static ZKClientService createZKClient(String zookeeperQuorum) {
    Preconditions.checkNotNull(zookeeperQuorum, "Missing ZooKeeper configuration '%s'", Constants.Zookeeper.QUORUM);
    return ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(zookeeperQuorum)
            .build(),
          RetryStrategies.exponentialDelay(BASE_DELAY, MAX_DELAY, TimeUnit.MILLISECONDS)
        )
      )
    );
  }

  @Override
  protected HttpResponse execute(HttpRequest request, int... allowedErrorCodes)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    if (mode == DISCOVERY) {
      return HttpRequests.execute(request);
    } else {
      return new RESTClient(clientConfig).execute(request, clientConfig.getAccessToken());
    }
  }

  @Override
  protected URL resolve(Id.Namespace namespace, String path) throws MalformedURLException {
    if (mode == DISCOVERY) {
      InetSocketAddress addr = getMetadataServiceAddress();
      String url = String.format("http://%s:%d%s/%s/%s", addr.getHostName(), addr.getPort(),
                                 Constants.Gateway.API_VERSION_3, String.format("namespaces/%s", namespace.getId()),
                                 path);
      return new URL(url);
    } else {
      return clientConfig.resolveNamespacedURLV3(namespace, path);
    }
  }


  private InetSocketAddress getMetadataServiceAddress() {
    Discoverable discoverable = endpointStrategySupplier.get().pick(3L, TimeUnit.SECONDS);
    if (discoverable != null) {
      return discoverable.getSocketAddress();
    }
    throw new ServiceUnavailableException(Constants.Service.METADATA_SERVICE);
  }

  public int getEntityNum(String tag, NamespaceId namespace)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return searchMetadata(
      namespace.toId(), tag,
      ImmutableSet.of(MetadataSearchTargetType.DATASET, MetadataSearchTargetType.STREAM)).getResults().size();
  }

  public Set<String> getTags(NamespaceId namespace)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    Set<MetadataSearchResultRecord> metadataSet =
      searchMetadata(
        namespace.toId(), "*",
        ImmutableSet.of(MetadataSearchTargetType.DATASET, MetadataSearchTargetType.STREAM)).getResults();
    Set<String> tagSet = new HashSet<>();
    for (MetadataSearchResultRecord mdsr : metadataSet) {
      Set<String> set = getTags(mdsr.getEntityId().toId(), MetadataScope.USER);
      tagSet.addAll(set);
    }
    return tagSet;
  }


  public Set<String> getEntityTags(NamespaceId namespace, String entityType, String entityName)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    if (entityType.toLowerCase().equals("dataset")) {
      DatasetId datasetId = new DatasetId(namespace.getNamespace(), entityName);
      return getTags(datasetId.toId(), MetadataScope.USER);
    } else {
      StreamId streamId = new StreamId(namespace.getNamespace(), entityName);
      return getTags(streamId.toId(), MetadataScope.USER);
    }
  }

  public void addTags(NamespaceId namespace, String entityType, String entityName, List<String> tagList)
    throws UnauthenticatedException, BadRequestException, NotFoundException, IOException, UnauthorizedException {
    if (entityType.toLowerCase().equals("dataset")) {
      DatasetId datasetId = new DatasetId(namespace.getNamespace(), entityName);
      addTags(datasetId.toId(), new HashSet<>(tagList));
    } else {
      StreamId streamId = new StreamId(namespace.getNamespace(), entityName);
      addTags(streamId.toId(), new HashSet<>(tagList));
    }
  }

  public boolean deleteTag(NamespaceId namespace, String entityType, String entityName, String tagName) {
    try {
      if (entityType.toLowerCase().equals("dataset")) {
        DatasetId datasetId = new DatasetId(namespace.getNamespace(), entityName);
        removeTag(datasetId.toId(), tagName);
      } else {
        StreamId streamId = new StreamId(namespace.getNamespace(), entityName);
        removeTag(streamId.toId(), tagName);
      }
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public List<HashMap<String, String>> getMetadataSearchRecords(NamespaceId namespace, String column)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    List<HashMap<String, String>> datasets = new ArrayList<>();
    Set<MetadataSearchResultRecord> metadataSet =
      searchMetadata(
        namespace.toId(), column,
        ImmutableSet.of(MetadataSearchTargetType.DATASET, MetadataSearchTargetType.STREAM)).getResults();
    for (MetadataSearchResultRecord mdsr : metadataSet) {
      Map<String, String> map = getProperties(mdsr.getEntityId().toId());
      HashMap<String, String> record = new HashMap<>();
      Schema datasetSchema = Schema.parseJson(map.get(SCHEMA));
      record.put(DataDictionaryHandler.ENTITY_NAME, mdsr.getEntityId().getEntityName());
      record.put(DataDictionaryHandler.TYPE, datasetSchema.getField(column).getSchema().getType().toString());
      datasets.add(record);
    }
    return datasets;
  }
}
