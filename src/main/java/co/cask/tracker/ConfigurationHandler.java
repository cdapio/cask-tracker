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

package co.cask.tracker;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * This class handles requests to configuration APIs
 */
public class ConfigurationHandler extends AbstractHttpServiceHandler {

  private static final Gson GSON = new Gson();
  private static final Type HASH_MAP_TYPE = new TypeToken<HashMap<String, String>>() {
  }.getType();

  private KeyValueTable configTable;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    configTable = context.getDataset(TrackerApp.CONFIG_DATASET_NAME);
  }

  @Path("/v1/config")
  @GET
  public void getAllConfigurations(HttpServiceRequest request, HttpServiceResponder responder) {

    Map<String, String> results = new HashMap<>();
    CloseableIterator<KeyValue<byte[], byte[]>> iter = configTable.scan(null, null);
    while (iter.hasNext()) {
      KeyValue<byte[], byte[]> kv = iter.next();
      results.put(Bytes.toString(kv.getKey()), Bytes.toString(kv.getValue()));
    }
    responder.sendJson(HttpResponseStatus.OK.getCode(), results);
  }

  @Path("/v1/config/{configKey}")
  @GET
  public void getConfigValue(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("configKey") String configKey,
                             @QueryParam("strict") @DefaultValue("false") String strict) {

    List<Map<String, String>> results = new ArrayList<>();
    // Return an array with a single result if strict is true
    if (strict.equalsIgnoreCase("true")) {
      byte[] configValue = configTable.read(configKey);
      if (configValue == null) {
        responder.sendError(HttpResponseStatus.NOT_FOUND.getCode(), String.format("No configuration found for %s",
                                                                                  configKey));
      } else {
        Map<String, String> map = new HashMap<>();
        map.put(configKey, Bytes.toString(configValue));
        results.add(map);
        responder.sendJson(HttpResponseStatus.OK.getCode(), results);
      }
      return;
    }

    // Return an array with multiple results if strict is false
    CloseableIterator<KeyValue<byte[], byte[]>> iter = configTable.scan(null, null);
    while (iter.hasNext()) {
      Map<String, String> map = new HashMap<>();
      KeyValue<byte[], byte[]> kv = iter.next();
      String key = Bytes.toString(kv.getKey());
      String value = Bytes.toString(kv.getValue());
      if (key.toLowerCase().contains(configKey.toLowerCase())) {
        map.put(key, value);
        results.add(map);
      }
    }
    if (results.isEmpty()) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.getCode(), String.format("No configuration found for %s",
                                                                                configKey));
      return;
    }
    responder.sendJson(HttpResponseStatus.OK.getCode(), results);
  }

  @Path("/v1/config/{configKey}")
  @POST
  @PUT
  public void setConfigValue(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("configKey") String configKey) {
    ByteBuffer requestContents = request.getContent();

    // Send error if empty request is received
    if (!requestContents.hasRemaining()) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), "Request body is empty.");
      return;
    }

    if (request.getMethod().equalsIgnoreCase("Post") && configTable.read(configKey) != null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(),
                          String.format("Configuration for %s already exists.", configKey));
      return;
    }

    String payload = StandardCharsets.UTF_8.decode(requestContents).toString();
    HashMap<String, String> value = GSON.fromJson(payload, HASH_MAP_TYPE);
    configTable.write(configKey, value.get("value"));
    responder.sendStatus(HttpResponseStatus.OK.getCode());
  }

  @Path("/v1/config/{configKey}")
  @DELETE
  public void deleteConfigValue(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("configKey") String configKey) {

    byte[] value = configTable.read(Bytes.toBytes(configKey));
    if (value == null) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.getCode(), String.format("No configuration found for %s",
                                                                                configKey));
    } else {
      configTable.delete(Bytes.toBytes(configKey));
      responder.sendStatus(HttpResponseStatus.OK.getCode());
    }
  }
}
