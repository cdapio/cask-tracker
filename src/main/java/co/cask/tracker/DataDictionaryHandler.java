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

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.tracker.entity.DictionaryResult;
import co.cask.tracker.utils.DiscoveryMetadataClient;
import co.cask.tracker.utils.ParameterCheck;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * This class handles requests to the DataDictionary API.
 */
public final class DataDictionaryHandler extends AbstractHttpServiceHandler {

  public static final String ENTITY_NAME = "entityName";
  public static final String TYPE = "type";
  public static final String IS_VALID = "isValid";
  public static final String RESULTS = "Results";
  public static final String ERROR = "Error";
  public static final Type LIST_TYPE = new TypeToken<List<String>>() {
  }.getType();
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(DataDictionaryHandler.class);

  private Table dataDictionaryTable;
  private byte[][] schema;

  @Property
  private String zookeeperQuorum;

  public DataDictionaryHandler(@Nullable String zookeeperQuorum) {
    this.zookeeperQuorum = zookeeperQuorum;
  }

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    dataDictionaryTable = context.getDataset(TrackerApp.DATA_DICTIONARY_DATASET_NAME);
    schema = new byte[][]{Bytes.toBytes(FieldNames.COLUMN_NAME.getName()),
      Bytes.toBytes(FieldNames.COLUMN_TYPE.getName()), Bytes.toBytes(FieldNames.IS_NULLABLE.getName()),
      Bytes.toBytes(FieldNames.IS_PII.getName()), Bytes.toBytes(FieldNames.DESCRIPTION.getName()),
      Bytes.toBytes(FieldNames.DATA_SETS.getName())};
  }

  @Path("/v1/dictionary/{columnName}")
  @POST
  public void add(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("columnName") String columnName) {
    ByteBuffer requestContents = request.getContent();

    // Send error if empty request is received.
    if (!requestContents.hasRemaining()) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), "Request body is empty.");
      return;
    }

    // Send error if column already exists in dataset.
    byte[] rowField = Bytes.toBytes(columnName.toLowerCase());
    byte[] existing = dataDictionaryTable.get(rowField, Bytes.toBytes(FieldNames.COLUMN_NAME.getName()));
    if (existing != null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), String.format("%s already exists in data" +
                                                                                    " dictionary", columnName));
      return;
    }

    // Send error if column type is invalid.
    String payload = StandardCharsets.UTF_8.decode(requestContents).toString();
    DictionaryResult colProperties = GSON.fromJson(payload, DictionaryResult.class);
    if (!ParameterCheck.isValidColumnType(colProperties.getColumnType())) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), String.format("%s is not a valid column type.",
                                                                                  columnName));
      return;
    }

    if (colProperties.getDescription() == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), String.format("Description can not be null for %s",
                                                                                  columnName));
      return;
    }

    boolean nullable = colProperties.isNullable() == null ? false : colProperties.isNullable();
    boolean pii = colProperties.isPII() == null ? false : colProperties.isPII();

    Map<String, Object> columnDatasetMetadata = getDatasetMetadata(request, columnName,
                                                                   colProperties.getColumnType());
    if ("false".equalsIgnoreCase((String) columnDatasetMetadata.get(IS_VALID))) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(),
                          String.format("Column %s exists with different type", columnName));
      return;
    }
    if (columnDatasetMetadata.get(FieldNames.DATA_SETS.getName()) != null) {
      colProperties.setDatasets((List<String>) columnDatasetMetadata.get(FieldNames.DATA_SETS.getName()));
    }
    // Add row to table and send  200 response code.
    byte[][] row = new byte[][]{Bytes.toBytes(columnName), Bytes.toBytes(colProperties.getColumnType()),
      Bytes.toBytes(nullable), Bytes.toBytes(pii), Bytes.toBytes(colProperties.getDescription()),
      Bytes.toBytes(colProperties.getDatasetsString())};
    dataDictionaryTable.put(rowField, schema, row);
    LOG.info("{} added to data dictionary", columnName);
    responder.sendStatus(HttpResponseStatus.OK.getCode());
  }

  @Path("/v1/dictionary")
  @GET
  public void getFullDictionary(HttpServiceRequest request, HttpServiceResponder responder) {
    List<DictionaryResult> results = new ArrayList<>();
    Row row;
    try (Scanner scanner = dataDictionaryTable.scan(null, null)) {
      while ((row = scanner.next()) != null) {
        results.add(createDictionaryResultFromRow(row));
      }
      responder.sendJson(HttpResponseStatus.OK.getCode(), results);
    }
  }

  @PUT
  @Path("/v1/dictionary/{columnName}")
  public void update(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("columnName") String columnName) {

    ByteBuffer requestContents = request.getContent();

    // Send error if empty request is received.
    if (!requestContents.hasRemaining()) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), "Request body is empty.");
      return;
    }

    // Send error if column does not already exist in dataset
    byte[] rowField = Bytes.toBytes(columnName.toLowerCase());
    Row existing = dataDictionaryTable.get(rowField);
    if (existing.isEmpty()) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.getCode(), String.format("%s is not present in data dictionary",
                                                                                columnName));
      return;
    }

    String payload = StandardCharsets.UTF_8.decode(requestContents).toString();
    DictionaryResult newColProperties = GSON.fromJson(payload, DictionaryResult.class);

    //Update the values of the field.
    Put put = new Put(rowField);
    if (newColProperties.getColumnType() != null) {
      // Send error if column type is invalid.
      if (!ParameterCheck.isValidColumnType(newColProperties.getColumnType())) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), String.format("%s is not a valid column type.",
                                                                                    columnName));
        return;
      }
      put.add(FieldNames.COLUMN_TYPE.getName(), newColProperties.getColumnType());
    }
    if (newColProperties.getDescription() != null) {
      put.add(FieldNames.DESCRIPTION.getName(), newColProperties.getDescription());
    }
    if (newColProperties.isNullable() != null) {
      put.add(FieldNames.IS_NULLABLE.getName(), newColProperties.isNullable());
    }
    if (newColProperties.isPII() != null) {
      put.add(FieldNames.IS_PII.getName(), newColProperties.isPII());
    }
    dataDictionaryTable.put(put);
    responder.sendStatus(HttpResponseStatus.OK.getCode());
  }

  @Path("/v1/dictionary/{columnName}")
  @DELETE
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("columnName") String columnName) {

    // Send error if column does not already exist in dataset
    byte[] rowField = Bytes.toBytes(columnName.toLowerCase());
    Row existing = dataDictionaryTable.get(rowField);
    if (existing.isEmpty()) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.getCode(), String.format("%s is not present in data dictionary",
                                                                                columnName));
      return;
    }
    dataDictionaryTable.delete(rowField);
    responder.sendStatus(HttpResponseStatus.OK.getCode());
  }

  @Path("/v1/dictionary/validate")
  @POST
  public void validate(HttpServiceRequest request, HttpServiceResponder responder) {

    ByteBuffer requestContents = request.getContent();

    // Send error if empty request is recieved
    if (!requestContents.hasRemaining()) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), "Request body is empty.");
      return;
    }
    String payload = StandardCharsets.UTF_8.decode(requestContents).toString();
    Schema recordSchema;
    try {
      recordSchema = Schema.parseJson(payload);
    } catch (IOException e) {
      LOG.error("Unable to parse schema {}", payload, e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), String.format("Unable to parse schema %s",
                                                                                  payload));
      return;
    }

    List<DictionaryResult> dictionaryResults = getDictionaryResultsfromSchema(recordSchema);
    List<Map> results = new ArrayList<>();
    for (DictionaryResult fromSchema: dictionaryResults) {
      Row row = dataDictionaryTable.get(new Get(fromSchema.getColumnName().toLowerCase()));
      Map<String, Object> result = new LinkedHashMap<>();
      if (row.isEmpty()) {
        result.put("columnName", fromSchema.getColumnName());
        List<String> reasonList = new ArrayList<>();
        reasonList.add("The column does not exist in the data dictionary.");
        result.put("reason", reasonList);
        results.add(result);
        continue;
      }
      DictionaryResult fromTable = createDictionaryResultFromRow(row);
      result = fromTable.validate(fromSchema);
      if (!result.isEmpty()) {
        results.add(result);
      }
    }
    if (results.isEmpty()) {
      responder.sendStatus(HttpResponseStatus.OK.getCode());
    } else {
      responder.sendJson(HttpResponseStatus.CONFLICT.getCode(), results);
    }
  }

  @Path("/v1/dictionary")
  @POST
  public void getDictionaryForSchema(HttpServiceRequest request, HttpServiceResponder responder) {
    ByteBuffer requestContents = request.getContent();
    // Send error if empty request is received.
    if (!requestContents.hasRemaining()) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.getCode(), "Request body is empty.");
      return;
    }

    String payload = StandardCharsets.UTF_8.decode(requestContents).toString();
    List<String> columns = GSON.fromJson(payload, LIST_TYPE);
    Map<String, List> results = new HashMap<>();
    List<String> errors = new ArrayList<>();
    List<DictionaryResult> dictionaryResults = new ArrayList<>();
    Row row;
    for (String column : columns) {
      row = dataDictionaryTable.get(new Get(column.toLowerCase()));
      if (row.isEmpty()) {
        errors.add(column);
      } else {
        dictionaryResults.add(createDictionaryResultFromRow(row));
      }
      results.put(RESULTS, dictionaryResults);
      results.put(ERROR, errors);
    }
    responder.sendJson(HttpResponseStatus.OK.getCode(), results);
  }

  public List<DictionaryResult> getDictionaryResultsfromSchema(Schema schema) {
    List<DictionaryResult> dictionaryResultList = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      dictionaryResultList.add(createDictionaryResultFromFieldSchema(field));
    }
    return dictionaryResultList;
  }

  private DictionaryResult createDictionaryResultFromFieldSchema(Schema.Field field) {
    String columnName = field.getName();
    Schema fieldSchema = field.getSchema();
    String coulmnType = (fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType())
      .toString();
    Boolean isNullable = fieldSchema.isNullable();
    return new DictionaryResult(columnName, coulmnType, isNullable, null, null, null);
  }

  /**
   * Method to create Dictionary result object from Row object returned from dictionary table
   *
   * @param row
   * @return {@link DictionaryResult} result
   */
  private DictionaryResult createDictionaryResultFromRow(Row row) {
    String datasetList;
    String columnName = row.getString(FieldNames.COLUMN_NAME.getName());
    String coulmnType = row.getString(FieldNames.COLUMN_TYPE.getName());
    Boolean isNullable = row.getBoolean(FieldNames.IS_NULLABLE.getName());
    Boolean isPII = row.getBoolean(FieldNames.IS_PII.getName());
    List<String> datasets;
    if ((datasetList = row.getString(FieldNames.DATA_SETS.getName())) == null) {
      datasets = new ArrayList<>();
    } else {
      datasets = Lists.newArrayList(Splitter.on(",").split(datasetList));
    }
    String description = row.getString(FieldNames.DESCRIPTION.getName());
    return new DictionaryResult(columnName, coulmnType, isNullable, isPII, description, datasets);
  }

  private Map<String, Object> getDatasetMetadata(HttpServiceRequest request, String column, String type) {
    NamespaceId namespaceId = new NamespaceId(getContext().getNamespace());
    Map<String, Object> results = new HashMap<>();
    List<String> entities = new ArrayList<>();
    results.put(IS_VALID, "true");
    try {
      List<HashMap<String, String>> searchRecords = DiscoveryMetadataClient.getInstance(request, zookeeperQuorum)
        .getMetadataSearchRecords(namespaceId, column);
      for (HashMap<String, String> map : searchRecords) {
        if (type.equalsIgnoreCase(map.get(TYPE))) {
          entities.add(map.get(ENTITY_NAME));
        } else {
          results.put(IS_VALID, "false");
          return results;
        }
      }
      // This exception is being thrown in the unit tests, to communicate with CDAP metadata from an app unit test. If
      // the DiscoveryMetadataClient could not be created the datasets for the column will be instantiated as an empty
      // list
    } catch (Exception e) {
      LOG.warn("Unable to fetch dataset information for {}", column, e);
    }
    results.put(FieldNames.DATA_SETS.getName(), entities);
    return results;
  }

  /**
   * Enum to hold field names for Data dictionary table
   */
  public enum FieldNames {
    COLUMN_NAME("columnName"),
    COLUMN_TYPE("columnType"),
    IS_NULLABLE("isNullable"),
    IS_PII("isPII"),
    DATA_SETS("datasets"),
    DESCRIPTION("description");

    private String name;

    FieldNames(String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }
  }
}
