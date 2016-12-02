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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import co.cask.tracker.entity.DictionaryResult;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for DataDictionary
 */
public class DataDictionaryTest extends TestBase {
  private static final Gson GSON = new Gson();
  private static final DictionaryResult dictionaryInput = new DictionaryResult(null, "String", true, false,
                                                                               "test description", null);
  private static final String requestJson = GSON.toJson(dictionaryInput);
  private static final DictionaryResult dictionaryInput2 = new DictionaryResult(null, "String", true, false,
                                                                                "this is a description", null);
  private static final String requestJson2 = GSON.toJson(dictionaryInput2);
  private static final DictionaryResult dictionaryInput3 = new DictionaryResult(null, "Int", null, null,
                                                                                "newDescription", null);
  private static final String requestJson3 = GSON.toJson(dictionaryInput3);
  private ServiceManager dictionaryServiceManager;
  private String colName;
  private String colNameSecond;
  private String colNameThird;

  @Before
  public void configureService() throws Exception {
    ApplicationManager testAppManager = deployApplication(TestDictionaryApp.class);
    dictionaryServiceManager = testAppManager.getServiceManager(TrackerService.SERVICE_NAME).start();
    dictionaryServiceManager.waitForStatus(true);
  }

  // Tests for Add functionality
  @Test
  public void testAdd() throws Exception {
    colName = "mycol";

    // Test for adding a column
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName,
                                 "POST", requestJson, HttpResponseStatus.OK.getCode());
    DataSetManager<Table> outputmanager = getDataset(TrackerApp.DATA_DICTIONARY_DATASET_NAME);
    Get get = new Get(colName.toLowerCase());
    Row result = outputmanager.get().get(get);
    Assert.assertTrue(result.getString(DataDictionaryHandler.FieldNames.COLUMN_TYPE.getName()).
      equalsIgnoreCase(Schema.Type.STRING.name()));
    Assert.assertEquals(result.getString(DataDictionaryHandler.FieldNames.COLUMN_NAME.getName()), colName);
    Assert.assertTrue(result.getBoolean(DataDictionaryHandler.FieldNames.IS_NULLABLE.getName()));
    Assert.assertFalse(result.getBoolean(DataDictionaryHandler.FieldNames.IS_PII.getName()));

    // Test for duplicate column add
    String response = TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName, "POST",
                                                   requestJson, HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals("mycol already exists in data dictionary", response);
    outputmanager.flush();
  }

  @Test
  public void testWithNullValues() throws Exception {
    colNameSecond = "colWithNullValues";
    // Test for adding column with optional FieldNames as null
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colNameSecond, "POST", requestJson3,
                                 HttpResponseStatus.OK.getCode());
    DataSetManager<Table> outputmanager = getDataset(TrackerApp.DATA_DICTIONARY_DATASET_NAME);
    Row result = outputmanager.get().get(new Get(colNameSecond.toLowerCase()));
    Assert.assertEquals(dictionaryInput3.getColumnType(),
                        result.getString(DataDictionaryHandler.FieldNames.COLUMN_TYPE.getName()));
    outputmanager.flush();
  }

  @Test
  public void testGetAll() throws Exception {
    colName = "firstCol";
    colNameSecond = "secondCol";

    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName, "POST", requestJson,
                                 HttpResponseStatus.OK.getCode());
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colNameSecond, "POST", requestJson2,
                                 HttpResponseStatus.OK.getCode());
    String response = TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary", "GET",
                                                   HttpResponseStatus.OK.getCode());
    Type listType = new TypeToken<ArrayList<DictionaryResult>>() {
    }.getType();
    List<DictionaryResult> dictionaryResults = new Gson().fromJson(response, listType);
    Assert.assertNotNull(dictionaryResults);
    Assert.assertTrue(dictionaryResults.size() > 1);
  }

  @Test
  public void testUpdate() throws Exception {
    colName = "newColUpdate";

    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName, "POST", requestJson,
                                 HttpResponseStatus.OK.getCode());
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName, "PUT", requestJson3,
                                 HttpResponseStatus.OK.getCode());

    DataSetManager<Table> outputmanager = getDataset(TrackerApp.DATA_DICTIONARY_DATASET_NAME);
    Row result = outputmanager.get().get(new Get(colName.toLowerCase()));
    String colType = result.getString(DataDictionaryHandler.FieldNames.COLUMN_TYPE.getName());
    String desc = result.getString(DataDictionaryHandler.FieldNames.DESCRIPTION.getName());
    Assert.assertEquals(desc, dictionaryInput3.getDescription());
    Assert.assertEquals(colType, dictionaryInput3.getColumnType());
    outputmanager.flush();
  }

  @Test
  public void testGetDictionaryFromSchema() throws Exception {
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + "col1",
                                 "POST", requestJson, HttpResponseStatus.OK.getCode());
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + "col2",
                                 "POST", requestJson2, HttpResponseStatus.OK.getCode());
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + "col3",
                                 "POST", requestJson2, HttpResponseStatus.OK.getCode());
    List<String> inputColumns = new ArrayList<>();
    inputColumns.add("col1");
    inputColumns.add("col2");
    inputColumns.add("col4");

    String response = TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary", "POST",
                                                   GSON.toJson(inputColumns), HttpResponseStatus.OK.getCode());
    Type hashMapType = new TypeToken<HashMap<String, List>>() {
    }.getType();
    HashMap result = GSON.fromJson(response, hashMapType);
    List<String> errors = (List<String>) result.get(DataDictionaryHandler.ERROR);
    List<DictionaryResult> dictionaryresults = (List<DictionaryResult>) result.get(DataDictionaryHandler.RESULTS);

    Assert.assertEquals(1, errors.size());
    Assert.assertEquals(2, dictionaryresults.size());
    Assert.assertEquals(errors.get(0), "col4");
  }

  @Test
  public void testValidate() throws Exception {
    colName = "columnValidate1";
    colNameSecond = "columnValidate2";
    colNameThird = "wrongCol";
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/" + colName, "POST", requestJson,
                                 HttpResponseStatus.OK.getCode());

    // Assert response for wrong column name
    DictionaryResult dictionaryResult = new DictionaryResult(colNameThird, "Float", false, false, null, null);
    String responseWithWrongCol = TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/validate",
                                                               "POST", GSON.toJson(dictionaryResult),
                                                               HttpResponseStatus.NOT_FOUND.getCode());
    Type hashMapType = new TypeToken<HashMap<String, String>>() {
    }.getType();
    HashMap results = GSON.fromJson(responseWithWrongCol, hashMapType);
    Assert.assertEquals(2, results.size());

    // Assert values with wrong schema
    dictionaryResult = new DictionaryResult(colName, "Float", false, false, null, null);
    String responseWithErrors = TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/validate",
                                                             "POST", GSON.toJson(dictionaryResult),
                                                             HttpResponseStatus.CONFLICT.getCode());
    Type linkedHashMapType = new TypeToken<LinkedHashMap<String, Object>>() {
    }.getType();
    HashMap result = GSON.fromJson(responseWithErrors, linkedHashMapType);
    List<String> reason = (List<String>) result.get("reason");

    Assert.assertEquals(1, reason.size());
    Assert.assertEquals(7, result.size());
    Assert.assertTrue("Float".equalsIgnoreCase(result.get("columnType").toString()));
    Assert.assertTrue("String".equalsIgnoreCase(result.get("expectedType").toString()));

    // Assert status code with correct schema
    dictionaryResult = new DictionaryResult(colName, "String", false, false, null, null);
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/dictionary/validate", "POST",
                                 GSON.toJson(dictionaryResult), HttpResponseStatus.OK.getCode());
  }

  // Tests for configurations

  @Test
  public void testAddConfiguration() throws Exception {
    String key = "myKey";
    String keyValueJson = "{ \"value\" : \"configValue\" }";
    // Test for adding a configuration
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/" + key, "POST", keyValueJson,
                                 HttpResponseStatus.OK.getCode());
    DataSetManager<KeyValueTable> outputmanager = getDataset(TrackerApp.CONFIG_DATASET_NAME);
    Assert.assertTrue(Bytes.toString(outputmanager.get().read(key)).equalsIgnoreCase("configValue"));

    // Test for duplicate configurations add
    String response = TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/" + key, "POST", keyValueJson,
                                                   HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals("Configuration for myKey already exists.", response);
    outputmanager.flush();
  }

  @Test
  public void testDeleteConfiguration() throws Exception {
    String key = "myDeleteKey";
    String keyValueJson = "{ \"value\" : \"configValue\" }";
    // Test for Deleting a non existing configuration
    String response = TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/" + key, "DELETE",
                                                   HttpResponseStatus.NOT_FOUND.getCode());
    Assert.assertEquals("No configuration found for myDeleteKey", response);

    // Test for deletion
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/" + key, "POST", keyValueJson,
                                 HttpResponseStatus.OK.getCode());
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/" + key, "DELETE",
                                 HttpResponseStatus.OK.getCode());
  }

  @Test
  public void testGetConfiguration() throws Exception {
    String key = "myGetKey";
    String key2 = "myGetKey2";
    String dummyKey = "dummyKey";
    String keyValueJson = "{ \"value\" : \"configValue\" }";

    // Test for retrieving all configurations
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/" + key, "POST", keyValueJson,
                                 HttpResponseStatus.OK.getCode());
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/" + key2, "POST", keyValueJson,
                                 HttpResponseStatus.OK.getCode());
    String response = TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/" + key, "GET",
                                                   HttpResponseStatus.OK.getCode());

    Type hashMapArrayType = new TypeToken<ArrayList<HashMap<String, String>>>() {
    }.getType();
    List<Map<String, String>> result = GSON.fromJson(response, hashMapArrayType);
    Assert.assertEquals(2, result.size());

    // Test for retrieving an element with strict true
    response = TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/" + key + "?strict=true", "GET",
                                            HttpResponseStatus.OK.getCode());
    result = GSON.fromJson(response, hashMapArrayType);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("configValue", result.get(0).get(key));

    // Test for retrieving non existing configuration
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/" + dummyKey + "?strict=true", "GET",
                                 HttpResponseStatus.NOT_FOUND.getCode());
    TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/" + dummyKey, "GET",
                                 HttpResponseStatus.NOT_FOUND.getCode());

    Type hashMapType = new TypeToken<HashMap<String, String>>() {
    }.getType();
    response = TestUtils.getServiceResponse(dictionaryServiceManager, "v1/config/", "GET",
                                            HttpResponseStatus.OK.getCode());
    Map<String, String> fullResult = GSON.fromJson(response, hashMapType);
    Assert.assertTrue(fullResult.size() != 0);
  }
}
