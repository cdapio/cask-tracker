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

import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.audit.payload.access.AccessType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.tracker.entity.AuditHistogramResult;
import co.cask.tracker.entity.AuditLogResponse;
import co.cask.tracker.entity.TagsResult;
import co.cask.tracker.entity.TopApplicationsResult;
import co.cask.tracker.entity.TopDatasetsResult;
import co.cask.tracker.entity.TopProgramsResult;
import co.cask.tracker.entity.TrackerMeterRequest;
import co.cask.tracker.entity.TrackerMeterResult;
import co.cask.tracker.entity.ValidateTagsResult;
import co.cask.tracker.utils.ParameterCheck;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link TrackerApp}.
 */
public class TrackerAppTest extends TestBase {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();
  private static ApplicationManager testAppManager;
  private static ServiceManager trackerServiceManager;

  private static final Type DATASET_LIST = new TypeToken<List<TopDatasetsResult>>() { }.getType();
  private static final Type PROGRAM_LIST = new TypeToken<List<TopProgramsResult>>() { }.getType();
  private static final Type APPLICATION_LIST = new TypeToken<List<TopApplicationsResult>>() { }.getType();
  private static final Type TIMESINCE_MAP = new TypeToken<Map<String, Long>>() { }.getRawType();

  private static final String TEST_JSON_TAGS = "[\"tag1\",\"tag2\",\"tag3\",\"ta*4\"]";
  private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTabcdefghijklmnopqrst123/*!";
  private static final int TEST_STRING_LIST_LENGTH = 3000;
  private static final int STRING_LENGTH = 60;
  private static final int SEED = 0;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Before
  public void configureStream() throws Exception {
    testAppManager = deployApplication(TestAuditLogPublisherApp.class);
    FlowManager testFlowManager = testAppManager.getFlowManager(StreamToAuditLogFlow.FLOW_NAME).start();
    testFlowManager.waitForStatus(true);

    trackerServiceManager = testAppManager.getServiceManager(TrackerService.SERVICE_NAME).start();
    trackerServiceManager.waitForStatus(true);

    StreamManager streamManager = getStreamManager("testStream");
    List<AuditMessage> testData = generateTestData();
    for (AuditMessage auditMessage : testData) {
      streamManager.send(GSON.toJson(auditMessage));

    }
    RuntimeMetrics metrics = testFlowManager.getFlowletMetrics("auditLogPublisher");
    metrics.waitForProcessed(testData.size(), 60L, TimeUnit.SECONDS);
  }

  @After
  public void destroyApp() throws Exception {
    testAppManager.stopAll();
    clear();
  }

  @Test
  public void testAuditLog() throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/auditlog/stream/stream1",
                                         HttpResponseStatus.OK.getCode());
    AuditLogResponse result = GSON.fromJson(response, AuditLogResponse.class);
    Assert.assertNotEquals(0, result.getTotalResults());
    response = getServiceResponse(trackerServiceManager,
                                  "v1/auditlog/dataset/ds1",
                                  HttpResponseStatus.OK.getCode());
    result = GSON.fromJson(response, AuditLogResponse.class);
    Assert.assertNotEquals(0, result.getTotalResults());
  }

  @Test
  public void testInvalidDatesError() throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/auditlog/stream/stream1?startTime=1&endTime=0",
                                         HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals(ParameterCheck.STARTTIME_GREATER_THAN_ENDTIME, response);
  }

  @Test
  public void testInvalidOffset() throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/auditlog/stream/stream1?offset=-1",
                                         HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals(ParameterCheck.OFFSET_INVALID, response);
  }

  @Test
  public void testInvalidLimit() throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/auditlog/stream/stream1?limit=-1",
                                         HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals(ParameterCheck.LIMIT_INVALID, response);
  }

  @Test
  public void testTopNDatasets() throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/auditmetrics/top-entities/datasets?limit=20",
                                         HttpResponseStatus.OK.getCode());
    List<TopDatasetsResult> result = GSON.fromJson(response, DATASET_LIST);
    Assert.assertEquals(6, result.size());
    long testTotal1 = result.get(0).getRead() + result.get(0).getWrite();
    long testTotal2 = result.get(1).getRead() + result.get(1).getWrite();
    Assert.assertEquals(true, testTotal1 > testTotal2);
  }

  @Test
  public void testTopNPrograms() throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/auditmetrics/top-entities/programs?limit=20",
                                         HttpResponseStatus.OK.getCode());
    List<TopProgramsResult> result = GSON.fromJson(response, PROGRAM_LIST);
    Assert.assertEquals(5, result.size());
    Assert.assertEquals(true, result.get(0).getValue() > result.get(1).getValue());
    Assert.assertEquals("service", result.get(0).getProgramType());
    Assert.assertEquals("b", result.get(0).getApplication());
  }

  @Test
  public void testTopNApplications() throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/auditmetrics/top-entities/applications?limit=20",
                                         HttpResponseStatus.OK.getCode());
    List<TopApplicationsResult> result = GSON.fromJson(response, APPLICATION_LIST);
    Assert.assertEquals(4, result.size());
    Assert.assertEquals(true, result.get(0).getValue() > result.get(1).getValue());
  }

  @Test
  public void testTimeSince() throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/auditmetrics/time-since?entityType=dataset&entityName=ds1",
                                         HttpResponseStatus.OK.getCode());
    Map<String, Long> resultMap = GSON.fromJson(response, TIMESINCE_MAP);
    Assert.assertEquals(2, resultMap.size());
  }

  @Test
  public void testGlobalAuditLogHistogram() throws Exception {
    String response = getServiceResponse(trackerServiceManager, "v1/auditmetrics/audit-histogram",
                                         HttpResponseStatus.OK.getCode());
    AuditHistogramResult result = GSON.fromJson(response, AuditHistogramResult.class);
    Collection<TimeValue> results = result.getResults();
    int total = 0;
    for (TimeValue t : results) {
      total += t.getValue();
    }
    // Total count should be equal to the number of events fed to the cube.
    Assert.assertEquals(17, total);
  }

  @Test
  public void testSpecificAuditLogHistogram() throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/auditmetrics/audit-histogram?entityType=dataset&entityName=ds1",
                                         HttpResponseStatus.OK.getCode());
    AuditHistogramResult result = GSON.fromJson(response, AuditHistogramResult.class);
    Collection<TimeValue> results = result.getResults();
    int total = 0;
    for (TimeValue t : results) {
      total += t.getValue();
    }
    // Total count should be equal to the number of events fed to the cube for ds1.
    Assert.assertEquals(5, total);
  }

  @Test
  public void testResolutionBucket() throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/auditmetrics/audit-histogram?entityType=dataset" +
                                           "&entityName=ds1&startTime=now-6d&endTime=now",
                                         HttpResponseStatus.OK.getCode());
    AuditHistogramResult result = GSON.fromJson(response, AuditHistogramResult.class);
    Assert.assertEquals(result.getBucketInterval(), "HOUR");
    response = getServiceResponse(trackerServiceManager,
                                  "v1/auditmetrics/audit-histogram?entityType=dataset&entityName=ds1" +
                                    "&startTime=now-8d&endTime=now",
                                  HttpResponseStatus.OK.getCode());
    result = GSON.fromJson(response, AuditHistogramResult.class);
    Assert.assertEquals(result.getBucketInterval(), "DAY");
  }

  @Test
  public void testTrackerEntityFilter() throws Exception {
    // Test dataset filter
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/auditmetrics/audit-histogram?entityType=dataset&entityName="
                                           + TrackerApp.AUDIT_LOG_DATASET_NAME,
                                         HttpResponseStatus.OK.getCode());
    AuditHistogramResult result = GSON.fromJson(response, AuditHistogramResult.class);
    Assert.assertEquals(0, result.getResults().size());

    // Test entity filter
    response = getServiceResponse(trackerServiceManager,
                                         "v1/auditmetrics/top-entities/programs?entityName=dsx&entityType=dataset",
                                         HttpResponseStatus.OK.getCode());
    List<TopProgramsResult> programsResults = GSON.fromJson(response, PROGRAM_LIST);
    Assert.assertEquals(0, programsResults.size());
  }

  /* Tests for Preferred Tags
   *
   */
  @Test
  public void testAddPreferredTags() throws Exception {
    List<String> testList = generateStringList(STRING_LENGTH, CHARACTERS, TEST_STRING_LIST_LENGTH);
    String response = getServiceResponse(trackerServiceManager, "v1/tags/promote",
                                         "POST", GSON.toJson(testList), HttpResponseStatus.OK.getCode());
    ValidateTagsResult result = GSON.fromJson(response, ValidateTagsResult.class);
    Assert.assertEquals(TEST_STRING_LIST_LENGTH, result.getInvalid() + result.getValid());
  }

  @Test
  public void testValidate() throws Exception {
    String response = getServiceResponse(trackerServiceManager, "v1/tags/validate",
                                         "POST", TEST_JSON_TAGS, HttpResponseStatus.OK.getCode());
    ValidateTagsResult result = GSON.fromJson(response, ValidateTagsResult.class);
    Assert.assertEquals(3, result.getValid());
    Assert.assertEquals(1, result.getInvalid());
  }

  @Test
  @Ignore
  // Ignored because there is no way to communicate with CDAP metadata from an app unit test
  public void testGetTags() throws Exception {
    getServiceResponse(trackerServiceManager, "v1/tags/promote", "POST", TEST_JSON_TAGS,
                       HttpResponseStatus.OK.getCode());
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/tags?type=preferred",
                                         HttpResponseStatus.OK.getCode());
    TagsResult result = GSON.fromJson(response, TagsResult.class);
    Assert.assertEquals(3, result.getPreferredSize());
  }

  @Test
  @Ignore
  // Ignored because there is no way to communicate with CDAP metadata from an app unit test
  public void testDeletePreferredTags() throws Exception {
    getServiceResponse(trackerServiceManager, "v1/tags/promote", "POST",
                       TEST_JSON_TAGS, HttpResponseStatus.OK.getCode());
    getServiceResponse(trackerServiceManager, "v1/tags/preferred?tag=tag1", "DELETE",
                       null, HttpResponseStatus.OK.getCode());
    String response = getServiceResponse(trackerServiceManager, "v1/tags?type=preferred",
                                         HttpResponseStatus.OK.getCode());
    TagsResult result = GSON.fromJson(response, TagsResult.class);
    Assert.assertEquals(2, result.getPreferredSize());
  }

  @Test
  public void testDemoteTags() throws Exception {
    getServiceResponse(trackerServiceManager, "v1/tags/demote", "POST", TEST_JSON_TAGS,
                       HttpResponseStatus.OK.getCode());
  }


  /* Tests for TruthMeter
   *
   */
  @Test
  public void testTruthMeter() throws Exception {
    List<String> datasets = new LinkedList<>();
    List<String> streams = new LinkedList<>();
    datasets.add("ds1");
    datasets.add("ds6");
    datasets.add("ds8");
    streams.add("strm123");
    TrackerMeterResult result = getTrackerMeterResponse(datasets, streams, HttpResponseStatus.OK.getCode());
    Assert.assertEquals(3, result.getDatasets().size());
    Assert.assertEquals(1, result.getStreams().size());
  }

  @Test
  public void testTimeRank() throws Exception {
    // ds8 and ds9 have the same timestamp and one identical audit msg each. Their score must be identical.
    List<String> datasets = new LinkedList<>();
    List<String> streams = new LinkedList<>();
    datasets.add("ds6");
    datasets.add("ds8");
    datasets.add("ds9");
    datasets.add("ds1");
    streams.add("strm123");
    streams.add("stream1");
    TrackerMeterResult result = getTrackerMeterResponse(datasets, streams, HttpResponseStatus.OK.getCode());
    Assert.assertEquals(result.getDatasets().get("ds8"), result.getDatasets().get("ds9"));
  }

  @Test
  public void testInvalidName() throws Exception {
    List<String> datasets = new LinkedList<>();
    List<String> streams = new LinkedList<>();
    datasets.add("ds_invalid");
    datasets.add("ds_does_not_exit");
    datasets.add("ds_test");
    streams.add("strm_test");
    TrackerMeterResult result = getTrackerMeterResponse(datasets, streams, HttpResponseStatus.OK.getCode());
    for (Map.Entry<String, Integer> entry : result.getDatasets().entrySet()) {
      Assert.assertEquals(0, (int) entry.getValue());
    }
    for (Map.Entry<String, Integer> entry : result.getStreams().entrySet()) {
      Assert.assertEquals(0, (int) entry.getValue());
    }
  }

  private TrackerMeterResult getTrackerMeterResponse(List<String> datasets,
                                                     List<String> streams,
                                                     int expectedResponse) throws Exception {
    String response = getServiceResponse(trackerServiceManager,
                                         "v1/tracker-meter", "POST",
                                         GSON.toJson(new TrackerMeterRequest(datasets, streams)),
                                         expectedResponse);
    return GSON.fromJson(response, TrackerMeterResult.class);
  }

  private List<String> generateStringList(int maxStringLength, String characters, int stringNum) {
    Random rng = new Random(SEED);
    List<String> list = new ArrayList<>();
    for (int i = 0; i < stringNum; i++) {
      list.add(generateString(rng, characters, maxStringLength));
    }
    return list;
  }

  private String generateString (Random rng, String characters, int maxLength) {
    int length = rng.nextInt(maxLength);
    char[] text = new char[length];
    for (int i = 0; i < length; i++) {
      text[i] = characters.charAt(rng.nextInt(characters.length()));
    }
    return new String(text);
  }



  // Request is GET by default
  private String getServiceResponse(ServiceManager serviceManager,
                                    String request,
                                    int expectedResponseCode) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), request);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(expectedResponseCode, connection.getResponseCode());
    String response;
    try {
      if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
      } else if (connection.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
        response = new String(ByteStreams.toByteArray(connection.getErrorStream()), Charsets.UTF_8);
      } else {
        throw new Exception("Invalid response code returned: " + connection.getResponseCode());
      }
    } finally {
      connection.disconnect();
    }
    return response;
  }

  // Overload (String Type). For requests other than GET.
  private String getServiceResponse(ServiceManager serviceManager,
                                    String request, String type, String postRequest,
                                    int expectedResponseCode) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), request);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod(type);

    //Feed JSON data if POST
    if (type.equals("POST")) {
      connection.setDoOutput(true);
      connection.getOutputStream().write(postRequest.getBytes());
    }

    Assert.assertEquals(expectedResponseCode, connection.getResponseCode());
    String response;
    try {
      if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
      } else if (connection.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
        response = new String(ByteStreams.toByteArray(connection.getErrorStream()), Charsets.UTF_8);
      } else {
        throw new Exception("Invalid response code returned: " + connection.getResponseCode());
      }
    } finally {
      connection.disconnect();
    }
    return response;
  }


  // Adapted from https://wiki.cask.co/display/CE/Audit+information+publishing
  private List<AuditMessage> generateTestData() {
    List<AuditMessage> testData = new ArrayList<>();
    testData.add(new AuditMessage(1456956659461L,
                                  NamespaceId.DEFAULT.stream("stream1"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("program_run:ns1.app2.flow.flow1.run1"))
                 )
    );
    testData.add(new AuditMessage(1456956659469L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    String metadataPayload = "{ \"previous\": { \"USER\": { \"properties\": { \"uk\": \"uv\", \"uk1\": \"uv2\" }, " +
      "\"tags\": [ \"ut1\", \"ut2\" ] }, \"SYSTEM\": { \"properties\": { \"sk\": \"sv\" }, \"tags\": [] } }, " +
      "\"additions\": { \"SYSTEM\": { \"properties\": { \"sk\": \"sv\" }, \"tags\": [ \"t1\", \"t2\" ] } }, " +
      "\"deletions\": { \"USER\": { \"properties\": { \"uk\": \"uv\" }, \"tags\": [ \"ut1\" ] } } }";
    MetadataPayload payload = GSON.fromJson(metadataPayload, MetadataPayload.class);
    testData.add(new AuditMessage(1456956659470L,
                                  EntityId.fromString("application:default.app1"),
                                  "user1",
                                  AuditType.METADATA_CHANGE,
                                  payload)
    );
    testData.add(new AuditMessage(1456956659471L,
                                  EntityId.fromString("dataset:default.ds1"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659472L,
                                  EntityId.fromString("dataset:default.ds1"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659473L,
                                  EntityId.fromString("dataset:default.ds6"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659468L,
                                  NamespaceId.DEFAULT.stream("strm123"),
                                  "user1",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("program_run:ns1.app1.flow.flow1.run1"))
                 )
    );
    testData.add(new AuditMessage(1456956659460L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659502L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659500L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659504L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.UNKNOWN,
                                                    EntityId.fromString("system_service:explore"))
                 )
    );
    testData.add(new AuditMessage(1456956659505L,
                                  NamespaceId.DEFAULT.dataset("ds3"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("program:ns1.b.SERVICE.program1"))
                 )
    );
    testData.add(new AuditMessage(1456956659506L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("program:ns1.a.SERVICE.program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659507L,
                                  NamespaceId.DEFAULT.dataset("ds1"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("program:ns1.b.SERVICE.program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659509L,
                                  NamespaceId.DEFAULT.dataset("ds8"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("program:ns1.b.SERVICE.program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659511L,
                                  NamespaceId.DEFAULT.dataset("ds9"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.READ,
                                                    EntityId.fromString("program:ns1.b.SERVICE.program2"))
                 )
    );
    testData.add(new AuditMessage(1456956659512L,
                                  EntityId.fromString("dataset:default.ds5"),
                                  "user1",
                                  AuditType.CREATE,
                                  AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659513L,
                                  NamespaceId.DEFAULT.dataset(TrackerApp.AUDIT_LOG_DATASET_NAME),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString("program:ns1.b.SERVICE.program1"))
                 )
    );
    testData.add(new AuditMessage(1456956659516L,
                                  NamespaceId.DEFAULT.dataset("dsx"),
                                  "user4",
                                  AuditType.ACCESS,
                                  new AccessPayload(AccessType.WRITE,
                                                    EntityId.fromString(String.format("program:ns1.%s.SERVICE.program1",
                                                                                      TrackerApp.APP_NAME))
                                  )
                 )
    );
    return testData;
  }
}
