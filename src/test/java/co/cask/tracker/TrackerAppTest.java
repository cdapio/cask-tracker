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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.metrics.RuntimeMetrics;
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
import co.cask.tracker.entity.AuditLogResponse;
import co.cask.tracker.entity.TopEntitiesResult;
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format
=======
>>>>>>> 3402650... Addressed code review comments
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
import co.cask.tracker.entity.TopEntitiesResultWrapper;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import scala.Product;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
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
  private static ServiceManager auditLogServiceManager;
  private static ServiceManager auditMetricsServiceManager;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Before
  public void configureStream() throws Exception {
    testAppManager = deployApplicationWithScalaJar(TestAuditLogPublisherApp.class, null);
    FlowManager testFlowManager = testAppManager.getFlowManager(StreamToAuditLogFlow.FLOW_NAME).start();
    testFlowManager.waitForStatus(true);

    auditLogServiceManager = testAppManager.getServiceManager(AuditLogService.SERVICE_NAME).start();
    auditLogServiceManager.waitForStatus(true);

    auditMetricsServiceManager = testAppManager.getServiceManager(AuditMetricsService.SERVICE_NAME).start();
    auditMetricsServiceManager.waitForStatus(true);

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
  public void testInvalidDatesError() throws Exception {
<<<<<<< HEAD
    String response = getServiceResponse(auditLogServiceManager,
      "auditlog/stream/stream1?startTime=1&endTime=0",
      HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals("\"startTime must be before endTime.\"", response);
  }

  @Test
  public void testInvalidOffset() throws Exception {
    String response = getServiceResponse(auditLogServiceManager,
      "auditlog/stream/stream1?offset=-1",
      HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals("\"offset cannot be negative.\"", response);
  }

  @Test
  public void testInvalidLimit() throws Exception {
    String response = getServiceResponse(auditLogServiceManager,
      "auditlog/stream/stream1?limit=-1",
      HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals("\"limit cannot be negative.\"", response);
  }

  @Test
  public void testTopNDatasets() throws Exception {
<<<<<<< HEAD
    String response = getServiceResponse(auditMetricsServiceManager,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
      "v1/auditmetrics/topEntities/datasets?limit=20",
      HttpResponseStatus.OK.getCode());
    TopEntitiesResultWrapper result = GSON.fromJson(response, TopEntitiesResultWrapper.class);
    Assert.assertEquals(4, result.getTotal());
=======
            "v1/auditmetrics/topEntities/datasets?limit=3",
=======
            "v1/auditmetrics/topEntities/datasets?limit=20",
>>>>>>> 7812549... Changes to TopNEntities, Tests, Tests Data, Handler
            HttpResponseStatus.OK.getCode());
=======
      "v1/auditmetrics/topEntities/datasets?limit=20",
      HttpResponseStatus.OK.getCode());
>>>>>>> 497f160... Fixed indentation
    TopEntitiesResultWrapper result = GSON.fromJson(response, TopEntitiesResultWrapper.class);
    Assert.assertEquals(4, result.getTotal());
=======
    String response = getServiceResponse(auditLogServiceManager,
      "auditlog/stream/stream1?startTime=1&endTime=0",
      HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals("\"startTime must be before endTime.\"", response);
  }

  @Test
  public void testInvalidOffset() throws Exception {
    String response = getServiceResponse(auditLogServiceManager,
      "auditlog/stream/stream1?offset=-1",
      HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals("\"offset cannot be negative.\"", response);
  }

  @Test
  public void testInvalidLimit() throws Exception {
    String response = getServiceResponse(auditLogServiceManager,
      "auditlog/stream/stream1?limit=-1",
      HttpResponseStatus.BAD_REQUEST.getCode());
    Assert.assertEquals("\"limit cannot be negative.\"", response);
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
  }


  @Test
<<<<<<< HEAD
  public void testTopNPrograms() throws Exception {
    String response = getServiceResponse(auditMetricsServiceManager,
      "v1/auditmetrics/topEntities/programs?limit=20",
      HttpResponseStatus.OK.getCode());
    TopEntitiesResultWrapper result = GSON.fromJson(response, TopEntitiesResultWrapper.class);
    Assert.assertEquals(4, result.getTotal());
<<<<<<< HEAD
    //Assert.assertTrue(results[0].getColumnValues().get("count") > results[1].getColumnValues().get("count"));
>>>>>>> 369a5f2... Fixed TopNDatasets to only query for entity_type = dataset
  }

<<<<<<< HEAD
  @Test
  public void testTopNPrograms() throws Exception {
=======
>>>>>>> 0779d97... Some changes to AuditLogTable. Updated tests.
    String response = getServiceResponse(auditMetricsServiceManager,
      "v1/auditmetrics/topEntities/programs?limit=20",
      HttpResponseStatus.OK.getCode());
    TopEntitiesResultWrapper result = GSON.fromJson(response, TopEntitiesResultWrapper.class);
    Assert.assertEquals(4, result.getTotal());
=======
>>>>>>> 497f160... Fixed indentation
  }

  @Test
<<<<<<< HEAD
  public void testTopNApplications() throws Exception {
    String response = getServiceResponse(auditMetricsServiceManager,
      "v1/auditmetrics/topEntities/applications?limit=20",
      HttpResponseStatus.OK.getCode());
<<<<<<< HEAD
=======
  public void testTopNDatasets() throws Exception {
    String response = getServiceResponse(auditMetricsServiceManager,
      "v1/auditmetrics/topEntities/datasets?limit=20",
      HttpResponseStatus.OK.getCode());
    TopEntitiesResultWrapper result = GSON.fromJson(response, TopEntitiesResultWrapper.class);
    Assert.assertEquals(4, result.getTotal());
  }


  @Test
  public void testTopNPrograms() throws Exception {
    String response = getServiceResponse(auditMetricsServiceManager,
      "v1/auditmetrics/topEntities/programs?limit=20",
      HttpResponseStatus.OK.getCode());
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
    TopEntitiesResultWrapper result = GSON.fromJson(response, TopEntitiesResultWrapper.class);
    Assert.assertEquals(4, result.getTotal());
  }

<<<<<<< HEAD
=======
  public void testTopNPrograms() throws Exception {
    String response = getServiceResponse(auditMetricsServiceManager,
            "v1/auditmetrics/topEntities/programs?limit=3",
            HttpResponseStatus.OK.getCode());
    TopEntitiesResult[] results = GSON.fromJson(response, TopEntitiesResult[].class);
    Assert.assertEquals(0, results.length);
  }

  @Test
  public void testTopNApplications() throws Exception {
<<<<<<< HEAD
    String response = getServiceResponse(auditMetricsServiceManager, "v1/auditmetrics/topEntities/applications?limit=3",
            HttpResponseStatus.OK.getCode());
    TopEntitiesResult[] results = GSON.fromJson(response, TopEntitiesResult[].class);
    Assert.assertEquals(0, results.length);
=======
  @Test
  public void testTopNApplications() throws Exception {
    String response = getServiceResponse(auditMetricsServiceManager,
      "v1/auditmetrics/topEntities/applications?limit=20",
      HttpResponseStatus.OK.getCode());
    TopEntitiesResultWrapper result = GSON.fromJson(response, TopEntitiesResultWrapper.class);
    Assert.assertEquals(4, result.getTotal());

>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
  }
>>>>>>> 0779d97... Some changes to AuditLogTable. Updated tests.
=======
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format
=======
    String response = getServiceResponse(auditMetricsServiceManager,
            "v1/auditmetrics/topEntities/applications?limit=20",
            HttpResponseStatus.OK.getCode());
=======
>>>>>>> 497f160... Fixed indentation
    TopEntitiesResultWrapper result = GSON.fromJson(response, TopEntitiesResultWrapper.class);
    Assert.assertEquals(4, result.getTotal());

  }
>>>>>>> 2d45a5b... trying to implement apps and programs. hopeless bug -_-



  private static ApplicationManager deployApplicationWithScalaJar(Class appClass, Config config) {
    URL classUrl = Product.class.getClassLoader().getResource("scala/Product.class");
    String path = classUrl.getFile();
    if (config != null) {
      return deployApplication(appClass, config, new File(URI.create(path.substring(0, path.indexOf("!/")))));
    } else {
      return deployApplication(appClass, new File(URI.create(path.substring(0, path.indexOf("!/")))));
    }
  }


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

  // Adapted from https://wiki.cask.co/display/CE/Audit+information+publishing
  private List<AuditMessage> generateTestData() {
    List<AuditMessage> testData = new ArrayList<>();
    testData.add(new AuditMessage(1456956659461L,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 497f160... Fixed indentation
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
        NamespaceId.DEFAULT.stream("stream1"),
        "user1",
        AuditType.ACCESS,
        new AccessPayload(AccessType.WRITE,
          EntityId.fromString("program_run:ns1.app2.flow.flow1.run1"))
      )
<<<<<<< HEAD
<<<<<<< HEAD
=======
                    NamespaceId.DEFAULT.stream("stream1"),
                    "user1",
                    AuditType.ACCESS,
                    new AccessPayload(AccessType.WRITE,
                            EntityId.fromString("program_run:ns1.app2.flow.flow1.run1"))
            )
>>>>>>> 2d45a5b... trying to implement apps and programs. hopeless bug -_-
    );
    testData.add(new AuditMessage(1456956659469L,
<<<<<<< HEAD
=======
    );
    testData.add(new AuditMessage(1456956659469L,
>>>>>>> 497f160... Fixed indentation
=======
    );
    testData.add(new AuditMessage(1456956659469L,
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
        NamespaceId.DEFAULT.dataset("ds1"),
        "user1",
        AuditType.ACCESS,
        new AccessPayload(AccessType.WRITE,
          EntityId.fromString("system_service:explore"))
      )
<<<<<<< HEAD
<<<<<<< HEAD
=======
                    NamespaceId.DEFAULT.dataset("ds1"),
                    "user1",
                    AuditType.ACCESS,
                    new AccessPayload(AccessType.WRITE,
                            EntityId.fromString("system_service:explore"))
            )
>>>>>>> 7812549... Changes to TopNEntities, Tests, Tests Data, Handler
=======
>>>>>>> 497f160... Fixed indentation
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 497f160... Fixed indentation
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
      EntityId.fromString("dataset:default.ds1"),
      "user1",
      AuditType.CREATE,
      AuditPayload.EMPTY_PAYLOAD));
<<<<<<< HEAD
<<<<<<< HEAD
=======
            EntityId.fromString("dataset:default.ds1"),
            "user1",
            AuditType.CREATE,
<<<<<<< HEAD
            new AccessPayload(AccessType.READ, EntityId.fromString("program_run:ns1.app1.flow.flow1.run1"))));
>>>>>>> 7812549... Changes to TopNEntities, Tests, Tests Data, Handler
=======
            AuditPayload.EMPTY_PAYLOAD));
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format
=======
>>>>>>> 497f160... Fixed indentation
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
    testData.add(new AuditMessage(1456956659472L,
      EntityId.fromString("dataset:default.ds1"),
      "user1",
      AuditType.CREATE,
      AuditPayload.EMPTY_PAYLOAD));
    testData.add(new AuditMessage(1456956659473L,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 497f160... Fixed indentation
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
      EntityId.fromString("dataset:default.ds6"),
      "user1",
      AuditType.CREATE,
      AuditPayload.EMPTY_PAYLOAD));
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
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
<<<<<<< HEAD
=======
            EntityId.fromString("dataset:default.ds6"),
            "user1",
            AuditType.CREATE,
            AuditPayload.EMPTY_PAYLOAD));
<<<<<<< HEAD
    testData.add(new AuditMessage(456956659474L,
            NamespaceId.DEFAULT.dataset("dset1"),
            "user1",
            AuditType.CREATE,
            new AccessPayload(AccessType.READ, EntityId.fromString("program_run:ns1.app1.flow.flow1.run1"))));
>>>>>>> 7812549... Changes to TopNEntities, Tests, Tests Data, Handler
=======
=======
>>>>>>> 497f160... Fixed indentation
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
<<<<<<< HEAD
>>>>>>> 0e92e89... Rerolled all changes so far and reimplemented topNDataset. topNDataset returns result in the expected format
=======
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
>>>>>>> 2d45a5b... trying to implement apps and programs. hopeless bug -_-
=======
>>>>>>> 3810a387ec0e4d686a2fbb96e88d2e638bc31e54
    return testData;
  }
}
