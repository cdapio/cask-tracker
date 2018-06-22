package co.cask.tracker.entity;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTable;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.codec.AuditMessageTypeAdapter;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests {@link AuditLogTable}
 */
public class AuditLogTableTest {
  private static AuditLogTable auditLogTable;
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
  }.getType();

  @BeforeClass
  public static void before() {
    DatasetSpecification spec = DatasetSpecification.builder("some", "table")
      .properties(new HashMap<String, String>()).build();
    InMemoryTable test = new InMemoryTable("test");
    auditLogTable = new AuditLogTable(spec, test);
  }

  @Test
  public void testWriteHelper() throws Exception {
    // expected AuditMessage Json
    AuditMessage expected = getAuditMessageV2();

    // verify write of new format can be converted back
    Put put = auditLogTable.writeHelper(expected);
    AuditMessage actual = AuditLogTable.createAuditMessage(new Result(put.getRow(), put.getValues()));
    Assert.assertEquals(jsonToMap(GSON.toJson(expected)), jsonToMap(GSON.toJson(actual)));
    // verify that the old format can be converted back
    Put oldAuditMessage = getAuditMessageV1();
    actual = AuditLogTable.createAuditMessage(new Result(oldAuditMessage.getRow(),
                                                         oldAuditMessage.getValues()));
    Assert.assertEquals(jsonToMap(GSON.toJson(expected)), jsonToMap(GSON.toJson(actual)));
  }

  private Put getAuditMessageV1() {
    // return the audit message in old format
    EntityId entityId = new ApplicationId("ns1", "app1", "v1");
    String entityType = EntityType.APPLICATION.toString().toLowerCase();
    String entityName = ((ApplicationId) entityId).getApplication();
    return new Put(auditLogTable.getKey(((ApplicationId) entityId).getNamespace(), entityType, entityName, 3000L))
      .add("timestamp", 3000L)
      .add("entityId", GSON.toJson(entityId))
      .add("user", "user1")
      .add("actionType", AuditType.METADATA_CHANGE.toString())
      .add("entityType", entityType)
      .add("entityName", entityName)
      .add("metadata", GSON.toJson(getPayload()));
  }

  private AuditMessage getAuditMessageV2() {
    return new AuditMessage(3000L,
                            MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns1")
                              .appendAsType(MetadataEntity.APPLICATION, "app1")
                              .append(MetadataEntity.VERSION, "v1").build(),
                            "user1", AuditType.METADATA_CHANGE, getPayload());
  }

  private MetadataPayload getPayload() {
    Map<String, String> userProperties = new HashMap<>();
    userProperties.put("uk", "uv");
    userProperties.put("uk1", "uv2");
    Map<String, String> systemProperties = new HashMap<>();
    systemProperties.put("sk", "sv");
    Set<String> userTags = new LinkedHashSet<>();
    userTags.add("ut1");
    userTags.add("ut2");
    Map<MetadataScope, Metadata> previous = new LinkedHashMap<>();
    previous.put(MetadataScope.USER, new Metadata(Collections.unmodifiableMap(userProperties),
                                                  Collections.unmodifiableSet(userTags)));
    previous.put(MetadataScope.SYSTEM, new Metadata(Collections.unmodifiableMap(systemProperties),
                                                    Collections.unmodifiableSet(
                                                      new LinkedHashSet<String>())));
    Map<String, String> sysPropertiesAdded = new HashMap<>();
    sysPropertiesAdded.put("sk", "sv");
    Set<String> systemTagsAdded = new LinkedHashSet<>();
    systemTagsAdded.add("t1");
    systemTagsAdded.add("t2");
    Map<MetadataScope, Metadata> additions = new HashMap<>();
    additions.put(MetadataScope.SYSTEM, new Metadata(Collections.unmodifiableMap(sysPropertiesAdded),
                                                     Collections.unmodifiableSet(systemTagsAdded)));
    Map<String, String> userPropertiesDeleted = new HashMap<>();
    userPropertiesDeleted.put("uk", "uv");
    Set<String> userTagsDeleted = new LinkedHashSet<>();
    userTagsDeleted.add("ut1");
    Map<MetadataScope, Metadata> deletions = new HashMap<>();
    deletions.put(MetadataScope.USER, new Metadata(Collections.unmodifiableMap(userPropertiesDeleted),
                                                   Collections.unmodifiableSet(userTagsDeleted)));
    return new MetadataPayload(previous, additions, deletions);
  }

  private Map<String, Object> jsonToMap(String json) {
    return GSON.fromJson(json, MAP_TYPE);
  }
}
