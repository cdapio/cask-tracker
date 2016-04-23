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

import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.FlowletQueueId;
import co.cask.cdap.proto.id.NamespacedArtifactId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.id.SystemServiceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A static class to make finding the name of an Entity easier.
 */
public class EntityIdHelper {
  private static final Logger LOG = LoggerFactory.getLogger(EntityIdHelper.class);

  public static String getEntityName(EntityId entityId) throws IOException {
    EntityType entityType = entityId.getEntity();
    String name;
    // Unfortunately, there's no generic way to get the name of the entity
    // so we need this switch statement and a bunch of casting.
    switch (entityType) {
      case APPLICATION:
        name = ((ApplicationId) entityId).getApplication();
        break;
      case ARTIFACT:
        name = ((NamespacedArtifactId) entityId).getArtifact();
        break;
      case DATASET:
        name = ((DatasetId) entityId).getDataset();
        break;
      case DATASET_MODULE:
        name = ((DatasetModuleId) entityId).getModule();
        break;
      case DATASET_TYPE:
        name = ((DatasetTypeId) entityId).getType();
        break;
      case FLOWLET:
        name = ((FlowletId) entityId).getFlowlet();
        break;
      case FLOWLET_QUEUE:
        name = ((FlowletQueueId) entityId).getQueue();
        break;
      case NOTIFICATION_FEED:
        name = ((NotificationFeedId) entityId).getFeed();
        break;
      case PROGRAM:
        name = ((ProgramId) entityId).getProgram();
        break;
      case PROGRAM_RUN:
        name = ((ProgramRunId) entityId).getRun();
        break;
      case SCHEDULE:
        name = ((ScheduleId) entityId).getSchedule();
        break;
      case STREAM:
        name = ((StreamId) entityId).getStream();
        break;
      case STREAM_VIEW:
        name = ((StreamViewId) entityId).getView();
        break;
      case SYSTEM_SERVICE:
        name = ((SystemServiceId) entityId).getService();
        break;
      default:
        throw new IOException("Unknown entity type: " + entityType);
    }
    return name;
  }
}
