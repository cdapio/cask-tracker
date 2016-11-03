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
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.FlowletQueueId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.ScheduleId;

import java.io.IOException;

/**
 * A class with static methods to find name, parent app, and program type of an entity.
 */

public class EntityIdHelper {
  /*
   * Get the parent application name of an entity if it has one. Returns empty String otherwise.
   */
  public static String getParentApplicationName(EntityId entityId) {
    EntityType entityType = entityId.getEntity();
    String name;
    switch (entityType) {
      case APPLICATION:
        name = ((ApplicationId) entityId).getApplication();
        break;
      case FLOWLET:
        name = ((FlowletId) entityId).getApplication();
        break;
      case FLOWLET_QUEUE:
        name = ((FlowletQueueId) entityId).getApplication();
        break;
      case PROGRAM:
        name = ((ProgramId) entityId).getApplication();
        break;
      case PROGRAM_RUN:
        name = ((ProgramRunId) entityId).getApplication();
        break;
      case SCHEDULE:
        name = ((ScheduleId) entityId).getApplication();
        break;
      default:
        name = "";
    }
    return name;
  }

  /*
   * Get the program type of an entity. Return empty String otherwise.
   */
  public static String getProgramType(EntityId entityId) throws IOException {
    if (entityId instanceof ProgramId) {
      return ((ProgramId) entityId).getType().name().toLowerCase();
    } else if (entityId instanceof ProgramRunId) {
      return ((ProgramRunId) entityId).getType().name().toLowerCase();
    }
    return "";
  }
}
