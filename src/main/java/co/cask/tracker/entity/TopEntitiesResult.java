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

package co.cask.tracker.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * A POJO to hold the results for the TopN query.
 */
public class TopEntitiesResult implements Comparable<TopEntitiesResult> {
  private final String namespace;
  private final String entityType;
  private final String entityName;
  private final Map<String, Long> columnValues;

  public TopEntitiesResult(String namespace, String entityType, String entityName) {
    this.namespace = namespace;
    this.entityType = entityType;
    this.entityName = entityName;
    this.columnValues = new HashMap<>();
  }

  public String getNamespace() {
    return namespace;
  }

  public String getEntityType() {
    return entityType;
  }

  public String getEntityName() {
    return entityName;
  }

  public Map<String, Long> getColumnValues() {
    return columnValues;
  }

  public void addAccessType(String type, long value) {
    this.columnValues.put(type, value);
  }

  @Override
  public int compareTo(TopEntitiesResult o) {
    return o.getColumnValues().get("count").compareTo(this.getColumnValues().get("count"));
  }
}
