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

/**
 * A POJO to hold the results for the TopN query.
 */
public class TopProgramsResult implements Comparable<TopProgramsResult> {
  private final String namespace;
  private final String entityName;
  private final String application;
  private final String programType;
  private long value;

  public TopProgramsResult(String namespace, String entityName, String application, String entityType, long value) {
    this.namespace = namespace;
    this.entityName = entityName;
    this.application = application;
    this.programType = entityType;
    this.value = value;
  }

  public long getValue() {
    return value;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getApplication() {
    return application;
  }

  public String getProgramType() {
    return programType;
  }

  public String getEntityName() {
    return entityName;
  }

  public void increment(long value) {
    this.value += value;
  }

  @Override
  public int compareTo(TopProgramsResult o) {
    return Long.compare(o.getValue(), getValue());
  }
}
