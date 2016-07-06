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
 *  POJO to hold a unique entity in the #same namespace#.
 */
public final class UniqueEntityHolder {

  private final String entityType;
  private final String entityName;

  public UniqueEntityHolder(String entityType, String entityName) {
    this.entityName = entityName;
    this.entityType = entityType;
  }

  public String getEntityType() {
    return entityType;
  }

  public String getEntityName() {
    return entityName;
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof UniqueEntityHolder)) {
      return false;
    }
    UniqueEntityHolder o = (UniqueEntityHolder) object;
    return (entityType.equals(o.getEntityType()) && entityName.equals(o.getEntityName()));
  }

  @Override
  public int hashCode() {
    return (String.format("%s-%s", entityType, entityName)).hashCode();
  }

}
