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
    private final Map<String, String> columnValues;

    public TopEntitiesResult(String entityName) {
        this.columnValues = new HashMap<>();
        columnValues.put("label", entityName);
        columnValues.put("read", 0 + "");
        columnValues.put("write", 0 + "");
    }

    public Map<String, String> getColumnValues() {
        return columnValues;
    }

    public void addAccessType(String type, String value) {
        this.columnValues.put(type, value);
    }

    @Override
    public int compareTo(TopEntitiesResult o) {
        Long thisTotal = Long.parseLong(columnValues.get("read")) + Long.parseLong(columnValues.get("write"));
        Long thatTotal = Long.parseLong(o.getColumnValues().get("read"))
                + Long.parseLong(o.getColumnValues().get("write"));
        return thatTotal.compareTo(thisTotal);
    }
}
