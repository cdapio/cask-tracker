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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.table.Table;

/**
 * A temp app to test the Data dictionary functionality
 */
public class TestDictionaryApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("TestDataDictionaryApp");
    setDescription("A temp app to test the Data dictionary functionality");
    createDataset(TrackerApp.DATA_DICTIONARY_DATASET_NAME, Table.class);
    addService(new TrackerService());
  }
}
