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

import java.util.List;

/**
 *  A POJO to hold the result of the validate tags request
 */
public final class ValidateTagsResult {

  private final int valid;
  private final int invalid;
  private final List<String> validTags;
  private final List<String> invalidTags;

  public ValidateTagsResult(List<String> validTags, List<String> invalidTags) {
    this.valid = validTags.size();
    this.invalid = invalidTags.size();
    this.validTags = validTags;
    this.invalidTags = invalidTags;
  }

  public int getValid() {
    return valid;
  }

  public int getInvalid() {
    return invalid;
  }

  public List<String> getValidTags() {
    return validTags;
  };
}
