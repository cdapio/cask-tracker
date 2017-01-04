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

import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import org.junit.Assert;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class containing common code for tests
 */
public final class TestUtils {

  private TestUtils() {
  }

  public static String getServiceResponse(ServiceManager serviceManager, String request, String type,
                                          String postRequest, int expectedResponseCode) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), request);
    HttpRequest httpRequest = new HttpRequest(HttpMethod.valueOf(type), url, null, null, null);
    HttpResponse response = HttpRequests.execute(httpRequest);
    List<Integer> expectedCodes = new ArrayList<>();
    expectedCodes.add(HttpURLConnection.HTTP_BAD_REQUEST);
    expectedCodes.add(HttpURLConnection.HTTP_CONFLICT);
    expectedCodes.add(HttpURLConnection.HTTP_NOT_FOUND);
    //Feed JSON data if POST
    if (type.equals("POST")) {
      response = HttpRequests.execute(HttpRequest.post(url).withBody(postRequest).build());
    } else if (type.equals("PUT")) {
      response = HttpRequests.execute(HttpRequest.put(url).withBody(postRequest).build());
    }
    return verifyResponseCode(expectedResponseCode, response, expectedCodes);
  }

  // Request is GET by default
  public static String getServiceResponse(ServiceManager serviceManager, String request, String type,
                                          int expectedResponseCode) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), request);
    HttpRequest httpRequest = new HttpRequest(HttpMethod.valueOf(type), url, null, null, null);
    HttpResponse response = HttpRequests.execute(httpRequest);
    List<Integer> expectedCodes = new ArrayList<>();
    expectedCodes.add(HttpURLConnection.HTTP_BAD_REQUEST);
    expectedCodes.add(HttpURLConnection.HTTP_NOT_FOUND);
    return verifyResponseCode(expectedResponseCode, response, expectedCodes);
  }

  public static String getServiceResponse(ServiceManager serviceManager, String request, int expectedResponseCode)
    throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), request);
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build());
    List<Integer> expectedCodes = new ArrayList<>();
    expectedCodes.add(HttpURLConnection.HTTP_BAD_REQUEST);
    return verifyResponseCode(expectedResponseCode, response, expectedCodes);
  }

  private static String verifyResponseCode(int expectedResponseCode, HttpResponse response, List<Integer> expectedCodes)
    throws Exception {
    Assert.assertEquals(expectedResponseCode, response.getResponseCode());
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK ||
      expectedCodes.contains(response.getResponseCode())) {
      return response.getResponseBodyAsString();
    } else {
      throw new Exception("Invalid response code returned: " + response.getResponseCode());
    }
  }
}
