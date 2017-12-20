/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.front50.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("spinnaker.qcloud")
public class QcloudStorageProperties {
  private String qcloudSecretId;
  private String qcloudSecretKey;
  private String qcloudRegion;
  private String storageBucket;
  private String storageAppId;
  private String rootFolder;

  public String getStorageConnectionString() {
    return "DefaultEndpointsProtocol=https;"
      + "SecretId=" + this.qcloudSecretId + ";"
      + "SecretKey=" + this.qcloudSecretKey +";"
      + "qcloudRegion=" + this.qcloudRegion +";"
      + "StorageBucket=" + this.storageBucket +";"
      + "StorageAppId=" + this.storageAppId;
  }

  public String getQcloudSecretId() {
    return qcloudSecretId;
  }

  public void setQcloudSecretId(String qcloudSecretId) {
    this.qcloudSecretId = qcloudSecretId;
  }

  public String getQcloudSecretKey() {
    return qcloudSecretKey;
  }

  public void setQcloudSecretKey(String qcloudSecretKey) {
    this.qcloudSecretKey = qcloudSecretKey;
  }

  public String getQcloudRegion() {
    return qcloudRegion;
  }

  public void setQcloudRegion(String qcloudRegion) {
    this.qcloudRegion = qcloudRegion;
  }

  public String getStorageBucket() {
    return storageBucket;
  }

  public void setStorageBucket(String storageBucket) {
    this.storageBucket = storageBucket;
  }

  public String getStorageAppId() {
    return storageAppId;
  }

  public void setStorageAppId(String storageAppId) {
    this.storageAppId = storageAppId;
  }

  public String getRootFolder() {
    return rootFolder;
  }

  public void setRootFolder(String rootFolder) {
    this.rootFolder = rootFolder;
  }
}
