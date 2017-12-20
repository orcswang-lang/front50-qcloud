package com.netflix.spinnaker.front50.model;

import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.front50.config.QcloudStorageProperties;
import com.netflix.spinnaker.front50.exception.NotFoundException;
import com.netflix.spinnaker.security.AuthenticatedRequest;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.*;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.transfer.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static net.logstash.logback.argument.StructuredArguments.value;

/**
 * Created by Orcswang on 2017/9/22.
 */
public class QcloudStorageService implements StorageService {

  private static final Logger log = LoggerFactory.getLogger(QcloudStorageService.class);

  private String encoding = "UTF-8";

  private String bucketName = "spinnaker";

  private String region = "ap-shanghai";

  private String rootFolder = "front50";

  private COSClient cosClient;

  private ObjectMapper objectMapper = new ObjectMapper();

  public QcloudStorageService(QcloudStorageProperties qcloudStorageProperties) {
    this.bucketName = qcloudStorageProperties.getStorageBucket();
    this.rootFolder = qcloudStorageProperties.getRootFolder();
    this.region = qcloudStorageProperties.getQcloudRegion();
    // 设置秘钥
    COSCredentials cred = new BasicCOSCredentials(qcloudStorageProperties.getStorageAppId(),
      qcloudStorageProperties.getQcloudSecretId(), qcloudStorageProperties.getQcloudSecretKey());
    // 设置区域, 这里设置为北京一区
    ClientConfig clientConfig = new ClientConfig(new Region(this.region));
    // 生成cos客户端对象
    this.cosClient = new COSClient(cred, clientConfig);
  }


  @Override
  public void ensureBucketExists() {
    try {
      this.cosClient.doesBucketExist(bucketName);
    } catch (CosServiceException e) {
      if (e.getStatusCode() == 404) {
        if (StringUtils.isNullOrEmpty(region)) {
          log.info("Creating bucket {} in default region", value("bucket", bucketName));
          this.cosClient.createBucket(bucketName);
        } else {
          log.info("Creating bucket {} in region {}",
            value("bucket", bucketName),
            value("region", this.region)
          );
          this.cosClient.createBucket(bucketName);
        }
      } else {
        throw e;
      }

    }
  }

  @Override
  public boolean supportsVersioning() {
    return false;
  }

  /**
   * 获取对象
   * @param objectType
   * @param objectKey
   * @param <T>
   * @return
   * @throws NotFoundException
   */
  @Override
  public <T extends Timestamped> T loadObject(ObjectType objectType, String objectKey) throws NotFoundException {
    try {
      COSObject cosObject = cosClient.getObject(bucketName, buildCosKey(objectType.group, objectKey, objectType.defaultMetadataFilename));
      T item = deserialize(cosObject, (Class<T>) objectType.clazz);
      item.setLastModified(cosObject.getObjectMetadata().getLastModified().getTime());
      return item;
    } catch (CosServiceException e) {
      if (e.getStatusCode() == 404) {
        throw new NotFoundException("Object not found (key: " + objectKey + ")");
      }
      throw e;
    } catch (IOException e) {
      throw new IllegalStateException("Unable to deserialize object (key: " + objectKey + ")", e);
    }
  }

  /**
   * 删除对象
   * @param objectType
   * @param objectKey
   */
  @Override
  public void deleteObject(ObjectType objectType, String objectKey) {
    String key = buildKeyPath(objectType.group, objectKey, objectType.defaultMetadataFilename);
    try {
      DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, key);
      cosClient.deleteObject(deleteObjectRequest);
      log.info("The delete Cos object is succeed.");
    } catch (CosClientException cle) {
      log.error("del Cos object failed.", cle);
      throw new CosClientException(cle);
    }
  }

  /**
   * 上传对象
   * @param objectType
   * @param objectKey
   * @param item
   * @param <T>
   */
  @Override
  public <T extends Timestamped> void storeObject(ObjectType objectType, String objectKey, T item) {
    String key = buildKeyPath(objectType.group, objectKey, objectType.defaultMetadataFilename);
    try {
      item.setLastModifiedBy(AuthenticatedRequest.getSpinnakerUser().orElse("anonymous"));
      byte[] bytes = objectMapper.writeValueAsBytes(item);
      InputStream inputStream = new ByteArrayInputStream(bytes);

      // 上传文件(推荐), 支持根据文件的大小自动选择单文件上传或者分块上传
      // 同时支持同时上传不同的文件
      TransferManager transferManager = new TransferManager(cosClient);
      //File localFile = new File("src/test/resources/len30M.txt");
      ObjectMetadata  objectMetadata = new ObjectMetadata();
      objectMetadata.setContentLength(bytes.length);
      objectMetadata.setContentEncoding(this.encoding);
      // transfermanger upload是异步上传
      Upload upload = transferManager.upload(bucketName, key, inputStream,objectMetadata);
      // 等待传输结束
      upload.waitForCompletion();
      transferManager.shutdownNow();
      log.info("{} object {} for  upload status:{}.",
        value("group", objectType.group),
        value("key", key),
        upload.getState().toString());
    } catch (CosClientException se) {
      logStorageException(se, key);
    }catch (Exception e) {
      log.error("Failed to retrieve {} object: {}: {}",
        value("group", objectType.group),
        value("key", key),
        value("exception", e.getMessage()));
    }

  }

  /**
   * 获取对象key
   * @param objectType
   * @return
   */
  @Override
  public Map<String, Long> listObjectKeys(ObjectType objectType) {
    long startTime = System.currentTimeMillis();
    String typedFolder = buildTypedFolder(rootFolder, objectType.group);
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
    listObjectsRequest.setBucketName(this.bucketName);
    // 设置 list 的 prefix, 表示 list 出来的文件 key 都是以这个 prefix 开始
    listObjectsRequest.setPrefix(typedFolder);
    // 设置 delimiter 为/, 即获取的是直接成员，不包含目录下的递归子成员
    listObjectsRequest.setDelimiter("");
    // 设置 marker, (marker 由上一次 list 获取到, 或者第一次 list marker 为空)
    listObjectsRequest.setMarker("");
    // 设置最多 list 100 个成员,（如果不设置, 默认为 1000 个，最大允许一次 list 1000 个 key）
    listObjectsRequest.setMaxKeys(1000);

    // list bucket下的成员
    ObjectListing objectListing = this.cosClient.listObjects(listObjectsRequest);
    List<COSObjectSummary> objectSummaries = objectListing.getObjectSummaries();
    // 获取下次 list 的 marker
    String nextMarker = objectListing.getNextMarker();
    // 判断是否已经 list 完, 如果 list 结束, 则 isTruncated 为 false, 否则为 true
    if(objectListing.isTruncated()){
      objectListing = this.cosClient.listNextBatchOfObjects(objectListing);
      objectSummaries.addAll(objectListing.getObjectSummaries());
    }
    log.info("Took {}ms to fetch {} object keys for {}",
      value("fetchTime", (System.currentTimeMillis() - startTime)),
      objectSummaries.size(),
      value("type", objectType.group));
    Map<String, Long> resultMap = objectSummaries
      .stream()
      .filter(s -> filterCosObjectSummary(s, objectType.defaultMetadataFilename))
      .collect(Collectors.toMap((s -> buildObjectKey(objectType, s.getKey())), (s -> s.getLastModified().getTime())));
    return resultMap;
  }

  @Override
  public <T extends Timestamped> Collection<T> listObjectVersions(ObjectType objectType, String objectKey, int maxResults) throws NotFoundException {
    return null;
  }

  /**
   * 获取最后修改
   * @param objectType
   * @return
   */
  @Override
  public long getLastModified(ObjectType objectType) {
    return 0;
  }



  /**
   * 异常处理
   * @param cosClientException
   * @param key
   */
  private void logStorageException(CosClientException cosClientException, String key) {
    String errorMsg = cosClientException.getMessage();
    String localizeErrorMsg = cosClientException.getLocalizedMessage();
    if (key.isEmpty()) {
      log.error("Exception occurred accessing object(s) from storage: localizeErrorMsg: {} {}",
        value("localizeErrorMsg", localizeErrorMsg),
        value("errorMsg", errorMsg));
    } else {
      log.error("Exception occurred accessing object(s) from storage: localizeErrorMsg: {} {}",
        value("key", key),
        value("localizeErrorMsg", localizeErrorMsg),
        value("errorMsg", errorMsg));
    }
  }

  private boolean filterCosObjectSummary(COSObjectSummary cosObjectSummary, String metadataFilename) {
    return cosObjectSummary.getKey().endsWith(metadataFilename);
  }

  private String buildCosKey(String group, String objectKey, String metadataFilename) {
    if (objectKey.endsWith(metadataFilename)) {
      return objectKey;
    }

    return (buildTypedFolder(rootFolder, group) + "/" + objectKey.toLowerCase() + "/" + metadataFilename).replace("//", "/");
  }

  private String buildObjectKey(ObjectType objectType, String key) {
    String resultKey = key
      .replaceAll(buildTypedFolder(rootFolder, objectType.group) + "/", "")
      .replaceAll("/" + objectType.defaultMetadataFilename, "");
    return resultKey;
  }

  private static String buildTypedFolder(String rootFolder, String type) {
    return (rootFolder + "/" + type).replaceAll("//", "/");
  }

  private String buildKeyPath(String type, String objectKey, String metadatafilename) {
    if (objectKey.endsWith(metadatafilename)) {
      return objectKey;
    }
    String key = this.rootFolder + "/" +type + "/" + objectKey.toLowerCase();
    if (!metadatafilename.isEmpty()) {
      key += "/" + metadatafilename.replace("//", "/");
    }
    return key;
  }

  private <T extends Timestamped> T deserialize(COSObject cosObject, Class<T> clazz) throws IOException {
    return objectMapper.readValue(cosObject.getObjectContent(), clazz);
  }
}
