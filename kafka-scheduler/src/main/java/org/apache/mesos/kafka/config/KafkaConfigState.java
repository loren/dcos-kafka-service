package org.apache.mesos.kafka.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.curator.framework.CuratorFramework;

import org.apache.mesos.config.FrameworkConfigurationService;

import org.apache.mesos.config.state.ConfigState;
import org.apache.mesos.kafka.state.KafkaStateUtils;
import org.apache.mesos.state.StateStoreException;

public class KafkaConfigState {
  private final Log log = LogFactory.getLog(KafkaConfigState.class);

  private CuratorFramework zkClient = null;
  private ConfigState configState = null;
  private String configTargetPath = null; 

  public KafkaConfigState(String frameworkName, String hosts, String rootZkPath) {
    this.configTargetPath = "/" + frameworkName + "/config_target";

    zkClient = KafkaStateUtils.createZkClient(hosts);
    configState = new ConfigState(frameworkName, rootZkPath, zkClient);
  }

  public void store(FrameworkConfigurationService configurationService, String version) throws StateStoreException {
    configState.store(configurationService, version);
  }

  public KafkaConfigService fetch(String version) throws StateStoreException {
    return KafkaConfigService.getHydratedConfig(configState.fetch(version));
  }

  public boolean hasTarget() {
    try {
      return null != zkClient.checkExists().forPath(configTargetPath); 
    } catch (Exception ex) {
      log.error("Failed to determine existence of target config with exception: " + ex);
      return false;
    }
  }

  public void setTargetName(String targetConfigName) {
    try {
      byte[] bytes = targetConfigName.getBytes("UTF-8");

      if (!hasTarget()) {
        zkClient.create().creatingParentsIfNeeded().forPath(configTargetPath, bytes);
      } else {
        zkClient.setData().forPath(configTargetPath, bytes);
      }
    } catch (Exception ex) {
      log.error("Failed to set target config with exception: " + ex);
    }
  }

  public String getTargetName() {
    try {
      byte[] bytes = zkClient.getData().forPath(configTargetPath);
      return new String(bytes, "UTF-8");
    } catch (Exception ex) {
      log.error("Failed to retrieve config target name with exception: " + ex);
      return null;
    }
  }

  public KafkaConfigService getTargetConfig() {
    return fetch(getTargetName());
  }
}