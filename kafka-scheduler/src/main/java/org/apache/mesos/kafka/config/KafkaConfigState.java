package org.apache.mesos.kafka.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.Labels;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.config.ConfigStore;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.config.CuratorConfigStore;
import org.apache.mesos.kafka.offer.OfferUtils;
import org.apache.mesos.kafka.offer.PersistentOfferRequirementProvider;
import org.apache.mesos.kafka.state.KafkaStateService;
import org.apache.mesos.protobuf.LabelBuilder;
import org.apache.mesos.state.StateStoreException;

import java.util.*;

/**
 * Stores and manages multiple Kafka framework configurations in persistent storage.
 * Each configuration is in the form of a {@link KafkaSchedulerConfiguration}.
 */
public class KafkaConfigState {
  private final Log log = LogFactory.getLog(KafkaConfigState.class);

  private ConfigStore configStore;
  private KafkaConfigurationFactory kafkaConfigurationFactory;
  /**
   * Creates a new Kafka config state manager based on the provided bootstrap information.
   */
  public KafkaConfigState(String frameworkName, String zkHost) {
    this.configStore = new CuratorConfigStore("/" + frameworkName, zkHost, new ExponentialBackoffRetry(1000, 3));
    this.kafkaConfigurationFactory = new KafkaConfigurationFactory();

  }

  public KafkaSchedulerConfiguration fetch(UUID version) throws StateStoreException {
    try {
      return (KafkaSchedulerConfiguration) configStore.fetch(version, this.kafkaConfigurationFactory);
    } catch (ConfigStoreException e) {
      log.error("Unable to fetch version: " + version + " Reason: " + e);
      throw new StateStoreException(e);
    }
  }

  /**
   * Returns whether a current target configuration exists.
   */
  public boolean hasTarget() {
    try {
      return configStore.getTargetConfig() != null;
    } catch (Exception ex) {
      log.error("Failed to determine existence of target config with exception: " + ex);
      return false;
    }
  }

  /**
   * Returns the name of the current target configuration.
   */
  public UUID getTargetName() {
    try {
      return configStore.getTargetConfig();
    } catch (Exception ex) {
      log.error("Failed to retrieve config target name with exception: " + ex);
      return null;
    }
  }

  /**
   * Returns the content of the current target configuration.
   */
  public KafkaSchedulerConfiguration getTargetConfig() {
    return fetch(getTargetName());
  }

  /**
   * Returns a list of all available configuration names.
   */
  public Collection<UUID> getConfigNames() {
    try {
      return configStore.list();
    } catch (ConfigStoreException e) {
      return Collections.EMPTY_LIST;
    }
  }

  /**
   * Stores the provided configuration against the provided version label.
   *
   * @throws StateStoreException if the underlying storage failed to write
   */
  public UUID store(KafkaSchedulerConfiguration configuration) throws StateStoreException {
    try {
      return configStore.store(configuration);
    } catch (Exception e) {
      String msg = "Failure to store configurations.";
      log.error(msg);
      throw new StateStoreException(msg, e);
    }
  }

  /**
   * Sets the name of the target configuration to be used in the future.
   */
  public void setTargetName(UUID targetConfigName) {
    try {
      configStore.setTargetConfig(targetConfigName);
    } catch (Exception ex) {
      log.error("Failed to set target config with exception: " + ex);
    }
  }

  public void syncConfigs(KafkaStateService state) {
    try {
      UUID targetName = getTargetName();
      List<String> duplicateConfigs = getDuplicateConfigs();

      List<TaskInfo> taskInfos = state.getTaskInfos();
      for (TaskInfo taskInfo : taskInfos) {
        replaceDuplicateConfig(state, taskInfo, duplicateConfigs, targetName);
      }
    } catch (Exception ex) {
      log.error("Failed to synchronized configurations with exception: " + ex);
    }
  }

  public void cleanConfigs(KafkaStateService state) {
    Set<UUID> activeConfigs = new HashSet<>();
    activeConfigs.add(getTargetName());
    activeConfigs.addAll(getTaskConfigs(state));

    log.info("Cleaning all configs which are NOT in the active list: " + activeConfigs); 

    for (UUID configName : getConfigNames()) {
      if (!activeConfigs.contains(configName)) {
        try {
          log.info("Removing config: " + configName);
          configStore.clear(configName);
        } catch (ConfigStoreException e) {
          log.error("Unable to clear config: " + configName + " Reason: " + e);
        }
      }
    }
  }

  private Set<UUID> getTaskConfigs(KafkaStateService state) {
    Set<UUID> activeConfigs = new HashSet<>();

    try {
      for (TaskInfo taskInfo : state.getTaskInfos()) {
        Labels labels = taskInfo.getLabels();
        for (Label label : labels.getLabelsList()) {
          if (label.getKey().equals(PersistentOfferRequirementProvider.CONFIG_TARGET_KEY)) {
            activeConfigs.add(UUID.fromString(label.getValue()));
          }
        }
      }
    } catch (Exception ex) {
      log.error("Failed to fetch configurations from TaskInfos with exception: " + ex);
    }

    return activeConfigs;
  }

  private void replaceDuplicateConfig(KafkaStateService state, TaskInfo taskInfo, List<String> duplicateConfigs, UUID targetName) {
    try {
      String taskConfig = OfferUtils.getConfigName(taskInfo);

      for (String duplicateConfig : duplicateConfigs) {
        if (taskConfig.equals(duplicateConfig)) {
          Labels labels = new LabelBuilder()
            .addLabel(PersistentOfferRequirementProvider.CONFIG_TARGET_KEY, targetName.toString())
            .build();

          TaskInfo newTaskInfo = TaskInfo.newBuilder(taskInfo).setLabels(labels).build();
          state.recordTaskInfo(newTaskInfo);
          return;
        }
      }
    } catch (Exception ex) {
      log.error("Failed to replace duplicate configuration for taskInfo: " + taskInfo + " with exception: " + ex);
    }
  }

  /**
   * Returns the list of configs which are duplicates of the current Target config.
   */
  private List<String> getDuplicateConfigs() {
    KafkaSchedulerConfiguration newTargetConfig = getTargetConfig();

    List<String> duplicateConfigs = new ArrayList<String>();
    final Collection<UUID> configNames = getConfigNames();
    for (UUID configName : configNames) {
      KafkaSchedulerConfiguration currTargetConfig = fetch(configName);

      final BrokerConfiguration currBrokerConfig = currTargetConfig.getBrokerConfiguration();
      final BrokerConfiguration newBrokerConfig = newTargetConfig.getBrokerConfiguration();

      final KafkaConfiguration currKafkaConfig = currTargetConfig.getKafkaConfiguration();
      final KafkaConfiguration newKafkaConfig = newTargetConfig.getKafkaConfiguration();

      if (currBrokerConfig.equals(newBrokerConfig) &&
          currKafkaConfig.equals(newKafkaConfig)) {
        log.info("Duplicate config detected: " + configName);
        duplicateConfigs.add(configName.toString());
      }
    }

    return duplicateConfigs;
  }
}
