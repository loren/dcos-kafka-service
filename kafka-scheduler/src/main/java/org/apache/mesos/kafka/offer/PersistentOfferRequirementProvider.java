package org.apache.mesos.kafka.offer;

import com.google.common.base.Joiner;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mesosphere.dcos.kafka.common.KafkaTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.Environment.Variable;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.kafka.config.*;
import org.apache.mesos.kafka.state.KafkaStateService;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.PlacementStrategy;
import org.apache.mesos.offer.ResourceUtils;
import org.apache.mesos.offer.TaskRequirement;
import org.apache.mesos.offer.TaskRequirement.InvalidTaskRequirementException;
import org.apache.mesos.protobuf.CommandInfoBuilder;
import org.apache.mesos.protobuf.EnvironmentBuilder;
import org.apache.mesos.protobuf.LabelBuilder;
import org.apache.mesos.protobuf.ValueBuilder;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class PersistentOfferRequirementProvider implements KafkaOfferRequirementProvider {
  private final Log log = LogFactory.getLog(PersistentOfferRequirementProvider.class);

  public static final String CONFIG_ID_KEY = "CONFIG_ID";
  public static final String CONFIG_TARGET_KEY = "config_target";

  private final KafkaConfigState configState;
  private final KafkaStateService kafkaStateService;
  private final PlacementStrategyManager placementStrategyManager;

  public PersistentOfferRequirementProvider(
      KafkaStateService kafkaStateService, KafkaConfigState configState) {
    this.configState = configState;
    this.kafkaStateService = kafkaStateService;
    this.placementStrategyManager = new PlacementStrategyManager(kafkaStateService);
  }

  @Override
  public OfferRequirement getNewOfferRequirement(String configName, int brokerId)
          throws TaskRequirement.InvalidTaskRequirementException {
    OfferRequirement offerRequirement = getNewOfferRequirementInternal(configName, brokerId);
    log.info("Got new OfferRequirement with TaskRequirements: " + offerRequirement.getTaskRequirements());

    return offerRequirement;
  }

  @Override
  public OfferRequirement getReplacementOfferRequirement(TaskInfo existingTaskInfo)
          throws TaskRequirement.InvalidTaskRequirementException {
    final ExecutorInfo existingExecutor = existingTaskInfo.getExecutor();

    final TaskInfo.Builder replacementTaskInfo = TaskInfo.newBuilder(existingTaskInfo);
    replacementTaskInfo.clearExecutor();
    replacementTaskInfo.clearTaskId();
    replacementTaskInfo.setTaskId(TaskID.newBuilder().setValue("").build()); // Set later

    final ExecutorInfo.Builder replacementExecutor = ExecutorInfo.newBuilder(existingExecutor);
    replacementExecutor.clearExecutorId();
    replacementExecutor.setExecutorId(ExecutorID.newBuilder().setValue("").build()); // Set later

    OfferRequirement offerRequirement = new OfferRequirement(
            Arrays.asList(replacementTaskInfo.build()),
            replacementExecutor.build(), null, null);

    log.info("Replacement OfferRequirement with TaskRequirements: " + offerRequirement.getTaskRequirements());
    log.info("Replacement OfferRequirement with ExecutorRequirement: " + offerRequirement.getExecutorRequirement());

    return offerRequirement;
  }

  @Override
  public OfferRequirement getUpdateOfferRequirement(String configName, TaskInfo taskInfo)
          throws TaskRequirement.InvalidTaskRequirementException {
    KafkaSchedulerConfiguration config = configState.fetch(UUID.fromString(configName));
    BrokerConfiguration brokerConfig = config.getBrokerConfiguration();

    TaskInfo.Builder taskBuilder = TaskInfo.newBuilder(taskInfo);
    final ExecutorInfo existingExecutor = taskBuilder.getExecutor();

    taskBuilder = updateConfigTarget(taskBuilder, configName);
    taskBuilder = updateCpu(taskBuilder, brokerConfig);
    taskBuilder = updateMem(taskBuilder, brokerConfig);
    taskBuilder = updateDisk(taskBuilder, brokerConfig);
    taskBuilder = updateCmd(taskBuilder, configName);
    taskBuilder = updateKafkaHeapOpts(taskBuilder, brokerConfig);

    final ExecutorInfo.Builder updatedExecutor = ExecutorInfo.newBuilder(existingExecutor);
    updatedExecutor.clearExecutorId();
    updatedExecutor.setExecutorId(ExecutorID.newBuilder().setValue("").build()); // Set later

    taskBuilder.clearExecutor();
    taskBuilder.clearTaskId();
    taskBuilder.setTaskId(TaskID.newBuilder().setValue("").build()); // Set later

    TaskInfo updatedTaskInfo = taskBuilder.build();

    try {
      OfferRequirement offerRequirement =
          new OfferRequirement(Arrays.asList(updatedTaskInfo), updatedExecutor.build(), null, null);

      log.info("Update OfferRequirement with TaskRequirements: " + offerRequirement.getTaskRequirements());
      log.info("Update OfferRequirement with ExecutorRequirement: " + offerRequirement.getExecutorRequirement());

      return offerRequirement;
    } catch (InvalidTaskRequirementException e) {
      log.warn("Failed to create update OfferRequirement with "
          + "OrigTaskInfo[" + taskInfo + "] NewTaskInfo[" + updatedTaskInfo + "]", e);
      return null;
    }
  }

  private String getKafkaHeapOpts(HeapConfig heapConfig) {
    return String.format("-Xms%1$dM -Xmx%1$dM", heapConfig.getSizeMb());
  }

  private TaskInfo.Builder updateKafkaHeapOpts(TaskInfo.Builder taskBuilder, BrokerConfiguration brokerConfig) {
    try {
      final CommandInfo oldCommand = CommandInfo.parseFrom(taskBuilder.getData());
      final Environment oldEnvironment = oldCommand.getEnvironment();

      final Map<String, String> newEnvMap = fromEnvironmentToMap(oldEnvironment);
      newEnvMap.put("KAFKA_HEAP_OPTS", getKafkaHeapOpts(brokerConfig.getHeap()));

      final CommandInfo.Builder newCommandBuilder = CommandInfo.newBuilder(oldCommand);
      newCommandBuilder.clearEnvironment();

      final List<Variable> newEnvironmentVariables = EnvironmentBuilder.createEnvironment(newEnvMap);
      newCommandBuilder.setEnvironment(Environment.newBuilder().addAllVariables(newEnvironmentVariables));

      taskBuilder.clearData();
      taskBuilder.setData(newCommandBuilder.build().toByteString());

      log.info("Updated env map:" + newEnvMap);
      return taskBuilder;
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  private Map<String, String> fromEnvironmentToMap(Environment environment) {
    Map<String, String> map = new HashMap<>();

    final List<Variable> variablesList = environment.getVariablesList();

    for (Variable variable : variablesList) {
      map.put(variable.getName(), variable.getValue());
    }

    return map;
  }

  private TaskInfo.Builder updateCpu(TaskInfo.Builder taskBuilder, BrokerConfiguration brokerConfig) {
    ValueBuilder valBuilder = new ValueBuilder(Value.Type.SCALAR);
    valBuilder.setScalar(brokerConfig.getCpus());
    return updateValue(taskBuilder, "cpus", valBuilder.build());
  }

  private TaskInfo.Builder updateMem(TaskInfo.Builder taskBuilder, BrokerConfiguration brokerConfig) {
    ValueBuilder valBuilder = new ValueBuilder(Value.Type.SCALAR);
    valBuilder.setScalar(brokerConfig.getMem());
    return updateValue(taskBuilder, "mem", valBuilder.build());
  }

  private TaskInfo.Builder updateDisk(TaskInfo.Builder taskBuilder, BrokerConfiguration brokerConfig) {
    ValueBuilder valBuilder = new ValueBuilder(Value.Type.SCALAR);
    valBuilder.setScalar(brokerConfig.getDisk());
    return updateValue(taskBuilder, "disk", valBuilder.build());
  }

  private TaskInfo.Builder updateValue(TaskInfo.Builder taskBuilder, String name, Value updatedValue) {
    List<Resource> updatedResources = new ArrayList<Resource>();

    for (Resource resource : taskBuilder.getResourcesList()) {
      if (name.equals(resource.getName())) {
        updatedResources.add(ResourceUtils.setValue(resource, updatedValue));
      } else {
        updatedResources.add(resource);
      }
    }

    taskBuilder.clearResources();
    taskBuilder.addAllResources(updatedResources);
    return taskBuilder;
  }

  private TaskInfo.Builder updateConfigTarget(TaskInfo.Builder taskBuilder, String configName) {
    LabelBuilder labelBuilder = new LabelBuilder();

    // Copy everything except config target label
    for (Label label : taskBuilder.getLabels().getLabelsList()) {
      String key = label.getKey();
      String value = label.getValue();

      if (!key.equals(CONFIG_TARGET_KEY)) {
        labelBuilder.addLabel(key, value);
      }
    }

    labelBuilder.addLabel(CONFIG_TARGET_KEY, configName);
    taskBuilder.setLabels(labelBuilder.build());
    return taskBuilder;
  }

  private TaskInfo.Builder updateCmd(TaskInfo.Builder taskBuilder, String configName) {
    EnvironmentBuilder envBuilder = new EnvironmentBuilder();

    try {
      final CommandInfo existingCommandInfo = CommandInfo.parseFrom(taskBuilder.getData());
      for (Variable variable : existingCommandInfo.getEnvironment().getVariablesList()) {
        if (variable.getName().equals(CONFIG_ID_KEY)) {
          envBuilder.addVariable(CONFIG_ID_KEY, configName);
        } else {
          envBuilder.addVariable(variable.getName(), variable.getValue());
        }
      }

      CommandInfo.Builder cmdBuilder = CommandInfo.newBuilder(existingCommandInfo);
      cmdBuilder.setEnvironment(envBuilder.build());

      taskBuilder.clearData();
      taskBuilder.setData(cmdBuilder.build().toByteString());

      return taskBuilder;
    } catch (Exception e) {
      log.error("Error parsing commandInfo: ", e);
      return null;
    }
  }

  private OfferRequirement getNewOfferRequirementInternal(String configName, int brokerId) throws TaskRequirement.InvalidTaskRequirementException {
    log.info("Getting new OfferRequirement for: " + configName);
    String overridePrefix = KafkaSchedulerConfiguration.KAFKA_OVERRIDE_PREFIX;
    String brokerName = OfferUtils.idToName(brokerId);
    Long port = 9092 + ThreadLocalRandom.current().nextLong(0, 1000);
    Long jmxPort = 11000 + ThreadLocalRandom.current().nextLong(0, 1000);

    String containerPath = "kafka-volume-" + UUID.randomUUID();

    KafkaSchedulerConfiguration config = configState.fetch(UUID.fromString(configName));
    BrokerConfiguration brokerConfig = config.getBrokerConfiguration();
    ExecutorConfiguration executorConfig = config.getExecutorConfiguration();

    String role = config.getServiceConfiguration().getRole();
    String principal = config.getServiceConfiguration().getPrincipal();
    String frameworkName = config.getServiceConfiguration().getName();

    // Construct Kafka launch command
    Map<String, String> taskEnv = new HashMap<>();
    taskEnv.put("FRAMEWORK_NAME", frameworkName);
    taskEnv.put("KAFKA_VER_NAME", config.getKafkaConfiguration().getKafkaVerName());
    taskEnv.put(CONFIG_ID_KEY, configName);
    taskEnv.put(overridePrefix + "ZOOKEEPER_CONNECT", config.getKafkaConfiguration().getZkAddress() + "/" + frameworkName);
    taskEnv.put(overridePrefix + "BROKER_ID", Integer.toString(brokerId));
    taskEnv.put(overridePrefix + "LOG_DIRS", containerPath + "/" + brokerName);
    taskEnv.put(overridePrefix + "PORT", Long.toString(port));
    taskEnv.put(overridePrefix + "LISTENERS", "PLAINTEXT://:" + port);
    taskEnv.put("KAFKA_HEAP_OPTS", getKafkaHeapOpts(brokerConfig.getHeap()));

    List<String> commands = new ArrayList<>();
    commands.add("export PATH=$(ls -d $MESOS_SANDBOX/jre*/bin):$PATH"); // find directory that starts with "jre" containing "bin"
    commands.add("$MESOS_SANDBOX/overrider/bin/kafka-config-overrider server $MESOS_SANDBOX/overrider/conf/scheduler.yml");
    final KafkaConfiguration kafkaConfiguration = config.getKafkaConfiguration();
    commands.add(String.format(
        "exec $MESOS_SANDBOX/%1$s/bin/kafka-server-start.sh " +
        "$MESOS_SANDBOX/%1$s/config/server.properties ",
        kafkaConfiguration.getKafkaVerName()));
    final String kafkaLaunchCommand = Joiner.on(" && ").join(commands);

    log.info("Configuring kafkaLaunchCommand as: " + kafkaLaunchCommand);

    final CommandInfoBuilder brokerTaskBuilder = new CommandInfoBuilder();
    brokerTaskBuilder.setCommand(kafkaLaunchCommand);
    brokerTaskBuilder
      .addEnvironmentVar("TASK_TYPE", KafkaTask.BROKER.name())
      .addEnvironmentVar("FRAMEWORK_NAME", frameworkName)
      .addEnvironmentVar("KAFKA_VER_NAME", kafkaConfiguration.getKafkaVerName())
      .addEnvironmentVar(CONFIG_ID_KEY, configName)
      .addEnvironmentVar(overridePrefix + "ZOOKEEPER_CONNECT", kafkaConfiguration.getZkAddress() + "/" + frameworkName)
      .addEnvironmentVar(overridePrefix + "BROKER_ID", Integer.toString(brokerId))
      .addEnvironmentVar(overridePrefix + "LOG_DIRS", containerPath + "/" + brokerName)
      .addEnvironmentVar(overridePrefix + "LISTENERS", "PLAINTEXT://:" + port)
      .addEnvironmentVar(overridePrefix + "PORT", Long.toString(port))
      .addEnvironmentVar("JMX_PORT", Long.toString(jmxPort));

    // Launch command for custom executor
    final String executorCommand = "./executor/bin/kafka-executor -Dlogback.configurationFile=executor/conf/logback.xml";

    CommandInfoBuilder executorCommandBuilder = new CommandInfoBuilder()
      .setCommand(executorCommand)
      .addEnvironmentVar("JAVA_HOME", "jre1.8.0_91")
      .addUri(brokerConfig.getJavaUri())
      .addUri(brokerConfig.getKafkaUri())
      .addUri(brokerConfig.getOverriderUri())
      .addUri(executorConfig.getExecutorUri());

    // Build Executor
    final Protos.ExecutorInfo.Builder executorBuilder = Protos.ExecutorInfo.newBuilder();

    executorBuilder
      .setName(brokerName)
      .setExecutorId(Protos.ExecutorID.newBuilder().setValue("").build()) // Set later
      .setFrameworkId(kafkaStateService.getFrameworkId())
      .setCommand(executorCommandBuilder.build())
      .addResources(ResourceUtils.getDesiredScalar(role, principal, "cpus", executorConfig.getCpus()))
      .addResources(ResourceUtils.getDesiredScalar(role, principal, "mem", executorConfig.getMem()))
      .addResources(ResourceUtils.getDesiredScalar(role, principal, "disk", executorConfig.getDisk()));

    // Build Task
    TaskInfo.Builder taskBuilder = TaskInfo.newBuilder();
    taskBuilder
      .setName(brokerName)
      .setTaskId(TaskID.newBuilder().setValue("").build()) // Set later
      .setSlaveId(SlaveID.newBuilder().setValue("").build()) // Set later
      .setData(brokerTaskBuilder.build().toByteString())
      .addResources(ResourceUtils.getDesiredScalar(role, principal, "cpus", brokerConfig.getCpus()))
      .addResources(ResourceUtils.getDesiredScalar(role, principal, "mem", brokerConfig.getMem()))
      .addResources(ResourceUtils.getDesiredRanges(
            role,
            principal,
            "ports",
            Arrays.asList(
              Range.newBuilder()
              .setBegin(port)
              .setEnd(port).build(),
              Range.newBuilder()
                      .setBegin(jmxPort)
                      .setEnd(jmxPort).build())
      ));

    if (brokerConfig.getDiskType().equals("MOUNT")) {
      taskBuilder.addResources(ResourceUtils.getDesiredMountVolume(
            role,
            principal,
            brokerConfig.getDisk(),
            containerPath));
    } else {
      taskBuilder.addResources(ResourceUtils.getDesiredRootVolume(
            role,
            principal,
            brokerConfig.getDisk(),
            containerPath));
    }

    taskBuilder
      .setLabels(Labels.newBuilder()
          .addLabels(Label.newBuilder()
            .setKey(CONFIG_TARGET_KEY)
            .setValue(configName))
          .build());

    log.info("TaskInfo.Builder contains executor: " + taskBuilder.hasExecutor());
    // Explicitly clear executor.
    taskBuilder.clearExecutor();
    taskBuilder.clearCommand();

    TaskInfo taskInfo = taskBuilder.build();
    log.info("TaskInfo contains executor: " + taskInfo.hasExecutor());

    final ExecutorInfo executorInfo = executorBuilder.build();

    PlacementStrategy placementStrategy = placementStrategyManager.getPlacementStrategy(config);
    List<SlaveID> avoidAgents = placementStrategy.getAgentsToAvoid(taskInfo);
    List<SlaveID> colocateAgents = placementStrategy.getAgentsToColocate(taskInfo);

    try {
      OfferRequirement offerRequirement = new OfferRequirement(
          Arrays.asList(taskInfo), executorInfo, avoidAgents, colocateAgents);
      log.info("Got new OfferRequirement with TaskInfo: " + taskInfo);
      return offerRequirement;
    } catch (InvalidTaskRequirementException e) {
      log.warn("Failed to create new OfferRequirement with TaskInfo: " + taskInfo, e);
      return null;
    }
  }
}
