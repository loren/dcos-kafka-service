package org.apache.mesos.kafka.scheduler;

import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableLookup;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.java8.Java8Bundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.mesos.kafka.cmd.CmdExecutor;
import org.apache.mesos.kafka.config.DropwizardConfiguration;
import org.apache.mesos.kafka.state.KafkaStateService;
import org.apache.mesos.kafka.web.BrokerCheck;
import org.apache.mesos.kafka.web.BrokerController;
import org.apache.mesos.kafka.web.ClusterController;
import org.apache.mesos.kafka.web.TopicController;
import org.apache.mesos.scheduler.plan.api.StageResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * Main entry point for the Scheduler.
 */
public final class Main extends Application<DropwizardConfiguration> {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    System.out.println("TEST BOO");
    new Main().run(args);
  }

  protected Main() {
    super();
  }

  ExecutorService kafkaSchedulerExecutorService = null;

  @Override
  public String getName() {
    return "DCOS Kafka Service";
  }

  @Override
  public void initialize(Bootstrap<DropwizardConfiguration> bootstrap) {
    super.initialize(bootstrap);

    StrSubstitutor strSubstitutor = new StrSubstitutor(new EnvironmentVariableLookup(false));
    strSubstitutor.setEnableSubstitutionInVariables(true);

    bootstrap.addBundle(new Java8Bundle());
    bootstrap.setConfigurationSourceProvider(
            new SubstitutingSourceProvider(
                    bootstrap.getConfigurationSourceProvider(),
                    strSubstitutor));
  }

  @Override
  public void run(DropwizardConfiguration configuration, Environment environment) throws Exception {
    LOGGER.info("" + configuration);

    final KafkaScheduler kafkaScheduler = new KafkaScheduler(configuration.getSchedulerConfiguration(), environment);

    registerJerseyResources(kafkaScheduler, environment, configuration);
    registerHealthChecks(kafkaScheduler, environment);

    kafkaSchedulerExecutorService = environment.lifecycle().
            executorService("KafkaScheduler")
            .minThreads(1)
            .maxThreads(2)
            .build();
    kafkaSchedulerExecutorService.submit(kafkaScheduler);
  }

  private void registerJerseyResources(
          KafkaScheduler kafkaScheduler,
          Environment environment,
          DropwizardConfiguration configuration) {
    final KafkaStateService kafkaState = kafkaScheduler.getKafkaState();
    environment.jersey().register(new ClusterController(
            configuration.getSchedulerConfiguration().getKafkaConfiguration().getKafkaZkUri(),
            kafkaScheduler.getConfigState(), kafkaState));
    environment.jersey().register(new BrokerController(kafkaState));
    environment.jersey().register(
            new TopicController(new CmdExecutor(configuration.getSchedulerConfiguration(), kafkaState), kafkaState));
    environment.jersey().register(new StageResource(kafkaScheduler.getStageManager()));
  }

  private void registerHealthChecks(
          KafkaScheduler kafkaScheduler,
          Environment environment) {

    environment.healthChecks().register(
        BrokerCheck.NAME,
        new BrokerCheck(
          kafkaScheduler.getStageManager(),
          kafkaScheduler.getKafkaState()));
  }
}
