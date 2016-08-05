package com.mesosphere.dcos.kafka.scheduler;

import com.google.common.collect.Iterators;
import com.mesosphere.dcos.kafka.config.*;
import com.mesosphere.dcos.kafka.offer.KafkaOfferRequirementProvider;
import com.mesosphere.dcos.kafka.repair.KafkaRepairOfferRequirementProvider;
import com.mesosphere.dcos.kafka.state.ClusterState;
import com.mesosphere.dcos.kafka.test.KafkaTestUtils;
import org.apache.mesos.dcos.Capabilities;
import org.apache.mesos.scheduler.repair.*;
import org.apache.mesos.scheduler.repair.constrain.LaunchConstrainer;
import org.apache.mesos.scheduler.repair.constrain.TestingLaunchConstrainer;
import org.apache.mesos.scheduler.repair.constrain.UnconstrainedLaunchConstrainer;
import org.apache.mesos.scheduler.repair.monitor.FailureMonitor;
import org.apache.mesos.scheduler.repair.monitor.NeverFailureMonitor;
import org.apache.mesos.scheduler.repair.monitor.TestingFailureMonitor;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.config.ConfigStoreException;
import com.mesosphere.dcos.kafka.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.kafka.offer.PersistentOperationRecorder;
import com.mesosphere.dcos.kafka.state.FrameworkState;
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.offer.ResourceUtils;
import org.apache.mesos.state.StateStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * This class tests the Kafka RepairScheduler
 */
public class RepairSchedulerTest {
    @Mock private FrameworkState frameworkState;
    @Mock private StateStore stateStore;
    @Mock private TaskFailureListener failureListener;
    @Mock private KafkaConfigState configState;
    @Mock private SchedulerDriver driver;
    @Mock private ServiceConfiguration serviceConfiguration;
    @Mock private ClusterState clusterState;
    @Captor private ArgumentCaptor<Protos.TaskID> taskIdCaptor;
    @Captor private ArgumentCaptor<Collection<Protos.OfferID>> offerIdCaptor;
    @Captor private ArgumentCaptor<Collection<Protos.Offer.Operation>> operationCaptor;

    @Before
    public void beforeEach() throws ConfigStoreException {
        MockitoAnnotations.initMocks(this);
        when(frameworkState.getFrameworkId()).thenReturn(KafkaTestUtils.testFrameworkId);
        when(configState.fetch(UUID.fromString(KafkaTestUtils.testConfigName)))
            .thenReturn(ConfigTestUtils.getTestKafkaSchedulerConfiguration());
        when(serviceConfiguration.getCount()).thenReturn(3);
    }

    @Test
    public void testKafkaRepairSchedulerConstruction() throws Exception {
        Assert.assertNotNull(getTestKafkaRepairScheduler());
    }

    /**
     * This test validates that an active launch constrainer stops the broker from launching
     */
    @Test
    public void testConstrainedReplaceMissingBroker() throws Exception {
        List<Protos.TaskInfo> taskInfos = Arrays.asList(
                getDummyBrokerTaskInfo(0),
                getDummyBrokerTaskInfo(2));
        when(stateStore.fetchTasks()).thenReturn(taskInfos);

        TestingLaunchConstrainer constrainer = new TestingLaunchConstrainer();
        constrainer.setCanLaunch(false);
        RepairScheduler repairScheduler = getTestKafkaRepairScheduler(constrainer, new NeverFailureMonitor());
        List<Protos.OfferID> acceptedOfferIds = repairScheduler.resourceOffers(driver, Arrays.asList(getTestOfferSufficientForNewBroker()), null);
        Assert.assertEquals(0, acceptedOfferIds.size());
    }

    @Test
    public void testReplaceMissingBroker() throws Exception {
        // Test replacement of Broker-1 when expecting 3 Brokers of Ids(0, 1, and 2)
        List<Protos.TaskInfo> taskInfos = Arrays.asList(
                getDummyBrokerTaskInfo(0),
                getDummyBrokerTaskInfo(2));
        when(stateStore.fetchTasks()).thenReturn(taskInfos);

        RepairScheduler repairScheduler = getTestKafkaRepairScheduler();
        List<Protos.OfferID> acceptedOfferIds = repairScheduler.resourceOffers(driver, Arrays.asList(getTestOfferSufficientForNewBroker()), null);
        Assert.assertEquals(1, acceptedOfferIds.size());
        Assert.assertEquals(KafkaTestUtils.testOfferId, acceptedOfferIds.get(0).getValue());
        verify(driver, times(1)).acceptOffers(
                offerIdCaptor.capture(),
                operationCaptor.capture(),
                anyObject());

        Assert.assertTrue(offerIdCaptor.getValue().containsAll(acceptedOfferIds));
        int expectedOperationCount = 8;
        Assert.assertEquals(expectedOperationCount, operationCaptor.getValue().size());
        Protos.Offer.Operation launchOperation = Iterators.get(operationCaptor.getValue().iterator(), expectedOperationCount - 1);
        Assert.assertEquals(Protos.Offer.Operation.Type.LAUNCH, launchOperation.getType());
        Assert.assertEquals("broker-1", launchOperation.getLaunch().getTaskInfos(0).getName());
    }


    @Test
    public void testReplaceMissingLastBroker() throws Exception {
        // Test replacement of Broker-2 when expecting 3 Brokers of Ids(0, 1, and 2)
        List<Protos.TaskInfo> taskInfos = Arrays.asList(
                getDummyBrokerTaskInfo(0),
                getDummyBrokerTaskInfo(1));
        when(stateStore.fetchTasks()).thenReturn(taskInfos);

        RepairScheduler repairScheduler = getTestKafkaRepairScheduler();
        List<Protos.OfferID> acceptedOfferIds = repairScheduler.resourceOffers(driver, Arrays.asList(getTestOfferSufficientForNewBroker()), null);
        Assert.assertEquals(1, acceptedOfferIds.size());
        Assert.assertEquals(KafkaTestUtils.testOfferId, acceptedOfferIds.get(0).getValue());
        verify(driver, times(1)).acceptOffers(
                offerIdCaptor.capture(),
                operationCaptor.capture(),
                anyObject());

        Assert.assertTrue(offerIdCaptor.getValue().containsAll(acceptedOfferIds));
        int expectedOperationCount = 8;
        Assert.assertEquals(expectedOperationCount, operationCaptor.getValue().size());
        Protos.Offer.Operation launchOperation = Iterators.get(operationCaptor.getValue().iterator(), expectedOperationCount - 1);
        Assert.assertEquals(Protos.Offer.Operation.Type.LAUNCH, launchOperation.getType());
        Assert.assertEquals("broker-2", launchOperation.getLaunch().getTaskInfos(0).getName());
    }


    /**
     * This tests the logic in replacing a failed broker.
     *
     * We mocked the {@link FrameworkState}, and so if there a bug in how it reports
     * the running and terminated tasks, this test doesn't verify that.
     */
    @Test
    public void testReplaceFailedBroker() throws Exception {
        List<Protos.TaskInfo> taskInfos = Arrays.asList(
                getDummyBrokerTaskInfo(0),
                getDummyBrokerTaskInfo(1),
                getDummyBrokerTaskInfo(2));
        List<Protos.TaskInfo> taskInfoAfterDelete = Arrays.asList(
                taskInfos.get(0),
                taskInfos.get(2));
        when(stateStore.fetchTasks())
                .thenReturn(taskInfoAfterDelete);
        when(stateStore.fetchTerminatedTasks())
                .thenReturn(Collections.singletonList(taskInfos.get(1)))
                .thenReturn(Collections.EMPTY_LIST);

        TestingFailureMonitor monitor = new TestingFailureMonitor(taskInfos.get(0), taskInfos.get(1));
        RepairScheduler repairScheduler = getTestKafkaRepairScheduler(new UnconstrainedLaunchConstrainer(), monitor);
        List<Protos.OfferID> acceptedOfferIds = repairScheduler.resourceOffers(driver, Arrays.asList(getTestOfferSufficientForNewBroker()), null);
        Assert.assertEquals(0, acceptedOfferIds.size());
        verify(failureListener, times(1)).taskFailed(taskIdCaptor.capture());
        Assert.assertTrue(taskIdCaptor.getValue().equals(taskInfos.get(1).getTaskId()));

        acceptedOfferIds = repairScheduler.resourceOffers(driver, Arrays.asList(getTestOfferSufficientForNewBroker()), null);
        Assert.assertEquals(1, acceptedOfferIds.size());
        Assert.assertEquals(KafkaTestUtils.testOfferId, acceptedOfferIds.get(0).getValue());
        verify(driver, times(1)).acceptOffers(
                offerIdCaptor.capture(),
                operationCaptor.capture(),
                anyObject());

        Assert.assertTrue(offerIdCaptor.getValue().containsAll(acceptedOfferIds));
        int expectedOperationCount = 8;
        Assert.assertEquals(expectedOperationCount, operationCaptor.getValue().size());
        Protos.Offer.Operation launchOperation = Iterators.get(operationCaptor.getValue().iterator(), expectedOperationCount - 1);
        Assert.assertEquals(Protos.Offer.Operation.Type.LAUNCH, launchOperation.getType());
        Assert.assertEquals("broker-1", launchOperation.getLaunch().getTaskInfos(0).getName());
    }

    private Protos.TaskInfo getDummyBrokerTaskInfo(Integer id) {
        return Protos.TaskInfo.newBuilder()
                .setName("broker-" + id)
                .setTaskId(Protos.TaskID.newBuilder()
                        .setValue("broker-" + id + "__" + UUID.randomUUID())
                        .build())
                .setSlaveId(Protos.SlaveID.newBuilder()
                        .setValue(KafkaTestUtils.testSlaveId)
                        .build())
                .build();
    }

    private RepairScheduler getTestKafkaRepairScheduler() throws Exception {
        return getTestKafkaRepairScheduler(new UnconstrainedLaunchConstrainer(), new NeverFailureMonitor());
    }

    private RepairScheduler getTestKafkaRepairScheduler(LaunchConstrainer constrainer, FailureMonitor monitor) throws Exception {
        return new RepairScheduler(
                KafkaTestUtils.testConfigName,
                stateStore,
                failureListener,
                getTestOfferRequirementProvider(),
                getTestOfferAccepter(),
                constrainer,
                monitor,
                new AtomicReference<>());
    }

    private OfferAccepter getTestOfferAccepter() {
        return new OfferAccepter(Arrays.asList(new PersistentOperationRecorder(frameworkState)));
    }

    private KafkaRepairOfferRequirementProvider getTestOfferRequirementProvider() throws Exception {
        KafkaOfferRequirementProvider kafkaOfferRequirementProvider = new PersistentOfferRequirementProvider(frameworkState, configState, clusterState);
        Capabilities capabilities = mock(Capabilities.class);
        when(capabilities.supportsNamedVips()).thenReturn(false);
        when(clusterState.getCapabilities()).thenReturn(capabilities);
        return new KafkaRepairOfferRequirementProvider(kafkaOfferRequirementProvider, stateStore, serviceConfiguration);
    }

    public Protos.Offer getTestOfferSufficientForNewBroker() {
        BrokerConfiguration brokerConfiguration = ConfigTestUtils.getTestBrokerConfiguration();
        ExecutorConfiguration executorConfiguration = ConfigTestUtils.getTestExecutorConfiguration();

        Protos.Resource cpu = ResourceUtils.getUnreservedScalar("cpus", brokerConfiguration.getCpus() + executorConfiguration.getCpus());
        Protos.Resource mem = ResourceUtils.getUnreservedScalar("mem", brokerConfiguration.getMem() + executorConfiguration.getMem());
        Protos.Resource disk = ResourceUtils.getUnreservedRootVolume(brokerConfiguration.getDisk() + executorConfiguration.getDisk());
        Protos.Value.Range portRange = Protos.Value.Range.newBuilder()
                .setBegin(brokerConfiguration.getPort())
                .setEnd(brokerConfiguration.getPort())
                .build();
        Protos.Resource ports = ResourceUtils.getUnreservedRanges("ports",  Arrays.asList(portRange));

        return Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue(KafkaTestUtils.testOfferId))
                .setFrameworkId(KafkaTestUtils.testFrameworkId)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(KafkaTestUtils.testSlaveId))
                .setHostname(KafkaTestUtils.testHostname)
                .addResources(cpu)
                .addResources(mem)
                .addResources(disk)
                .addResources(ports)
                .build();
    }
}
