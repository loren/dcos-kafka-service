package com.mesosphere.dcos.kafka.plan;

import com.mesosphere.dcos.kafka.test.KafkaTestUtils;
import org.apache.mesos.Protos;
import com.mesosphere.dcos.kafka.config.KafkaConfigState;
import com.mesosphere.dcos.kafka.offer.PersistentOfferRequirementProvider;
import com.mesosphere.dcos.kafka.state.FrameworkState;
import org.apache.mesos.offer.OfferRequirement;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.UUID;

import static org.mockito.Mockito.when;

/**
 * This class tests the KafkaUpdateBlock class.
 */
public class KafkaUpdateBlockTest {
    @Mock private FrameworkState frameworkState;
    @Mock private KafkaConfigState configState;
    private PersistentOfferRequirementProvider offerRequirementProvider;
    private KafkaUpdateBlock updateBlock;

    @Before
    public void beforeEach() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(frameworkState.getFrameworkId()).thenReturn(KafkaTestUtils.testFrameworkId);
        when(configState.fetch(UUID.fromString(KafkaTestUtils.testConfigName))).thenReturn(KafkaTestUtils.getTestKafkaSchedulerConfiguration());
        offerRequirementProvider = new PersistentOfferRequirementProvider(frameworkState, configState);
        updateBlock =
                new KafkaUpdateBlock(
                        frameworkState,
                        offerRequirementProvider,
                        KafkaTestUtils.testConfigName,
                        0);
    }

    @Test
    public void testKafkaUpdateBlockConstruction() {
        Assert.assertNotNull(updateBlock);
    }

    @Test
    public void testStart() {
        OfferRequirement offerRequirement = updateBlock.start();
        Assert.assertNotNull(offerRequirement);
        Assert.assertEquals(1, offerRequirement.getTaskRequirements().size());
        Assert.assertNotNull(offerRequirement.getExecutorRequirement());
    }

    @Test
    public void testUpdateWhilePending() {
        Assert.assertTrue(updateBlock.isPending());
        updateBlock.update(getRunningTaskStatus("bad-task-id"));
        Assert.assertTrue(updateBlock.isPending());
    }

    @Test
    public void testUpdateUnknownTaskId() {
        Assert.assertTrue(updateBlock.isPending());
        updateBlock.start();
        updateBlock.updateOfferStatus(true);
        Assert.assertTrue(updateBlock.isInProgress());
        updateBlock.update(getRunningTaskStatus("bad-task-id"));
        Assert.assertTrue(updateBlock.isInProgress());
    }

    @Test
    public void testReconciliationUpdate() {
        Assert.assertTrue(updateBlock.isPending());
        updateBlock.start();
        updateBlock.updateOfferStatus(true);
        Assert.assertTrue(updateBlock.isInProgress());
        Protos.TaskID taskId = updateBlock.getPendingTaskIds().get(0);
        Protos.TaskStatus reconciliationTaskStatus = getRunningTaskStatus(taskId.getValue());
        reconciliationTaskStatus = Protos.TaskStatus.newBuilder(reconciliationTaskStatus)
                .setReason(Protos.TaskStatus.Reason.REASON_RECONCILIATION)
                .build();
        updateBlock.update(reconciliationTaskStatus);
        Assert.assertTrue(updateBlock.isInProgress());
    }

    @Test
    public void testUpdateExpectedTaskIdRunning() {
        Assert.assertTrue(updateBlock.isPending());
        updateBlock.start();
        updateBlock.updateOfferStatus(true);
        Assert.assertTrue(updateBlock.isInProgress());
        Protos.TaskID taskId = updateBlock.getPendingTaskIds().get(0);
        updateBlock.update(getRunningTaskStatus(taskId.getValue()));
        Assert.assertTrue(updateBlock.isComplete());
    }

    @Test
    public void testUpdateExpectedTaskIdTerminated() {
        Assert.assertTrue(updateBlock.isPending());
        updateBlock.start();
        updateBlock.updateOfferStatus(true);
        Assert.assertTrue(updateBlock.isInProgress());
        Protos.TaskID taskId = updateBlock.getPendingTaskIds().get(0);
        updateBlock.update(getFailedTaskStatus(taskId.getValue()));
        Assert.assertTrue(updateBlock.isPending());
    }

    private Protos.TaskStatus getRunningTaskStatus(String taskId) {
        return getTaskStatus(taskId, Protos.TaskState.TASK_RUNNING);
    }

    private Protos.TaskStatus getFailedTaskStatus(String taskId) {
        return getTaskStatus(taskId, Protos.TaskState.TASK_FAILED);
    }

    private Protos.TaskStatus getTaskStatus(String taskId, Protos.TaskState state) {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(taskId))
                .setState(state)
                .build();
    }
}
