package com.mesosphere.dcos.kafka.repair;

import com.mesosphere.dcos.kafka.config.ServiceConfiguration;
import com.mesosphere.dcos.kafka.offer.KafkaOfferRequirementProvider;
import com.mesosphere.dcos.kafka.offer.OfferUtils;
import com.mesosphere.dcos.kafka.plan.KafkaUpdateBlock;
import org.apache.mesos.scheduler.repair.RepairOfferRequirementProvider;
import com.mesosphere.dcos.kafka.state.FrameworkState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.state.StateStore;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * This class implements the {@link RepairOfferRequirementProvider} interface for the Kafka framework.
 */
public class KafkaRepairOfferRequirementProvider implements RepairOfferRequirementProvider {
    private static final Log log = LogFactory.getLog(KafkaOfferRequirementProvider.class);

    private final KafkaOfferRequirementProvider offerRequirementProvider;
    private final StateStore stateStore;
    private final ServiceConfiguration serviceConfiguration;

    public KafkaRepairOfferRequirementProvider(KafkaOfferRequirementProvider offerRequirementProvider, StateStore stateStore, ServiceConfiguration serviceConfiguration) {
        this.offerRequirementProvider = offerRequirementProvider;
        this.stateStore = stateStore;
        this.serviceConfiguration = serviceConfiguration;
    }

    /**
     * This figures out whether any brokers are missing, and chooses one at random to attempt to replace.
     *
     * The actual replacement offer requirements come from the {@link KafkaOfferRequirementProvider}.
     *
     * @param targetConfigName The current configuration we're moving towards
     * @param block
     * @return
     * @throws InvalidRequirementException
     * @throws ConfigStoreException
     */
    @Override
    public Optional<OfferRequirement> maybeGetNewOfferRequirement(String targetConfigName, Block block) throws InvalidRequirementException, ConfigStoreException {
        List<Integer> missingBrokerIds = getMissingBrokerIds(block);
        log.info("Missing brokerIds: " + missingBrokerIds);
        if (missingBrokerIds.size() > 0) {
            Integer brokerId = missingBrokerIds.get(new Random().nextInt(missingBrokerIds.size()));
            try {
                return Optional.of(offerRequirementProvider.getNewOfferRequirement(targetConfigName, brokerId));
            } catch (IOException | URISyntaxException e) {
                log.warn("Error when getting offer requirements", e);
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * This creates a replacement {@link OfferRequirement} from the terminated {@link TaskInfo}
     * by delegating to {@link KafkaOfferRequirementProvider}.
     * @param terminatedTask The task that has stopped and needs to be relaunched.
     * @return
     * @throws InvalidRequirementException
     */
    @Override
    public OfferRequirement getReplacementOfferRequirement(TaskInfo terminatedTask) throws InvalidRequirementException {
        return offerRequirementProvider.getReplacementOfferRequirement(terminatedTask);
    }

    /**
     * Computes whether any brokers are currently missing.
     *
     * First, checks whether there should be any brokers.
     *
     * If there should be brokers, then it finds the id of "last" broker,
     * which is required to be the largest id.
     *
     * Next, it scans all the {@link TaskInfo} currently known to to the
     * {@link FrameworkState}, and adds any missing brokers to the list,
     * unless that broker is being worked on by the current block, in which
     * case it pretends that broker exists.
     *
     * @param block The current {@link Block} of the current plan
     * @return A list of the integer numbers of the missing brokers.
     */
    private List<Integer> getMissingBrokerIds(Block block) {
        List<Integer> missingBrokerIds = new ArrayList<>();

        Integer lastExpectedBrokerId = getLastExpectedBrokerId(block);
        log.info("lastExpectedBrokerId="+lastExpectedBrokerId);

        if (lastExpectedBrokerId < 0) {
            return missingBrokerIds;
        }

        List<TaskInfo> brokerTasks = null;
        try {
            brokerTasks = new ArrayList<>(stateStore.fetchTasks());
        } catch (Exception ex) {
            log.error("Failed to fetch TaskInfos with exception: " + ex);
            return missingBrokerIds;
        }

        if (block != null) {
            log.info("current block: " + block.getName());
        }
        log.info("all tasks: "+brokerTasks.stream().map(TaskInfo::getName).collect(Collectors.joining(", ")));

        for (Integer i = 0; i <= lastExpectedBrokerId; i++) {
            if (!brokerExists(brokerTasks, i)) {
                String brokerName = OfferUtils.idToName(i);
                if (block == null || !brokerName.equals(block.getName())) {
                    missingBrokerIds.add(i);
                }
            }
        }

        return missingBrokerIds;
    }

    /**
     * Determines whether a given broker id is present in the list of tasks
     * @param brokerTasks The list of tasks to search for the broker id
     * @param brokerId The broker id to search for
     * @return true if that broker is represented by one of the tasks, false otherwise
     */
    private boolean brokerExists(List<TaskInfo> brokerTasks, int brokerId) {
        String brokerName = OfferUtils.idToName(brokerId);

        for (TaskInfo brokerTask : brokerTasks) {
            if (brokerTask.getName().equals(brokerName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Computes the last (which should mean largest) broker id.
     *
     * If there's no active {@link Block}, this will be the number of active tasks minus one,
     * as this framework assumes that only brokers will ever be launchHappened.
     *
     * If the current block is updating a Kafka broker indexed {@literal i},
     * we use the previous broker {@literal i-1}.
     *
     * In all other cases, we return {@literal -1} as a special sentinal value.
     *
     * @param block The current block
     * @return the index of the largest/last broker, or -1 if we don't know
     */
    private Integer getLastExpectedBrokerId(Block block) {
        if (block == null) {
            return serviceConfiguration.getCount() - 1;
        } else if (block instanceof KafkaUpdateBlock) {
            int brokerId = ((KafkaUpdateBlock)block).getBrokerId();
            return brokerId - 1;
        } else {
            // Reconciliation block
            return -1;
        }
    }
}
