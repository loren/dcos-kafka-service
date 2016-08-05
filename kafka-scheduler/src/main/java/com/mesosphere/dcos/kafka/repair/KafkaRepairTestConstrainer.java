package com.mesosphere.dcos.kafka.repair;

import com.mesosphere.dcos.kafka.offer.OfferUtils;
import org.apache.mesos.scheduler.repair.constrain.LaunchConstrainer;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * This implementation can be used to test a simple 3 node kafka setup, by preventing broker 2 from ever launching.
 * This makes it easier to observe the behavior of stopped tasks in the API.
 */
public class KafkaRepairTestConstrainer implements LaunchConstrainer {
    @Override
    public void launchHappened(Protos.Offer.Operation launchOperation) {
        // this doesn't affect this implementation's behavior
    }

    @Override
    public boolean canLaunch(OfferRequirement offerRequirement) {
        Set<String> names = offerRequirement.getTaskRequirements().stream()
                .map(it -> it.getTaskInfo().getName())
                .collect(Collectors.toSet());
        if (names.contains(OfferUtils.idToName(2))) {
            return false;
        }
        return true;
    }
}
