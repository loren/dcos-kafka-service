package com.mesosphere.dcos.kafka.scheduler;

import com.mesosphere.dcos.kafka.offer.KafkaOfferRequirementProvider;
import com.mesosphere.dcos.kafka.offer.OfferUtils;
import com.mesosphere.dcos.kafka.plan.KafkaUpdateBlock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.SchedulerDriver;
import com.mesosphere.dcos.kafka.state.FrameworkState;
import org.apache.mesos.offer.*;
import org.apache.mesos.scheduler.plan.Block;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class KafkaRepairScheduler {
  private final Log log = LogFactory.getLog(KafkaRepairScheduler.class);

  private final String targetConfigName;
  private final FrameworkState frameworkState;
  private final OfferAccepter offerAccepter;
  private final KafkaOfferRequirementProvider offerReqProvider;

  public KafkaRepairScheduler(
    String targetConfigName,
    FrameworkState frameworkState,
    KafkaOfferRequirementProvider offerReqProvider,
    OfferAccepter offerAccepter) {
    this.targetConfigName = targetConfigName;
    this.frameworkState = frameworkState;
    this.offerReqProvider = offerReqProvider;
    this.offerAccepter = offerAccepter;
  }

  public List<OfferID> resourceOffers(SchedulerDriver driver, List<Offer> offers, Block block)
      throws Exception {
    List<OfferID> acceptedOffers = new ArrayList<OfferID>();
    List<TaskInfo> terminatedTasks = getTerminatedTasks(block);

    OfferRequirement offerReq = null;

    if (terminatedTasks.size() > 0) {
      TaskInfo terminatedTask = terminatedTasks.get(new Random().nextInt(terminatedTasks.size()));
      offerReq = offerReqProvider.getReplacementOfferRequirement(terminatedTask);
    } else {
      List<Integer> missingBrokerIds = getMissingBrokerIds(block);
      log.info("Missing brokerIds: " + missingBrokerIds);
      if (missingBrokerIds.size() > 0) {
        Integer brokerId = missingBrokerIds.get(new Random().nextInt(missingBrokerIds.size()));
        offerReq = offerReqProvider.getNewOfferRequirement(targetConfigName, brokerId);
      }
    }

    if (offerReq != null) {
      OfferEvaluator offerEvaluator = new OfferEvaluator();
      List<OfferRecommendation> recommendations = offerEvaluator.evaluate(offerReq, offers);
      acceptedOffers = offerAccepter.accept(driver, recommendations);
    }

    return acceptedOffers;
  }

  private List<TaskInfo> getTerminatedTasks(Block block) {
    List<TaskInfo> filteredTerminatedTasks = new ArrayList<TaskInfo>();

    try {
      if (block == null) {
        return frameworkState.getTerminatedTaskInfos();
      }

      String brokerName = block.getName();
      for (TaskInfo taskInfo : frameworkState.getTerminatedTaskInfos()) {
        if (!taskInfo.getName().equals(brokerName)) {
          filteredTerminatedTasks.add(taskInfo);
        }
      }
    } catch (Exception ex) {
      log.error("Failed to fetch terminated tasks.");
    }

    return filteredTerminatedTasks;
  }

  private List<Integer> getMissingBrokerIds(Block block) {
    List<Integer> missingBrokerIds = new ArrayList<>();

    Integer lastExpectedBrokerId = getLastExpectedBrokerId(block);

    if (!(lastExpectedBrokerId >= 0)) {
      return missingBrokerIds;
    }

    List<TaskInfo> brokerTasks = null;
    try {
      brokerTasks = frameworkState.getTaskInfos();
    } catch (Exception ex) {
      log.error("Failed to fetch TaskInfos with exception: " + ex);
      return missingBrokerIds;
    }

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

  private boolean brokerExists(List<TaskInfo> brokerTasks, int brokerId) {
    String brokerName = OfferUtils.idToName(brokerId);

    for (TaskInfo brokerTask : brokerTasks) {
      if (brokerTask.getName().equals(brokerName)) {
        return true;
      }
    }

    return false;
  }

  private Integer getLastExpectedBrokerId(Block block) {
    if (block == null) {
      try {
        return frameworkState.getTaskInfos().size() - 1;
      } catch (Exception ex) {
        log.error("Failed to fetch TaskInfos with exception: " + ex);
        return -1;
      }
    } else if (block instanceof KafkaUpdateBlock) {
      int brokerId = ((KafkaUpdateBlock)block).getBrokerId();
      return brokerId - 1;
    } else {
      // Reconciliation block
      return -1;
    }
  }
}
