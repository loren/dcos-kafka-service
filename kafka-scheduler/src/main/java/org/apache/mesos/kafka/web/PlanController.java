package org.apache.mesos.kafka.web;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.mesos.kafka.scheduler.KafkaScheduler;

import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.DefaultPlanManager;
import org.apache.mesos.scheduler.plan.Phase;

import org.json.JSONArray;
import org.json.JSONObject;

@Path("/v1/plan")
@Produces("application/json")
public class PlanController {
  private final Log log = LogFactory.getLog(PlanController.class);
  private DefaultPlanManager planManager = KafkaScheduler.getPlanManager();

  @GET
  public Response listPhases() {
    try {
      List<? extends Phase> phases = planManager.getPhases();
      JSONObject obj = new JSONObject();
      obj.put("phases", phasesToJsonArray(phases));
      return Response.ok(obj.toString(), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      log.error("Failed to fetch phases with exception: " + ex);
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/{phaseId}")
  public Response listBlocks(@PathParam("phaseId") String phaseId) {
    try {
      Phase phase = getPhase(Integer.parseInt(phaseId));
      JSONObject obj = new JSONObject();
      List<? extends Block> blocks = phase.getBlocks();
      if (blocks != null) {
        obj.put("blocks", blocksToJsonArray(blocks));
        return Response.ok(obj.toString(), MediaType.APPLICATION_JSON).build();
      } else {
        return Response.serverError().build();
      }
    } catch (Exception ex) {
      log.error("Failed to fetch blocks for phase: " + phaseId + " with exception: " + ex);
      return Response.serverError().build();
    }
  }

  @PUT
  public Response executeCmd(@QueryParam("cmd") String cmd) {
    try {
      JSONObject obj = new JSONObject();

      switch(cmd) {
        case "continue":
          planManager.proceed();
          obj.put("Result", "Received cmd: " + cmd);
          break;
        case "interrupt":
          planManager.interrupt();
          obj.put("Result", "Received cmd: " + cmd);
          break;
        default:
          log.error("Unrecognized cmd: " + cmd);
          return Response.serverError().build();
      }

      return Response.ok(obj.toString(), MediaType.APPLICATION_JSON).build();

    } catch (Exception ex) {
      log.error("Failed to execute cmd: " + cmd + "  with exception: " + ex);
      return Response.serverError().build();
    }
  }

  private JSONArray phasesToJsonArray(List<? extends Phase> phases) {
    List<JSONObject> phaseObjs = new ArrayList<JSONObject>();

    for (Phase phase : phases) {
      JSONObject obj = new JSONObject();
      obj.put(Integer.toString(phase.getId()), phase.getName());
      phaseObjs.add(obj);
    }

    return new JSONArray(phaseObjs);
  }

  private JSONArray blocksToJsonArray(List<? extends Block> blocks) {
    List<JSONObject> blockObjs = new ArrayList<JSONObject>();

    for (Block block : blocks) {
      JSONObject descObj = new JSONObject();
      descObj.put("name", block.getName());
      descObj.put("status", block.getStatus().name());

      JSONObject blockObj = new JSONObject();
      blockObj.put(Integer.toString(block.getId()), descObj);

      blockObjs.add(blockObj);
    }

    return new JSONArray(blockObjs);
  }

  private Phase getPhase(int id) {
    List<? extends Phase> phases = planManager.getPhases();

    for (Phase phase : phases) {
      if (phase.getId() == id) {
        return phase;
      }
    }

    return null;
  }
}