// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Quincy scheduling cost model, as described in the SOSP 2009 paper.

#include <string>

#include "scheduling/quincy_cost_model.h"

DEFINE_int64(flow_max_arc_cost, 1000, "The maximum cost of an arc");

namespace firmament {

QuincyCostModel::QuincyCostModel() { }

// The cost of leaving a task unscheduled should be higher than the cost of scheduling it.
Cost_t QuincyCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  int64_t half_max_arc_cost = FLAGS_flow_max_arc_cost / 2;
  return half_max_arc_cost + rand() % half_max_arc_cost + 1;
  //  return 5ULL;
}

// The costfrom the unscheduled to the sink is 0. Setting it to a value greater than
// zero affects all the unscheduled tasks. It is better to affect the cost of not running
// a task through the cost from the task to the unscheduled aggregator.
Cost_t QuincyCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0ULL;
}

// The cost from the task to the cluster aggregator models how expensive is a task to run
// on any node in the cluster. The cost of the topology's arcs are the same for all the tasks.
Cost_t QuincyCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

Cost_t QuincyCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 3) + 1;
}

Cost_t QuincyCostModel::ClusterAggToResourceNodeCost(ResourceID_t target) {
  return rand() % (FLAGS_flow_max_arc_cost / 4) + 1;
}

Cost_t QuincyCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  return rand() % (FLAGS_flow_max_arc_cost / 4) + 1;
}

// The cost from the resource leaf to the sink is 0.
Cost_t QuincyCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0ULL;
}

Cost_t QuincyCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t QuincyCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t QuincyCostModel::TaskToEquivClassAggregator(TaskID_t task_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

}  // namespace firmament
