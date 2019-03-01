/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#include "scheduling/flow/cpu_cost_model.h"
#include "scheduling/flow/coco_cost_model.h"

#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/label_utils.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/cost_model_utils.h"
#include "scheduling/flow/flow_graph_manager.h"

DEFINE_uint64(max_multi_arcs_for_cpu, 50, "Maximum number of multi-arcs.");

DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

CpuCostModel::CpuCostModel(shared_ptr<ResourceMap_t> resource_map,
                           //shared_ptr<JobMap_t> job_map,
                           shared_ptr<TaskMap_t> task_map,
                           shared_ptr<KnowledgeBase> knowledge_base)
  : resource_map_(resource_map), /*job_map_(job_map),*/ task_map_(task_map),
    knowledge_base_(knowledge_base) {
}

ArcDescriptor CpuCostModel::TaskToUnscheduledAgg(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  return ArcDescriptor(2560000 + td.priority() * 3000, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::UnscheduledAggToSink(JobID_t job_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::TaskToResourceNode(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::ResourceNodeToResourceNode(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  return ArcDescriptor(0LL, CapacityFromResNodeToParent(destination), 0ULL);
}

ArcDescriptor CpuCostModel::LeafResourceNodeToSink(ResourceID_t resource_id) {
  return ArcDescriptor(0LL, FLAGS_max_tasks_per_pu, 0ULL);
}

ArcDescriptor CpuCostModel::TaskContinuation(TaskID_t task_id) {
  // TODO(ionel): Implement before running with preemption enabled.
  //return ArcDescriptor(0LL, 1ULL, 0ULL);
  const TaskDescriptor& td = GetTask(task_id);
  return ArcDescriptor((td.priority())*3000, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::TaskPreemption(TaskID_t task_id) {
  // TODO(ionel): Implement before running with preemption enabled.
  // Check for unscheduled high priority tasks,
  // Add this task in preemptible tasks set if this task is of low priority,
  // If there is already associated task set then update for other higher
  // priority task.
  // for high_priority_task in unscheduled_high_priority_tasks:
  //       if( hp_task_requirement_satisfied[high_priority_task] != true)
  //       {
  //           if( high_priority_task.resource_Req <= task.resource_request )
  //                hp_task_to_preemptible_task_set[high_priority_task].insert(task)
  //                hp_task_requirement_satisfied[high_priority_task] = true;
  //                task_needs_to_prempted = true;
  //           else
               // TODO: When HP task needs more resources than single task
  //       }
  //
  // Unscheduled task has unscheduled_time > scheduling_interval.
  /*
  vector<TaskID_t> task_to_priority;
  for(auto it = task_map_.begin(); it != task_map_.end(); it++) {
    if(it.second->state() == TaskDescriptor::RUNNABLE && it.second->total_unscheduled_time() > sched_interval) {
      if(it.second->priority < td->priority() )
      //unordered_set<TaskDescriptor> preemptible_tasks;
     // task_to_premptable_tasks.insert(pair<TaskID_t, unordered_set<TaskID_t>>(it.second->uid(), preemptible_tasks);
    }
  }
  bool preempt_task = false;
  if(preempt_task) {
    return ArcDescriptor(0LL, 1ULL, 0ULL);
  } else {
    return ArcDescriptor(2560000, 1ULL, 0ULL);
  }*/
  const TaskDescriptor& low_priority_running_task_td = GetTask(task_id);
  LOG(INFO) << "..........................................................................................................................";
  LOG(INFO) << "DEBUG: Task for which we are updating preemption cost is : " << low_priority_running_task_td.uid();
  LOG(INFO) << "DEBUG: Inside TaskPreemption: running_task_td.priority(): " << low_priority_running_task_td.priority();
  LOG(INFO) << "..........................................................................................................................";
  if (find(running_high_priority_tasks.begin(), running_high_priority_tasks.end(), task_id) != running_high_priority_tasks.end() ) {
    LOG(INFO) << "DEBUG: Current task which is running is itself high priority, so not preempting this";
    return ArcDescriptor(2560000, 1ULL, 0ULL);
  }
  LOG(INFO) << "DEBUG: No of high priority tasks which are pending due to resource exaustion: " << pending_high_priority_tasks.size();
  for (auto high_priority_task_id : pending_high_priority_tasks) {
    const TaskDescriptor& high_priority_task_td = GetTask(high_priority_task_id);
    LOG(INFO) << "------------------------------------------------------------------------------------------------------------------------";
    LOG(INFO) << "DEBUG: High priority pending tasks: high_priority_task_td.priority(): " << high_priority_task_td.priority() << "\n "
              << "high_priority_task_td.resourcer_request().cpu_cores(:)" << high_priority_task_td.resource_request().cpu_cores() << "\n"
              << "high_priority_task_td.uid() : " << high_priority_task_td.uid() << "\n"
              << "------------------------------------------------------------------------------------------------------------------------";
    /*TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
    JobID_t job_id = JobIDFromString(td_ptr->job_id());
    JobDescriptor* jd_ptr = FindOrNull(*job_map_, job_id);
    CHECK_NOTNULL(jd_ptr);*/
    //if ((td_ptr != jd_ptr->mutable_root_task()) && (low_priority_running_task_td.priority() >= 100) && (low_priority_running_task_td.state() == TaskDescriptor::RUNNING) {
    if (high_priority_task_td.resource_request().cpu_cores() <= low_priority_running_task_td.resource_request().cpu_cores()) {
      pending_high_priority_tasks.erase(high_priority_task_id);
      return ArcDescriptor(0LL, 1ULL, 0ULL);
    }
    else
      return ArcDescriptor(2560000, 1ULL, 0ULL);
    }
  //}
  return ArcDescriptor(2560000, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                       EquivClass_t ec) {
  //return ArcDescriptor(0LL, 1ULL, 0ULL);
  const TaskDescriptor& td = GetTask(task_id);
  return ArcDescriptor((td.priority()) * 3000, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  // The arcs between ECs an machine can only carry unit flow.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuCostModel::EquivClassToEquivClass(
    EquivClass_t ec1,
    EquivClass_t ec2) {
  /*uint64_t* required_net_rx_bw = FindOrNull(ec_rx_bw_requirement_, ec1);
  CHECK_NOTNULL(required_net_rx_bw);
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec2);
  CHECK_NOTNULL(machine_res_id);
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, *machine_res_id);
  CHECK_NOTNULL(rs);
  const ResourceDescriptor& rd = rs->topology_node().resource_desc();
  CHECK_EQ(rd.type(), ResourceDescriptor::RESOURCE_MACHINE);
  uint64_t available_net_rx_bw = rd.max_available_resources_below().net_rx_bw();
  uint64_t* index = FindOrNull(ec_to_index_, ec2);
  CHECK_NOTNULL(index);
  uint64_t ec_index = *index + 1;
  if (available_net_rx_bw < *required_net_rx_bw * ec_index) {
    return ArcDescriptor(0LL, 0ULL, 0ULL);
  }
  return ArcDescriptor(static_cast<int64_t>(ec_index) *
                       static_cast<int64_t>(*required_net_rx_bw) -
                       static_cast<int64_t>(available_net_rx_bw) + 1280000,
                       1ULL, 0ULL); */
  /* float* required_cpu_cores = FindOrNull(ec_cpu_cores_requirement_, ec1);
  LOG(INFO) << "Required cpu cores: " << *required_cpu_cores;
  CHECK_NOTNULL(required_cpu_cores);
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec2);
  CHECK_NOTNULL(machine_res_id);
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, *machine_res_id);
  CHECK_NOTNULL(rs);
  const ResourceDescriptor& rd = rs->topology_node().resource_desc();
  CHECK_EQ(rd.type(), ResourceDescriptor::RESOURCE_MACHINE);
  float available_cpu_cores = rd.available_resources().cpu_cores();
  //LOG(INFO) << "Available CPU cores: " << available_cpu_cores << " for machine " << static_cast<int64_t>(machine_res_id);
  uint64_t* index = FindOrNull(ec_to_index_, ec2);
  CHECK_NOTNULL(index);
  uint64_t ec_index = *index + 1;
  if (available_cpu_cores < *required_cpu_cores * ec_index) {
    return ArcDescriptor(0LL, 0ULL, 0ULL);
  }
  int64_t cost = static_cast<int64_t>(ec_index) * static_cast<int64_t>(*required_cpu_cores) - static_cast<int64_t>(available_cpu_cores) + 1280000;
  LOG(INFO) << "Cost of arc: " << cost;
  return ArcDescriptor(cost,
                       1ULL, 0ULL);
  */
  CostVector_t* resource_request = FindOrNull(ec_resource_requirement_, ec1);
  CHECK_NOTNULL(resource_request);
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec2);
  LOG(INFO) << "DEBUG: ec2: " << ec2;
  CHECK_NOTNULL(machine_res_id);
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, *machine_res_id);
  CHECK_NOTNULL(rs);
  const ResourceDescriptor& rd = rs->topology_node().resource_desc();
  CHECK_EQ(rd.type(), ResourceDescriptor::RESOURCE_MACHINE);
  CostVector_t available_resources;
  available_resources.cpu_cores_ = static_cast<uint32_t>(rd.available_resources().cpu_cores());
  available_resources.ram_cap_ = static_cast<uint32_t>(rd.available_resources().ram_cap());
  uint64_t* index = FindOrNull(ec_to_index_, ec2);
  CHECK_NOTNULL(index);
  uint64_t ec_index = *index;
  if ((available_resources.cpu_cores_ < resource_request->cpu_cores_ * ec_index) ||
      (available_resources.ram_cap_ < resource_request->ram_cap_ * ec_index)) {
    return ArcDescriptor(0LL, 0ULL, 0ULL);
  }
  //int64_t cpu_cost = static_cast<int64_t>(ec_index) * (resource_request->cpu_cores_) - (available_resources.cpu_cores_) + 1280000;
  //int64_t ram_cost = static_cast<int64_t>(ec_index) * (resource_request->ram_cap_) - (available_resources.ram_cap_) + 1280000;
  available_resources.cpu_cores_ = rd.available_resources().cpu_cores() - ec_index * resource_request->cpu_cores_;
  available_resources.ram_cap_ = rd.available_resources().ram_cap() - ec_index * resource_request->ram_cap_;
  int64_t cpu_cost = ((rd.resource_capacity().cpu_cores() - available_resources.cpu_cores_) / rd.resource_capacity().cpu_cores()) * 1000;
  int64_t ram_cost = ((rd.resource_capacity().ram_cap() - available_resources.ram_cap_) / rd.resource_capacity().ram_cap()) * 1000;
  int64_t cost = cpu_cost + ram_cost;
  LOG(INFO) << "DEBUG: rd.resource_capacity().cpu_cores()  : " << rd.resource_capacity().cpu_cores() << "\n"
            << "       rd.available_resources().cpu_cores(): " << rd.available_resources().cpu_cores() << "\n"
            << "       rd.resource_capacity().ram_cap()    : " << rd.resource_capacity().ram_cap() << "\n"
            << "       rd.available_resources().ram_cap()  : " << rd.available_resources().ram_cap() << " ** (2-1)/2*1000 **";
  LOG(INFO) << "DEBUG: Cost between eq class and machine is " << cost;
  return ArcDescriptor(cost,
                       1ULL, 0ULL);
}

vector<EquivClass_t>* CpuCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  /*
  // Get the equivalence class for the task's required cpu and label selectors.
  vector<EquivClass_t>* ecs = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  float* task_required_cpu_cores = FindOrNull(task_cpu_cores_requirement_, task_id);
  LOG(INFO) << "Task's required cpu cores: " << *task_required_cpu_cores;
  CHECK_NOTNULL(task_required_cpu_cores);
  size_t cpu_cores_selectors_hash = scheduler::HashSelectors(td_ptr->label_selectors());
  boost::hash_combine(cpu_cores_selectors_hash, *task_required_cpu_cores);
  EquivClass_t cpu_cores_selectors_ec = static_cast<EquivClass_t>(cpu_cores_selectors_hash);
  LOG(INFO) << "CPU core selectors EC: " << static_cast<int64_t>(cpu_cores_selectors_ec);
  ecs->push_back(cpu_cores_selectors_ec);
  InsertIfNotPresent(&ec_cpu_cores_requirement_, cpu_cores_selectors_ec, *task_required_cpu_cores);
  InsertIfNotPresent(&ec_to_label_selectors, cpu_cores_selectors_ec,
                     td_ptr->label_selectors()); */
  /*
  // Get the equivalence class for the task's required rx bw and label selectors.
  uint64_t* task_required_rx_bw = FindOrNull(task_rx_bw_requirement_, task_id);
  CHECK_NOTNULL(task_required_rx_bw);
  size_t rx_bw_selectors_hash = scheduler::HashSelectors(td_ptr->label_selectors());
  boost::hash_combine(rx_bw_selectors_hash, *task_required_rx_bw);
  EquivClass_t rx_bw_selectors_ec = static_cast<EquivClass_t>(rx_bw_selectors_hash);
  InsertIfNotPresent(&ec_rx_bw_requirement_, rx_bw_selectors_ec, *task_required_rx_bw);
  InsertIfNotPresent(&ec_to_label_selectors, rx_bw_selectors_ec,
                     td_ptr->label_selectors());*/
  // Get the equivalence class for the resource request: cpu and memory
  vector<EquivClass_t>* ecs = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  CostVector_t* task_resource_request = FindOrNull(task_resource_requirement_, task_id);
  CHECK_NOTNULL(task_resource_request);
  size_t resource_req_selectors_hash = scheduler::HashSelectors(td_ptr->label_selectors());
  boost::hash_combine(resource_req_selectors_hash, task_resource_request->cpu_cores_);
  EquivClass_t resource_request_ec = static_cast<EquivClass_t>(resource_req_selectors_hash);
  ecs->push_back(resource_request_ec);
  InsertIfNotPresent(&ec_resource_requirement_, resource_request_ec, *task_resource_request);
  InsertIfNotPresent(&ec_to_label_selectors, resource_request_ec,
                     td_ptr->label_selectors());
  return ecs;
}

vector<ResourceID_t>* CpuCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<ResourceID_t>* machine_res = new vector<ResourceID_t>();
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec);
  if (machine_res_id) {
    machine_res->push_back(*machine_res_id);
  }
  return machine_res;
}

vector<ResourceID_t>* CpuCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* pref_res = new vector<ResourceID_t>();
  return pref_res;
}

vector<EquivClass_t>* CpuCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t ec) {
  vector<EquivClass_t>* pref_ecs = new vector<EquivClass_t>();
  /* uint64_t* required_net_rx_bw = FindOrNull(ec_rx_bw_requirement_, ec);
  if (required_net_rx_bw) {
    const RepeatedPtrField<LabelSelector>* label_selectors =
      FindOrNull(ec_to_label_selectors, ec);
    CHECK_NOTNULL(label_selectors);
    // if EC is a rx bw EC then connect it to machine ECs.
    for (auto& ec_machines : ecs_for_machines_) {
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, ec_machines.first);
      CHECK_NOTNULL(rs);
      const ResourceDescriptor& rd = rs->topology_node().resource_desc();
      if (!scheduler::SatisfiesLabelSelectors(rd, *label_selectors))
        continue;
      uint64_t available_net_rx_bw =
        rd.max_available_resources_below().net_rx_bw();
      ResourceID_t res_id = ResourceIDFromString(rd.uuid());
      vector<EquivClass_t>* ecs_for_machine =
        FindOrNull(ecs_for_machines_, res_id);
      CHECK_NOTNULL(ecs_for_machine);
      uint64_t index = 0;
      for (uint64_t cur_rx_bw = *required_net_rx_bw;
           cur_rx_bw <= available_net_rx_bw && index < ecs_for_machine->size();
           cur_rx_bw += *required_net_rx_bw) {
        pref_ecs->push_back(ec_machines.second[index]);
        index++;
      }
    }
  } */
  /*
  float* required_cpu_cores = FindOrNull(ec_cpu_cores_requirement_, ec);
  if (required_cpu_cores) {
    const RepeatedPtrField<LabelSelector>* label_selectors =
      FindOrNull(ec_to_label_selectors, ec);
    CHECK_NOTNULL(label_selectors);
    // if EC is a rx bw EC then connect it to machine ECs.
    for (auto& ec_machines : ecs_for_machines_) {
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, ec_machines.first);
      CHECK_NOTNULL(rs);
      const ResourceDescriptor& rd = rs->topology_node().resource_desc();
      if (!scheduler::SatisfiesLabelSelectors(rd, *label_selectors))
        continue;
      float available_cpu_cores=
        rd.available_resources().cpu_cores();
      LOG(INFO) << "Available cores before selecting machine ECs: " << available_cpu_cores;
      ResourceID_t res_id = ResourceIDFromString(rd.uuid());
      vector<EquivClass_t>* ecs_for_machine =
        FindOrNull(ecs_for_machines_, res_id);
      CHECK_NOTNULL(ecs_for_machine);
      uint64_t index = 0;
      float cur_cpu_cores;
      for (cur_cpu_cores = *required_cpu_cores;
           cur_cpu_cores <= available_cpu_cores && index < ecs_for_machine->size();
           cur_cpu_cores += *required_cpu_cores) {
        pref_ecs->push_back(ec_machines.second[index]);
        index++;
      }
      LOG(INFO) << "Current cpu cores at the end of for loop: " << cur_cpu_cores;
      LOG(INFO) << "Index value at the end of for loop: " << index;
    }
  }
  */
  CostVector_t* task_resource_request = FindOrNull(ec_resource_requirement_, ec);
  if (task_resource_request) {
    const RepeatedPtrField<LabelSelector>* label_selectors =
      FindOrNull(ec_to_label_selectors, ec);
    CHECK_NOTNULL(label_selectors);
    for (auto& ec_machines : ecs_for_machines_) {
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, ec_machines.first);
      CHECK_NOTNULL(rs);
      const ResourceDescriptor& rd = rs->topology_node().resource_desc();
      if (!scheduler::SatisfiesLabelSelectors(rd, *label_selectors))
        continue;
      CostVector_t available_resources;
      available_resources.cpu_cores_ = static_cast<uint32_t>(rd.available_resources().cpu_cores());
      available_resources.ram_cap_ = static_cast<uint32_t>(rd.available_resources().ram_cap());
      LOG(INFO) << "Available resources before selecting machine ECs: "
                << "Cores: " << available_resources.cpu_cores_ << ", "
                << "Memory: " << available_resources.ram_cap_;
      ResourceID_t res_id = ResourceIDFromString(rd.uuid());
      vector<EquivClass_t>* ecs_for_machine =
        FindOrNull(ecs_for_machines_, res_id);
      CHECK_NOTNULL(ecs_for_machine);
      uint64_t index = 0;
      CostVector_t cur_resource;
      for (cur_resource = *task_resource_request;
           cur_resource.cpu_cores_ <= available_resources.cpu_cores_ && cur_resource.ram_cap_ <= available_resources.ram_cap_ && index < ecs_for_machine->size();
           cur_resource.cpu_cores_ += task_resource_request->cpu_cores_ , cur_resource.ram_cap_ += task_resource_request->ram_cap_ ) {
        pref_ecs->push_back(ec_machines.second[index]);
        LOG(INFO) << "DEBUG: pref_ecs: ec_machines.second[index]: " << ec_machines.second[index];
        index++;
      }
    }
  }
  LOG(INFO) << "No. of preferred ecs: " << pref_ecs->size();
  return pref_ecs;
}

void CpuCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  const ResourceDescriptor& rd = rtnd_ptr->resource_desc();
  // Keep track of the new machine
  CHECK(rd.type() == ResourceDescriptor::RESOURCE_MACHINE);
  ResourceID_t res_id = ResourceIDFromString(rd.uuid());
  vector<EquivClass_t> machine_ecs;
  for (uint64_t index = 0; index < FLAGS_max_multi_arcs_for_cpu; ++index) {
    EquivClass_t multi_machine_ec = GetMachineEC(rd.friendly_name(), index);
    machine_ecs.push_back(multi_machine_ec);
    CHECK(InsertIfNotPresent(&ec_to_index_, multi_machine_ec, index));
    CHECK(InsertIfNotPresent(&ec_to_machine_, multi_machine_ec, res_id));
    LOG(INFO) << "DEBUG: MApping" << multi_machine_ec << ", " << res_id;
  }
  LOG(INFO) << "DEBUG: Size of machine ECs for res: " << machine_ecs.size() << " for " << rd.friendly_name();
  CHECK(InsertIfNotPresent(&ecs_for_machines_, res_id, machine_ecs));
}

void CpuCostModel::AddTask(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  /*uint64_t required_net_rx_bw = td.resource_request().net_rx_bw();*/
  /*float required_cpu_cores = td.resource_request().cpu_cores();*/
  CostVector_t resource_request;
  resource_request.cpu_cores_ = static_cast<uint32_t>(td.resource_request().cpu_cores());
  resource_request.ram_cap_ = static_cast<uint32_t>(td.resource_request().ram_cap());
  LOG(INFO) << "Requested cpu cores at AddTask: " << resource_request.cpu_cores_;
  LOG(INFO) << "Requested mem at AddTask: " << resource_request.ram_cap_;
  /*CHECK(InsertIfNotPresent(&task_rx_bw_requirement_, task_id,
                           required_net_rx_bw));*/
  /*CHECK(InsertIfNotPresent(&task_cpu_cores_requirement_, task_id,
                           required_cpu_cores));*/
  CHECK(InsertIfNotPresent(&task_resource_requirement_, task_id,
                           resource_request));
  /* Remove this if same works inside ScheduleJobs
  if(td.priority() > 100) {
    pending_low_priority_tasks.insert(task_id);
  }
  else
    pending_high_priority_tasks.insert(task_id);
  */
}

void CpuCostModel::RemoveMachine(ResourceID_t res_id) {
  // vector<EquivClass_t>* ecs = FindOrNull(ecs_for_machines_, res_id);
  // CHECK_NOTNULL(ecs);
  // for (EquivClass_t& ec : *ecs) {
  //   CHECK_EQ(ec_to_machine_.erase(ec), 1);
  //   CHECK_EQ(ec_to_index_.erase(ec), 1);
  // }
  // CHECK_EQ(ecs_for_machines_.erase(res_id), 1);
}

void CpuCostModel::RemoveTask(TaskID_t task_id) {
  // CHECK_EQ(task_rx_bw_requirement_.erase(task_id), 1);
  CHECK_EQ(task_resource_requirement_.erase(task_id), 1);
}

EquivClass_t CpuCostModel::GetMachineEC(const string& machine_name,
                                        uint64_t ec_index) {
  uint64_t hash = HashString(machine_name);
  boost::hash_combine(hash, ec_index);
  return static_cast<EquivClass_t>(hash);
}

FlowGraphNode* CpuCostModel::GatherStats(FlowGraphNode* accumulator,
                                         FlowGraphNode* other) {
  if (!accumulator->IsResourceNode()) {
    return accumulator;
  }

  if (other->resource_id_.is_nil()) {
    // The other node is not a resource node.
    if (other->type_ == FlowNodeType::SINK) {
      accumulator->rd_ptr_->set_num_running_tasks_below(
          static_cast<uint64_t>(
              accumulator->rd_ptr_->current_running_tasks_size()));
      accumulator->rd_ptr_->set_num_slots_below(FLAGS_max_tasks_per_pu);
    }
    return accumulator;
  }

  CHECK_NOTNULL(other->rd_ptr_);
  ResourceDescriptor* rd_ptr = accumulator->rd_ptr_;
  CHECK_NOTNULL(rd_ptr);
  /*
  // Use the KB to find load information and compute available resources
  ResourceID_t machine_res_id =
    MachineResIDForResource(accumulator->resource_id_);

  accumulator->rd_ptr_->set_num_running_tasks_below(
      accumulator->rd_ptr_->num_running_tasks_below() +
      other->rd_ptr_->num_running_tasks_below());
  accumulator->rd_ptr_->set_num_slots_below(
      accumulator->rd_ptr_->num_slots_below() +
      other->rd_ptr_->num_slots_below()); */
 /*
  if (accumulator->type_ == FlowNodeType::PU) {
    // Base case: (PU -> SINK). We are at a PU and we gather the statistics.
    CHECK(other->resource_id_.is_nil());
    // Get the RD for the machine
    ResourceStatus* machine_rs_ptr =
      FindPtrOrNull(*resource_map_, machine_res_id);
    CHECK_NOTNULL(machine_rs_ptr);
    ResourceDescriptor* machine_rd_ptr = machine_rs_ptr->mutable_descriptor();
    // Grab the latest available resource sample from the machine
    ResourceStats latest_stats;
    // Take the most recent sample for now
    bool have_sample =
      knowledge_base_->GetLatestStatsForMachine(machine_res_id, &latest_stats);
    if (have_sample) {
      VLOG(2) << "Updating PU " << accumulator->resource_id_ << "'s "
              << "resource stats!";
      // Get the CPU stats for this PU
      string label = rd_ptr->friendly_name();
      uint64_t idx = label.find("PU #");
      if (idx != string::npos) {
        string core_id_substr = label.substr(idx + 4, label.size() - idx - 4);
        uint32_t core_id = strtoul(core_id_substr.c_str(), 0, 10);
        float available_cpu_cores =
          latest_stats.cpus_stats(core_id).cpu_capacity() *
          (1.0 - latest_stats.cpus_stats(core_id).cpu_utilization());
        rd_ptr->mutable_available_resources()->set_cpu_cores(
            available_cpu_cores);
        rd_ptr->mutable_max_available_resources_below()->set_cpu_cores(
            available_cpu_cores);
        rd_ptr->mutable_min_available_resources_below()->set_cpu_cores(
            available_cpu_cores);
        VLOG(1) << "Updated the available CPU cores: " << available_cpu_cores << " for core ID: "
                << core_id;
      }
   }

  } */
  if (accumulator->type_ == FlowNodeType::MACHINE) {
    LOG(INFO) << "DEBUG: We are at Macine, now we will print runnable tasks: " << other->rd_ptr_->current_running_tasks_size();
    auto running_tasks = other->rd_ptr_->current_running_tasks();
    for (auto& task_id : running_tasks) {
      //const TaskDescriptor& td = GetTask(task_id);
      TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
      if(td) {
        if (td->priority() >= 100) {
          running_low_priority_tasks.insert(task_id);
        } else
          running_high_priority_tasks.insert(task_id);
      }
    }
    // Grab the latest available resource sample from the machine
    ResourceStats latest_stats;
    // Take the most recent sample for now
    bool have_sample =
      knowledge_base_->GetLatestStatsForMachine(accumulator->resource_id_,
                                                &latest_stats);
    if (have_sample) {
      LOG(INFO) << "DEBUG: Size of cpu stats: " << latest_stats.cpus_stats_size();
      uint32_t core_id = 0;
      float available_cpu_cores = latest_stats.cpus_stats(0).cpu_allocatable();
      cumulative_available_cpu_cores[accumulator->resource_id_] = available_cpu_cores;
       // latest_stats.cpus_stats(core_id).cpu_capacity() *
       // (1.0 - latest_stats.cpus_stats(core_id).cpu_utilization());
      //auto available_ram_cap = latest_stats.mem_capacity() *
      auto available_ram_cap = latest_stats.mem_allocatable();
       // (1.0 - latest_stats.mem_utilization());
      LOG(INFO) << "DEBUG: Stats from latest machine sample: "
                << "Available cpu: " << available_cpu_cores << "\n"
                << "Available mem: " << available_ram_cap;
      rd_ptr->mutable_available_resources()->set_cpu_cores(
        available_cpu_cores);
      rd_ptr->mutable_available_resources()->set_ram_cap(
        available_ram_cap);
/*      rd_ptr->mutable_available_resources()->set_net_tx_bw(
          rd_ptr->resource_capacity().net_tx_bw() -
          latest_stats.net_tx_bw());
      rd_ptr->mutable_max_available_resources_below()->set_net_tx_bw(
          rd_ptr->resource_capacity().net_tx_bw() -
          latest_stats.net_tx_bw());
      rd_ptr->mutable_min_available_resources_below()->set_net_tx_bw(
          rd_ptr->resource_capacity().net_tx_bw() -
          latest_stats.net_tx_bw());
      rd_ptr->mutable_available_resources()->set_net_rx_bw(
          rd_ptr->resource_capacity().net_rx_bw() -
          latest_stats.net_rx_bw());
      rd_ptr->mutable_max_available_resources_below()->set_net_rx_bw(
          rd_ptr->resource_capacity().net_rx_bw() -
          latest_stats.net_rx_bw());
      rd_ptr->mutable_min_available_resources_below()->set_net_rx_bw(
          rd_ptr->resource_capacity().net_rx_bw() -
          latest_stats.net_rx_bw());*/
     for (auto task_id : pending_high_priority_tasks) {
       const TaskDescriptor& td = GetTask(task_id);
       if (TaskFitsInResource(task_id, accumulator->resource_id_)) {
        LOG(INFO) << "DEBUG: cumulative_available_cpu_cores[accumulator->resource_id_]" << cumulative_available_cpu_cores[accumulator->resource_id_];
        cumulative_available_cpu_cores[accumulator->resource_id_] -= td.resource_request().cpu_cores();
        LOG(INFO) << "DEBUG: After deducting resource cumulative_available_cpu_cores[accumulator->resource_id_]" << cumulative_available_cpu_cores[accumulator->resource_id_];
        pending_high_priority_tasks.erase(task_id);
       }
     }
    }
  }
  return accumulator;
}

bool CpuCostModel::TaskFitsInResource(TaskID_t task_id, ResourceID_t res_id) {
  const TaskDescriptor& td = GetTask(task_id);
  auto cores = td.resource_request().cpu_cores();
  return (cores <= cumulative_available_cpu_cores[res_id]);
}

void CpuCostModel::PrepareStats(FlowGraphNode* accumulator) {
  if (!accumulator->IsResourceNode()) {
    return;
  }
  CHECK_NOTNULL(accumulator->rd_ptr_);
  accumulator->rd_ptr_->clear_num_running_tasks_below();
  accumulator->rd_ptr_->clear_num_slots_below();
  running_low_priority_tasks.clear();
  running_high_priority_tasks.clear();
  //pending_low_priority_tasks.clear();
  //pending_high_priority_tasks.clear();
}

FlowGraphNode* CpuCostModel::UpdateStats(FlowGraphNode* accumulator,
                                         FlowGraphNode* other) {
  return accumulator;
}

ResourceID_t CpuCostModel::MachineResIDForResource(ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  ResourceTopologyNodeDescriptor* rtnd = rs->mutable_topology_node();
  while (rtnd->resource_desc().type() != ResourceDescriptor::RESOURCE_MACHINE) {
    if (rtnd->parent_id().empty()) {
      LOG(FATAL) << "Non-machine resource " << rtnd->resource_desc().uuid()
                 << " has no parent!";
    }
    rs = FindPtrOrNull(*resource_map_, ResourceIDFromString(rtnd->parent_id()));
    rtnd = rs->mutable_topology_node();
  }
  return ResourceIDFromString(rtnd->resource_desc().uuid());
}

}  // namespace firmament
