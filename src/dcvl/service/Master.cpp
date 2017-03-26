#include "dcvl/service/Master.h"
#include "dcvl/util/Net.h"
#include "dcvl/message/Message.h"
#include "dcvl/util/Configuration.h"
#include "dcvl/topology/Topology.h"
#include "dcvl/topology/TopologyLoader.h"
#include "dcvl/base/Constants.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <map>
#include <list>
#include <cassert>

namespace dcvl {
    namespace service {
        Master::Master(const dcvl::base::NetAddress& host) : 
                _MasterHost(host),
                _WorkerCount(0) {
            OnConnection(std::bind(&Master::OnConnect, this, std::placeholders::_1));
            OnCommand(dcvl::message::Command::Type::Join, this, &Master::OnJoin);
            OnCommand(dcvl::message::Command::Type::AskField, this, &Master::OnAskField);
        }

        Master::Master(const dcvl::util::Configuration& configuration) : 
                Master(dcvl::base::NetAddress(
                    configuration.GetProperty(ConfigurationKey::MasterHost),
                    configuration.GetIntegerProperty(ConfigurationKey::MasterPort))) {
            _WorkerCount = configuration.GetIntegerProperty(ConfigurationKey::WorkerCount);
            _configuration.reset(new dcvl::util::Configuration(configuration));

            LOG(LOG_DEBUG) << "Need workers: " << _WorkerCount;
        }

        void Master::OnConnect(WorkerContext* context) {
        }

        void Master::OnJoin(WorkerContext* context, const dcvl::message::Command& command,
                dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor) {
            std::string joinerType = command.GetArgument(0).GetStringValue();
            std::string WorkerHost = command.GetArgument(1).GetStringValue();
            int32_t WorkerPort = command.GetArgument(2).GetInt32Value();

            LOG(LOG_DEBUG) << "Join node: " << joinerType;
            
            WorkerContext WorkerContext;
            base::Variants::const_iterator currentIterator = command.GetArguments().cbegin() + 3;
            WorkerContext.Deserialize(currentIterator);

            LOG(LOG_DEBUG) << "Worker name: " << WorkerContext.GetId();
            LOG(LOG_DEBUG) << "Host: " << WorkerHost;
            LOG(LOG_DEBUG) << "Port: " << WorkerPort;
            LOG(LOG_DEBUG) << "Spout count: " << WorkerContext.GetSpoutCount();
            LOG(LOG_DEBUG) << "Bolt count: " << WorkerContext.GetBoltCount();
            LOG(LOG_DEBUG) << "Task info count: " << WorkerContext.GetTaskInfos().size();
            LOG(LOG_DEBUG) << "Free spout count: " << WorkerContext.GetFreeSpouts().size();
            LOG(LOG_DEBUG) << "Free bolt count: " << WorkerContext.GetFreeBolts().size();
            LOG(LOG_DEBUG) << "Busy spout count: " << WorkerContext.GetBusySpouts().size();
            LOG(LOG_DEBUG) << "Busy bolt count: " << WorkerContext.GetBusyBolts().size();

            WorkerContext.SetNetAddress(dcvl::base::NetAddress(
                    WorkerHost, WorkerPort));
            WorkerContext.PrepareTaskInfos();
            _Workers.push_back(WorkerContext);

            // Response
            dcvl::message::Response response(dcvl::message::Response::Status::Successful);
            response.AddArgument({ NodeType::Master });

            Responsor(response);

            // Initialize command clients
            dcvl::base::NetAddress WorkerAddress(WorkerHost,
                WorkerPort);
            dcvl::util::NetConnector* WorkerConnector =
                    new dcvl::util::NetConnector(WorkerAddress);
            dcvl::message::CommandClient* WorkerCommandClient =
                    new dcvl::message::CommandClient(WorkerConnector);

            _WorkerClients.insert({WorkerContext.GetId(),
                    std::shared_ptr<dcvl::message::CommandClient>(WorkerCommandClient)});

            SendHeartbeat(WorkerContext.GetId(), 0);

            // Initialize topology
            if ( _Workers.size() == _WorkerCount ) {
                std::string topologyName = _configuration->GetProperty(ConfigurationKey::TopologyName);
                dcvl::topology::Topology* topology =
                        dcvl::topology::TopologyLoader::GetInstance().GetTopology(topologyName).get();
                SubmitTopology(topology);
            }
        }

        void Master::OnAskField(WorkerContext* context, const dcvl::message::Command& command,
                                dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor)
        {
            std::string sourceTaskName = command.GetArgument(0).GetStringValue();
            std::string destTaskName = command.GetArgument(1).GetStringValue();
            TaskPathName taskPathName = { sourceTaskName, destTaskName };
            std::string fieldValue = command.GetArgument(2).GetStringValue();

            auto taskPairIter = _fieldsDestinations.find(taskPathName);
            if ( taskPairIter == _fieldsDestinations.end() ) {
                _fieldsDestinations.insert({ taskPathName, std::map<std::string, task::ExecutorPosition>() });
                taskPairIter = _fieldsDestinations.find(taskPathName);
            }

            std::map<std::string, task::ExecutorPosition>& destinations = taskPairIter->second;
            auto destinationPairIter = destinations.find(fieldValue);
            if ( destinationPairIter == destinations.end() ) {
                std::vector<task::ExecutorPosition>& candidates = _fieldsCandidates[taskPathName];
                int32_t positionIndex = rand() % candidates.size();

                destinations.insert({fieldValue, candidates[positionIndex]});
                destinationPairIter = destinations.find(fieldValue);
            }

            task::ExecutorPosition destination = destinationPairIter->second;
            base::Variants destinationVariants;
            destination.Serialize(destinationVariants);

            dcvl::message::Response response(dcvl::message::Response::Status::Successful);
            response.AddArguments(destinationVariants);

            Responsor(response);
        }

        void Master::OnOrderId(WorkerContext* context, const dcvl::message::Command& command,
                               dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor)
        {
            std::string topologyName = command.GetArgument(0).GetStringValue();

            int64_t orderId = _orderIds[topologyName];
            _orderIds[topologyName] = orderId + 1;

            dcvl::message::Response response(dcvl::message::Response::Status::Successful);
            response.AddArgument(orderId);

            Responsor(response);
        }

        std::list<task::TaskInfo> Master::GetAllSpoutTasks(
                const std::map<std::string, dcvl::spout::SpoutDeclarer>& spoutDeclarers,
                dcvl::topology::Topology* topology)
        {
            std::list<dcvl::task::TaskInfo> originSpoutTasks;
            for ( const auto& spoutPair : spoutDeclarers ) {
                dcvl::spout::SpoutDeclarer spoutDeclarer = spoutPair.second;
                LOG(LOG_DEBUG) << "Spout " << spoutDeclarer.GetTaskName();
                LOG(LOG_DEBUG) << "ParallismHint: " << spoutDeclarer.GetParallismHint();

                int32_t parallismHint = spoutDeclarer.GetParallismHint();
                for ( int32_t taskIndex = 0; taskIndex != parallismHint; ++ taskIndex ) {
                    dcvl::task::TaskInfo taskInfo;
                    taskInfo.SetTopologyName(topology->GetName());
                    taskInfo.SetTaskName(spoutDeclarer.GetTaskName());

                    originSpoutTasks.push_back(taskInfo);
                }
            }

            return originSpoutTasks;
        }

        std::map<std::string, std::vector<dcvl::task::TaskInfo*>>
                Master::AllocateSpoutTasks(std::list<dcvl::task::TaskInfo>& originSpoutTasks)
        {
            std::map<std::string, std::vector<dcvl::task::TaskInfo*>> nameToSpoutTasks;
            // Allocate task for every worker
            for ( WorkerContext& WorkerContext : _Workers ) {
                if ( !originSpoutTasks.size() ) {
                    break;
                }

                while ( true ) {
                    if ( !originSpoutTasks.size() ) {
                        break;
                    }

                    // If useNextSpout return -1, the spout slots is used up
                    int32_t spoutIndex = WorkerContext.useNextSpout();
                    if ( spoutIndex == -1 ) {
                        break;
                    }

                    // Put the spout task into spout slot
                    dcvl::task::TaskInfo taskInfo = originSpoutTasks.front();
                    taskInfo.SetWorkerContext(&WorkerContext);
                    taskInfo.SetExecutorIndex(WorkerContext.GetExecutorIndex(
                            WorkerContext::ExecutorType::Spout, spoutIndex));
                    originSpoutTasks.pop_front();
                    WorkerContext.SetSpoutTaskInfo(spoutIndex, taskInfo);

                    // Insert the spout task pointer into mapper
                    std::string taskName = taskInfo.GetTaskName();
                    auto spoutTasksPair = nameToSpoutTasks.find(taskName);
                    if ( spoutTasksPair == nameToSpoutTasks.end() ) {
                        nameToSpoutTasks.insert({taskName, std::vector<dcvl::task::TaskInfo*>()});
                        spoutTasksPair = nameToSpoutTasks.find(taskName);
                    }

                    spoutTasksPair->second.push_back(&(WorkerContext.GetSpoutTaskInfo(spoutIndex)));
                }
            }

            return nameToSpoutTasks;
        }

        std::list<task::TaskInfo> Master::GetAllBoltTasks(dcvl::topology::Topology* topology,
                const std::map<std::string, dcvl::bolt::BoltDeclarer>& boltDeclarers)
        {
            std::list<dcvl::task::TaskInfo> originBoltTasks;
            for ( const auto& boltPair : boltDeclarers ) {
                dcvl::bolt::BoltDeclarer boltDeclarer = boltPair.second;
                LOG(LOG_DEBUG) << "Bolt " << boltDeclarer.GetTaskName();
                LOG(LOG_DEBUG) << "Source: " << boltDeclarer.GetSourceTaskName();
                LOG(LOG_DEBUG) << "ParallismHint: " << boltDeclarer.GetParallismHint();

                int32_t parallismHint = boltDeclarer.GetParallismHint();
                for ( int32_t taskIndex = 0; taskIndex != parallismHint; ++ taskIndex ) {
                    dcvl::task::TaskInfo taskInfo;
                    taskInfo.SetTopologyName(topology->GetName());
                    taskInfo.SetTaskName(boltDeclarer.GetTaskName());

                    originBoltTasks.push_back(taskInfo);
                }
            }

            return originBoltTasks;
        }

        std::map<std::string, std::vector<dcvl::task::TaskInfo*>>
            Master::AllocateBoltTasks(std::list<dcvl::task::TaskInfo>& originBoltTasks)
        {
            std::map<std::string, std::vector<dcvl::task::TaskInfo*>> nameToBoltTasks;
            // Allocate bolt tasks
            for ( WorkerContext& WorkerContext : _Workers ) {
                if ( !originBoltTasks.size() ) {
                    break;
                }

                while ( true ) {
                    if ( !originBoltTasks.size() ) {
                        break;
                    }

                    // If useNextBolt return -1, the bolt slots is used up
                    int32_t boltIndex = WorkerContext.useNextBolt();
                    if ( boltIndex == -1 ) {
                        break;
                    }

                    // Put the bolt task into bolt slot
                    dcvl::task::TaskInfo taskInfo = originBoltTasks.front();
                    taskInfo.SetWorkerContext(&WorkerContext);
                    taskInfo.SetExecutorIndex(WorkerContext.GetExecutorIndex(
                            WorkerContext::ExecutorType::Bolt, boltIndex));
                    originBoltTasks.pop_front();
                    WorkerContext.SetBoltTaskInfo(boltIndex, taskInfo);

                    // Insert the bolt task pointer into mapper
                    std::string taskName = taskInfo.GetTaskName();
                    auto boltTasksPair = nameToBoltTasks.find(taskName);
                    if ( boltTasksPair == nameToBoltTasks.end() ) {
                        nameToBoltTasks.insert({taskName, std::vector<dcvl::task::TaskInfo*>()});
                        boltTasksPair = nameToBoltTasks.find(taskName);
                    }

                    boltTasksPair->second.push_back(&(WorkerContext.GetBoltTaskInfo(boltIndex)));
                }
            }

            return nameToBoltTasks;
        }

        std::vector<task::TaskInfo*> Master::FindTask(
                const std::map<std::string, std::vector<task::TaskInfo*>>& nameToBoltTasks,
                const std::map<std::string, std::vector<task::TaskInfo*>>& nameToSpoutTasks,
                const std::string& sourceTaskName)
        {
            auto spoutTaskPair = nameToSpoutTasks.find(sourceTaskName);
            if ( spoutTaskPair != nameToSpoutTasks.end() ) {
                return spoutTaskPair->second;
            }

            auto boltTaskPair = nameToBoltTasks.find(sourceTaskName);
            if ( boltTaskPair != nameToBoltTasks.end() ) {
                return boltTaskPair->second;
            }

            return std::vector<task::TaskInfo*>();
        }

        std::vector<task::TaskInfo*> Master::FindTask(
                const std::map<std::string, std::vector<task::TaskInfo*>>& nameToBoltTasks,
                const std::string& sourceTaskName)
        {
            auto boltTaskPair = nameToBoltTasks.find(sourceTaskName);
            if ( boltTaskPair != nameToBoltTasks.end() ) {
                return boltTaskPair->second;
            }

            return std::vector<task::TaskInfo*>();
        }

        void Master::ShowTaskInfos(const std::vector<dcvl::task::TaskInfo>& taskInfos)
        {
            for ( const dcvl::task::TaskInfo& taskInfo : taskInfos ) {
                if ( !taskInfo.GetWorkerContext() ) {
                    continue;
                }

                LOG(LOG_DEBUG) << "    Worker: " << taskInfo.GetWorkerContext()->GetId();
                LOG(LOG_DEBUG) << "    Exectuor index: " << taskInfo.GetExecutorIndex();
                LOG(LOG_DEBUG) << "    Paths: ";
                const std::list<dcvl::task::PathInfo>& paths = taskInfo.GetPaths();

                for ( const dcvl::task::PathInfo& path : paths ) {
                    LOG(LOG_DEBUG) << "      Path: ";
                    int32_t groupMethod = path.GetGroupMethod();
                    LOG(LOG_DEBUG) << "        Group method: " << groupMethod;
                    if ( path.GetGroupMethod() == dcvl::task::PathInfo::GroupMethod::Global) {
                        LOG(LOG_DEBUG) << "        Destination host: " <<
                                     path.GetDestinationExecutors()[0].GetWorker().GetHost();
                        LOG(LOG_DEBUG) << "        Destination port: " <<
                                     path.GetDestinationExecutors()[0].GetWorker().GetPort();
                        LOG(LOG_DEBUG) << "        Destination executor index: " <<
                                     path.GetDestinationExecutors()[0].GetExecutorIndex();
                    }
                }
            }
        }

        void Master::SyncWithWorkers()
        {
            for ( WorkerContext& WorkerContext : _Workers ) {
                std::string WorkerId = WorkerContext.GetId();
                LOG(LOG_DEBUG) << "Sync meta data with supervisr: " << WorkerId;
                std::shared_ptr<dcvl::message::CommandClient> WorkerClient =
                        _WorkerClients[WorkerId];

                WorkerClient->GetConnector()->Connect([WorkerId, WorkerClient, &WorkerContext, this]
                (const util::SocketError&){
                    dcvl::message::Command command(dcvl::message::Command::Type::SyncMetadata);

                    // 1 means Master to Worker
                    // 2 means Worker to Master
                    command.AddArgument({ 1 });

                    base::Variants WorkerContextVariants;
                    WorkerContext.Serialize(WorkerContextVariants);
                    command.AddArguments(WorkerContextVariants);
                    WorkerClient->SendCommand(command,
                        [WorkerId, this](const dcvl::message::Response& response, const message::CommandError& error) -> void {
                        if ( error.GetType() != message::CommandError::Type::NoError ) {
                            LOG(LOG_ERROR) << error.what();
                            return;
                        }

                        if ( response.GetStatus() == dcvl::message::Response::Status::Successful ) {
                            LOG(LOG_DEBUG) << "Sync with " << WorkerId << " successfully.";
                        }
                        else {
                            LOG(LOG_DEBUG) << "Sync with " << WorkerId << " failed.";
                        }
                    });
                });
            }
        }

        void Master::ShowWorkerTaskInfos()
        {
            LOG(LOG_DEBUG) << "================ Allocate result ================";
            for ( WorkerContext& WorkerContext : _Workers ) {
                LOG(LOG_DEBUG) << WorkerContext.GetId();
                LOG(LOG_DEBUG) << "  Host: " << WorkerContext.GetNetAddress().GetHost();
                LOG(LOG_DEBUG) << "  Port: " << WorkerContext.GetNetAddress().GetPort();

                LOG(LOG_DEBUG) << "  Tasks: ";
                const std::vector<dcvl::task::TaskInfo>& taskInfos =
                        WorkerContext.GetTaskInfos();
                ShowTaskInfos(taskInfos);
            }
        }

        void Master::CalculateTaskPaths(
                const std::map<std::string, std::vector<dcvl::task::TaskInfo*>>& nameToBoltTasks,
                const std::map<std::string, dcvl::bolt::BoltDeclarer>& boltDeclarers,
                const std::map<std::string, std::vector<dcvl::task::TaskInfo*>>& nameToSpoutTasks)
        {
            for ( const auto& boltPair : boltDeclarers ) {
                dcvl::bolt::BoltDeclarer boltDeclarer = boltPair.second;
                // No setted source task
                if ( boltDeclarer.GetSourceTaskName().empty() ) {
                    continue;
                }

                std::string sourceTaskName = boltDeclarer.GetSourceTaskName();
                std::vector<dcvl::task::TaskInfo*> sourceTasks =
                        FindTask(nameToBoltTasks, nameToSpoutTasks, sourceTaskName);

                std::string destTaskName = boltDeclarer.GetTaskName();
                std::vector<dcvl::task::TaskInfo*> destTasks =
                        FindTask(nameToBoltTasks, destTaskName);

                std::vector<task::ExecutorPosition>  destExecutorPositions;
                for ( dcvl::task::TaskInfo* destTask : destTasks ) {
                    destExecutorPositions.push_back(task::ExecutorPosition(
                        destTask->GetWorkerContext()->GetNetAddress(),
                        destTask->GetExecutorIndex()
                    ));
                }

                if ( boltDeclarer.GetGroupMethod() == task::TaskDeclarer::GroupMethod::Global ) {
                    for ( dcvl::task::TaskInfo* sourceTask : sourceTasks ) {
                        int32_t destTaskIndex = rand() % destTasks.size();
                        dcvl::task::TaskInfo* destTask = destTasks[destTaskIndex];

                        dcvl::task::PathInfo pathInfo;
                        pathInfo.SetGroupMethod(dcvl::task::PathInfo::GroupMethod::Global);
                        pathInfo.SetDestinationTask(destTask->GetTaskName());
                        pathInfo.SetDestinationExecutors({task::ExecutorPosition(
                                destTask->GetWorkerContext()->GetNetAddress(),
                                destTask->GetExecutorIndex()
                        )});

                        sourceTask->AddPath(pathInfo);
                    }
                }
                else if ( boltDeclarer.GetGroupMethod() == task::TaskDeclarer::GroupMethod::Field ) {
                    // Resolve the destination by field when run task.
                    for ( dcvl::task::TaskInfo* sourceTask : sourceTasks ) {
                        dcvl::task::PathInfo pathInfo;
                        pathInfo.SetGroupMethod(dcvl::task::PathInfo::GroupMethod::Field);
                        pathInfo.SetDestinationTask(destTaskName);
                        pathInfo.SetFieldName(boltDeclarer.GetGroupField());

                        sourceTask->AddPath(pathInfo);
                    }

                    TaskPathName taskPathName = { sourceTaskName, destTaskName };
                    _fieldsCandidates[taskPathName] = destExecutorPositions;
                }
                else if ( boltDeclarer.GetGroupMethod() == task::TaskDeclarer::GroupMethod::Random ) {
                    // Resolve the destination by field when run task.
                    for ( dcvl::task::TaskInfo* sourceTask : sourceTasks ) {
                        dcvl::task::PathInfo pathInfo;
                        pathInfo.SetGroupMethod(dcvl::task::PathInfo::GroupMethod::Random);
                        pathInfo.SetDestinationTask(destTaskName);
                        pathInfo.SetDestinationExecutors(destExecutorPositions);

                        sourceTask->AddPath(pathInfo);
                    }
                }
                else {
                    LOG(LOG_ERROR) << "Unsupported group method occured";
                    exit(EXIT_FAILURE);
                }
            }
        }

        void Master::ShowWorkerMetadata()
        {
            LOG(LOG_DEBUG) << "================ Worker metadata ================";
            for ( WorkerContext& WorkerContext : _Workers ) {
                LOG(LOG_DEBUG) << "Worker name: " << WorkerContext.GetId();
                LOG(LOG_DEBUG) << "  Spout count: " << WorkerContext.GetSpoutCount();
                LOG(LOG_DEBUG) << "  Bolt count: " << WorkerContext.GetBoltCount();
                LOG(LOG_DEBUG) << "  Task info count: " << WorkerContext.GetTaskInfos().size();
                LOG(LOG_DEBUG) << "  Free spout count: " << WorkerContext.GetFreeSpouts().size();
                LOG(LOG_DEBUG) << "  Free bolt count: " << WorkerContext.GetFreeBolts().size();
                LOG(LOG_DEBUG) << "  Busy spout count: " << WorkerContext.GetBusySpouts().size();
                LOG(LOG_DEBUG) << "  Busy bolt count: " << WorkerContext.GetBusyBolts().size();
            }
        }

        void Master::SubmitTopology(dcvl::topology::Topology* topology) {
            LOG(LOG_INFO) << "Submit topology: " << topology->GetName();

            _orderIds[topology->GetName()] = 0;

            const std::map<std::string, dcvl::spout::SpoutDeclarer>& spoutDeclarers =
                    topology->GetSpoutDeclarers();
            const std::map<std::string, dcvl::bolt::BoltDeclarer>& boltDeclarers =
                    topology->GetBoltDeclarers();

            // Allocate task and send to Worker
            std::list<dcvl::task::TaskInfo> originSpoutTasks =
                    GetAllSpoutTasks(spoutDeclarers, topology);
            std::map<std::string, std::vector<dcvl::task::TaskInfo*>> nameToSpoutTasks =
                    AllocateSpoutTasks(originSpoutTasks);

            std::list<dcvl::task::TaskInfo> originBoltTasks =
                    GetAllBoltTasks(topology, boltDeclarers);
            std::map<std::string, std::vector<dcvl::task::TaskInfo*>> nameToBoltTasks =
                    AllocateBoltTasks(originBoltTasks);

            CalculateTaskPaths(nameToBoltTasks, boltDeclarers, nameToSpoutTasks);
            ShowWorkerTaskInfos();
            ShowWorkerMetadata();
            SyncWithWorkers();
        }

        const int32_t MAX_HEARTBEAT_FAILED_TIMES = 5;
        void Master::SendHeartbeat(const std::string WorkerId, int32_t sendTimes)
        {
            LOG(LOG_DEBUG) << "Sending heartbeat to " << WorkerId;

            std::shared_ptr<dcvl::message::CommandClient> commandClient =
                    _WorkerClients.at(WorkerId);

            commandClient->GetConnector()->Connect([commandClient, WorkerId, sendTimes, this]
            (const util::SocketError& error){
                if ( error.GetType() != util::SocketError::Type::NoError ) {
                    LOG(LOG_DEBUG) << "Sendtimes: " << sendTimes;
                    if ( sendTimes >= MAX_HEARTBEAT_FAILED_TIMES ) {
                        return;
                    }

                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                    this->SendHeartbeat(WorkerId, sendTimes + 1);

                    return;
                }

                LOG(LOG_DEBUG) << "Connected to " << WorkerId;
                dcvl::message::Command command(dcvl::message::Command::Type::Heartbeat);

                commandClient->SendCommand(command,
                    [WorkerId, sendTimes, this](const dcvl::message::Response& response, const message::CommandError& error) {
                    if ( error.GetType() != message::CommandError::Type::NoError ) {
                        LOG(LOG_ERROR) << error.what();
                    }

                    if ( response.GetStatus() == dcvl::message::Response::Status::Successful ) {
                        LOG(LOG_INFO) << WorkerId << " alived.";
                    }
                    else {
                        LOG(LOG_ERROR) << WorkerId << " dead.";
                    }
                });
            });
        }
    }
}
