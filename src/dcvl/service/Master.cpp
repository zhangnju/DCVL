#include "dcvl/service/President.h"
#include "dcvl/util/NetConnector.h"
#include "dcvl/message/CommandClient.h"
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
        President::President(const dcvl::base::NetAddress& host) : 
                CommandServer(new dcvl::util::NetListener(host)),
                _presidentHost(host),
                _managerCount(0) {
            OnConnection(std::bind(&President::OnConnect, this, std::placeholders::_1));
            OnCommand(dcvl::message::Command::Type::Join, this, &President::OnJoin);
            OnCommand(dcvl::message::Command::Type::AskField, this, &President::OnAskField);
        }

        President::President(const dcvl::util::Configuration& configuration) : 
                President(dcvl::base::NetAddress(
                    configuration.GetProperty(ConfigurationKey::PresidentHost),
                    configuration.GetIntegerProperty(ConfigurationKey::PresidentPort))) {
            _managerCount = configuration.GetIntegerProperty(ConfigurationKey::ManagerCount);
            _configuration.reset(new dcvl::util::Configuration(configuration));

            LOG(LOG_DEBUG) << "Need managers: " << _managerCount;
        }

        void President::OnConnect(ManagerContext* context) {
        }

        void President::OnJoin(ManagerContext* context, const dcvl::message::Command& command,
                dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor) {
            std::string joinerType = command.GetArgument(0).GetStringValue();
            std::string managerHost = command.GetArgument(1).GetStringValue();
            int32_t managerPort = command.GetArgument(2).GetInt32Value();

            LOG(LOG_DEBUG) << "Join node: " << joinerType;
            
            ManagerContext managerContext;
            base::Variants::const_iterator currentIterator = command.GetArguments().cbegin() + 3;
            managerContext.Deserialize(currentIterator);

            LOG(LOG_DEBUG) << "Manager name: " << managerContext.GetId();
            LOG(LOG_DEBUG) << "Host: " << managerHost;
            LOG(LOG_DEBUG) << "Port: " << managerPort;
            LOG(LOG_DEBUG) << "Spout count: " << managerContext.GetSpoutCount();
            LOG(LOG_DEBUG) << "Bolt count: " << managerContext.GetBoltCount();
            LOG(LOG_DEBUG) << "Task info count: " << managerContext.GetTaskInfos().size();
            LOG(LOG_DEBUG) << "Free spout count: " << managerContext.GetFreeSpouts().size();
            LOG(LOG_DEBUG) << "Free bolt count: " << managerContext.GetFreeBolts().size();
            LOG(LOG_DEBUG) << "Busy spout count: " << managerContext.GetBusySpouts().size();
            LOG(LOG_DEBUG) << "Busy bolt count: " << managerContext.GetBusyBolts().size();

            managerContext.SetNetAddress(dcvl::base::NetAddress(
                    managerHost, managerPort));
            managerContext.PrepareTaskInfos();
            _managers.push_back(managerContext);

            // Response
            dcvl::message::Response response(dcvl::message::Response::Status::Successful);
            response.AddArgument({ NodeType::President });

            Responsor(response);

            // Initialize command clients
            dcvl::base::NetAddress managerAddress(managerHost,
                managerPort);
            dcvl::util::NetConnector* managerConnector =
                    new dcvl::util::NetConnector(managerAddress);
            dcvl::message::CommandClient* managerCommandClient =
                    new dcvl::message::CommandClient(managerConnector);

            _managerClients.insert({managerContext.GetId(),
                    std::shared_ptr<dcvl::message::CommandClient>(managerCommandClient)});

            SendHeartbeat(managerContext.GetId(), 0);

            // Initialize topology
            if ( _managers.size() == _managerCount ) {
                std::string topologyName = _configuration->GetProperty(ConfigurationKey::TopologyName);
                dcvl::topology::Topology* topology =
                        dcvl::topology::TopologyLoader::GetInstance().GetTopology(topologyName).get();
                SubmitTopology(topology);
            }
        }

        void President::OnAskField(ManagerContext* context, const dcvl::message::Command& command,
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

        void President::OnOrderId(ManagerContext* context, const dcvl::message::Command& command,
                               dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor)
        {
            std::string topologyName = command.GetArgument(0).GetStringValue();

            int64_t orderId = _orderIds[topologyName];
            _orderIds[topologyName] = orderId + 1;

            dcvl::message::Response response(dcvl::message::Response::Status::Successful);
            response.AddArgument(orderId);

            Responsor(response);
        }

        std::list<task::TaskInfo> President::GetAllSpoutTasks(
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
                President::AllocateSpoutTasks(std::list<dcvl::task::TaskInfo>& originSpoutTasks)
        {
            std::map<std::string, std::vector<dcvl::task::TaskInfo*>> nameToSpoutTasks;
            // Allocate task for every manager
            for ( ManagerContext& managerContext : _managers ) {
                if ( !originSpoutTasks.size() ) {
                    break;
                }

                while ( true ) {
                    if ( !originSpoutTasks.size() ) {
                        break;
                    }

                    // If useNextSpout return -1, the spout slots is used up
                    int32_t spoutIndex = managerContext.useNextSpout();
                    if ( spoutIndex == -1 ) {
                        break;
                    }

                    // Put the spout task into spout slot
                    dcvl::task::TaskInfo taskInfo = originSpoutTasks.front();
                    taskInfo.SetManagerContext(&managerContext);
                    taskInfo.SetExecutorIndex(managerContext.GetExecutorIndex(
                            ManagerContext::ExecutorType::Spout, spoutIndex));
                    originSpoutTasks.pop_front();
                    managerContext.SetSpoutTaskInfo(spoutIndex, taskInfo);

                    // Insert the spout task pointer into mapper
                    std::string taskName = taskInfo.GetTaskName();
                    auto spoutTasksPair = nameToSpoutTasks.find(taskName);
                    if ( spoutTasksPair == nameToSpoutTasks.end() ) {
                        nameToSpoutTasks.insert({taskName, std::vector<dcvl::task::TaskInfo*>()});
                        spoutTasksPair = nameToSpoutTasks.find(taskName);
                    }

                    spoutTasksPair->second.push_back(&(managerContext.GetSpoutTaskInfo(spoutIndex)));
                }
            }

            return nameToSpoutTasks;
        }

        std::list<task::TaskInfo> President::GetAllBoltTasks(dcvl::topology::Topology* topology,
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
            President::AllocateBoltTasks(std::list<dcvl::task::TaskInfo>& originBoltTasks)
        {
            std::map<std::string, std::vector<dcvl::task::TaskInfo*>> nameToBoltTasks;
            // Allocate bolt tasks
            for ( ManagerContext& managerContext : _managers ) {
                if ( !originBoltTasks.size() ) {
                    break;
                }

                while ( true ) {
                    if ( !originBoltTasks.size() ) {
                        break;
                    }

                    // If useNextBolt return -1, the bolt slots is used up
                    int32_t boltIndex = managerContext.useNextBolt();
                    if ( boltIndex == -1 ) {
                        break;
                    }

                    // Put the bolt task into bolt slot
                    dcvl::task::TaskInfo taskInfo = originBoltTasks.front();
                    taskInfo.SetManagerContext(&managerContext);
                    taskInfo.SetExecutorIndex(managerContext.GetExecutorIndex(
                            ManagerContext::ExecutorType::Bolt, boltIndex));
                    originBoltTasks.pop_front();
                    managerContext.SetBoltTaskInfo(boltIndex, taskInfo);

                    // Insert the bolt task pointer into mapper
                    std::string taskName = taskInfo.GetTaskName();
                    auto boltTasksPair = nameToBoltTasks.find(taskName);
                    if ( boltTasksPair == nameToBoltTasks.end() ) {
                        nameToBoltTasks.insert({taskName, std::vector<dcvl::task::TaskInfo*>()});
                        boltTasksPair = nameToBoltTasks.find(taskName);
                    }

                    boltTasksPair->second.push_back(&(managerContext.GetBoltTaskInfo(boltIndex)));
                }
            }

            return nameToBoltTasks;
        }

        std::vector<task::TaskInfo*> President::FindTask(
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

        std::vector<task::TaskInfo*> President::FindTask(
                const std::map<std::string, std::vector<task::TaskInfo*>>& nameToBoltTasks,
                const std::string& sourceTaskName)
        {
            auto boltTaskPair = nameToBoltTasks.find(sourceTaskName);
            if ( boltTaskPair != nameToBoltTasks.end() ) {
                return boltTaskPair->second;
            }

            return std::vector<task::TaskInfo*>();
        }

        void President::ShowTaskInfos(const std::vector<dcvl::task::TaskInfo>& taskInfos)
        {
            for ( const dcvl::task::TaskInfo& taskInfo : taskInfos ) {
                if ( !taskInfo.GetManagerContext() ) {
                    continue;
                }

                LOG(LOG_DEBUG) << "    Manager: " << taskInfo.GetManagerContext()->GetId();
                LOG(LOG_DEBUG) << "    Exectuor index: " << taskInfo.GetExecutorIndex();
                LOG(LOG_DEBUG) << "    Paths: ";
                const std::list<dcvl::task::PathInfo>& paths = taskInfo.GetPaths();

                for ( const dcvl::task::PathInfo& path : paths ) {
                    LOG(LOG_DEBUG) << "      Path: ";
                    int32_t groupMethod = path.GetGroupMethod();
                    LOG(LOG_DEBUG) << "        Group method: " << groupMethod;
                    if ( path.GetGroupMethod() == dcvl::task::PathInfo::GroupMethod::Global) {
                        LOG(LOG_DEBUG) << "        Destination host: " <<
                                     path.GetDestinationExecutors()[0].GetManager().GetHost();
                        LOG(LOG_DEBUG) << "        Destination port: " <<
                                     path.GetDestinationExecutors()[0].GetManager().GetPort();
                        LOG(LOG_DEBUG) << "        Destination executor index: " <<
                                     path.GetDestinationExecutors()[0].GetExecutorIndex();
                    }
                }
            }
        }

        void President::SyncWithManagers()
        {
            for ( ManagerContext& managerContext : _managers ) {
                std::string managerId = managerContext.GetId();
                LOG(LOG_DEBUG) << "Sync meta data with supervisr: " << managerId;
                std::shared_ptr<dcvl::message::CommandClient> managerClient =
                        _managerClients[managerId];

                managerClient->GetConnector()->Connect([managerId, managerClient, &managerContext, this]
                (const util::SocketError&){
                    dcvl::message::Command command(dcvl::message::Command::Type::SyncMetadata);

                    // 1 means President to Manager
                    // 2 means Manager to President
                    command.AddArgument({ 1 });

                    base::Variants managerContextVariants;
                    managerContext.Serialize(managerContextVariants);
                    command.AddArguments(managerContextVariants);
                    managerClient->SendCommand(command,
                        [managerId, this](const dcvl::message::Response& response, const message::CommandError& error) -> void {
                        if ( error.GetType() != message::CommandError::Type::NoError ) {
                            LOG(LOG_ERROR) << error.what();
                            return;
                        }

                        if ( response.GetStatus() == dcvl::message::Response::Status::Successful ) {
                            LOG(LOG_DEBUG) << "Sync with " << managerId << " successfully.";
                        }
                        else {
                            LOG(LOG_DEBUG) << "Sync with " << managerId << " failed.";
                        }
                    });
                });
            }
        }

        void President::ShowManagerTaskInfos()
        {
            LOG(LOG_DEBUG) << "================ Allocate result ================";
            for ( ManagerContext& managerContext : _managers ) {
                LOG(LOG_DEBUG) << managerContext.GetId();
                LOG(LOG_DEBUG) << "  Host: " << managerContext.GetNetAddress().GetHost();
                LOG(LOG_DEBUG) << "  Port: " << managerContext.GetNetAddress().GetPort();

                LOG(LOG_DEBUG) << "  Tasks: ";
                const std::vector<dcvl::task::TaskInfo>& taskInfos =
                        managerContext.GetTaskInfos();
                ShowTaskInfos(taskInfos);
            }
        }

        void President::CalculateTaskPaths(
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
                        destTask->GetManagerContext()->GetNetAddress(),
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
                                destTask->GetManagerContext()->GetNetAddress(),
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

        void President::ShowManagerMetadata()
        {
            LOG(LOG_DEBUG) << "================ Manager metadata ================";
            for ( ManagerContext& managerContext : _managers ) {
                LOG(LOG_DEBUG) << "Manager name: " << managerContext.GetId();
                LOG(LOG_DEBUG) << "  Spout count: " << managerContext.GetSpoutCount();
                LOG(LOG_DEBUG) << "  Bolt count: " << managerContext.GetBoltCount();
                LOG(LOG_DEBUG) << "  Task info count: " << managerContext.GetTaskInfos().size();
                LOG(LOG_DEBUG) << "  Free spout count: " << managerContext.GetFreeSpouts().size();
                LOG(LOG_DEBUG) << "  Free bolt count: " << managerContext.GetFreeBolts().size();
                LOG(LOG_DEBUG) << "  Busy spout count: " << managerContext.GetBusySpouts().size();
                LOG(LOG_DEBUG) << "  Busy bolt count: " << managerContext.GetBusyBolts().size();
            }
        }

        void President::SubmitTopology(dcvl::topology::Topology* topology) {
            LOG(LOG_INFO) << "Submit topology: " << topology->GetName();

            _orderIds[topology->GetName()] = 0;

            const std::map<std::string, dcvl::spout::SpoutDeclarer>& spoutDeclarers =
                    topology->GetSpoutDeclarers();
            const std::map<std::string, dcvl::bolt::BoltDeclarer>& boltDeclarers =
                    topology->GetBoltDeclarers();

            // Allocate task and send to manager
            std::list<dcvl::task::TaskInfo> originSpoutTasks =
                    GetAllSpoutTasks(spoutDeclarers, topology);
            std::map<std::string, std::vector<dcvl::task::TaskInfo*>> nameToSpoutTasks =
                    AllocateSpoutTasks(originSpoutTasks);

            std::list<dcvl::task::TaskInfo> originBoltTasks =
                    GetAllBoltTasks(topology, boltDeclarers);
            std::map<std::string, std::vector<dcvl::task::TaskInfo*>> nameToBoltTasks =
                    AllocateBoltTasks(originBoltTasks);

            CalculateTaskPaths(nameToBoltTasks, boltDeclarers, nameToSpoutTasks);
            ShowManagerTaskInfos();
            ShowManagerMetadata();
            SyncWithManagers();
        }

        const int32_t MAX_HEARTBEAT_FAILED_TIMES = 5;
        void President::SendHeartbeat(const std::string managerId, int32_t sendTimes)
        {
            LOG(LOG_DEBUG) << "Sending heartbeat to " << managerId;

            std::shared_ptr<dcvl::message::CommandClient> commandClient =
                    _managerClients.at(managerId);

            commandClient->GetConnector()->Connect([commandClient, managerId, sendTimes, this]
            (const util::SocketError& error){
                if ( error.GetType() != util::SocketError::Type::NoError ) {
                    LOG(LOG_DEBUG) << "Sendtimes: " << sendTimes;
                    if ( sendTimes >= MAX_HEARTBEAT_FAILED_TIMES ) {
                        return;
                    }

                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                    this->SendHeartbeat(managerId, sendTimes + 1);

                    return;
                }

                LOG(LOG_DEBUG) << "Connected to " << managerId;
                dcvl::message::Command command(dcvl::message::Command::Type::Heartbeat);

                commandClient->SendCommand(command,
                    [managerId, sendTimes, this](const dcvl::message::Response& response, const message::CommandError& error) {
                    if ( error.GetType() != message::CommandError::Type::NoError ) {
                        LOG(LOG_ERROR) << error.what();
                    }

                    if ( response.GetStatus() == dcvl::message::Response::Status::Successful ) {
                        LOG(LOG_INFO) << managerId << " alived.";
                    }
                    else {
                        LOG(LOG_ERROR) << managerId << " dead.";
                    }
                });
            });
        }
    }
}
