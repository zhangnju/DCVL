/**
 * licensed to the apache software foundation (asf) under one
 * or more contributor license agreements.  see the notice file
 * distributed with this work for additional information
 * regarding copyright ownership.  the asf licenses this file
 * to you under the apache license, version 2.0 (the
 * "license"); you may not use this file except in compliance
 * with the license.  you may obtain a copy of the license at
 *
 * http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 */

#include "dcvl/service/Worker.h"
#include "dcvl/message/CommandClient.h"
#include "dcvl/util/NetConnector.h"
#include "dcvl/util/Configuration.h"
#include "dcvl/topology/Topology.h"
#include "dcvl/topology/TopologyLoader.h"
#include "dcvl/task/SpoutExecutor.h"
#include "dcvl/task/BoltExecutor.h"
#include "dcvl/spout/ISpout.h"
#include "dcvl/bolt/IBolt.h"
#include "dcvl/collector/OutputCollector.h"
#include "dcvl/collector/OutputQueue.h"
#include "dcvl/collector/TaskQueue.h"
#include "dcvl/base/Constants.h"

namespace dcvl {
    namespace service {

    Worker::Worker(const dcvl::util::Configuration& configuration) :
            CommandServer(new dcvl::util::NetListener(dcvl::base::NetAddress(
                    configuration.GetProperty(ConfigurationKey::WorkerHost),
                    configuration.GetIntegerProperty(ConfigurationKey::WorkerPort)))),
            _host(configuration.GetProperty(ConfigurationKey::WorkerHost)),
            _port(configuration.GetIntegerProperty(ConfigurationKey::WorkerPort)) {
        _WorkerConfiguration.reset(new dcvl::util::Configuration(configuration));
        _name = configuration.GetProperty(ConfigurationKey::WorkerName);

        InitMasterConnector();
        InitSelfContext();
        ReserveExecutors();
        InitEvents();
    }

    void Worker::InitMasterConnector()
    {
        dcvl::base::NetAddress MasterAddress(_WorkerConfiguration->GetProperty(ConfigurationKey::MasterPort),
            _WorkerConfiguration->GetIntegerProperty(ConfigurationKey::MasterPort));
        _MasterConnector = new dcvl::util::NetConnector(MasterAddress);
        _MasterClient = new dcvl::message::CommandClient(_MasterConnector);
    }

    void Worker::ReserveExecutors()
    {
        _spoutExecutors.resize(_WorkerConfiguration->GetIntegerProperty(ConfigurationKey::SpoutCount));
        _boltExecutors.resize(_WorkerConfiguration->GetIntegerProperty(ConfigurationKey::BoltCount));
        _spoutCollectors.resize(_WorkerConfiguration->GetIntegerProperty(ConfigurationKey::SpoutCount));
        _boltCollectors.resize(_WorkerConfiguration->GetIntegerProperty(ConfigurationKey::BoltCount));
        _boltTaskQueues.resize(_WorkerConfiguration->GetIntegerProperty(ConfigurationKey::BoltCount));

        for ( auto& boltTask : _boltTaskQueues ) {
            boltTask.reset(new collector::TaskQueue);
        }

        _outputDispatcher.SetQueue(std::shared_ptr<collector::OutputQueue>(
            new collector::OutputQueue()));
        _outputDispatcher.SetSelfAddress(dcvl::base::NetAddress(_host, _port));
        _outputDispatcher.SetSelfTasks(_boltTaskQueues);
        _outputDispatcher.SetSelfSpoutCount(static_cast<int32_t>(_spoutExecutors.size()));

        dcvl::base::NetAddress MasterAddress(_WorkerConfiguration->GetProperty(ConfigurationKey::MasterHost),
            _WorkerConfiguration->GetIntegerProperty(ConfigurationKey::MasterPort));
        _MasterConnector = new dcvl::util::NetConnector(MasterAddress);
        _MasterClient = new dcvl::message::CommandClient(_MasterConnector);
        _outputDispatcher.SetMasterClient(_MasterClient);

        _outputDispatcher.Start();
    }

    void Worker::InitEvents()
    {
        OnConnection(std::bind(&Worker::OnConnect, this, std::placeholders::_1));
        OnCommand(dcvl::message::Command::Type::Heartbeat, this, &Worker::OnHeartbeat);
        OnCommand(dcvl::message::Command::Type::SyncMetadata, this, &Worker::OnSyncMetadata);
        OnCommand(dcvl::message::Command::Type::SendTuple, this, &Worker::OnSendTuple);
    }

    void Worker::InitTaskFieldsMap()
    {
        const std::map<std::string, dcvl::spout::SpoutDeclarer>& spoutDeclarers =
                _topology->GetSpoutDeclarers();
        for ( const auto& spoutDeclarerPair : spoutDeclarers ) {
            const spout::SpoutDeclarer& spoutDeclarer = spoutDeclarerPair.second;

            _taskFields[spoutDeclarer.GetTaskName()] = &spoutDeclarer.GetFields();
            _taskFieldsMap[spoutDeclarer.GetTaskName()] = &spoutDeclarer.GetFieldsMap();
        }

        const std::map<std::string, dcvl::bolt::BoltDeclarer>& boltDeclarers =
                _topology->GetBoltDeclarers();
        for ( const auto& boltDeclarerPair : boltDeclarers ) {
            const bolt::BoltDeclarer& boltDeclarer = boltDeclarerPair.second;

            _taskFields[boltDeclarer.GetTaskName()] = &boltDeclarer.GetFields();
            _taskFieldsMap[boltDeclarer.GetTaskName()] = &boltDeclarer.GetFieldsMap();
        }

        _outputDispatcher.SetTaskFields(_taskFields);
        _outputDispatcher.SetTaskFieldsMap(_taskFieldsMap);
    }

    void Worker::OnConnect(WorkerContext* context) {
    }

    void Worker::JoinMaster(JoinMasterCallback callback) {
        dcvl::message::CommandClient* commandClient = _MasterClient;

        _MasterConnector->Connect([commandClient, callback, this](const util::SocketError&) {
            dcvl::message::Command command(dcvl::message::Command::Type::Join);
            command.AddArgument({ NodeType::Worker });
            command.AddArgument({ this->_host });
            command.AddArgument({ this->_port });
            std::vector<dcvl::base::Variant> context;
            _selfContext->Serialize(context);
            command.AddArguments(context);

            commandClient->SendCommand(command, [callback](const dcvl::message::Response& response,
                    const message::CommandError& error) {
                if ( error.GetType() != message::CommandError::Type::NoError ) {
                    LOG(LOG_ERROR) << error.what();
                    return;
                }

                callback(response);
            });
        });
    }

    void Worker::OnHeartbeat(WorkerContext* context, const message::Command& command,
        dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor)
    {
        dcvl::message::Response response(dcvl::message::Response::Status::Successful);
        response.AddArgument({ _name });

        Responsor(response);
    }

    void Worker::OnSyncMetadata(WorkerContext* context, const message::Command& command,
        message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor)
    {
        const std::vector<dcvl::base::Variant>& arguments = command.GetArguments();

        int32_t syncMethod = arguments[0].GetInt32Value();
        if ( syncMethod != 1 ) {
            dcvl::message::Response response(dcvl::message::Response::Status::Failed);
            Responsor(response);

            return;
        }

        dcvl::message::Response response(dcvl::message::Response::Status::Successful);
        base::Variants::const_iterator currentIterator = arguments.cbegin() + 1;
        _selfContext->Deserialize(currentIterator);

        OwnWorkerTasks();
        _outputDispatcher.SetTaskInfos(_selfContext->GetTaskInfos());

        ShowWorkerMetadata();
        ShowTaskInfos();

        std::string topologyName = _WorkerConfiguration->GetProperty(ConfigurationKey::TopologyName);
        _topology = dcvl::topology::TopologyLoader::GetInstance().GetTopology(topologyName);

        InitTaskFieldsMap();
        InitExecutors();

        Responsor(response);
    }

    void Worker::OnSendTuple(WorkerContext* context, const message::Command& command,
                                 message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor)
    {
        const base::Variants& arguments = command.GetArguments();
        base::Variants::const_iterator it = arguments.cbegin();

        base::NetAddress sourceAddress;
        sourceAddress.Deserialize(it);

        task::ExecutorPosition destination;
        destination.Deserialize(it);

        base::Tuple tuple;
        tuple.Deserialize(it);
        tuple.SetFields(_taskFields[tuple.GetSourceTask()]);
        tuple.SetFieldsMap(_taskFieldsMap[tuple.GetSourceTask()]);

        int32_t executorIndex = destination.GetExecutorIndex();
        int32_t boltIndex = executorIndex - _selfContext->GetSpoutCount();

        std::shared_ptr<dcvl::collector::TaskQueue> taskQueue = _boltTaskQueues[boltIndex];
        collector::TaskItem* taskItem =
                new collector::TaskItem(executorIndex, tuple);
        taskQueue->Push(taskItem);

        dcvl::message::Response response(dcvl::message::Response::Status::Successful);
        Responsor(response);
    }

    void Worker::InitSelfContext() {
        this->_selfContext.reset(new WorkerContext);
        _selfContext->SetId(_name);
        _selfContext->SetSpoutCount(_WorkerConfiguration->GetIntegerProperty(ConfigurationKey::SpoutCount));
        _selfContext->SetBoltCount(_WorkerConfiguration->GetIntegerProperty(ConfigurationKey::BoltCount));
        _selfContext->SetTaskInfos(std::vector<dcvl::task::TaskInfo>(_selfContext->GetSpoutCount() + _selfContext->GetBoltCount()));

        std::set<int32_t> freeSpouts;
        int spoutCount = _selfContext->GetSpoutCount();
        for ( int32_t spoutIndex = 0; spoutIndex != spoutCount; ++ spoutIndex ) {
            freeSpouts.insert(spoutIndex);
        }
        _selfContext->SetFreeSpouts(freeSpouts);

        std::set<int32_t> freeBolts;
        int boltCount = _selfContext->GetBoltCount();
        for ( int32_t boltIndex = 0; boltIndex != boltCount; ++ boltIndex ) {
            freeBolts.insert(boltIndex);
        }
        _selfContext->SetFreeBolts(freeBolts);
    }

    void Worker::InitSpoutExecutors()
    {
        LOG(LOG_DEBUG) << "Init spout executors";
        const std::map<std::string, dcvl::spout::SpoutDeclarer>& spoutDeclarers =
                _topology->GetSpoutDeclarers();
        std::set<int32_t> busySpouts = _selfContext->GetBusySpouts();
        for ( int32_t spoutIndex : busySpouts ) {
            dcvl::task::TaskInfo& spoutTask = _selfContext->GetSpoutTaskInfo(spoutIndex);
            std::string taskName = spoutTask.GetTaskName();
            const dcvl::spout::SpoutDeclarer& spoutDeclarer = spoutDeclarers.at(taskName);

            std::shared_ptr<collector::OutputQueue> outputQueue = _outputDispatcher.GetQueue();
            collector::OutputCollector* collector = new collector::OutputCollector(spoutIndex,
                    taskName, outputQueue);
            _spoutCollectors[spoutIndex].reset(collector);

            spout::ISpout* spout = spoutDeclarer.GetSpout()->Clone();
            spout->Prepare(_spoutCollectors[spoutIndex]);

            std::shared_ptr<task::SpoutExecutor> spoutExecutor(new task::SpoutExecutor);
            spoutExecutor->SetSpout(spout);
            int32_t flowParam = _WorkerConfiguration->GetIntegerProperty(ConfigurationKey::SpoutFlowParam);
            spoutExecutor->SetFlowParam(flowParam);
            _spoutExecutors[spoutIndex] = spoutExecutor;
        }
    }

    void Worker::InitBoltExecutors()
    {
        LOG(LOG_DEBUG) << "Init bolt executors";
        const std::map<std::string, dcvl::bolt::BoltDeclarer>& boltDeclarers =
                _topology->GetBoltDeclarers();
        std::set<int32_t> busyBolts = _selfContext->GetBusyBolts();
        int32_t spoutCount = _selfContext->GetSpoutCount();
        for ( int32_t boltIndex : busyBolts ) {
            LOG(LOG_DEBUG) << boltIndex;

            dcvl::task::TaskInfo& boltTask = _selfContext->GetBoltTaskInfo(boltIndex);
            std::string taskName = boltTask.GetTaskName();
            const dcvl::bolt::BoltDeclarer& boltDeclarer = boltDeclarers.at(taskName);

            std::shared_ptr<collector::OutputQueue> outputQueue = _outputDispatcher.GetQueue();
            collector::OutputCollector* collector = new collector::OutputCollector(
                        spoutCount + boltIndex, taskName, outputQueue);
            _boltCollectors[boltIndex].reset(collector);

            bolt::IBolt* bolt = boltDeclarer.GetBolt()->Clone();
            bolt->Prepare(_boltCollectors[boltIndex]);

            std::shared_ptr<task::BoltExecutor> boltExecutor(new task::BoltExecutor);
            _boltExecutors[boltIndex] = boltExecutor;
            boltExecutor->SetTaskQueue(_boltTaskQueues[boltIndex]);
            boltExecutor->SetBolt(bolt);
        }
    }

    void Worker::InitExecutors()
    {
        InitSpoutExecutors();
        InitBoltExecutors();

        std::set<int32_t> busyBolts = _selfContext->GetBusyBolts();
        std::set<int32_t> busySpouts = _selfContext->GetBusySpouts();

        for ( int32_t boltIndex : busyBolts ) {
            _boltExecutors[boltIndex]->Start();
        }

        for ( int32_t spoutIndex : busySpouts ) {
            _spoutExecutors[spoutIndex]->Start();
        }
    }

    void Worker::OwnWorkerTasks()
    {
        std::vector<dcvl::task::TaskInfo>& taskInfos = _selfContext->GetTaskInfos();
        for ( dcvl::task::TaskInfo& taskInfo : taskInfos ) {
            taskInfo.SetWorkerContext(_selfContext.get());
        }
    }

    void Worker::ShowWorkerMetadata()
    {
        LOG(LOG_DEBUG) << "Worker name: " << _selfContext->GetId();
        LOG(LOG_DEBUG) << "  Spout count: " << _selfContext->GetSpoutCount();
        LOG(LOG_DEBUG) << "  Bolt count: " << _selfContext->GetBoltCount();
        LOG(LOG_DEBUG) << "  Task info count: " << _selfContext->GetTaskInfos().size();
        LOG(LOG_DEBUG) << "  Free spout count: " << _selfContext->GetFreeSpouts().size();
        LOG(LOG_DEBUG) << "  Free bolt count: " << _selfContext->GetFreeBolts().size();
        LOG(LOG_DEBUG) << "  Busy spout count: " << _selfContext->GetBusySpouts().size();
        LOG(LOG_DEBUG) << "  Busy bolt count: " << _selfContext->GetBusyBolts().size();
    }

    void Worker::ShowTaskInfos()
    {
        const std::vector<dcvl::task::TaskInfo>& taskInfos = _selfContext->GetTaskInfos();
        for ( const dcvl::task::TaskInfo& taskInfo : taskInfos ) {
            if ( !taskInfo.GetWorkerContext() ) {
                continue;
            }

            LOG(LOG_DEBUG) << "    Worker: " << taskInfo.GetWorkerContext()->GetId();
            LOG(LOG_DEBUG) << "    Exectuor index: " << taskInfo.GetExecutorIndex();
            LOG(LOG_DEBUG) << "    Task name: " << taskInfo.GetTaskName();
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
}
}
