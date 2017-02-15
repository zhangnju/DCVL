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

#pragma once

#include "dcvl/message/CommandServer.h"
#include "dcvl/util/NetListener.h"
#include "dcvl/base/NetAddress.h"
#include "dcvl/service/ManagerContext.h"
#include "dcvl/collector/OutputDispatcher.h"

#include <functional>
#include <memory>

namespace dcvl {
    namespace util {
        class NetConnector;
        class Configuration;
    }

    namespace message {
        class CommandClient;
    }

    namespace task {
        class SpoutExecutor;
        class BoltExecutor;
    }

    namespace topology {
        class Topology;
    }

    namespace collector {
        class OutputCollector;
        class TaskQueue;
    }

    namespace service {
        class Manager : public dcvl::message::CommandServer<ManagerContext> {
        public:
            typedef std::function<void(const dcvl::message::Response& response)> JoinPresidentCallback;
            
            Manager(const dcvl::util::Configuration& configuration);

            void OnConnect(ManagerContext* context);

            void JoinPresident(JoinPresidentCallback callback);

            void OnHeartbeat(ManagerContext* context, const dcvl::message::Command& command,
                            dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor);
            void OnSyncMetadata(ManagerContext* context, const dcvl::message::Command& command,
                            dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor);
            void OnSendTuple(ManagerContext* context, const dcvl::message::Command& command,
                            dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor);


        private:
            void InitSelfContext();
            void InitExecutors();
            void OwnManagerTasks();
            void ShowManagerMetadata();
            void ShowTaskInfos();
            void InitSpoutExecutors();
            void InitBoltExecutors();
            void InitPresidentConnector();
            void ReserveExecutors();
            void InitEvents();
            void InitTaskFieldsMap();

        private:
            std::string _name;
            std::string _host;
            int32_t _port;
            std::shared_ptr<dcvl::util::Configuration> _managerConfiguration;
            dcvl::util::NetConnector* _presidentConnector;
            dcvl::message::CommandClient* _presidentClient;
            std::shared_ptr<dcvl::service::ManagerContext> _selfContext;
            std::vector<std::shared_ptr<dcvl::task::SpoutExecutor>> _spoutExecutors;
            std::vector<std::shared_ptr<dcvl::task::BoltExecutor>> _boltExecutors;
            std::vector<std::shared_ptr<dcvl::collector::OutputCollector>> _spoutCollectors;
            std::vector<std::shared_ptr<dcvl::collector::OutputCollector>> _boltCollectors;
            std::vector<std::shared_ptr<dcvl::collector::TaskQueue>> _boltTaskQueues;
            std::shared_ptr<topology::Topology> _topology;
            dcvl::collector::OutputDispatcher _outputDispatcher;
            std::map<std::string, const std::vector<std::string>*> _taskFields;
            std::map<std::string, const std::map<std::string, int32_t>*> _taskFieldsMap;
        };
    }
}
