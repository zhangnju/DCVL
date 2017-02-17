#pragma once

#include "dcvl/message/CommandServer.h"
#include "dcvl/util/NetListener.h"
#include "dcvl/base/NetAddress.h"
#include "dcvl/service/WorkerContext.h"

namespace dcvl {
    namespace util {
        class Configuration;
    }

    namespace topology {
        class Topology;
    }

    namespace message {
        class CommandClient;
    }

    namespace spout {
        class SpoutDeclarer;
    }

    namespace bolt {
        class BoltDeclarer;
    }

    namespace service {
        typedef std::pair<std::string, std::string> TaskPathName;

        class Master : public dcvl::message::CommandServer<WorkerContext> {
        public:
            Master(const dcvl::base::NetAddress& host);
            Master(const dcvl::util::Configuration& configuration);

            void OnConnect(WorkerContext* context);
            void OnJoin(WorkerContext* context, const dcvl::message::Command& command,
                dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor);
            void OnAskField(WorkerContext* context, const dcvl::message::Command& command,
                dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor);
            void OnOrderId(WorkerContext* context, const dcvl::message::Command& command,
                           dcvl::message::CommandServer<dcvl::message::BaseCommandServerContext>::Responsor Responsor);
            void SubmitTopology(dcvl::topology::Topology* topology);
            
        private:
            void SendHeartbeat(const std::string WorkerId, int32_t sendTimes);
            std::list<dcvl::task::TaskInfo> GetAllSpoutTasks(const std::map<std::string, dcvl::spout::SpoutDeclarer>& spoutDeclarers, dcvl::topology::Topology* topology);
            void AllocateSpoutTasks(std::map<std::string, dcvl::task::TaskInfo*> nameToSpoutTasks, std::list<dcvl::task::TaskInfo> originSpoutTasks);
            std::map<std::string, std::vector<task::TaskInfo*> > AllocateSpoutTasks(std::list<task::TaskInfo>& originSpoutTasks);
            std::list<dcvl::task::TaskInfo> GetAllBoltTasks(dcvl::topology::Topology* topology, const std::map<std::string, dcvl::bolt::BoltDeclarer>& boltDeclarers);
            std::map<std::string, std::vector<task::TaskInfo*> > AllocateBoltTasks(std::list<task::TaskInfo>& originBoltTasks);
            std::vector<task::TaskInfo*> FindTask(
                    const std::map<std::string, std::vector<task::TaskInfo*> >& nameToBoltTasks,
                    const std::map<std::string, std::vector<task::TaskInfo*> >& nameToSpoutTasks,
                    const std::string& sourceTaskName);
            std::vector<task::TaskInfo*> FindTask(
                    const std::map<std::string, std::vector<task::TaskInfo*> >& nameToBoltTasks,
                    const std::string& sourceTaskName);
            void CalculateTaskPaths(
                    const std::map<std::string, std::vector<task::TaskInfo*> >& nameToBoltTasks,
                    const std::map<std::string, dcvl::bolt::BoltDeclarer>& boltDeclarers,
                    const std::map<std::string, std::vector<task::TaskInfo*> >& nameToSpoutTasks);
            void ShowWorkerMetadata();
            void ShowWorkerTaskInfos();
            void ShowTaskInfos(const std::vector<dcvl::task::TaskInfo>& taskInfos);
            void SyncWithWorkers();

        private:
            dcvl::base::NetAddress _MasterHost;
            std::vector<WorkerContext> _Workers;
            int32_t _WorkerCount;
            std::shared_ptr<dcvl::util::Configuration> _configuration;
            std::map<std::string, std::shared_ptr<dcvl::message::CommandClient>> _WorkerClients;
            std::map<TaskPathName, std::vector<task::ExecutorPosition>> _fieldsCandidates;
            std::map<TaskPathName, std::map<std::string, task::ExecutorPosition>> _fieldsDestinations;
            std::map<std::string, int64_t> _orderIds;
        };
    }
}
