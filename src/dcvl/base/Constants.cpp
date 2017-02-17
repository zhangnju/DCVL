#include "dcvl/base/Constants.h"

namespace dcvl {

std::string NodeType::Master = "Master";
std::string NodeType::Worker = "Worker";

std::string ConfigurationKey::MasterHost = "Master.host";
std::string ConfigurationKey::MasterPort = "Master.port";
std::string ConfigurationKey::WorkerCount = "Master.Worker.count";
std::string ConfigurationKey::WorkerName = "Worker.name";
std::string ConfigurationKey::WorkerHost = "Worker.host";
std::string ConfigurationKey::WorkerPort = "Worker.port";
std::string ConfigurationKey::TopologyName = "topology.name";
std::string ConfigurationKey::SpoutCount = "Worker.spout.num";
std::string ConfigurationKey::BoltCount = "Worker.bolt.num";
std::string ConfigurationKey::SpoutFlowParam = "spout.flow.param";
}
