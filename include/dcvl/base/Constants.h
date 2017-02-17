#pragma once

#include <string>

namespace dcvl {

class NodeType {
public:
    static std::string Master;
    static std::string Worker;
};

class ConfigurationKey {
public:
    static std::string MasterHost;
    static std::string MasterPort;
    static std::string WorkerName;
    static std::string WorkerHost;
    static std::string WorkerPort;
    static std::string WorkerCount;
    static std::string TopologyName;
    static std::string SpoutCount;
    static std::string BoltCount;
    static std::string SpoutFlowParam;
};
}
