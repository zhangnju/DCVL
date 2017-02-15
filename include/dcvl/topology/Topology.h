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

#include "dcvl/spout/SpoutDeclarer.h"
#include "dcvl/bolt/BoltDeclarer.h"

#include <memory>
#include <map>
#include <string>

namespace dcvl {
    namespace spout {
        class ISpout;
    }

    namespace bolt {
        class IBolt;
    }

    namespace topology {
        class Topology {
        public:
            Topology(const std::string& name);

            dcvl::spout::SpoutDeclarer& SetSpout(const std::string& spoutName, dcvl::spout::ISpout* spout);
            dcvl::bolt::BoltDeclarer& SetBolt(const std::string& boltName, dcvl::bolt::IBolt* bolt);

            const std::string& GetName() const {
                return _name;
            }

            const std::map<std::string, dcvl::spout::SpoutDeclarer>& GetSpoutDeclarers() const {
                return _spoutDeclarers;
            }

            const std::map<std::string, dcvl::bolt::BoltDeclarer>& GetBoltDeclarers() const {
                return _boltDeclarers;
            }

        private:
            std::string _name;
            std::map<std::string, dcvl::spout::SpoutDeclarer> _spoutDeclarers;
            std::map<std::string, dcvl::bolt::BoltDeclarer> _boltDeclarers;
        };
    }
}
