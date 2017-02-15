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

#include "dcvl/topology/Topology.h"

namespace dcvl {
    namespace topology {
        Topology::Topology(const std::string& name) : _name(name) {
        }

        dcvl::spout::SpoutDeclarer& Topology::SetSpout(const std::string& spoutName, dcvl::spout::ISpout* spout) {
            _spoutDeclarers.insert({ spoutName, dcvl::spout::SpoutDeclarer(spoutName, spout) });

            return _spoutDeclarers[spoutName];
        }

        dcvl::bolt::BoltDeclarer& Topology::SetBolt(const std::string& boltName, dcvl::bolt::IBolt* bolt) {
            _boltDeclarers.insert({ boltName, dcvl::bolt::BoltDeclarer(boltName, bolt) });

            return _boltDeclarers[boltName];
        }
    }
}
