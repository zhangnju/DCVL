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

#include "dcvl/multilang/c/HCTopology.h"
#include "dcvl/multilang/c/HCSpout.h"
#include "dcvl/multilang/c/HCBolt.h"
#include "dcvl/topology/TopologyBuilder.h"
#include "dcvl/topology/LocalTopology.h"
#include "dcvl/topology/ITopology.h"

using dcvl::topology::TopologyBuilder;
using dcvl::topology::LocalTopology;
using dcvl::topology::ITopology;
using dcvl::spout::CSpoutWrapper;
using dcvl::bolt::CBoltWrapper;

void StartTopology(CTopology * cTopology)
{
    LocalTopology localTopology;
    TopologyBuilder topologyBuilder;

    for ( int32_t spoutIndex = 0; spoutIndex < cTopology->spoutCount; spoutIndex ++  ) {
        topologyBuilder.SetSpout(cTopology->spoutNames[spoutIndex],
            new CSpoutWrapper(cTopology->cSpouts + spoutIndex));
    }

    for ( int32_t boltIndex = 0; boltIndex < cTopology->boltCount; boltIndex ++ ) {
        topologyBuilder.SetBolt(cTopology->boltNames[boltIndex],
            new CBoltWrapper(cTopology->cBolts + boltIndex),
            cTopology->boltSources[boltIndex]);
    }

    ITopology* topology = topologyBuilder.Build();
    localTopology.Submit("hello", std::shared_ptr<ITopology>(topology));

    std::this_thread::sleep_for(std::chrono::milliseconds(1000 * 10));
}

