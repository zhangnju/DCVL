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

#include "Operation.h"
#include <set>

namespace dcvl {
    namespace trident {
        class BaseAggregater : public Operation {
        public:
            virtual void Execute(const TridentTuple& tuple,
                TridentCollector* collector);

            virtual void Init(int32_t batchId, TridentCollector* collector) = 0;
            virtual void Aggregate(const TridentTuple& tuple, TridentCollector* collector) = 0;
            virtual void Complete(TridentCollector* collector) = 0;

        private:
            std::set<int32_t> _batches;
        };
    }
}
