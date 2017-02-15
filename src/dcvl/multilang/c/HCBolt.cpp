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

#include "dcvl/multilang/c/HCBolt.h"
#include "dcvl/base/Values.h"
#include "dcvl/base/Fields.h"
#include "dcvl/base/OutputCollector.h"
#include "dcvl/bolt/IBolt.h"
#include "logging/Logging.h"

#include <iostream>

using dcvl::base::Fields;
using dcvl::base::Values;
using dcvl::base::OutputCollector;
using dcvl::bolt::IBolt;

namespace dcvl {
    namespace bolt {
        CBoltWrapper::CBoltWrapper(const CBolt* cBolt) : _cBolt(*cBolt),
            _collector(nullptr) {
        }

        Fields CBoltWrapper::DeclareFields() const
        {
            return Fields();
        }

        void CBoltWrapper::Prepare(OutputCollector & outputCollector)
        {
            _collector = &outputCollector;
            _cBolt.onPrepare(_boltIndex);
        }

        void CBoltWrapper::Cleanup()
        {
            _cBolt.onCleanup(_boltIndex);
        }

        void CBoltWrapper::Execute(const Values& values)
        {
            CValues cValues;
            Values2CValues(values, &cValues);

            _cBolt.onExecute(_boltIndex, this, Emit, &cValues);

            delete[] cValues.values;
        }

        IBolt * CBoltWrapper::Clone() const
        {
            int32_t boltIndex = _cBolt.onClone();
            CBoltWrapper* bolt = new CBoltWrapper(&_cBolt);
            bolt->_boltIndex = boltIndex;

            return bolt;
        }

        voistd::coutapper::Emit(CBoltWrapper* bolt, CValues* cValues) {
            LOG(LOG_DEBUG) << "Emit";
            Values values;
            CValues2Values(*cValues, &values);
            bolt->_collector->Emit(values);
        }
    }
}
    
using dcvl::bolt::CBoltWrapper;

voidstd::coutt(CBolt* cBolt) {
    int32_t boltIndex = cBolt->onClone();
    LOG(LOG_DEBUG) << "Bolt index: " << boltIndex;

    CBoltWrapper* boltWrapper = new CBoltWrapper(cBolt);
    IBolt* bolt = boltWrapper->Clone();

    OutputCollector outputCollector("helo");
    bolt->Prepare(outputCollector);
    bolt->Execute(Values({ "hello world" }));
    bolt->Cleanup();

    delete bolt;
    delete boltWrapper;
}
