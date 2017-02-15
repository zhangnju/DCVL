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

#include "dcvl/bolt/IBolt.h"

class SplitSentenceBolt : public dcvl::bolt::IBolt {
public:
    virtual dcvl::bolt::IBolt* Clone() override {
        return new SplitSentenceBolt;
    }
    virtual void Prepare(std::shared_ptr<dcvl::collector::OutputCollector> outputCollector) override;
    virtual void Cleanup() override;
    virtual std::vector<std::string> DeclareFields() override;
    virtual void Execute(const dcvl::base::Tuple& tuple) override;

private:
    std::shared_ptr<dcvl::collector::OutputCollector> _outputCollector;
};
