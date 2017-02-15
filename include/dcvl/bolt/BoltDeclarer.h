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

#include "dcvl/task/TaskDeclarer.h"
#include <memory>
#include <string>
#include <map>
#include <vector>

namespace dcvl {
    namespace bolt {
        class IBolt;

        class BoltDeclarer : public dcvl::task::TaskDeclarer {
        public:
            BoltDeclarer() = default;
            BoltDeclarer(const std::string& boltName, IBolt* bolt);

            BoltDeclarer& ParallismHint(int32_t parallismHint) {
                SetParallismHint(parallismHint);

                return *this;
            }
            
            BoltDeclarer& Global(const std::string& sourceTaskName) {
                SetSourceTaskName(sourceTaskName);
                SetGroupMethod(dcvl::task::TaskDeclarer::GroupMethod::Global);

                return *this;
            }

            BoltDeclarer& Field(const std::string& sourceTaskName, const std::string& groupField) {
                SetSourceTaskName(sourceTaskName);
                SetGroupMethod(dcvl::task::TaskDeclarer::GroupMethod::Field);
                SetGroupField(groupField);

                return *this;
            }

            BoltDeclarer& Random(const std::string& sourceTaskName) {
                SetSourceTaskName(sourceTaskName);
                SetGroupMethod(dcvl::task::TaskDeclarer::GroupMethod::Random);

                return *this;
            }

            const std::string& GetGroupField() const {
                return _groupField;
            }

            void SetGroupField(const std::string& groupField) {
                _groupField = groupField;
            }

            std::shared_ptr<IBolt> GetBolt() const {
                return _bolt;
            }

            const std::vector<std::string>& GetFields() const {
                return _fields;
            }

            const std::map<std::string, int32_t>& GetFieldsMap() const {
                return _fieldsMap;
            }

        private:
            std::shared_ptr<IBolt> _bolt;
            std::string _groupField;
            std::vector<std::string> _fields;
            std::map<std::string, int32_t> _fieldsMap;
        };
    }
}
