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

#include <string>
#include <vector>
#include "dcvl/base/ByteArray.h"
#include "dcvl/base/Variant.h"

namespace dcvl {
    namespace message {
        class Command {
        public:
            struct Type {
                enum {
                    Invalid = 0,
                    Join,
                    Heartbeat,
                    SyncMetadata,
                    SendTuple,
                    AskField
                };
            };

            Command(int32_t type = Type::Invalid) : _type(type) {
            }

            Command(int32_t type, std::vector<dcvl::base::Variant>& arguments) :
                    _type(type), _arguments(arguments) {
            }

            int32_t GetType() const {
                return _type;
            }

            void SetType(int32_t type) {
                _type = type;
            }

            dcvl::base::Variant GetArgument(int32_t index) const {
                return _arguments[index];
            }

            int32_t GetArgumentCount() const {
                return static_cast<int32_t>(_arguments.size());
            }

            const std::vector<dcvl::base::Variant>& GetArguments() const {
                return _arguments;
            }

            void AddArgument(const dcvl::base::Variant& argument) {
                _arguments.push_back(argument);
            }

            void AddArguments(const std::vector<dcvl::base::Variant>& arguments) {
                for ( const dcvl::base::Variant& argument : arguments ) {
                    _arguments.push_back(argument);
                }
            }

            void Deserialize(const dcvl::base::ByteArray& data);
            dcvl::base::ByteArray Serialize() const;

        private:
            int32_t _type;
            std::vector<dcvl::base::Variant> _arguments;
        };

        class Response {
        public:
            struct Status {
                enum {
                    Failed = 0,
                    Successful = 1
                };
            };

            Response(int32_t status = Status::Failed) : _status(status) {
            }

            Response(int32_t status, std::vector<dcvl::base::Variant>& arguments) :
                _status(status), _arguments(arguments) {
            }

            int32_t GetStatus() const {
                return _status;
            }

            void SetStatus(int32_t status) {
                _status = status;
            }

            dcvl::base::Variant GetArgument(int32_t index) const {
                return _arguments[index];
            }

            int32_t GetArgumentCount() const {
                return static_cast<int32_t>(_arguments.size());
            }

            const std::vector<dcvl::base::Variant>& GetArguments() const {
                return _arguments;
            }

            void AddArguments(const std::vector<dcvl::base::Variant>& arguments) {
                for ( const dcvl::base::Variant& argument : arguments ) {
                    _arguments.push_back(argument);
                }
            }

            void AddArgument(const dcvl::base::Variant& argument) {
                _arguments.push_back(argument);
            }

            void Deserialize(const dcvl::base::ByteArray& data);
            dcvl::base::ByteArray Serialize() const;

        private:
            int32_t _status;
            std::vector<dcvl::base::Variant> _arguments;
        };
    }
}
