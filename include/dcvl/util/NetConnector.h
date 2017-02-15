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

#include "dcvl/util/Socket.h"
#include "dcvl/base/NetAddress.h"
#include <cstdint>
#include <functional>
#include <memory>

namespace dcvl {
    namespace util {
        class NetConnector {
        public:
            typedef std::function<void(const SocketError&)> ConnectCallback;
            typedef std::function<void(char* resultBuffer, int32_t readSize, const SocketError&)> DataReceiver;

            NetConnector(const dcvl::base::NetAddress& host) :
                _host(host) {
            }

            const dcvl::base::NetAddress& GetHost() const {
                return _host;
            }

            void SetHost(const dcvl::base::NetAddress& host) {
                _host = host;
            }

            void Connect();
            void Connect(ConnectCallback callback);
            int32_t SendAndReceive(const char* buffer, int32_t size, char* resultBuffer, int32_t resultSize);
            void SendAndReceive(const char* buffer, int32_t size, DataReceiver receiver);

        private:
            dcvl::base::NetAddress _host;
            std::shared_ptr<TcpClient> _client;
        };
    }
}
