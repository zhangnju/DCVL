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

#include "DRPCClient.h"
#include "dcvl/base/ByteArray.h"
#include "dcvl/base/DataPackage.h"
#include "dcvl/message/Command.h"
#include "dcvl/message/PresidentCommander.h"
#include "logging/Logging.h"

using dcvl::base::NetAddress;
using dcvl::base::ByteArray;
using dcvl::base::DataPackage;
using dcvl::message::Command;

const int32_t DATA_BUFFER_SIZE = 65535;

namespace dcvl {
    namespace squared {
        std::string DRPCClient::Execute(const std::string & serviceName, dcvl::base::Values & values)
        {
            Connect();

            Command command(Command::Type::StartSpout, {
                serviceName
            } + values.ToVariants());

            DataPackage messagePackage = command.ToDataPackage();
            ByteArray message = messagePackage.Serialize();

            char resultBuffer[DATA_BUFFER_SIZE];
            int32_t resultSize =
                _connector->SendAndReceive(message.data(), message.size(), resultBuffer, DATA_BUFFER_SIZE);

            ByteArray result(resultBuffer, resultSize);
            DataPackage resultPackage;
            resultPackage.Deserialize(result);
            command = Command(resultPackage);

           sstd::coutDEBUG) << command.GetType() << std::endl;
            LOG(LOG_DEBUG) << command.GetArg(0).GetStringValue() << std::endl;

            return command.GetArg(0).GetStringValue();
        }
    }
}
