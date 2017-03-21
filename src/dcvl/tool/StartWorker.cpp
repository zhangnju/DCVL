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

#include "dcvl/service/Worker.h"
#include "dcvl/util/Configuration.h"
#include "dcvl/base/Constants.h"

#include <iostream>
#include <string>

using namespace std;

void StartWorker(const std::string& configFileName);

int main(int argc, char* argv[])
{
    if ( argc < 2 ) {
        return EXIT_FAILURE;
    }

    StartWorker(argv[1]);

    return EXIT_SUCCESS;
}
bool IsWorker(dcvl::util::Configuration WorkerConfiguration)
{
	//get the local IP 
	//compare the local IP with the setting of worker IP
}
void StartWorker(const std::string& configFileName) {
    using dcvl::ConfigurationKey;

    dcvl::util::Configuration WorkerConfiguration;
    WorkerConfiguration.Parse(configFileName);

    LOG(LOG_INFO) << WorkerConfiguration.GetProperty(ConfigurationKey::MasterHost);
    LOG(LOG_INFO) << WorkerConfiguration.GetIntegerProperty(ConfigurationKey::MasterPort);
    LOG(LOG_INFO) << WorkerConfiguration.GetProperty(ConfigurationKey::WorkerHost);
    LOG(LOG_INFO) << WorkerConfiguration.GetIntegerProperty(ConfigurationKey::WorkerPort);

	if (IsWorker(WorkerConfiguration))
	{
		dcvl::service::Worker Worker(WorkerConfiguration);
		Worker.JoinMaster([&Worker](const dcvl::message::Response& response) {
			if (response.GetStatus() != dcvl::message::Response::Status::Successful) {
				LOG(LOG_ERROR) << "Can't join Master.";
				LOG(LOG_ERROR) << "Exit with failure.";

				exit(EXIT_FAILURE);
			}
			else {
				LOG(LOG_INFO) << "Join successfully";
			}

			Worker.StartListen();
		});
	}
	return;
}
