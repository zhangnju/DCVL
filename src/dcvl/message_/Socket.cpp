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

#ifdef WIN32
#pragma comment(lib, "ws2_32.lib")

#include "dcvl/util/Socket.h"

#include <Winsock2.h>  
#include <cstdio>

namespace dcvl {
    namespace util {
        WSAInitializer::WSAInitializer() {
            if ( WSAStartup(MAKEWORD(2, 2), &_wsaData) != 0 )
            {
                LOG(LOG_FATAL) << "Failed to load Winsock.";
                exit(-1);
            }
        }

        WSAInitializer& WSAInitializer::InitInstance()
        {
            static WSAInitializer instance;

            return instance;
        }

        WSAInitializer::~WSAInitializer()
        {
            WSACleanup();
        }

    }
}
#endif
