#pragma once

#include "dcvl/util/NetListener.h"
#include "dcvl/message/Command.h"
#include "dcvl/base/ByteArray.h"
#include "logging/Logging.h"

#include <functional>
#include <cstdint>
#include <map>

namespace dcvl {
    namespace util {
        class TcpConnection;
    }

    namespace message {
        class BaseCommandServerContext {
        public:
            const std::string& GetId() const {
                return _id;
            }

            void SetId(const std::string& id) {
                _id = id;
            }

        private:
            std::string _id;
        };

        template <class CommandServerContext>
        class CommandServer {
        public:
            typedef std::function<void(const Response& response)> Responsor;
            typedef std::function<void(CommandServerContext* context)> ConnectHandler;
            typedef std::function<void(CommandServerContext* context, const Command& command, Responsor)> CommandHandler;

            CommandServer(dcvl::util::NetListener* listener = nullptr) : _listener(listener) {
            }

            virtual ~CommandServer();
            void StartListen();

            void OnConnection(ConnectHandler handler) {
                _connectHandler = handler;
            }

            template <class ObjectType, class HandlerType>
            void OnCommand(int32_t commandType, ObjectType* self, HandlerType handler) {
                OnCommand(commandType, std::bind(handler, self, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
            }

            void OnCommand(int32_t commandType, CommandHandler handler) {
                _commandHandlers.insert({ commandType, handler });
            }

            void Response(dcvl::util::TcpConnection* connection, const Response& response);
            
            void SetListener(dcvl::util::NetListener* listener) {
                _listener = listener;
            }

        private:
            dcvl::util::NetListener* _listener;
            std::map<int32_t, CommandHandler> _commandHandlers;
            ConnectHandler _connectHandler;
        };

        template <class CommandServerContext>
        CommandServer<CommandServerContext>::~CommandServer() {
            if ( _listener ) {
                delete _listener;
                _listener = nullptr;
            }
        }

        template <class CommandServerContext>
        void CommandServer<CommandServerContext>::StartListen() {
            std::map<int32_t, CommandHandler>& commandHandlers = _commandHandlers;
            ConnectHandler connectHandler = _connectHandler;

            _listener->OnConnection([this, connectHandler, commandHandlers](std::shared_ptr<dcvl::util::TcpConnection> connection) {
                CommandServerContext* context = new CommandServerContext;
                dcvl::util::TcpConnection* rawConnection = connection.get();

                _connectHandler(context);

                connection->OnData([this, commandHandlers, context, rawConnection]
                        (const char* buffer, int32_t size, const util::SocketError& error) {
                    if ( error.GetType() != util::SocketError::Type::NoError ) {
                        std::cout << error.what();
                        return;
                    }

                    dcvl::base::ByteArray commandBytes(buffer, size);
                    Command command;
                    command.Deserialize(commandBytes);

                    int32_t commandType = command.GetType();
                    try {
                        CommandHandler handler = commandHandlers.at(commandType);
                        Responsor Responsor = std::bind(&CommandServer::Response, this, rawConnection, std::placeholders::_1);

                        handler(context, command, Responsor);
                    }
                    catch ( const std::exception& e ) {
                        LOG(LOG_ERROR) << "Some errors in command handler";
                        LOG(LOG_ERROR) << e.what();

                        Responsor Responsor = std::bind(&CommandServer::Response, this, rawConnection, std::placeholders::_1);

                        dcvl::message::Response response(dcvl::message::Response::Status::Failed);
                        Responsor(response);
                    }
                });
            });

            _listener->StartListen();
        }

        template <class CommandServerContext>
        void CommandServer<CommandServerContext>::Response(dcvl::util::TcpConnection* connection, const dcvl::message::Response& response) {
            dcvl::base::ByteArray responseBytes = response.Serialize();
            connection->Send(responseBytes.data(), responseBytes.size());
        }
    }
}
