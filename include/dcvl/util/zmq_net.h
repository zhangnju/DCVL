#ifndef DCVL_NET_ZMQ_NET_H_
#define DCL_NET_ZMQ_NET_H_

#include "dcvl/util/Net.h"
#include "dcvl/message/Message.h"
#include <iostream>
#include <limits>
#include <thread>
#include <zmq.h>
#include <unordered_set>
namespace dcvl {

class ZMQNetWrapper : public NetInterface {
public:
#if 0
  void Init(bool IsMaster) override {
    // get machine file 
    if (active_) return;
    //ParseMachineFile(MV_CONFIG_machine_file, &machine_lists_);
	//parse config file
	//int port = 55555;// MV_CONFIG_port;

    //size_ = static_cast<int>(machine_lists_.size());
   // CHECK(size_ > 0);
    std::unordered_set<std::string> local_ip;
   // net::GetLocalIPAddress(&local_ip);

    context_ = zmq_ctx_new();
    zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 256);

    for (auto ip : machine_lists_) {
      if (local_ip.find(ip) != local_ip.end()) { // my rank
        rank_ = static_cast<int>(senders_.size());
        Entity sender; sender.endpoint = ""; sender.socket = nullptr;
        senders_.push_back(sender);
        receiver_.socket = zmq_socket(context_, ZMQ_DEALER);
        receiver_.endpoint = ip + ":" + std::to_string(port);
        int rc = zmq_bind(receiver_.socket, ("tcp://" + receiver_.endpoint).c_str());  
      } else {
        Entity sender;
        sender.socket = zmq_socket(context_, ZMQ_DEALER);
        sender.endpoint = ip + ":" + std::to_string(port);
        int rc = zmq_connect(sender.socket, ("tcp://" + sender.endpoint).c_str());
        senders_.push_back(sender);
      }
    }
    if(receiver_.socket==NULL)
		return;
    active_ = true;
	std::cout << name().c_str() << "net util inited, rank =" << rank() << ", size = " << size() << std::endl;
  }
#else
	//before Init function, we should check local IP addr and then set the flag of IsMatser
	//for master node, it will be set REP mode
	//for worker node, the command channel wil be set REQ mode, and the data channel will be
	//set PUSH/PULL mode
	void Init(bool IsMaster) override {
		if (active_) return;
		context_ = zmq_ctx_new();
		zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 256);
 
		if (IsMaster)
		{
			zmq::socket_t master_socket(context, ZMQ_REP);
			zmq_socket(context_, ZMQ_REP);
			master_socket.bind("tcp://*:5555");

		}
		else
		{
		}
	}
#endif
  virtual int Bind(int rank, char* endpoint) override {
    rank_ = rank;
    std::string ip_port(endpoint);
    if (context_ == nullptr) { 
      context_ = zmq_ctx_new(); 
    }
    if(context_==NULL)
		return -1;
    if (receiver_.socket == nullptr) 
      receiver_.socket = zmq_socket(context_, ZMQ_DEALER);
    receiver_.endpoint = ip_port;
    int rc = zmq_bind(receiver_.socket, ("tcp://" + receiver_.endpoint).c_str());
    if (rc == 0) {
      int linger = 0;
      return 0;
    }
    else {
      std::cout<<"Failed to bind the socket for receiver, ip:port ="<<endpoint<<std::endl;
      return -1;
    }
  }

  // Connect with other endpoints
  virtual int Connect(int* ranks, char* endpoints[], int size) override {
    if(receiver_.socket==NULL)
		return -1;
    if(context_==NULL)
		return -1;
    size_ = size;
    senders_.resize(size_);
    for (auto i = 0; i < size; ++i) {
      int rank = ranks[i];
      std::string ip_port(endpoints[i]);
      if (ip_port == receiver_.endpoint) {
        rank_ = rank;
        continue;
      }
      senders_[rank].socket = zmq_socket(context_, ZMQ_DEALER);
      senders_[rank].endpoint = ip_port;
      int rc = zmq_connect(senders_[rank].socket, ("tcp://" + senders_[rank].endpoint).c_str());
      if (rc != 0) {
        std::cout<<"Failed to connect the socket for sender, rank ="<<rank<<"ip:port = "<<endpoints[i]<<std::endl;
        return -1;
      }
    }
    active_ = true;
    return 0;
  }

  void Finalize() override {
    for (auto i = 0; i < senders_.size(); ++i) {
      if (i != rank_) {
        int linger = 0;
        zmq_setsockopt(senders_[i].socket, ZMQ_LINGER, &linger, sizeof(linger));
        int rc = zmq_close(senders_[i].socket);
        if (rc != 0) {
			std::cout << "rc =" << rc << ", i = " << i << ", rank =" << rank_ << std::endl;
        }
      }
    }
    int linger = 0;
    zmq_setsockopt(receiver_.socket, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_close(receiver_.socket);

    std::cout<<"zmq finalize: before close context"<<std::endl;
    zmq_ctx_shutdown(context_);
    //CHECK_NOTNULL(context_);
    zmq_ctx_term(context_);
    context_ = nullptr;
    std::cout<<"zmq finalize: close context"<<std::endl;
  }

  bool active() const override { return active_; }
  int rank() const override { return rank_; }
  int size() const override { return size_; }
  std::string name() const override { return "ZeroMQ"; }

  int Send(MessagePtr& msg) override {
    int size = 0;
    int dst = msg->dst();
    void* socket = senders_[dst].socket;
    if(socket==NULL)
		return -1;
    int send_size;
    send_size = zmq_send(socket, msg->header(), 
      Message::kHeaderSize, msg->data().size() > 0 ? ZMQ_SNDMORE : 0);
    if(Message::kHeaderSize != send_size)
		return -1;
    size += send_size;
    for (size_t i = 0; i < msg->data().size(); ++i) {
      Blob blob = msg->data()[i];
      size_t blob_size = blob.size();
      //CHECK_NOTNULL(blob.data());
      send_size = zmq_send(socket, &blob_size, sizeof(size_t), ZMQ_SNDMORE);
      //CHECK(send_size == sizeof(size_t));
      send_size = zmq_send(socket, blob.data(), static_cast<int>(blob.size()),
        i == msg->data().size() - 1 ? 0 : ZMQ_SNDMORE);
      //CHECK(send_size == blob_size);
      size += static_cast<int>(blob_size + sizeof(size_t));
    }
    return size;
  }

  int Recv(MessagePtr* msg_ptr) override {
    if (!msg_ptr->get()) msg_ptr->reset(new Message());
    int size = 0;
    int recv_size;
    size_t blob_size;
    int more;
    size_t more_size = sizeof(more);
    // Receiving a Message from multiple zmq_recv
    if(msg_ptr==NULL)
		return -1;
    MessagePtr& msg = *msg_ptr;
    msg->data().clear();
    //CHECK(msg.get());
    recv_size = zmq_recv(receiver_.socket, msg->header(), Message::kHeaderSize, ZMQ_DONTWAIT);
    if (recv_size < 0) { return -1; }
    //CHECK(Message::kHeaderSize == recv_size);

    size += recv_size;
    zmq_getsockopt(receiver_.socket, ZMQ_RCVMORE, &more, &more_size);

    while (more) {
      recv_size = zmq_recv(receiver_.socket, &blob_size, sizeof(size_t), 0);
      //CHECK(recv_size == sizeof(size_t));
      size += recv_size;
      zmq_getsockopt(receiver_.socket, ZMQ_RCVMORE, &more, &more_size);
      //CHECK(more);
      Blob blob(blob_size);
      recv_size = zmq_recv(receiver_.socket, blob.data(), blob.size(), 0);
      //CHECK(recv_size == blob_size);
      size += recv_size;
      msg->Push(blob);
      zmq_getsockopt(receiver_.socket, ZMQ_RCVMORE, &more, &more_size);
    }
    return size;
  }


  void SendTo(int rank, char* buf, int len) const override {
    int send_size = 0;
    while (send_size < len) {
      int cur_size = zmq_send(senders_[rank].socket, buf + send_size, len - send_size, 0);
      if (cur_size < 0) { std::cout<<"socket send error"<<cur_size<<std::endl; }
      send_size += cur_size;
    }
  }

  void RecvFrom(int, char* buf, int len) const override {
    // note: rank is not used here
    int recv_size = 0;
    while (recv_size < len) {
      int cur_size = zmq_recv(receiver_.socket, buf + recv_size, len - recv_size, 0);
	  if (cur_size < 0) { std::cout << "socket receive error" << cur_size << std::endl; }
      recv_size += cur_size;
    }
  }

  void SendRecv(int send_rank, char* send_buf, int send_len,
    int recv_rank, char* recv_buf, int recv_len) const override {
    // send first
    SendTo(send_rank, send_buf, send_len);
    // then recv
    RecvFrom(recv_rank, recv_buf, recv_len);
    // wait for send complete
  }

  int thread_level_support() override { 
    return NetThreadLevel::THREAD_MULTIPLE; 
  }

protected:
  void ParseMachineFile(std::string filename, 
                        std::vector<std::string>* result) {
    //CHECK_NOTNULL(result);
    FILE* file;
    char str[32];
    int i = 0;
#ifdef _MSC_VER
    fopen_s(&file, filename.c_str(), "r");
#else
    file = fopen(filename.c_str(), "r");
#endif
    //CHECK_NOTNULL(file);
#ifdef _MSC_VER
    while (fscanf_s(file, "%s", &str, 32) > 0) {
#else
    while (fscanf(file, "%s", &str) > 0) {
#endif
      result->push_back(str);
    }
    fclose(file);
  }

  bool active_;
  void* context_;

  struct Entity {
    std::string endpoint;
    void* socket;
  };

  Entity receiver_;
  Entity sender_;
  //std::vector<Entity> senders_;

  int rank_;
  int size_;
  std::vector<std::string> machine_lists_;
};
} 

#endif 
