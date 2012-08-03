// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <iostream>
#include <set>
#include <sstream>
#include <stdlib.h>
#include <fstream>
#include <iostream>
#include <unistd.h>

#include "msg/Message.h"
#include "msg/SimpleMessenger.h"
#include "msg/msg_types.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"
#include "messages/MBlob.h"
#include "include/utime.h"
#include "test/bench/detailed_stat_collector.h"
#include "common/Semaphore.h"

namespace po = boost::program_options;
using namespace std;

static utime_t cur_time()
{
  struct timeval tv;
  gettimeofday(&tv, 0);
  return utime_t(&tv);
}

class BlobDispatcherRec : public Dispatcher {
  Semaphore *sem;
public:
  BlobDispatcherRec(CephContext *cct, Semaphore *sem)
    : Dispatcher(cct), sem(sem) {}
  bool ms_dispatch(Message *m) {
    m->put();
    sem->Put();
    return true;
  }
  bool ms_handle_reset(Connection *con) { return true; }
  void ms_handle_remote_reset(Connection *con) {}
  void ms_handle_connect(Connection *con) {}
};


class BlobDispatcher : public Dispatcher {
  Messenger *m;
  DetailedStatCollector::Aggregator agg;
public:
  BlobDispatcher(Messenger *m, CephContext *cct) : Dispatcher(cct), m(m) {}
  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);
  void ms_handle_connect(Connection *con);

  void dump();
};

void BlobDispatcher::dump()
{
  JSONFormatter f;
  f.open_object_section("throughput");
  agg.dump(&f);
  f.close_section();
  f.flush(std::cout);
  std::cout << std::endl;
}

void BlobDispatcher::ms_handle_connect(Connection *con)
{
  std::cout << "Got Connetion!" << con << std::endl;
}

void BlobDispatcher::ms_handle_remote_reset(Connection *con)
{
  std::cout << "Lost COnnection!" << con << std::endl;
}

bool BlobDispatcher::ms_handle_reset(Connection *con)
{
  std::cout << "Lost COnnection!" << con << std::endl;
  return true;
}

bool BlobDispatcher::ms_dispatch(Message *_m)
{
  MBlob *msg = static_cast<MBlob*>(_m);
  agg.add(
    DetailedStatCollector::Op(
      "message",
      msg->time,
      cur_time() - msg->time,
      msg->bl.length(),
      0));
  if (cur_time() - agg.get_last() >= 1)
    dump();
  bufferlist bl;
  m->send_message(new MBlob(bl, cur_time()),
    msg->get_connection());
  msg->put();
  return true;
}

int server(CephContext *cct,
	   po::variables_map vm, entity_addr_t server_addr)
{
  uint64_t nonce = getpid() + (1000000 * (uint64_t)1);
  boost::scoped_ptr<Messenger> msger(
    new SimpleMessenger(cct, entity_name_t::CLIENT(-1),
			"test-server", nonce));
  boost::scoped_ptr<Dispatcher> dispatcher(new BlobDispatcher(msger.get(), cct));

  msger->set_cluster_protocol(24);
  msger->add_dispatcher_head(dispatcher.get());
  msger->bind(server_addr);
  msger->start();
  while (1) sleep(200);
  msger->shutdown();
  msger->wait();
  return 0;
}

int client(CephContext *cct,
	   po::variables_map vm, entity_addr_t server_addr)
{
  Semaphore sem; 
  for (unsigned i = 0; i < vm["max-in-flight"].as<unsigned>(); ++i) sem.Put();
  uint64_t nonce = getpid() + (1000000 * (uint64_t)1);
  boost::scoped_ptr<Dispatcher> dispatcher(new BlobDispatcherRec(cct, &sem));
  boost::scoped_ptr<Messenger> msger(
    new SimpleMessenger(cct, entity_name_t::CLIENT(-1),
			"test-client", nonce));

  msger->add_dispatcher_head(dispatcher.get());
  msger->set_cluster_protocol(24);
  entity_name_t server_name = entity_name_t::OSD(0);
  entity_inst_t server(server_name, server_addr);

  bufferptr bp(buffer::create_page_aligned(vm["size"].as<unsigned>()));
  bufferlist bl;
  bl.push_back(bp);
  msger->start();
  Connection *con = msger->get_connection(server);
  while (1) {
    sem.Get();
    msger->send_message(new MBlob(bl, cur_time()), con);
  }
  con->put();
  msger->shutdown();
  msger->wait();
  return 0;
}

int main(int argc, char **argv)
{
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("role", po::value<string>()->default_value("server"),
     "server or client")
    ("server-addr", po::value<string>()->default_value("127.0.0.1:12345"),
     "server addr")
    ("disable-nagle", po::value<bool>()->default_value("true"),
     "disable nagle")
    ("debug-ms", po::value<unsigned>()->default_value(0),
     "ms debug level")
    ("debug-to-stderr", po::value<bool>()->default_value(false),
     "send debug to stderr")
    ("max-in-flight", po::value<unsigned>()->default_value(100),
     "max unacknoledged messages")
    ("size", po::value<unsigned>()->default_value(4<<20),
     "size to send")
    ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  
  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  cct->_conf->subsys.set_log_level(
    ceph_subsys_ms,
    vm["debug-ms"].as<unsigned>());
  if (vm["debug-to-stderr"].as<bool>())
    cct->_conf->set_val("log_to_stderr", "1");
  if (!vm["disable-nagle"].as<bool>())
    cct->_conf->set_val("ms_tcp_nodelay", "false");
  cct->_conf->apply_changes(NULL);
  
  entity_addr_t server_addr;
  if (!server_addr.parse(vm["server-addr"].as<string>().c_str())) {
    std::cerr << "invalid addr: " << vm["server-addr"].as<string>()
	      << std::endl;
    std::cerr << desc << std::endl;
    return 1;
  }
  
  boost::scoped_ptr<Messenger> msg;
  if (vm["role"].as<string>() == "client") {
    client(cct, vm, server_addr);
  } else if (vm["role"].as<string>() == "server") {
    server(cct, vm, server_addr);
  } else {
    std::cerr << "role not client or server" << std::endl;
    std::cerr << desc << std::endl;
    return 1;
  }

  return 0;
}
