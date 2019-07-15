#include "common/ceph_time.h"
#include "messages/MPing.h"
#include "crimson/auth/DummyAuth.h"
#include "crimson/common/log.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Messenger.h"

#include <map>
#include <random>
#include <boost/program_options.hpp>
#include <seastar/core/app-template.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>

namespace bpo = boost::program_options;

namespace {

seastar::logger& logger() {
  return ceph::get_logger(ceph_subsys_ms);
}

static std::random_device rd;
static std::default_random_engine rng{rd()};
static bool verbose = false;

static seastar::future<> test_echo(unsigned rounds,
                                   double keepalive_ratio,
                                   bool v2)
{
  struct test_state {
    struct Server final
        : public ceph::net::Dispatcher,
          public seastar::peering_sharded_service<Server> {
      ceph::net::Messenger *msgr = nullptr;
      ceph::auth::DummyAuthClientServer dummy_auth;

      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::make_ready_future<>();
      }
      seastar::future<> ms_dispatch(ceph::net::Connection* c,
                                    MessageRef m) override {
        if (verbose) {
          logger().info("server got {}", *m);
        }
        // reply with a pong
        return c->send(make_message<MPing>());
      }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce,
                             const entity_addr_t& addr) {
        auto&& fut = ceph::net::Messenger::create(name, lname, nonce);
        return fut.then([this, addr](ceph::net::Messenger *messenger) {
            return container().invoke_on_all([messenger](auto& server) {
                server.msgr = messenger->get_local_shard();
                server.msgr->set_default_policy(ceph::net::SocketPolicy::stateless_server(0));
                server.msgr->set_require_authorizer(false);
                server.msgr->set_auth_client(&server.dummy_auth);
                server.msgr->set_auth_server(&server.dummy_auth);
              }).then([messenger, addr] {
                return messenger->bind(entity_addrvec_t{addr});
              }).then([this, messenger] {
                return messenger->start(this);
              });
          });
      }
      seastar::future<> shutdown() {
        ceph_assert(msgr);
        return msgr->shutdown();
      }
    };

    struct Client final
        : public ceph::net::Dispatcher,
          public seastar::peering_sharded_service<Client> {

      struct PingSession : public seastar::enable_shared_from_this<PingSession> {
        unsigned count = 0u;
        mono_time connected_time;
        mono_time finish_time;
      };
      using PingSessionRef = seastar::shared_ptr<PingSession>;

      unsigned rounds;
      std::bernoulli_distribution keepalive_dist;
      ceph::net::Messenger *msgr = nullptr;
      std::map<ceph::net::Connection*, seastar::promise<>> pending_conns;
      std::map<ceph::net::Connection*, PingSessionRef> sessions;
      ceph::auth::DummyAuthClientServer dummy_auth;

      Client(unsigned rounds, double keepalive_ratio)
        : rounds(rounds),
          keepalive_dist(std::bernoulli_distribution{keepalive_ratio}) {}

      PingSessionRef find_session(ceph::net::Connection* c) {
        auto found = sessions.find(c);
        if (found == sessions.end()) {
          ceph_assert(false);
        }
        return found->second;
      }

      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::now();
      }
      seastar::future<> ms_handle_connect(ceph::net::ConnectionRef conn) override {
        logger().info("{}: connected to {}", *conn, conn->get_peer_addr());
        auto session = seastar::make_shared<PingSession>();
        auto [i, added] = sessions.emplace(conn.get(), session);
        std::ignore = i;
        ceph_assert(added);
        session->connected_time = mono_clock::now();
        return seastar::now();
      }
      seastar::future<> ms_dispatch(ceph::net::Connection* c,
                                    MessageRef m) override {
        auto session = find_session(c);
        ++(session->count);
        if (verbose) {
          logger().info("client ms_dispatch {}", session->count);
        }

        if (session->count == rounds) {
          logger().info("{}: finished receiving {} pongs", *c, session->count);
          session->finish_time = mono_clock::now();
          return container().invoke_on_all([c](auto &client) {
              auto found = client.pending_conns.find(c);
              ceph_assert(found != client.pending_conns.end());
              found->second.set_value();
            });
        } else {
          return seastar::now();
        }
      }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce) {
        return ceph::net::Messenger::create(name, lname, nonce)
          .then([this](ceph::net::Messenger *messenger) {
            return container().invoke_on_all([messenger](auto& client) {
                client.msgr = messenger->get_local_shard();
                client.msgr->set_default_policy(ceph::net::SocketPolicy::lossy_client(0));
                client.msgr->set_auth_client(&client.dummy_auth);
                client.msgr->set_auth_server(&client.dummy_auth);
              }).then([this, messenger] {
                return messenger->start(this);
              });
          });
      }

      seastar::future<> shutdown() {
        ceph_assert(msgr);
        return msgr->shutdown();
      }

      // Note: currently we don't support foreign dispatch a message because:
      // 1. it is not effecient because each ref-count modification needs
      //    a cross-core jump, so it should be discouraged.
      // 2. messenger needs to be modified to hold a wrapper for the sending
      //    message because it can be a nested seastar smart ptr or not.
      // 3. in 1:1 mapping OSD, there is no need to do foreign dispatch.
      seastar::future<> dispatch_pingpong(const entity_addr_t& peer_addr,
					  bool foreign_dispatch) {
#ifndef CRIMSON_MSGR_SEND_FOREIGN
	ceph_assert(!foreign_dispatch);
#endif
        mono_time start_time = mono_clock::now();
        return msgr->connect(peer_addr, entity_name_t::TYPE_OSD)
          .then([this, foreign_dispatch, start_time](auto conn) {
            return seastar::futurize_apply([this, conn, foreign_dispatch] {
                if (foreign_dispatch) {
                  return do_dispatch_pingpong(&**conn);
                } else {
                  // NOTE: this could be faster if we don't switch cores in do_dispatch_pingpong().
                  return container().invoke_on(conn->get()->shard_id(), [conn = &**conn](auto &client) {
                      return client.do_dispatch_pingpong(conn);
                    });
                }
              }).finally([this, conn, start_time] {
                return container().invoke_on(conn->get()->shard_id(), [conn, start_time](auto &client) {
                    auto session = client.find_session(&**conn);
                    std::chrono::duration<double> dur_handshake = session->connected_time - start_time;
                    std::chrono::duration<double> dur_pingpong = session->finish_time - session->connected_time;
                    logger().info("{}: handshake {}, pingpong {}",
                                  **conn, dur_handshake.count(), dur_pingpong.count());
                  });
              });
          });
      }

     private:
      seastar::future<> do_dispatch_pingpong(ceph::net::Connection* conn) {
        return container().invoke_on_all([conn](auto& client) {
            auto [i, added] = client.pending_conns.emplace(conn, seastar::promise<>());
            std::ignore = i;
            ceph_assert(added);
          }).then([this, conn] {
            return seastar::do_with(0u, 0u,
                                    [this, conn](auto &count_ping, auto &count_keepalive) {
                return seastar::do_until(
                  [this, conn, &count_ping, &count_keepalive] {
                    bool stop = (count_ping == rounds);
                    if (stop) {
                      logger().info("{}: finished sending {} pings with {} keepalives",
                                    *conn, count_ping, count_keepalive);
                    }
                    return stop;
                  },
                  [this, conn, &count_ping, &count_keepalive] {
                    return seastar::repeat([this, conn, &count_ping, &count_keepalive] {
                        if (keepalive_dist(rng)) {
                          return conn->keepalive()
                            .then([&count_keepalive] {
                              count_keepalive += 1;
                              return seastar::make_ready_future<seastar::stop_iteration>(
                                seastar::stop_iteration::no);
                            });
                        } else {
                          return conn->send(make_message<MPing>())
                            .then([&count_ping] {
                              count_ping += 1;
                              return seastar::make_ready_future<seastar::stop_iteration>(
                                seastar::stop_iteration::yes);
                            });
                        }
                      });
                  }).then([this, conn] {
                    auto found = pending_conns.find(conn);
                    return found->second.get_future();
                  });
              });
          });
      }
    };
  };

  logger().info("test_echo(rounds={}, keepalive_ratio={}, v2={}):",
                rounds, keepalive_ratio, v2);
  return seastar::when_all_succeed(
      ceph::net::create_sharded<test_state::Server>(),
      ceph::net::create_sharded<test_state::Server>(),
      ceph::net::create_sharded<test_state::Client>(rounds, keepalive_ratio),
      ceph::net::create_sharded<test_state::Client>(rounds, keepalive_ratio))
    .then([rounds, keepalive_ratio, v2](test_state::Server *server1,
                                        test_state::Server *server2,
                                        test_state::Client *client1,
                                        test_state::Client *client2) {
      // start servers and clients
      entity_addr_t addr1;
      addr1.parse("127.0.0.1:9010", nullptr);
      entity_addr_t addr2;
      addr2.parse("127.0.0.1:9011", nullptr);
      if (v2) {
        addr1.set_type(entity_addr_t::TYPE_MSGR2);
        addr2.set_type(entity_addr_t::TYPE_MSGR2);
      } else {
        addr1.set_type(entity_addr_t::TYPE_LEGACY);
        addr2.set_type(entity_addr_t::TYPE_LEGACY);
      }
      return seastar::when_all_succeed(
          server1->init(entity_name_t::OSD(0), "server1", 1, addr1),
          server2->init(entity_name_t::OSD(1), "server2", 2, addr2),
          client1->init(entity_name_t::OSD(2), "client1", 3),
          client2->init(entity_name_t::OSD(3), "client2", 4))
      // dispatch pingpoing
        .then([client1, client2, server1, server2] {
          return seastar::when_all_succeed(
              // test connecting in parallel, accepting in parallel
#ifdef CRIMSON_MSGR_SEND_FOREIGN
	      // operate the connection reference from a foreign core
	      client1->dispatch_pingpong(server1->msgr->get_myaddr(), true),
	      client2->dispatch_pingpong(server2->msgr->get_myaddr(), true),
#endif
	      // operate the connection reference from a local core
              client1->dispatch_pingpong(server2->msgr->get_myaddr(), false),
              client2->dispatch_pingpong(server1->msgr->get_myaddr(), false));
      // shutdown
        }).finally([client1] {
          logger().info("client1 shutdown...");
          return client1->shutdown();
        }).finally([client2] {
          logger().info("client2 shutdown...");
          return client2->shutdown();
        }).finally([server1] {
          logger().info("server1 shutdown...");
          return server1->shutdown();
        }).finally([server2] {
          logger().info("server2 shutdown...");
          return server2->shutdown();
        }).finally([] {
          logger().info("test_echo() done!\n");
        });
    });
}

static seastar::future<> test_concurrent_dispatch(bool v2)
{
  struct test_state {
    struct Server final
      : public ceph::net::Dispatcher,
        public seastar::peering_sharded_service<Server> {
      ceph::net::Messenger *msgr = nullptr;
      int count = 0;
      seastar::promise<> on_second; // satisfied on second dispatch
      seastar::promise<> on_done; // satisfied when first dispatch unblocks
      ceph::auth::DummyAuthClientServer dummy_auth;

      seastar::future<> ms_dispatch(ceph::net::Connection* c,
                                    MessageRef m) override {
        switch (++count) {
        case 1:
          // block on the first request until we reenter with the second
          return on_second.get_future()
            .then([this] {
              return container().invoke_on_all([](Server& server) {
                  server.on_done.set_value();
                });
            });
        case 2:
          on_second.set_value();
          return seastar::now();
        default:
          throw std::runtime_error("unexpected count");
        }
      }

      seastar::future<> wait() { return on_done.get_future(); }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce,
                             const entity_addr_t& addr) {
        return ceph::net::Messenger::create(name, lname, nonce, 0)
          .then([this, addr](ceph::net::Messenger *messenger) {
            return container().invoke_on_all([messenger](auto& server) {
                server.msgr = messenger->get_local_shard();
                server.msgr->set_default_policy(ceph::net::SocketPolicy::stateless_server(0));
                server.msgr->set_auth_client(&server.dummy_auth);
                server.msgr->set_auth_server(&server.dummy_auth);
              }).then([messenger, addr] {
                return messenger->bind(entity_addrvec_t{addr});
              }).then([this, messenger] {
                return messenger->start(this);
              });
          });
      }

      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::make_ready_future<>();
      }
    };

    struct Client final
      : public ceph::net::Dispatcher,
        public seastar::peering_sharded_service<Client> {
      ceph::net::Messenger *msgr = nullptr;
      ceph::auth::DummyAuthClientServer dummy_auth;

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce) {
        return ceph::net::Messenger::create(name, lname, nonce, 0)
          .then([this](ceph::net::Messenger *messenger) {
            return container().invoke_on_all([messenger](auto& client) {
                client.msgr = messenger->get_local_shard();
                client.msgr->set_default_policy(ceph::net::SocketPolicy::lossy_client(0));
                client.msgr->set_auth_client(&client.dummy_auth);
                client.msgr->set_auth_server(&client.dummy_auth);
              }).then([this, messenger] {
                return messenger->start(this);
              });
          });
      }

      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::make_ready_future<>();
      }
    };
  };

  logger().info("test_concurrent_dispatch(v2={}):", v2);
  return seastar::when_all_succeed(
      ceph::net::create_sharded<test_state::Server>(),
      ceph::net::create_sharded<test_state::Client>())
    .then([v2](test_state::Server *server,
             test_state::Client *client) {
      entity_addr_t addr;
      addr.parse("127.0.0.1:9010", nullptr);
      if (v2) {
        addr.set_type(entity_addr_t::TYPE_MSGR2);
      } else {
        addr.set_type(entity_addr_t::TYPE_LEGACY);
      }
      addr.set_family(AF_INET);
      return seastar::when_all_succeed(
          server->init(entity_name_t::OSD(4), "server3", 5, addr),
          client->init(entity_name_t::OSD(5), "client3", 6))
        .then([server, client] {
          return client->msgr->connect(server->msgr->get_myaddr(),
                                      entity_name_t::TYPE_OSD);
        }).then([](ceph::net::ConnectionXRef conn) {
          // send two messages
          (*conn)->send(make_message<MPing>());
          (*conn)->send(make_message<MPing>());
        }).then([server] {
          return server->wait();
        }).finally([client] {
          logger().info("client shutdown...");
          return client->msgr->shutdown();
        }).finally([server] {
          logger().info("server shutdown...");
          return server->msgr->shutdown();
        }).finally([] {
          logger().info("test_concurrent_dispatch() done!\n");
        });
    });
}

}

int main(int argc, char** argv)
{
  seastar::app_template app;
  app.add_options()
    ("verbose,v", bpo::value<bool>()->default_value(false),
     "chatty if true")
    ("rounds", bpo::value<unsigned>()->default_value(512),
     "number of pingpong rounds")
    ("keepalive-ratio", bpo::value<double>()->default_value(0.1),
     "ratio of keepalive in ping messages");
  return app.run(argc, argv, [&app] {
    auto&& config = app.configuration();
    verbose = config["verbose"].as<bool>();
    auto rounds = config["rounds"].as<unsigned>();
    auto keepalive_ratio = config["keepalive-ratio"].as<double>();
    return test_echo(rounds, keepalive_ratio, false)
    .then([rounds, keepalive_ratio] {
      return test_echo(rounds, keepalive_ratio, true);
    }).then([] {
      return test_concurrent_dispatch(false);
    }).then([] {
      return test_concurrent_dispatch(true);
    }).then([] {
      std::cout << "All tests succeeded" << std::endl;
    }).handle_exception([] (auto eptr) {
      std::cout << "Test failure" << std::endl;
      return seastar::make_exception_future<>(eptr);
    });
  });
}
