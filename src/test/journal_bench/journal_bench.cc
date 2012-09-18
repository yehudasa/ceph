#include <stdlib.h>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "global/global_init.h"
#include "common/config.h"
#include "common/Finisher.h"
#include "os/FileJournal.h"
#include "include/Context.h"
#include "common/Mutex.h"
#include "common/safe_io.h"
#include "test/bench/detailed_stat_collector.h"
#include "common/Semaphore.h"

namespace po = boost::program_options;

Finisher *finisher;
Cond sync_cond;
uuid_d fsid;

static utime_t cur_time()
{
  struct timeval tv;
  gettimeofday(&tv, 0);
  return utime_t(&tv);
}

class C_LogJournaled : public Context {
  utime_t time;
  unsigned op_size;
  DetailedStatCollector::Aggregator *agg;
  Semaphore *sem;

public:
  C_LogJournaled(utime_t t, unsigned size,
                 DetailedStatCollector::Aggregator *a, Semaphore *s) :
    time(t), op_size(size), agg(a), sem(s)
  {
  }

  void finish(int r) {
    sem->Put();
    agg->add(
      DetailedStatCollector::Op(
        "journaled",
        time,
        cur_time() - time,
        op_size,
        0));
    if (cur_time() - agg->get_last() >= 1)
      dump();
  }

  void dump() {
    JSONFormatter f;
    f.open_object_section("throughput");
    agg->dump(&f);
    f.close_section();
    f.flush(std::cout);
    std::cout << std::endl;
  }
};


int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("dio", po::value<bool>()->default_value(false),
     "direct io")
    ("aio", po::value<bool>()->default_value(false),
     "async io")
    ("debug-to-stderr", po::value<bool>()->default_value(false),
     "send debug to stderr")
    ("max-in-flight", po::value<unsigned>()->default_value(50),
     "max in-flight entries")
    ("op-size", po::value<unsigned>()->default_value(1<<10),
     "size to send")
    ("journal-path", po::value<string>()->default_value("/tmp/journal"),
     "path to journal")
    ("journal-size", po::value<unsigned>()->default_value(500),
     "size of journal in MB")
    ;

  po::variables_map vm;
  po::parsed_options parsed =
    po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
  po::store(
    parsed,
    vm);
  po::notify(vm);

  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
  }

  vector<const char *> ceph_options, def_args;
  vector<string> ceph_option_strings = po::collect_unrecognized(
    parsed.options, po::include_positional);
  ceph_options.reserve(ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  global_init(
    &def_args, ceph_options, CEPH_ENTITY_TYPE_CLIENT,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  DetailedStatCollector::Aggregator agg;

  char mb[20];
  sprintf(mb, "%d", vm["journal-size"].as<unsigned>());
  g_ceph_context->_conf->set_val("osd_journal_size", mb);
  g_ceph_context->_conf->apply_changes(NULL);

  unsigned op_size = vm["op-size"].as<unsigned>();
  unsigned max_in_flight = vm["max-in-flight"].as<unsigned>();
  string path = vm["journal-path"].as<string>();

  finisher = new Finisher(g_ceph_context);

  finisher->start();

  fsid.generate_random();
  FileJournal j(fsid, finisher, &sync_cond, path.c_str(),
                vm["dio"].as<bool>(), vm["aio"].as<bool>());
  j.create();
  j.make_writeable();

  Semaphore sem;
  for (unsigned i = 0; i < vm["max-in-flight"].as<unsigned>(); ++i) sem.Put();

  for (uint64_t seq = 1; ; ++seq) {
    bufferlist bl;
    while (bl.length() < op_size) {
      char foo[1024];
      memset(foo, 1, sizeof(foo));
      bl.append(foo, sizeof(foo));
    }

    if ((seq % max_in_flight) == 0)
      j.committed_thru(seq - max_in_flight);

    sem.Get();
    j.submit_entry(seq, bl, 0,
                   new C_LogJournaled(cur_time(), op_size, &agg, &sem));
    j.commit_start();
  }
  wait();

  j.close();

  finisher->stop();

  unlink(path.c_str());

  return 0;
}
