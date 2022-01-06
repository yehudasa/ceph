// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_ADMIN_COPY_H
#define RGW_ADMIN_COPY_H

#include <string>

#include "common/Formatter.h"
#include "common/errno.h"
#include "common/perf_counters_collection.h"

// It can be any non-existing zone ID.
#define BUCKET_COPY_SOURCE_ZONE_ID "bucket-copy-source-zone"

#define RUNNER_BUFFER_SIZE 1000

namespace bucket_copy {

class S3ListBucketEntry {
public:
  struct Owner {
    string id;
    string display_name;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("ID", id, obj, true);
      RGWXMLDecoder::decode_xml("DisplayName", display_name, obj, true);
    }

    void dump_xml(Formatter *f) const {
      f->dump_string("ID", id);
      f->dump_string("DisplayName", display_name);
    }
  };

  string key;
  string etag;
  uint64_t size{0}; // rgw_bucket_dir_entry_meta::accounted_size
  ceph::real_time mtime;
  Owner owner;

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

class S3ListBucketResp {
public:
  struct CommonPrefix {
    string value;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Prefix", value, obj, true);
    }

    void dump_xml(Formatter *f) const {
      f->dump_string("Prefix", value);
    }
  };

  vector<CommonPrefix> common_prefixes;
  vector<S3ListBucketEntry> contents;
  string delimiter;
  bool is_truncated;
  uint32_t max_keys{0};
  string name;
  string prefix;
  string marker;
  string next_marker;

  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

class BucketObjLister {
protected:
  CephContext *cct{nullptr};
  RGWRESTConn *conn{nullptr};

  string bucket_name;
  string prefix;
  string delimiter;
  bool fetch_owner;

  bool allow_unordered;

private:
  bool is_truncated;
  string next_marker;
  string last_obj_key;

public:
  BucketObjLister(CephContext *_cct,
                  RGWRESTConn *_conn,
                  const string &_bucket_name,
                  const string &_prefix = "",
                  const string &_delimiter = "",
                  bool _fetch_owner = false,
                  bool _allow_unordered = false):
    cct(_cct),
    conn(_conn),
    bucket_name(_bucket_name),
    prefix(_prefix),
    delimiter(_delimiter),
    fetch_owner(_fetch_owner),
    allow_unordered(_allow_unordered) {}

  ~BucketObjLister() {}

  const std::string& get_next_token() const;
  int fetch_next(S3ListBucketResp &resp, uint64_t max_keys);
};

class Stats {
private:
  CephContext *cct;
  PerfCountersRef logger;

public:
  enum {
    l_first = 101010,

    l_copy_ok,
    l_copy_enoent,
    l_copy_err,

    l_copy_bytes_transferred,

    l_list_ok,
    l_list_err,
    l_list_latency,

    l_last,
  };

  Stats(const Stats &rhs) = delete; // no copy
  Stats& operator=(const Stats &rhs) = delete; // no assignment

  explicit Stats(CephContext *_cct) : cct(_cct) {
    PerfCountersBuilder b(cct, "bucket-copy", Stats::l_first, Stats::l_last);

    // do not share these counters with ceph-mgr
    b.set_prio_default(PerfCountersBuilder::PRIO_DEBUGONLY);

    b.add_u64_counter(Stats::l_copy_ok, "copy_ok", "Number of objects copied");
    b.add_u64_counter(Stats::l_copy_enoent, "copy_enoent", "Number of objects failed to copy due to ENOENT");
    b.add_u64_counter(Stats::l_copy_err, "copy_err", "Number of objects failed to copy due to other errors");
    b.add_u64_avg(Stats::l_copy_bytes_transferred, "copy_bytes_transferred", "Number of bytes transferred for object copy");
    b.add_u64_counter(Stats::l_list_ok, "list_ok", "Number of successful list bucket operations");
    b.add_u64_counter(Stats::l_list_err, "list_err", "Number of failed list bucket operations");
    b.add_time_avg(l_list_latency, "list_latency", "List bucket operation latency");

    logger = { b.create_perf_counters(), cct };
    cct->get_perfcounters_collection()->add(logger.get());
  }
  ~Stats() {}

  void dump(Formatter *f) const {
    if (logger) {
      logger->dump_formatted(f, false);
    }
  }

  void print() const {
    JSONFormatter formatter(true);
    formatter.open_object_section("stats");
    dump(&formatter);
    formatter.close_section();
    formatter.flush(cout);
  }

  PerfCountersRef& get_logger() {
    return logger;
  }

  void count_copy(int r, uint64_t bytes_transferred) {
    if (r >= 0) {
      logger->inc(Stats::l_copy_ok);
      logger->inc(Stats::l_copy_bytes_transferred, bytes_transferred);
    } else if (r == -ENOENT) {
      logger->inc(Stats::l_copy_enoent);
    } else {
      logger->inc(Stats::l_copy_err);
    }
  }

  void count_list(int r, utime_t latency) {
    if (r >= 0) {
      logger->inc(Stats::l_list_ok);
    } else {
      logger->inc(Stats::l_list_err);
    }
    logger->tinc(Stats::l_list_latency, latency);
  }
};

class CopyObjTask {
private:
  RGWRados *store = nullptr;
  Stats &stats;
  RGWBucketInfo &dest_bucket_info;
  const rgw_bucket &dest_bucket;
  rgw_bucket src_bucket;
  string obj_key;

public:
  CopyObjTask(RGWRados *_store,
              Stats &_stats,
              RGWBucketInfo &_dest_bucket_info,
              const rgw_bucket &_dest_bucket,
              const rgw_bucket &_src_bucket,
              string _obj_key)
    : store(_store),
      stats(_stats),
      dest_bucket_info(_dest_bucket_info),
      dest_bucket(_dest_bucket),
      src_bucket(_src_bucket),
      obj_key(std::move(_obj_key)) { }

  int run();
};

template <class T>
class Runner {
public:
  explicit Runner(size_t threads,
                  size_t buf_max_len = RUNNER_BUFFER_SIZE)
    : threads_(threads),
      buf_max_len_(buf_max_len) {
    for (auto &t : threads_) {
      t = std::thread(&Runner::handler, this);
    }
  }

  Runner(const Runner& rhs) = delete;
  Runner& operator=(const Runner& rhs) = delete;
  Runner(Runner&& rhs) = delete;
  Runner& operator=(Runner&& rhs) = delete;

  ~Runner() {
    done();
  }

  void submit(T&& task) {
    std::unique_lock<std::mutex> lock(mtx_);
    full_cond_.wait(lock, [this] {
      return tasks_.size() < buf_max_len_;
    });
    tasks_.emplace(std::forward<T>(task));
    fill_cond_.notify_one();
  }

  void done() {
    std::unique_lock<std::mutex> lock(mtx_);
    done_ = true;
    fill_cond_.notify_all();
    lock.unlock();

    for (auto &t : threads_) {
      if (t.joinable()) {
        t.join();
      }
    }
  }

  int status() {
    return status_;
  }

private:
  std::mutex mtx_;
  std::condition_variable fill_cond_;
  std::condition_variable full_cond_;
  std::queue<T> tasks_;
  std::vector<std::thread> threads_;
  size_t buf_max_len_;

  volatile bool done_ = false;
  volatile int status_ = 0;

  void handler() {
    std::unique_lock<std::mutex> lock(mtx_);
    while (true) {
      fill_cond_.wait(lock, [this] {
        return tasks_.size() || done_;
      });

      if (!tasks_.empty()) {
        auto task = std::move(tasks_.front());
        tasks_.pop();
        full_cond_.notify_one();
        lock.unlock();        
        
        int r = task.run();

        lock.lock();
        if (r) {
          status_ = r;
        }
      }
      else if (done_) {
        break;
      }
    }
  }
};

int copy_remote_bucket(RGWRados *store,
                       RGWBucketInfo &dest_bucket_info,
                       const rgw_bucket &dest_bucket,
                       const string &tenant,
                       const string &bucket_name,
                       const string &object_prefix,
                       const list<string> &endpoints,
                       const RGWAccessKey &key);

} // namespace bucket_copy

#endif /*RGW_ADMIN_COPY_H */
