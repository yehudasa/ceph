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

#define COPY_OBJECT_ATTEMPTS            5
#define COPY_OBJECT_RETRY_SLEEP_SECONDS 5
#define LIST_BUCKET_ATTEMPTS            5
#define LIST_BUCKET_RETRY_SLEEP_SECONDS 5

#define RUNNER_BUFFER_SIZE 1000

namespace bucket_copy {

class S3ListBucketEntry
{
public:
  struct Owner
  {
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

class S3ListBucketResp
{
public:
  struct CommonPrefix
  {
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

class BucketObjLister
{
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
  int fetch_next(unique_ptr<S3ListBucketResp> &resp, uint64_t max_keys);
};

class Stats
{
private:
  std::unique_ptr<PerfCounters> logger;
  std::mutex mtx;

public:
  enum {
    l_first = 101010,

    l_copy_ok,
    l_copy_err,

    l_bytes_transferred,

    l_last,
  };

  Stats(const Stats &rhs) = delete; // no copy
  Stats& operator=(const Stats &rhs) = delete; // no assignment

  Stats();
  ~Stats();

  void dump(Formatter *f) const {
    if (logger) {
      logger->dump_formatted(f, false);
    }
  }

  void count(int r, uint64_t bytes_transferred);
  void reset();
};

class CopyObjTask {
private:
  RGWRados *store = nullptr;
  Stats *stats = nullptr;

  RGWBucketInfo &dest_bucket_info;
  const rgw_bucket &dest_bucket;
  rgw_bucket src_bucket;
  string obj_key;

public:
  CopyObjTask(RGWRados *_store,
              Stats *_stats,
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
    {
      std::unique_lock<std::mutex> lock(mtx_);
      full_cond_.wait(lock, [this] {
        return tasks_.size() < buf_max_len_;
      });
      tasks_.emplace(std::forward<T>(task));
    }
    cond_.notify_one();
  }

  void done() {
    std::unique_lock<std::mutex> lock(mtx_);
    done_ = true;
    lock.unlock();
    cond_.notify_all();

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
  std::condition_variable cond_;
  std::queue<T> tasks_;
  std::condition_variable full_cond_;
  std::vector<std::thread> threads_;
  size_t buf_max_len_;

  volatile bool done_ = false;
  volatile int status_ = 0;

  void handler() {
    std::unique_lock<std::mutex> lock(mtx_);
    while (true) {
      cond_.wait(lock, [this] {
        return tasks_.size() || done_;
      });

      if (!tasks_.empty()) {
        auto task = std::move(tasks_.front());
        tasks_.pop();
        lock.unlock();
        full_cond_.notify_one();

        // If error has occurred, record the error status and discard the
        // remaining tasks.
        if (!status_) {
          int r = task.run();
          if (r) {
            status_ = r;
          }
        }

        lock.lock();
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
