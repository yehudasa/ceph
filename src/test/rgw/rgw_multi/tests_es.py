import json
import urllib
import logging

import boto
import boto.s3.connection

from nose.tools import eq_ as eq

from rgw_multi.multisite import *
from rgw_multi.tests import *
from rgw_multi.zone_es import *

log = logging.getLogger(__name__)


def check_es_configured():
    realm = get_realm()
    zonegroup = realm.master_zonegroup()

    es_zones = zonegroup.zones_by_type.get("elasticsearch")
    if not es_zones:
        raise SkipTest("Requires at least one ES zone")

def is_es_zone(zone_conn):
    if not zone_conn:
        return False

    return zone_conn.zone.tier_type() == "elasticsearch"

def verify_search(bucket_name, src_keys, result_keys, f):
    check_keys = []
    for k in src_keys:
        if bucket_name:
            if bucket_name != k.bucket.name:
                continue
        if f(k):
            check_keys.append(k)
    check_keys.sort(key = lambda l: (l.bucket.name, l.name, l.version_id))

    log.debug('check keys:' + dump_json(check_keys))
    log.debug('result keys:' + dump_json(result_keys))

    for k1, k2 in zip_longest(check_keys, result_keys):
        assert k1
        assert k2
        check_object_eq(k1, k2)

def do_check_mdsearch(conn, bucket, src_keys, req_str, src_filter):
    if bucket:
        bucket_name = bucket.name
    else:
        bucket_name = ''
    req = MDSearch(conn, bucket_name, req_str)
    result_keys = req.search(sort_key = lambda k: (k.bucket.name, k.name, k.version_id))
    verify_search(bucket_name, src_keys, result_keys, src_filter)

def test_es_object_search():
    check_es_configured()

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns, buckets_per_zone = 2)

    min_size = 10
    content = 'a' * min_size

    src_keys = []

    owner = None

    max_keys = 5

    etags = []
    names = []

    obj_prefix=''.join(random.choice(string.ascii_lowercase) for _ in range(6))

    # don't wait for meta sync just yet
    for zone, bucket in zone_bucket:
        for count in xrange(0, max_keys):
            objname = obj_prefix + str(count)
            k = new_key(zone, bucket.name, objname)
            k.set_contents_from_string(content + 'x' * count)

            if not owner:
                for list_key in bucket.list_versions():
                    owner = list_key.owner
                    break

            k = bucket.get_key(k.name, version_id = k.version_id)
            k.owner = owner # owner is not set when doing get_key()

            src_keys.append(k)
            names.append(k.name)

    max_size = min_size + count - 1

    zonegroup_meta_checkpoint(zonegroup)

    targets = []
    for target_conn in zonegroup_conns.zones:
        if not is_es_zone(target_conn):
            continue

        targets.append(target_conn)

    buckets = []
    # make sure all targets are synced
    for source_conn, bucket in zone_bucket:
        buckets.append(bucket)
        for target_conn in targets:
            zone_bucket_checkpoint(target_conn.zone, source_conn.zone, bucket.name)

    for target_conn in targets:

        # bucket checks
        for bucket in buckets:
            # check name
            do_check_mdsearch(target_conn.conn, None, src_keys , 'bucket == ' + bucket.name, lambda k: k.bucket.name == bucket.name)
            do_check_mdsearch(target_conn.conn, bucket, src_keys , 'bucket == ' + bucket.name, lambda k: k.bucket.name == bucket.name)

        # check on all buckets
        for key in src_keys:
            # limiting to checking specific key name, otherwise could get results from
            # other runs / tests
            do_check_mdsearch(target_conn.conn, None, src_keys , 'name == ' + key.name, lambda k: k.name == key.name)

        # check on specific bucket
        for bucket in buckets:
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name < ' + key.name, lambda k: k.name < key.name)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name <= ' + key.name, lambda k: k.name <= key.name)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name == ' + key.name, lambda k: k.name == key.name)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name >= ' + key.name, lambda k: k.name >= key.name)
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name > ' + key.name, lambda k: k.name > key.name)

            do_check_mdsearch(target_conn.conn, bucket, src_keys , 'name == ' + names[0] + ' or name >= ' + names[2],
                              lambda k: k.name == names[0] or k.name >= names[2])

            # check etag
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'etag < ' + key.etag[1:-1], lambda k: k.etag < key.etag)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'etag == ' + key.etag[1:-1], lambda k: k.etag == key.etag)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'etag > ' + key.etag[1:-1], lambda k: k.etag > key.etag)

            # check size
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'size < ' + str(key.size), lambda k: k.size < key.size)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'size <= ' + str(key.size), lambda k: k.size <= key.size)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'size == ' + str(key.size), lambda k: k.size == key.size)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'size >= ' + str(key.size), lambda k: k.size >= key.size)
            for key in src_keys:
                do_check_mdsearch(target_conn.conn, bucket, src_keys , 'size > ' + str(key.size), lambda k: k.size > key.size)

