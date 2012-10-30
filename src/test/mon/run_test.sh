#!/bin/bash
# vim: ts=8 sw=2 smarttab
#
# $0.sh - run mon workload generator
set -e

crush_map_fn=test.crush.map
create_crush=0
clobber_crush=0
new_cluster=0
do_run=0
num_osds=0

bin_test=./test_mon_osd_workloadgen

usage() {
  echo "usage: $1 [options..] <num-osds>"
  echo 
  echo "options:"
  echo "  -n, --new-cluster    Create a new cluster from scratch with 'vstart.sh'"
  echo "  -r, --run            Run the test"
  echo "  --create-crush       Create new CRUSH map"
  echo "  --clobber-crush      Clobber an existing CRUSH map previously generated"
  echo
}

while [[ $# -gt 0 ]];
do
  case "$1" in
    --create-crush)
      create_crush=1
      shift
      ;;
    --clobber-crush)
      clobber_crush=1
      shift
      ;;
    -n | --new-cluster)
      new_cluster=1
      shift
      ;;
    -r | --run)
      do_run=1
      shift
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "$1: unknown option" >&2
      usage $0
      exit 1
      ;;
    *)
      num_osds=$1
      shift
      break
      ;;
  esac
done

if [[ ! -e $crush_map_fn ]]; then
  echo "** Crush map for testing with stubs does not exist."
  if [[ $create_crush -ne 1 ]]; then
    echo "** Specify '--create-crush' if you want us to create a"
    echo "   crush map of our own."
    exit 1
  fi
  echo "** Creating a crush map of our own."
else
  if [[ $create_crush -eq 1 ]]; then
    echo "** Crush map for testing with stubs already exists."
    if [[ $clobber_crush -ne 1 ]]; then
      echo "** If you want to recreate it, you must let us know"
      echo "   by specifying '--clobber-crush' alongside with"
      echo "   '--create-crush'"
      exit 1
    else
      echo "** Clobbering existing crush map file used for tests"
    fi
  fi
fi

if [[ $num_osds -le 0 ]]; then
  echo "** ERROR: must specify a number of OSDs greater than 0."
  usage $0 ;
  exit 1
fi

if [[ $new_cluster -eq 1 ]]; then
  echo "** Creating a new cluster using 'vstart.sh'"
  ./init-ceph stop
  rm -fr dev/ out/
  mkdir dev
  ./vstart.sh -n -l -d mon
fi

./init-ceph start mon

if [[ $create_crush -eq 1 ]]; then
  tmp_crush_fn="/tmp/tmp.crush-`uuidgen`"
  echo "** generate crush map in $tmp_crush_fn"
  cat << EOF > $tmp_crush_fn
  # begin crush map

  # devices

  # types
  type 0 osd
  type 1 host
  type 2 rack
  type 3 row
  type 4 room
  type 5 datacenter
  type 6 root

  # buckets
  root default {
  id -1   # do not change unnecessarily
  # weight 0.000
  alg straw
  hash 0  # rjenkins1
}

root testing {
id -2
alg straw
hash 0  # rjenkins1
}

# rules
rule data {
ruleset 0
type replicated
min_size 1
max_size 10
step take default
step choose firstn 0 type osd
step emit
}
rule metadata {
ruleset 1
type replicated
min_size 1
max_size 10
step take default
step choose firstn 0 type osd
step emit
}
rule rbd {
ruleset 2
type replicated
min_size 1
max_size 10
step take default
step choose firstn 0 type osd
step emit
}

rule testingdata {
ruleset 0
type replicated
min_size 1
max_size 10
step take testing
step choose firstn 0 type osd
step emit
}

rule testingmetadata {
ruleset 1
type replicated
min_size 1
max_size 10
step take testing
step choose firstn 0 type osd
step emit
}

rule testingrbd {
ruleset 2
type replicated
min_size 1
max_size 10
step take testing
step choose firstn 0 type osd
step emit
}
# end crush map
EOF
echo "** compile new crush map version"
./crushtool -c $tmp_crush_fn -o $crush_map_fn
echo "** set new crush map"
./ceph osd setcrushmap -i $crush_map_fn
rm $tmp_crush_fn
fi

./ceph auth get-or-create-key osd.admin mon 'allow rwx' osd 'allow *'
./ceph auth export osd.admin | grep -v "export auth" >> keyring

osd_ids=""

for osd in `seq 1 $num_osds`; do
  id=`./ceph osd create`
  osd_ids="$osd_ids $id"
  echo "osd.$id"
  ./ceph osd crush set $id osd.$id 1.0 host=testhost rack=testrack root=testing
done

if [[ $do_run -eq 1 ]]; then
  if [[ $osd_ids == "" ]]; then
    echo "** ERROR: unable to obtain osd ids as added to the mon cluster!"
    echo "** SOMETHING WENT AWFULLY WRONG!"
    exit 1
  fi
  args=""
  for id in $osd_ids; do
    args="$args --stub-id $id"
  done

  echo "** Running test"
  $bin_test -c ./ceph.conf --keyring keyring $args
fi
