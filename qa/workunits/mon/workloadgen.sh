#!/bin/bash
# vim: ts=8 sw=2 smarttab
#
# $0.sh - run mon workload generator

d() {
  [ $DEBUG -eq 1 ] && echo "## DEBUG ## $*"
}

d "Start workunit"

crush_map_fn=test.crush.map
create_crush=0
clobber_crush=0
new_cluster=0
do_run=0
num_osds=0

# Assume the test is in PATH
bin_test=test_mon_osd_workloadgen

num_osds=$1
if [[ $num_osds == "" ]]; then
  num_osds=10
fi

extra=
[ ! -z $TEST_CEPH_CONF ] && extra="-c $TEST_CEPH_CONF"

d "checking osd tree"

crush_testing_root="`ceph $extra osd tree | grep 'root[ \t]\+testing'`"

d "$crush_testing_root"

if [[ "$crush_testing_root" == "" ]]; then
  d "set create_crush"
  create_crush=1
fi

d "generate run_id (create_crush = $create_crush)"

run_id=`uuidgen`

d "run_id = $run_id ; create_crush = $create_crush"

if [[ $create_crush -eq 1 ]]; then
  tmp_crush_fn="/tmp/ceph.$run_id.crush"
  ceph $extra osd getcrushmap -o $tmp_crush_fn
  crushtool -d $tmp_crush_fn -o $tmp_crush_fn.plain
  cat << EOF >> $tmp_crush_fn.plain
root testing {
  id -2
  alg straw
  hash 0  # rjenkins1
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
EOF

  crushtool -c $tmp_crush_fn.plain -o $tmp_crush_fn
  if [[ $? -eq 1 ]]; then
    echo "Error compiling test crush map; probably need newer crushtool"
    echo "NOK"
    exit 1
  fi

  d "created crush"

  ceph $extra osd setcrushmap -i $tmp_crush_fn
fi

keyring="/tmp/ceph.$run_id.keyring"

ceph $extra auth get-or-create-key osd.admin mon 'allow rwx' osd 'allow *'
ceph $extra auth export | grep -v "export" > $keyring

osd_ids=""

for osd in `seq 1 $num_osds`; do
  id=`ceph $extra osd create`
  osd_ids="$osd_ids $id"
  d "osd.$id"
  ceph $extra osd crush set $id osd.$id 1.0 host=testhost rack=testrack root=testing
done

d "osds: $osd_ids"

args=""
f=
l=
for i in $osd_ids; do
  d "i: $i"
  if [[ $args == "" ]]; then
    args="--stub-id $i"
    f=$i
  fi
  if [[ $l != "" ]]; then
    if [[ $i -gt $(($l+1)) ]]; then
      args="$args..$l --stub-id $i"
      f=$i
    fi
  fi
  l=$i
done
if [[ $l -gt $f ]]; then
  args="$args..$l"
fi
  
d "running: $args"

$bin_test $extra --keyring $keyring $args
