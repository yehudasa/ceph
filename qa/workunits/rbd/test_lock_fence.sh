#!/bin/bash  -x
# can't use -e because of background process

IMAGE=rbdrw-image
LOCKID=rbdrw

RBDRW=test/rbdrw.py

if [ ! -e $RBDRW ] ; then
	# wget to same dir as ceph.conf
	RBDRW=${CEPH_CONF%ceph.conf}rbdrw.py
	wget -q -O- https://raw.github.com/ceph/ceph/$CEPH_REF/src/test/rbdrw.py > $RBDRW
fi

rbd create $IMAGE --size 10 --format 2

# rbdrw loops doing I/O to $IMAGE after locking with lockid $LOCKID
python $RBDRW $IMAGE $LOCKID &
iochild=$!

# give client time to lock and start reading/writing
sleep 5

clientaddr=$(rbd lock list $IMAGE | tail -1 | awk '{print $NF;}')
clientid=$(rbd lock list $IMAGE | tail -1 | awk '{print $1;}')
echo "clientaddr: $clientaddr"
echo "clientid: $clientid"

ceph osd blacklist add $clientaddr

wait $iochild
rbdrw_exitcode=$?
if [ $rbdrw_exitcode != 108 ]
then
	echo "wrong exitcode from rbdrw: $rbdrw_exitcode"
	exit 1
else
	echo "rbdrw stopped with ESHUTDOWN"
fi

set -x
ceph osd blacklist rm $clientaddr
rbd lock remove $IMAGE $LOCKID $clientid
# rbdrw will have exited with an existing watch, so, until #3527 is fixed,
# hang out until the watch expires
sleep 30
rbd rm $IMAGE
