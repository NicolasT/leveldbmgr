#!/bin/bash -xu

LEVELDBMGR=$1
DB=$2

test -d $DB && echo "DB exists" && exit 1

$LEVELDBMGR -p $DB get foo && exit 1

$LEVELDBMGR -p $DB create || exit 1
$LEVELDBMGR -p $DB get foo && exit 1

$LEVELDBMGR -p $DB set foo bar || exit 1
$LEVELDBMGR -p $DB get foo || exit 1
$LEVELDBMGR -p $DB delete foo || exit 1
$LEVELDBMGR -p $DB get foo && exit 1

pushd $DB
echo -n bar | $LEVELDBMGR set foo || exit 1
echo -n foo | $LEVELDBMGR get || exit 1
$LEVELDBMGR get -n foo || exit 1
echo -n foo | $LEVELDBMGR delete || exit 1
popd

$LEVELDBMGR -v -p $DB get -n foo && exit 1

echo "All done"
