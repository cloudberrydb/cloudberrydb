#!/bin/ksh
disks=`format </dev/null | grep c.t.d | nawk '{print $2}'`

getspeed1()
{
   ptime dd if=/dev/rdsk/${1}s0 of=/dev/null bs=64k count=1024 2>&1 |
       nawk '$1 == "real" { printf("%.0f\n", 67.108864 / $2) }'
}

getspeed()
{
   for iter in 1 2 3
   do
       getspeed1 $1
   done | sort -n | tail -2 | head -1
}

for disk in $disks
do
   echo $disk `getspeed $disk` MB/sec
done

