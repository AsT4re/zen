#!/bin/bash

randLat()
{
  echo "scale=7; ($RANDOM*170/32767)-85" | bc | awk '{printf "%.10g", $1}'
}

randLong()
{
  echo "scale=7; ($RANDOM*360/32767)-180" | bc | awk '{printf "%.11g", $1}'
}

for (( p=1; p<=$1; p++ ))
do
  begEndLat=$(randLat)
  begEndLong=$(randLong)

  echo -n "$begEndLong $begEndLat" >> newfile

  NBCOORDS="$(( ( RANDOM % 5 )  + 1 ))"

  echo $NBCOORDS

  for (( c=1; c<=$NBCOORDS; c++ ))
  do
    echo -n " $(randLong) $(randLat)" >> newfile
  done

  echo " $begEndLong $begEndLat" >> newfile
done
