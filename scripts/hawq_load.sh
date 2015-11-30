for i in {2..4}; do
  rsync -azv /data/test.tgz poc$i:/data/ &
done

for i in {1..4}; do
  ssh poc$i "mkdir -p /data/poc; cd /data/poc; tar xzf ../test.tgz &"
done

for i in {1..4}; do
  ssh poc$i "sed -i.bak 's/;//g' test/*.csv"
  # ssh poc$i "for i in {0..79}; do
  #  start=$(($i * $step + 1))
  #  end=$(($start + $step -1 ))
  # mv /data/poc/test/{$start..$end}.csv /data/poc/test$(($i+1))
  #done

  ssh poc$i "mkdir /data/poc/test{1..80}; \
            mv /data/poc/test/{1..50}.csv  /data/poc/test1; \
            mv /data/poc/test/{51..100}.csv  /data/poc/test2; \
            mv /data/poc/test/{101..150}.csv  /data/poc/test3; \
            mv /data/poc/test/{151..200}.csv  /data/poc/test4; \
            mv /data/poc/test/{201..250}.csv  /data/poc/test5; \
            mv /data/poc/test/{251..300}.csv  /data/poc/test6; \
            mv /data/poc/test/{301..350}.csv  /data/poc/test7; \
            mv /data/poc/test/{351..400}.csv  /data/poc/test8; \
            mv /data/poc/test/{401..450}.csv  /data/poc/test9; \
            mv /data/poc/test/{451..500}.csv  /data/poc/test10; \
            mv /data/poc/test/{501..550}.csv  /data/poc/test11; \
            mv /data/poc/test/{551..600}.csv  /data/poc/test12; \
            mv /data/poc/test/{601..650}.csv  /data/poc/test13; \
            mv /data/poc/test/{651..700}.csv  /data/poc/test14; \
            mv /data/poc/test/{701..750}.csv  /data/poc/test15; \
            mv /data/poc/test/{751..800}.csv  /data/poc/test16; \
            mv /data/poc/test/{801..850}.csv  /data/poc/test17; \
            mv /data/poc/test/{851..900}.csv  /data/poc/test18; \
            mv /data/poc/test/{901..950}.csv  /data/poc/test19; \
            mv /data/poc/test/{951..1000}.csv  /data/poc/test20; \
            mv /data/poc/test/{1001..1050}.csv  /data/poc/test21; \
            mv /data/poc/test/{1051..1100}.csv  /data/poc/test22; \
            mv /data/poc/test/{1101..1150}.csv  /data/poc/test23; \
            mv /data/poc/test/{1151..1200}.csv  /data/poc/test24; \
            mv /data/poc/test/{1201..1250}.csv  /data/poc/test25; \
            mv /data/poc/test/{1251..1300}.csv  /data/poc/test26; \
            mv /data/poc/test/{1301..1350}.csv  /data/poc/test27; \
            mv /data/poc/test/{1351..1400}.csv  /data/poc/test28; \
            mv /data/poc/test/{1401..1450}.csv  /data/poc/test29; \
            mv /data/poc/test/{1451..1500}.csv  /data/poc/test30; \
            mv /data/poc/test/{1501..1550}.csv  /data/poc/test31; \
            mv /data/poc/test/{1551..1600}.csv  /data/poc/test32; \
            mv /data/poc/test/{1601..1650}.csv  /data/poc/test33; \
            mv /data/poc/test/{1651..1700}.csv  /data/poc/test34; \
            mv /data/poc/test/{1701..1750}.csv  /data/poc/test35; \
            mv /data/poc/test/{1751..1800}.csv  /data/poc/test36; \
            mv /data/poc/test/{1801..1850}.csv  /data/poc/test37; \
            mv /data/poc/test/{1851..1900}.csv  /data/poc/test38; \
            mv /data/poc/test/{1901..1950}.csv  /data/poc/test39; \
            mv /data/poc/test/{1951..2000}.csv  /data/poc/test40; \
            mv /data/poc/test/{2001..2050}.csv  /data/poc/test41; \
            mv /data/poc/test/{2051..2100}.csv  /data/poc/test42; \
            mv /data/poc/test/{2101..2150}.csv  /data/poc/test43; \
            mv /data/poc/test/{2151..2200}.csv  /data/poc/test44; \
            mv /data/poc/test/{2201..2250}.csv  /data/poc/test45; \
            mv /data/poc/test/{2251..2300}.csv  /data/poc/test46; \
            mv /data/poc/test/{2301..2350}.csv  /data/poc/test47; \
            mv /data/poc/test/{2351..2400}.csv  /data/poc/test48; \
            mv /data/poc/test/{2401..2450}.csv  /data/poc/test49; \
            mv /data/poc/test/{2451..2500}.csv  /data/poc/test50; \
            mv /data/poc/test/{2501..2550}.csv  /data/poc/test51; \
            mv /data/poc/test/{2551..2600}.csv  /data/poc/test52; \
            mv /data/poc/test/{2601..2650}.csv  /data/poc/test53; \
            mv /data/poc/test/{2651..2700}.csv  /data/poc/test54; \
            mv /data/poc/test/{2701..2750}.csv  /data/poc/test55; \
            mv /data/poc/test/{2751..2800}.csv  /data/poc/test56; \
            mv /data/poc/test/{2801..2850}.csv  /data/poc/test57; \
            mv /data/poc/test/{2851..2900}.csv  /data/poc/test58; \
            mv /data/poc/test/{2901..2950}.csv  /data/poc/test59; \
            mv /data/poc/test/{2951..3000}.csv  /data/poc/test60; \
            mv /data/poc/test/{3001..3050}.csv  /data/poc/test61; \
            mv /data/poc/test/{3051..3100}.csv  /data/poc/test62; \
            mv /data/poc/test/{3101..3150}.csv  /data/poc/test63; \
            mv /data/poc/test/{3151..3200}.csv  /data/poc/test64; \
            mv /data/poc/test/{3201..3250}.csv  /data/poc/test65; \
            mv /data/poc/test/{3251..3300}.csv  /data/poc/test66; \
            mv /data/poc/test/{3301..3350}.csv  /data/poc/test67; \
            mv /data/poc/test/{3351..3400}.csv  /data/poc/test68; \
            mv /data/poc/test/{3401..3450}.csv  /data/poc/test69; \
            mv /data/poc/test/{3451..3500}.csv  /data/poc/test70; \
            mv /data/poc/test/{3501..3550}.csv  /data/poc/test71; \
            mv /data/poc/test/{3551..3600}.csv  /data/poc/test72; \
            mv /data/poc/test/{3601..3650}.csv  /data/poc/test73; \
            mv /data/poc/test/{3651..3700}.csv  /data/poc/test74; \
            mv /data/poc/test/{3701..3750}.csv  /data/poc/test75; \
            mv /data/poc/test/{3751..3800}.csv  /data/poc/test76; \
            mv /data/poc/test/{3801..3850}.csv  /data/poc/test77; \
            mv /data/poc/test/{3851..3900}.csv  /data/poc/test78; \
            mv /data/poc/test/{3901..3950}.csv  /data/poc/test79; \
            mv /data/poc/test/{3951..4000}.csv  /data/poc/test80; "
done

# for i in {2..6}; do rsync -azv /data/poc/test* poc$i:/data/poc/; done

for i in {1..4}; do
  ssh nd$i "chmod -R 777 /data/poc; "
done

su - gpadmin
for i in {1..4}; do
  ssh nd$i "source /usr/local/hawq/greenplum_path.sh; export GPLOAD_HOME=/data/poc/; gpfdist -d $GPLOAD_HOME -p 8081 -l /data/poc/gpfdist.log &"
done

scp my_load.yml.template poc6:/data/poc/my_load.yml.template
ssh poc6
cd /data/poc
for i in {1..4}; do
  sed 's/FILE_LOCATION/\/test'$i'\/*.csv/g' my_load.yml.template > my_load$i.yml
done
sed 's/FILE_LOCATION/\/test\/*.csv/g' my_load.yml.template > my_load5.yml

screen -R upload
su - gpadmin
for i in {1..5}; do
  echo "Loading my_load$i.yml..."
  gpload -f /data/poc/my_load$i.yml -l /data/poc/my_load$i.log
done
