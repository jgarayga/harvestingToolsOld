#!/bin/zsh

LOCK=upload.lock
if [ -e $LOCK ]; then
 echo An update is running with pid $(cat $LOCK)
 echo Remove the lock file $LOCK if the job crashed
 exit
else
 echo $$ > $LOCK
fi

date=`date`


echo
echo "============================"
echo "running Upload at" $date
echo
echo "certificate:"
source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env_3_2.sh
X509_USER_PROXY=$HOME/x509up
echo
echo "sourcing EOS"
source /afs/cern.ch/project/eos/installation/client/etc/setup.sh
#
# note: before installing cronjob, run voms-proxy-init -hours=100000 with X509_USER_PROXY set to sth not on /tmp
#
voms-proxy-info

echo 
echo "----------------------------"
#for i in "data/dqmoffline" "mc/mc"
for i in "mc/mc"
#for i in "data/dqmoffline" 
do
echo 
echo "==== Upload of new files in" $i
#Castordir=/castor/cern.ch/cms/store/temp/dqm/offline/harvesting_output/$i
Castordir=/eos/cms/store/group/comm_dqm/harvesting_output/$i

# #for CMSSW in `nsls $Castordir | grep -v 3_6_ | grep -v 3_7_ | grep -v 3_8 | grep -v 3_9 | grep -v 3_10 | grep -v 3_11 | grep -v 4_1 | grep -v hlt1`;
# #for CMSSW in 5_2_5 5_3_3_patch3 5_3_2_patch5 5_3_2_patch4
#for CMSSW in `eos ls $Castordir`;
#for CMSSW in 5_3_7_patch5
#for CMSSW in 5_3_8_patch3
#for CMSSW in 5_3_7_alcapatch1
#for CMSSW in 5_3_6_patch1
#for CMSSW in 6_0_1_PostLS1v2_patch3 5_3_9_patch1 5_3_9_patch3 5_2_3_patch2 5_2_3_patch4 5_3_7 5_3_7_patch4 5_3_3_patch1 5_3_6_patch1 5_3_6 5_3_8_patch3 4_4_2_patch8
#for CMSSW in 5_3_9_patch1 5_3_7 5_3_8_patch3 4_4_2_patch8
#for CMSSW in 5_3_10 5_3_10_patch1 5_3_10_patch2 4_4_2_patch8 5_3_9_patch3 5_3_9_patch1 5_3_10
for CMSSW in 5_3_9_patch3

do
CMSSWdir=$Castordir/$CMSSW
##New command for EOS
for dataset in `eos ls $CMSSWdir`;
#for dataset in `eos ls $CMSSWdir | grep "Neutrino"`;
#for dataset in `eos ls $CMSSWdir | grep "Neutrino"`;
#for dataset in `eos ls $CMSSWdir |grep "MinimumBias__Run2012C-26Nov2012-v2"`
#for dataset in HcalNZS__Run2012B-laserHCALskim_534p2_24Oct2012-v1__DQM 
# ##Old command for CASTOR
# # for dataset in `nsls $CMSSWdir`;
# #for dataset in `nsls $CMSSWdir | grep "TprimeTprimeToTHTH_M-500"`;
# #for dataset in SingleMu__Run2012B-13Jul2012-v1__DQM MinimumBias__Run2012B-13Jul2012-v1__DQM JetHT__Run2012B-13Jul2012-v1__DQM
do
datasetdir=$CMSSWdir/$dataset
echo $CMSSW ":" $dataset
# for run in `nsls $datasetdir`;
for run in `eos ls $datasetdir`;
do
rundir=$datasetdir/$run
# for nevents in `nsls $rundir`;
for nevents in `eos ls $rundir`;
do
neventsdir=$rundir/$nevents
# for section in `nsls $neventsdir`;
for section in `eos ls $neventsdir`;
do
sectiondir=$neventsdir/$section
# for file in `nsls $sectiondir`;
for file in `eos ls $sectiondir`;
do
echo $file | grep -q $Castordir
if [ $? -eq 0 ]
then
rootfile=$file
else
rootfile=$sectiondir/$file
fi
#size=`rfstat $rootfile | grep Size | perl -pe 's/Size \(bytes\)    \: //'`
size=`eos ls -l $rootfile | awk '{print $5}'`

if [ $size -ne 0 ];
then
## Definition of ffile changed due to upgrade to crab 2_7_5
##ffile=DQM_V0$(echo $rootfile | perl -pe 's/.*\/DQM_V0// ; s/_1.root/.root/ ; s/_2.root/.root/; s/_3.root/.root/ ; s/_4.root/.root/ ; s/_5.root/.root/ ; s/_1.root/.root/')
ffile=DQM_V0$(echo $rootfile | perl -pe 's/.*\/DQM_V0// ; s/__DQM.*/__DQM.root/')
file_test=`grep -c "$ffile" upload_bookkeeping.txt`
if [ $file_test -eq 0 ];
then
# xrdcp "root://castorcms/"$rootfile"?svcClass=t1transfer" /tmp/$ffile
xrdcp "root://eoscms.cern.ch/"$rootfile"?svcClass=t1transfer" /tmp/$ffile

if [ `echo $?` != 0 ];
then
break
else
echo $i | grep -q data
if [ $? -eq 0 ] ; then
    echo $CMSSW | grep -q hlt
    if [ $? -eq 0 ] ; then 
	server="https://cmsweb.cern.ch/dqm/relval"
    fi
    echo $CMSSW | grep -q Commisioning
    if [ $? -eq 0 ] ; then
	server="https://cmsweb.cern.ch/dqm/relval"
    else
	server="https://cmsweb.cern.ch/dqm/offline"
    fi
else
    server="https://cmsweb.cern.ch/dqm/relval"
fi
# server="https://cmsweb.cern.ch/dqm/relval"
../VisMonitoring/DQMServer/scripts/visDQMUpload $server /tmp/$ffile
if [ `echo $?` != 0 ];
then
echo "------------------------------------------------------------------------------------------------"
echo /tmp/$ffile could not be uploaded to $server
echo "------------------------------------------------------------------------------------------------"
rm /tmp/$ffile
break
else
echo "------------------------------------------------------------------------------------------------"
echo $ffile is uploaded to $server
echo "------------------------------------------------------------------------------------------------"
echo $ffile >> upload_bookkeeping.txt


dataset_test=`grep -c "$dataset" dataset_bookkeeping.txt`
if [ $dataset_test -eq 0 ];
then
echo $dataset >> dataset_bookkeeping.txt
echo $dataset
fi

fi
rm /tmp/$ffile
fi
fi
fi
done
done
done
done
done
done
done

#cp upload_bookkeeping.txt /afs/cern.ch/user/n/npietsch/harvesting_backup/upload_bookkeeping_backup.txt

echo "done upload"
echo
echo "============================"
echo "... done Upload of" $date 
echo "                at" `date`
echo

rm -f $LOCK
