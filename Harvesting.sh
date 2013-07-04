#!/bin/zsh

export SCRAM_ARCH=slc5_amd64_gcc434

. /afs/cern.ch/cms/cmsset_default.sh
source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh

source /afs/cern.ch/cms/ccs/wm/scripts/Crab/crab.sh
pushd /afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/harvesting/CMSSW_4_2_8/src
cmsenv
popd

#pushd /afs/cern.ch/cms/sw/${SCRAM_ARCH}/cms/dbs-client/DBS_2_0_9_patch_4-cms/lib
#source setup.sh
#popd

. /afs/cern.ch/cms/caf/setup.sh

export X509_USER_PROXY=$HOME/x509up
# update by typing on the shell voms-proxy-init -voms cms -valid 192:00
# voms-proxy-init --rfc -valid 19:0
voms-proxy-info

LOCK=harvesting.lock
if [ -e $LOCK ]; then
 echo At `date`
 echo An update is running with pid $(cat $LOCK)
 echo Remove the lock file $LOCK if the job crashed
 exit
else
 echo $$ > $LOCK
 echo "lock generated"
fi

basedir=/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/harvesting
bindir=$basedir/bin
outdir=$basedir/bin/test
castordir=/castor/cern.ch/cms/store/temp/dqm/offline/harvesting_output/
cd $bindir
pwd

####
# remove previous stuff
echo "cleaning previous jobs"
rm -fr $basedir/CMSSW*/harvesting_area/*__DQM_*site_* 
rm -fr $basedir/CMSSW*/harvesting_area/crab*
rm -fr $basedir/CMSSW*/harvesting_area/harvesting* 
rm -fr $basedir/CMSSW*/harvesting_area/multicrab*
echo "cleaning done"

#for arch mainrel in "slc5_amd64_gcc434" "CMSSW_4_" "slc5_amd64_gcc462" "CMSSW_5_"
#for arch mainrel in "slc5_amd64_gcc462" "CMSSW_5_"
#for arch mainrel in "slc5_amd64_gcc434" "CMSSW_4_"
#for arch mainrel in "slc5_ia32_gcc434" "CMSSW_3_"
for arch mainrel in "slc5_amd64_gcc462" "CMSSW_5_"

do

export SCRAM_ARCH=$arch
date=`date +%y%m%d%H%M%S`

echo 
echo "=========================================================="
echo Start Harvesting script at `date`
echo "=========================================================="
echo 

outfiledate=DQM_${date}.txt
prevfile=DQM_prev.txt
todofiledate=DQM_${date}.todo
zerobfile=zerob.txt
datafiledate=DQM_${date}.datasets

echo $outfiledate
rm -fr $outfiledate help
touch $outfiledate
rm $todofiledate
touch $todofiledate

####
# run DBS

echo 
echo "###############################################################"
echo Checking DBS for new datasets at `date`
echo
echo

startyear=`date +%y`
startyear=`expr $startyear + 2000`
startmonth=`date +%m`
startdate=$startyear-$startmonth-`date +%d`
i=0
for ((  i = 0 ;  i < 1;  i++  ))
do
enddate=$startdate
startmonth=`expr $startmonth - 1`
if test $startmonth -le 0
then
startyear=`expr $startyear - 1`
startmonth=`expr $startmonth + 12`
fi
startdate=$startyear-$startmonth-`date +%d`
#startdate=2012-11-01
#enddate=2012-10-20
echo $startdate $enddate

dbs search --query="find dataset, release, dataset.tag, datatype, run  where dataset=/*/*/DQM and file.numevents >0 and site=srm-eoscms.cern.ch and dataset.createdate>"$startdate" and dataset.createdate < "$enddate"" and file.status = valid | grep DQM | grep ::All | grep -v preprod | grep -v /CMSSW | grep -v HLT | grep -v HIRun2011 | grep -v Cosmic | grep $mainrel>> $outfiledate

#dbs search --query="find dataset, release, dataset.tag, datatype, run where dataset=/TauPlusX/Run2012A-laserHCALskim_534p2_24Oct2012-v1/DQM and file.numevents >0 and site=srm-eoscms.cern.ch and dataset.createdate>"$startdate" and dataset.createdate < "$enddate"" and file.status = valid | grep DQM | grep ::All | grep -v preprod | grep -v /CMSSW | grep -v HLT | grep -v HIRun2011 | grep -v Cosmic | grep $mainrel>> $outfiledate
#dbs search --query="find dataset, release, dataset.tag, datatype, run  where dataset=/*/*laserHCALskim_534p2_24Oct2012*/DQM and file.numevents >0 and site=srm-eoscms.cern.ch and dataset.createdate>"$startdate" and dataset.createdate < "$enddate"" and file.status = valid | grep DQM | grep ::All | grep -v preprod | grep -v /CMSSW | grep -v HLT | grep -v HIRun2011 | grep -v Cosmic | grep $mainrel>> $outfiledate
#dbs search --query="find dataset, release, dataset.tag, datatype, run  where dataset=/QCD_Pt-15to30_TuneZ2star_8TeV_pythia6/Summer12-PU_S7_START52_V9-v2/DQM and file.numevents >0 and site=srm-eoscms.cern.ch" and file.status = valid | grep DQM | grep ::All | grep -v preprod | grep -v /CMSSW | grep -v HLT | grep -v HIRun2011 | grep -v Cosmic | grep $mainrel>> $outfiledate
#dbs search --query="find dataset, release, dataset.tag, datatype, run  where dataset=/DoubleElectron/Run2012C-24Aug2012-v1/DQM and file.numevents >0 and site=srm-eoscms.cern.ch" and file.status = valid | grep DQM | grep ::All | grep -v preprod | grep -v /CMSSW | grep -v HLT | grep -v HIRun2011 | grep -v Cosmic | grep $mainrel>> $outfiledate
#dbs search --query="find dataset, release, dataset.tag, datatype, run  where dataset=/MinBias_TuneZ2star_8TeV-pythia6/Summer12_DR53X-DEBUG_PU_S10_START53_V7A-v1/DQM and file.numevents >0 and site=srm-eoscms.cern.ch" and file.status = valid | grep DQM | grep ::All | grep -v preprod | grep -v /CMSSW | grep -v HLT | grep -v HIRun2011 | grep $mainrel>> $outfiledate
#dbs search --query="find dataset, release, dataset.tag, datatype, run  where dataset=/*/Fall11-HLTBPh2011_START44_V9B-v*/DQM and file.numevents >0 and site=srm-eoscms.cern.ch and dataset.createdate>"$startdate" and dataset.createdate < "$enddate"" and file.status = valid | grep DQM | grep ::All | grep -v preprod | grep -v /CMSSW | grep -v HIRun2011 | grep -v Cosmics | grep $mainrel>> $outfiledate
done
####
# sort outfile
sort -u $outfiledate > help 
mv help $outfiledate
sort -u  $prevfile > help
mv help $prevfile
diff $outfiledate $prevfile | grep "<" | awk '{print $6" "$2" "$3" "$4" "$5}'  | sort -u > help
echo `date` >> $zerobfile

#python RunList.py -j JSON.txt -d help

rm help2
cat help | awk '{print $2" "$3" "$4" "$5" "$1}' |sort -u > help2
mv help2 help

for dataset rel tag type run in `cat help | awk '{print $1" "$2" "$3" "$4" "$5}'`
do
checkpath=$castordir
#checkpath=/castor/cern.ch/cms/store/temp/dqm/offline/harvesting_output/TEST
if  test $type = "mc"
then
checkpath=$checkpath"mc/mc/"
else
checkpath=$checkpath"data/dqmoffline/"
fi
rellow=`echo $rel| tr '[:upper:]' '[:lower:]'`
checkpath="$checkpath"`echo $rellow|sed 's/cmssw_//g'`
checkpath="$checkpath"/`echo $dataset|sed 's/.\(.*\)/\1/'|sed 's/\//__/g'`
checkpath="$checkpath"/run_"$run"/nevents

#######  fix
size=-1
rootfile=1
for section in `nsls $checkpath`;
do
sectiondir=$checkpath/$section
for file in `nsls $sectiondir`;
do
echo $file | grep -q $castordir
if [ $? -eq 0 ]
then
rootfile=$file
else
rootfile=$sectiondir/$file
fi
### end fix

### how it was before the fix
#checkpath="$checkpath"/`echo $dataset|sed 's/.\(.*\)/\1/'|sed 's/\//__/g'`_
#checkpath="$checkpath"`printf %.9d run`_site_01
#size=-1
#rootfile=1
#for file in `nsls $checkpath`
#do
#rootfile=$checkpath/$file
### end how it was before the fix

size=`rfstat $rootfile | grep Size | perl -pe 's/Size \(bytes\)    \: //'`
if test $size -gt 0
then
echo $dataset   $rel   $tag   $type   $run >> $prevfile
else
echo $dataset   $rel   $tag   $type   $run >> $zerobfile
fi
done
### fix
done
### end fix
if [[ "$rootfile" == "1" ]]
    then
    echo $checkpath "deleted"
    nsrm -rf $checkpath
    echo $checkpath "deleted"

fi
if test $size -lt 0
then
#nsrm -rf $checkpath
echo $dataset $rel $tag $type >> $todofiledate
fi
done
rm help
sort -u $prevfile > help
mv help $prevfile
sort -u $todofiledate > help
mv help $todofiledate

###
# if non-zero then produce todo files and all the rest
if [ `wc -l $todofiledate | awk '{print $1}'` -ne 0 ] ; then
#cp $todofiledate $outdir/$todofiledate

#rm -fr DQM_${date}.$i
for i in `awk '{print $2}' $todofiledate | sort -u` ; do
echo $i
more $todofiledate | grep $i" " | grep "mc" | awk '{print $1" "$2" "$3" "$4}' > DQM_${date}.mc.$i
more $todofiledate | grep $i" " | grep "data" | awk '{print $1" "$2" "$3" "$4}' > DQM_${date}.data.$i
done

###
# remove zero-length files
for file in `ls DQM_${date}.mc.CMSSW* DQM_${date}.data.CMSSW*` ; do
count=`wc -l $file | awk '{print $1}'` 
echo $count
if [ $count -eq 0 ] ; then 
ls -al $file ; rm $file 
fi
done

### 
# copy to output
cat DQM_${date}.*.CMSSW*
#cp DQM_${date}.*.CMSSW* $outdir/.

##-------------------------------------
##-------------------------------------
##-------------------------------------

#####
# now submit jobs
#

#cd $outdir

##-------------------------------------------
## loop over all datasets in reverse alphabetical order (MC first)
##-------------------------------------------

for i in `ls -r DQM_${date}.*.CMSSW*` ; do

for dataset in `cat $i | awk {'print $1'} | uniq` ; do
cmssw=`cat $i | grep $dataset | awk {'print $2'} | uniq`
tag=`cat $i |  grep $dataset | awk {'print $3'} | uniq`
dtype=`cat $i |  grep $dataset | awk {'print $4'} | uniq`
if [ $dtype = "data" ]
then
htype="DQMoffline"
else
htype="MC"
fi
echo $dataset | grep -q preprod
if [ $? -eq 0 ] ; then
htype="dqmHarvestingPOG"
fi

echo $i $dataset $cmssw $tag $dtype $htype

if [ $cmssw = "CMSSW_3_6_1_patch4" ] ; then ; cmssw=CMSSW_3_6_1_patch4 ; fi
if [ $cmssw = "CMSSW_3_7_0_patch2" ] ; then ; cmssw=CMSSW_3_7_0_patch4 ; fi
if [ $cmssw = "CMSSW_3_7_0_patch3" ] ; then ; cmssw=CMSSW_3_7_0_patch4 ; fi
if [ $cmssw = "CMSSW_3_6_2" ] ; then ; cmssw=CMSSW_3_6_3 ; fi


[ -d $basedir/$cmssw ]
if [ `echo $?` != 0 ];
then
echo 
echo "####################################################################"
echo Setting up $cmssw at `date`
echo 
echo 
#cd /afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/harvesting
cd $basedir
scramv1 project CMSSW $cmssw
cd $cmssw/src
cmsenv
cvs co DQM/Integration/scripts/harvesting_tools
cvs co -r 1.303 Configuration/PyReleaseValidation
#addpkg HLTrigger/Configuration V11-03-99-23
# cvs co -r CMSSW_3_6_1 Configuration/PyReleaseValidation/python/ConfigBuilder.py
# addpkg Configuration/StandardSequences
# cvs update -r 1.9 Configuration/StandardSequences/python/Harvesting_cff.py
# sed -i 's/postValidation\*hltpostvalidation_prod/hltpostvalidation_prod/'  Configuration/StandardSequences/python/Harvesting_cff.py
scramv1 b
cd ..
mkdir harvesting_area
cd harvesting_area
ln -s ../src/DQM/Integration/scripts/harvesting_tools/cmsHarvester.py .
ln -s ../src/DQM/Integration/scripts/harvesting_tools/check_harvesting.pl .
fi
cd $basedir/$cmssw/src
cmsenv
# rm -r ../harvesting_area/cmsHarvester.py
# ln -s DQM/Integration/scripts/harvesting_tools/cmsHarvester.py ../harvesting_area/.
# cp ../../bin/cmsHarvester.py ../harvesting_area/cmsHarvester.py
scramv1 b

export VO_CMS_SW_DIR=/afs/cern.ch/cms/sw
eval `scramv1 runtime -sh`
cd ../harvesting_area

echo  
echo "####################################################################"
echo Running the harvester at `date` ...
echo 
echo 

if [ $dtype = "data" ] ; then 
    ./cmsHarvester.py --dataset=$dataset --harvesting_type=$htype \
	--globaltag=$tag --site=srm-eoscms.cern.ch --force --saveByLumiSection --no-t1access --castordir=$castordir \
	# --Jsonfile=/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/harvesting/bin/JSON.txt
else
    ./cmsHarvester.py --dataset=$dataset --harvesting_type=$htype \
	--globaltag=$tag --site=srm-eoscms.cern.ch --no-ref-hists --force --no-t1access \
        --castordir=$castordir
fi

echo 
echo "####################################################################" 
echo Start creating and submitting jobs at `date`
echo 
echo 

multicrab -create -submit

rm -fr harvesting_accounting.txt	 

cd $bindir	 
	 
done
done

##---------------------------------------------------
## ... end loop
##---------------------------------------------------

fi

cd $bindir
#rm -fr help2 help1
#rm -f $LOCK

echo 
echo "=========================================================="
echo End Harvesting script at `date`
echo "=========================================================="
echo
done

rm -fr help2 help1
rm -f $LOCK
