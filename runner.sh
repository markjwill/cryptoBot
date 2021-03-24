#!/bin/bash

scriptName="bot.py"
logName="BTCUSDT.log"
path="/home/pi/"

#set this script to run on reboot
croncmd="bash ${path}runner.sh"
cronjob="@reboot $croncmd"
( crontab -l | grep -v -F "$croncmd" ; echo "$cronjob" ) | crontab -

scriptPath="${path}${scriptName}"
logPath="${path}${logName}"

cruncherLog="${path}crunchers.log"

OLDTIME=15

SKIP=0

function restartCruncher() {
    pids=$(ps aux | grep "[/]cruncher.py" | awk '{print $2}')
    for pid in ${pids[@]}; do
        echo killing ${pid}
        kill ${pid}
    done
    python3 ${path}cruncher.py >> ${cruncherLog} 2>&1 &
}

function restartCruncherFutureFill() {
    pids=$(ps aux | grep "[/]cruncherFutureFill.py" | awk '{print $2}')
    for pid in ${pids[@]}; do
        echo killing ${pid}
        kill ${pid}
    done
    python3 ${path}cruncherFutureFill.py >> ${cruncherLog} 2>&1 &
}

#make sure db backup is in the crontab
croncmd="mysqldump --single-transaction --all-databases | pigz -1 > ${path}dbBackup.log > ${path}dbBackup.sql.gz 2>&1"
cronjob="30 23 * * * $croncmd"
( crontab -l | grep -v -F "$croncmd" ; echo "$cronjob" ) | crontab -

function startTrader() {
    echo "###########   START  BOT   ###########" >>"${logPath}" 2>&1
    echo ">>>###########   START  BOT   ###########<<<"
    python3 "${scriptPath}" >>"${logPath}" 2>&1 &
    sleep 10
}

function restartTrader() {
    echo "###########  RE START BOT  ###########" >>"${logPath}" 2>&1
    echo ">>>###########  RE START BOT  ###########<<<"
    pids=$(ps aux | grep "[/]${scriptName}" | awk '{print $2}')
    for pid in ${pids[@]}; do
        echo killing ${pid}
        kill ${pid}
    done
    startTrader
}


if [ "0" ==  $(ps aux | grep "[/]${scriptName}" | wc -l) ]; then
    startTrader
    sleep $OLDTIME
fi

#start data crunders
restartCruncher
restartCruncherFutureFill

while true; do

    timeString=$(tail -n 1 "${logPath}")
    time=$(date -d "${timeString}" +"%s" 2>:)
    time=$(( ${time} + 0 ))
    if [[ "${time}" -lt "1" && "${SKIP}" -gt "5" ]]; then
        SKIP=0
        now=$(date +%s)
        last=$(( $time + $(( 60 * 5 )) ))
        if [ "$last" -lt "$now" ]; then
            echo "###########  BOT TIME OUT  ###########" >>"${logPath}" 2>&1
            echo ">>>###########  BOT TIME OUT  ###########<<<"
            restartTrader
        fi
    fi
    if [[ "${time}" -lt "1" ]]; then
        SKIP=$(( $SKIP + 1))
    else
        SKIP=0
    fi
    echo "lastLine: ${timeString} SKIP: ${SKIP} time: ${time}"
    # Get current and file times
    CURTIME=$(date +%s)
    FILETIME=$(stat $scriptPath -c %Y)
    TIMEDIFF=$(expr $CURTIME - $FILETIME)

    # Check if file older
    if [ $TIMEDIFF -lt $OLDTIME ]; then
       echo "########### SCRIPT CHANGES ###########" >>"${logPath}" 2>&1
       echo ">>>########### SCRIPT CHANGES ###########<<<"
       restartTrader
    fi

    FILETIME=$(stat ${path}cruncher.py -c %Y)
    TIMEDIFF=$(expr $CURTIME - $FILETIME)

    # Check if file older
    if [ $TIMEDIFF -lt $OLDTIME ]; then
       echo "########### CRUNCHER CHANGES ###########" >>"${cruncherLog}" 2>&1
       echo ">>>########### CRUNCHER CHANGES ###########<<<"
       restartCruncher
    fi

    FILETIME=$(stat ${path}cruncherFutureFill.py -c %Y)
    TIMEDIFF=$(expr $CURTIME - $FILETIME)

    # Check if file older
    if [ $TIMEDIFF -lt $OLDTIME ]; then
       echo "########### CRUNCHER FUTURE FILL CHANGES ###########" >>"${cruncherLog}" 2>&1
       echo ">>>########### CRUNCHER FUTURE FILL CHANGES ###########<<<"
       restartCruncherFutureFill
    fi

    echo "tick"
    sleep $OLDTIME
done
