#!/bin/bash

scriptName="bot.py"
logName="BTCUSDT.log"
path="/home/pi/"


croncmd="bash ${path}runner.sh"
cronjob="@reboot $croncmd"
( crontab -l | grep -v -F "$croncmd" ; echo "$cronjob" ) | crontab -

scriptPath="${path}${scriptName}"
logPath="${path}${logName}"

OLDTIME=15

SKIP=0

#start data crunders
python3 ${path}cruncher.py >> cruncher.log 2>&1 &
python3 ${path}cruncherFutureFill.py >> cruncher.log 2>&1 &

#make sure db back is in the crontab
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
    echo "tick"
    sleep $OLDTIME
done
