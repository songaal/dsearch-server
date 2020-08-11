#!/bin/bash


SERVER_HOME=


if [ -z "$java_path" ] ; then
	java_path=java
fi

LOGS="${SERVER_HOME}/logs"
OUTPUT_LOG=$LOGS/output.log

JVM_OPTS="-Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError"
JAVA_OPTS="-server -Dfile.encoding=UTF-8"
SPRING_OPTS="--spring.config.location=${SERVER_HOME}/application.yml"


java -jar ${$SERVER_HOME}/fastcatx-server.jar ${JVM_OPTS} ${JAVA_OPTS} ${SPRING_OPTS}


print_option() {
	echo "usage: $0 | start | stop | restart "
}

start_daemon() {
	# prevent killed by Hup, ctrl-c
	trap '' 1 2
	"$java_path" ${$SERVER_HOME}/fastcatx-server.jar ${JVM_OPTS} ${JAVA_OPTS} ${SPRING_OPTS} > $OUTPUT_LOG 2>&1 &
                	PID=`echo "$!"`
                	sleep 1
                	if ps -p $PID > /dev/null
	then
		echo $PID > ".pid"
		echo "################################"
		echo "Start server PID = $PID"
		echo "$java_path ${$SERVER_HOME}/fastcatx-server.jar ${JVM_OPTS} ${JAVA_OPTS} ${SPRING_OPTS} > $OUTPUT_LOG 2>&1 &"
		echo "################################"
		#tail can be got signal ctrl-c
		trap - 2
		return 0
	else
		echo "[ERROR] Fail to start server. Check details at logs/output.log file."
		echo "---------------------------"
		tail -1 $OUTPUT_LOG
		echo "---------------------------"
	fi
	return 1
}

stop_daemon() {
	if [ -f ".pid" ] ; then
		PID=`cat ".pid"`
		if ps -p $PID > /dev/null
		then
			echo "################################"
			echo "Stop Daemon PID = $PID"
			ps -p "$PID"
			echo "kill $PID"
			echo "################################"
			kill "$PID"
			return 0
		else
			echo "Cannot find pid $PID to stop"
		fi
	else
		echo "Cannot stop daemon: .pid file not found"
		ps -ef|grep org.fastcatsearch.server.Bootstrap|grep -v grep
	fi
	return 1
}


if [ "$1" = "start" ] ; then
	if(start_daemon)
	then
		if [ "$2" != "notail" ] ; then
			tail -f $LOGS/system.log
		fi
	fi
elif [ "$1" = "stop" ] ; then
	if(stop_daemon)
	then
		if [ "$2" != "notail" ] ; then
			tail -f $LOGS/system.log
		fi
	fi
elif [ "$1" = "restart" ] ; then
		stop_daemon
		sleep 1
		if(start_daemon)
		then
			if [ "$2" != "notail" ] ; then
				tail -f $LOGS/system.log
			fi
		fi

elif [ -z "$1" ] ; then
	print_option

else
	echo "Unknown command : $1"
	print_option
fi


