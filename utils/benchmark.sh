#!/bin/bash

DURATION=60
INTERVAL=1
CPU_OUTPUT="cpu.log"
RAM_OUTPUT="ram.log"
DISK_OUTPUT="disk.log"
NET_OUTPUT="network.log"

INTERFACE="ens5"
DISK_DEVICE="md0"

echo "Benchmarking started."

echo "time,cpu_utilization" >$CPU_OUTPUT
echo "time,total,used,free,available" >$RAM_OUTPUT
echo "time,disk_utilization" >$DISK_OUTPUT
echo "time,rx_kB_s,tx_kB_s" >"$NET_OUTPUT"

START=$(date +%s.%N)
END=$(echo "$START + $DURATION" | bc)

PREV_TOTAL=0
PREV_IDLE=0
PREV_IO_TIME=0
PREV_TIME=$START

NUM_CORES=$(nproc)

PREV_RX_BYTES=$(cat /sys/class/net/"$INTERFACE"/statistics/rx_bytes)
PREV_TX_BYTES=$(cat /sys/class/net/"$INTERFACE"/statistics/tx_bytes)
while (($(echo "$(date +%s.%N) < $END" | bc -l))); do
	CURRENT_TIME=$(date +%s.%N)

	# -----------------------------
	# 4. CPU Utilization
	# -----------------------------
	CPU_LINE=$(awk '/^cpu / {print $0}' /proc/stat)
	CPU_VALUES=($CPU_LINE)

	USER=${CPU_VALUES[1]}
	NICE=${CPU_VALUES[2]}
	SYSTEM=${CPU_VALUES[3]}
	IDLE=${CPU_VALUES[4]}
	IOWAIT=${CPU_VALUES[5]}
	IRQ=${CPU_VALUES[6]}
	SOFTIRQ=${CPU_VALUES[7]}
	STEAL=${CPU_VALUES[8]:-0}
	GUEST=${CPU_VALUES[9]:-0}
	GUEST_NICE=${CPU_VALUES[10]:-0}

	TOTAL=$((USER + NICE + SYSTEM + IDLE + IOWAIT + IRQ + SOFTIRQ + STEAL + GUEST + GUEST_NICE))

	ACTIVE=$((TOTAL - IDLE))

	TOTAL_DIFF=$((TOTAL - PREV_TOTAL))
	IDLE_DIFF=$((IDLE - PREV_IDLE))

	if [[ $TOTAL_DIFF -ne 0 ]]; then
		CPU_UTIL=$(echo "scale=2; (100 * ($TOTAL_DIFF - $IDLE_DIFF) / $TOTAL_DIFF) * $NUM_CORES" | bc)
	else
		CPU_UTIL=0
	fi

	echo "$CURRENT_TIME,$CPU_UTIL" >>$CPU_OUTPUT

	PREV_TOTAL=$TOTAL
	PREV_IDLE=$IDLE

	# -----------------------------
	# 4. RAM Usage
	# -----------------------------
	free -m | awk '/Mem:/ {print systime(), $2, $3, $4, $7}' >>$RAM_OUTPUT

	# -----------------------------
	# 4. Disk utilization
	# -----------------------------
	IO_TIME=$(awk -v disk="$DISK_DEVICE" '$3 == disk {print $13}' /proc/diskstats)
	if [[ -n "$IO_TIME" ]]; then
		ELAPSED_TIME=$(echo "$CURRENT_TIME - $PREV_TIME" | bc -l)
		IO_DELTA=$(echo "$IO_TIME - $PREV_IO_TIME" | bc)
		UTILIZATION=$(echo "scale=2; ($IO_DELTA / ($ELAPSED_TIME * 1000)) * 100" | bc)
		echo "$CURRENT_TIME,$UTILIZATION" >>$DISK_OUTPUT
		PREV_IO_TIME=$IO_TIME
	fi

	# -----------------------------
	# 4. Network throughput
	# -----------------------------
	RX_BYTES=$(cat /sys/class/net/"$INTERFACE"/statistics/rx_bytes)
	TX_BYTES=$(cat /sys/class/net/"$INTERFACE"/statistics/tx_bytes)

	RX_DIFF=$((RX_BYTES - PREV_RX_BYTES))
	TX_DIFF=$((TX_BYTES - PREV_TX_BYTES))

	if [[ $(echo "$ELAPSED_TIME > 0" | bc) -eq 1 ]]; then
		RX_KBPS=$(echo "scale=2; $RX_DIFF / ($ELAPSED_TIME * 1024 * 1024)" | bc)
		TX_KBPS=$(echo "scale=2; $TX_DIFF / ($ELAPSED_TIME * 1024 * 1024)" | bc)
	else
		RX_KBPS=0
		TX_KBPS=0
	fi

	echo "$CURRENT_TIME,$RX_KBPS,$TX_KBPS" >>"$NET_OUTPUT"

	PREV_RX_BYTES=$RX_BYTES
	PREV_TX_BYTES=$TX_BYTES
	PREV_TIME=$CURRENT_TIME
	sleep $INTERVAL
done

echo "CPU utilization data collected in $CPU_OUTPUT"
echo "RAM data collected in $RAM_OUTPUT"
echo "Disk utilization data collected in $DISK_OUTPUT"
echo "Network throughput data collected in $NET_OUTPUT"
