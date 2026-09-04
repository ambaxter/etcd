#!/bin/bash

#set -x

RATIO_LIST="${RATIO_LIST:-1/128 1/8 1/4 1/2 2/1 4/1 8/1 128/1}"
VALUE_SIZE_POWER_RANGE="${VALUE_SIZE_POWER_RANGE:-8 14}"
CONN_CLI_COUNT_POWER_RANGE="${CONN_CLI_COUNT_POWER_RANGE:-5 11}"
REPEAT_COUNT="${REPEAT_COUNT:-5}"
RUN_COUNT="${RUN_COUNT:-200000}"
CMP_WAL_LIST="${CMP_WAL_LIST:-off}"
PG_CMP_WAL_LIST="${PG_CMP_WAL_LIST:-off pglz lz4}"
CMP_ORIOLE_LIST="${CMP_ORIOLE_LIST:--1}"
PG_CMP_ORIOLE_LIST="${PG_CMP_ORIOLE_LIST:--1 1}"
KV_TYPE_LIST="${KV_TYPE_LIST:-bucket}"
PG_KV_TYPE_LIST="${PG_KV_TYPE_LIST:-bucket bucket_keys lbr_now_norm lbr_now_nifs lbr_kid_now_norm lbr_kid_now_nifs}"

KEY_SIZE="${KEY_SIZE:-256}"
KEY_SPACE_SIZE="${KEY_SPACE_SIZE:-$((1024 * 64))}"
BACKEND_SIZE="${BACKEND_SIZE:-$((20 * 1024 * 1024 * 1024))}"
GOAL_DB_SIZE="${GOAL_DB_SIZE:-$((1024 * 1024 * 1024))}"
RANGE_RESULT_LIMIT="${RANGE_RESULT_LIMIT:-100}"
CLIENT_PORT="${CLIENT_PORT:-23790}"

COMMIT=
COLLECTED_DB_SIZE=

ETCD_ROOT_DIR="$(cd $(dirname $0) && pwd)/../.."
ETCD_BIN_DIR="${ETCD_ROOT_DIR}/bin"
ETCD_BIN="${ETCD_BIN_DIR}/etcd"
ETCD_COMPOSE_DIR="${ETCD_ROOT_DIR}/contrib/pgetcd/compose"

WORKING_DIR="$(mktemp -d)"
CURRENT_DIR="$(pwd -P)"
OUTPUT_FILE="${CURRENT_DIR}/bolt-result-$(date '+%Y%m%d%H%M').csv"
POD_YAML_FILE="${ETCD_COMPOSE_DIR}/etcd_compose.yaml"

trap ctrl_c INT

CURRENT_ETCD_PID=

function ctrl_c() {
  # capture ctrl-c and kill server
  echo "terminating..."
  kill_etcd_server ${CURRENT_ETCD_PID}
  exit 0
}

function quit() {
  if [ ! -z ${CURRENT_ETCD_PID} ]; then
    kill_etcd_server ${CURRENT_ETCD_PID}
  fi
  exit $1
}

function check_prerequisite() {
  # check initial parameters
  if [ -f "${OUTPUT_FILE}" ]; then
    echo "file ${OUTPUT_FILE} already exists."
    exit 1
  fi
  pushd ${ETCD_ROOT_DIR} > /dev/null
  COMMIT=$(git log --pretty=format:'%h' -n 1)
  if [ $? -ne 0 ]; then
    COMMIT=N/A
  fi
  popd > /dev/null
  cat >"${OUTPUT_FILE}" <<EOF
commit,key_size,key_space_size,backend_size,range_result_limit,kv_type,cmp_oriole,cmp_wal,init_ms,empty_compact_ms,empty_defrag_ms,init_db_size,ratio,conn_size,value_size,read_ops_per_s,read_average_response_s,read_slowest_response_s,read_fastest_response_s,read_stddev_response_s,write_ops_per_s,write_average_response_s,write_slowest_response_s,write_fastest_response_s,write_stddev_response_s,completed_db_size,final_compact_ms,final_defrag_ms,final_db_size
EOF

}

function run_etcd_server() {
  # delete existing data directories
  [ -d "db" ] && rm -rf db
  [ -d "default.etcd" ] && rm -rf default.etcd/
  echo "start etcd server in the background"
  CMP_WAL="${CMP_WAL_STR}" CMP_PRI="${CMP_ORIOLE_STR}" CMP_TST="${CMP_ORIOLE_STR}" CMP_DEF="${CMP_ORIOLE_STR}" KV_TYPE="${KV_TYPE_STR}" podman compose -f "${POD_YAML_FILE}" --profile etcd up -d --wait
    &>/dev/null &
}

function init_etcd_db() {
  #initialize etcd database
  INIT_START=$(date +%s)
  echo "initialize etcd database to ${KEY_SPACE_SIZE} keys"
  podman run --replace --name initdb --rm --network etcd_bridge pgetcd:latest benchmark put --sequential-keys \
    --key-space-size=${KEY_SPACE_SIZE} \
    --val-size=${VALUE_SIZE} --key-size=${KEY_SIZE} \
    --endpoints http://etcd:${CLIENT_PORT} \
    --total=${KEY_SPACE_SIZE} \
    &>/dev/null
  INIT_END=$(date +%s)
  DIFF=$((${INIT_END} - ${INIT_START}))
  echo "init took ${DIFF} seconds"
}

function compact_etcd_db() {
  status=$(podman run --replace --name status --rm --network etcd_bridge pgetcd:latest etcdctl endpoint status --endpoints "http://etcd:${CLIENT_PORT}" --write-out="json")
  rev=$(echo $status | egrep -o '"revision":[0-9]*' | egrep -o '[0-9].*')
  podman run --replace --name compact --rm --network etcd_bridge pgetcd:latest etcdctl --command-timeout=1000s compact $rev --physical=true --endpoints "http://etcd:${CLIENT_PORT}"
}

function defrag_etcd_db() {
  podman run --replace --name defrag --rm --network etcd_bridge pgetcd:latest etcdctl --command-timeout=1000s defrag --endpoints "http://etcd:${CLIENT_PORT}"
}

function kill_etcd_server() {
  # kill etcd server
  podman compose -f "${POD_YAML_FILE}" --profile etcd down -v  2>/dev/null
}

function collect_db_size() {
    COLLECTED_DB_SIZE=$(podman run --replace --name db_size --rm --network etcd_bridge pgetcd:latest etcdctl endpoint status \
      --endpoints "http://etcd:23790" --write-out="json" \
      2>/dev/null | egrep -o '"dbSize":[0-9]*' | egrep -o '[0-9].*')
}

while getopts ":w:c:p:l:vhnode" OPTION; do
  case $OPTION in
  h)
    echo "usage: $(basename $0) [-h] [-w WORKING_DIR] [-c RUN_COUNT] [-p PORT] [-l RANGE_QUERY_LIMIT] [-d] [-v]" >&2
    exit 1
    ;;
  w)
    WORKING_DIR="${OPTARG}"
    ;;
  c)
    RUN_COUNT="${OPTARG}"
    ;;
  p)
    CLIENT_PORT="${OPTARG}"
    ;;
  n)
    POD_YAML_FILE="${ETCD_COMPOSE_DIR}/db/oriole17_compose.yaml"
    OUTPUT_FILE="${CURRENT_DIR}/oriole17_result-$(date '+%Y%m%d%H%M').csv"
    CMP_WAL_LIST="${PG_CMP_WAL_LIST}"
    CMP_ORIOLE_LIST="${PG_CMP_ORIOLE_LIST}"
    KV_TYPE_LIST="${PG_KV_TYPE_LIST}"
    ;;
  o)
    POD_YAML_FILE="${ETCD_COMPOSE_DIR}/db/oriole18_compose.yaml"
    OUTPUT_FILE="${CURRENT_DIR}/oriole18_result-$(date '+%Y%m%d%H%M').csv"
    CMP_WAL_LIST="${PG_CMP_WAL_LIST}"
    CMP_ORIOLE_LIST="${PG_CMP_ORIOLE_LIST}"
    KV_TYPE_LIST="${PG_KV_TYPE_LIST}"
    ;;
  d)
    POD_YAML_FILE="${ETCD_COMPOSE_DIR}/db/postgres17_compose.yaml"
    OUTPUT_FILE="${CURRENT_DIR}/postgres17_result-$(date '+%Y%m%d%H%M').csv"
    CMP_WAL_LIST="${PG_CMP_WAL_LIST}"
    KV_TYPE_LIST="${PG_KV_TYPE_LIST}"
    ;;
  e)
    POD_YAML_FILE="${ETCD_COMPOSE_DIR}/db/postgres18_compose.yaml"
    OUTPUT_FILE="${CURRENT_DIR}/postgres18_result-$(date '+%Y%m%d%H%M').csv"
    CMP_WAL_LIST="${PG_CMP_WAL_LIST}"
    KV_TYPE_LIST="${PG_KV_TYPE_LIST}"
    ;;
  v)
    set -x
    ;;
  l)
    RANGE_RESULT_LIMIT="${OPTARG}"
    ;;
  \?)
    echo "usage: $(basename $0) [-h] [-w WORKING_DIR] [-c RUN_COUNT] [-p PORT] [-l RANGE_QUERY_LIMIT] [-v]" >&2
    exit 1
    ;;
  esac
done
shift "$((${OPTIND} - 1))"

#  KEY_SPACE_SIZE=$(($GOAL_DB_SIZE / ($KEY_SIZE + $VALUE_SIZE)))

check_prerequisite

pushd "${WORKING_DIR}" > /dev/null

# progress stats management
ITER_TOTAL=$((
  $(echo ${CMP_ORIOLE_LIST} | wc | awk "{print \$2}") * \
  $(echo ${CMP_WAL_LIST} | wc | awk "{print \$2}") * \
  $(echo ${KV_TYPE_LIST} | wc | awk "{print \$2}") * \
  $(echo ${RATIO_LIST} | wc | awk "{print \$2}") * \
  $(seq ${VALUE_SIZE_POWER_RANGE} | wc | awk "{print \$2}") * \
  $(seq ${CONN_CLI_COUNT_POWER_RANGE} | wc | awk "{print \$2}")))
ITER_CURRENT=0
PERCENTAGE_LAST_PRINT=0
PERCENTAGE_PRINT_THRESHOLD=5

for CMP_ORIOLE_STR in ${CMP_ORIOLE_LIST}; do
  for CMP_WAL_STR in ${CMP_WAL_LIST}; do
    for KV_TYPE_STR in ${KV_TYPE_LIST}; do
      for VALUE_SIZE_POWER in $(seq ${VALUE_SIZE_POWER_RANGE}); do
        VALUE_SIZE=$((2 ** ${VALUE_SIZE_POWER}))
        run_etcd_server
        START=$(date +%s%N)
        init_etcd_db
        INIT_MS=$((($(date +%s%N) - $START)/1000000))
        START=$(date +%s%N)
        compact_etcd_db
        EMPTY_COMPACT_MS=$((($(date +%s%N) - $START)/1000000))
        START=$(date +%s%N)
        defrag_etcd_db
        EMPTY_DEFRAG_MS=$((($(date +%s%N) - $START)/1000000))
        for RATIO_STR in ${RATIO_LIST}; do
          RATIO=$(echo "scale=4; ${RATIO_STR}" | bc -l)
            for CONN_CLI_COUNT_POWER in $(seq ${CONN_CLI_COUNT_POWER_RANGE}); do

              # progress stats management
              ITER_CURRENT=$((${ITER_CURRENT} + 1))
              PERCENTAGE_CURRENT=$(echo "scale=3; ${ITER_CURRENT}/${ITER_TOTAL}*100" | bc -l)
              if [ "$(echo "${PERCENTAGE_CURRENT} - ${PERCENTAGE_LAST_PRINT} > ${PERCENTAGE_PRINT_THRESHOLD}" |
                bc -l)" -eq 1 ]; then
                PERCENTAGE_LAST_PRINT=${PERCENTAGE_CURRENT}
                echo "${PERCENTAGE_CURRENT}% completed"
              fi

              collect_db_size
              INIT_DB_SIZE="${COLLECTED_DB_SIZE}"

              CONN_CLI_COUNT=$((2 ** ${CONN_CLI_COUNT_POWER}))

              START=$(date +%s)
              echo -n "run with setting [cmp_db: ${CMP_ORIOLE_STR}, cmp_wal: ${CMP_WAL_STR}, kv: ${KV_TYPE_STR}, val: ${VALUE_SIZE}, ratio: ${RATIO_STR}, conn: ${CONN_CLI_COUNT}]"
              READ_OPS_PER_S=""
              READ_SLOWEST_RESPONSE_S=""
              READ_AVERAGE_RESPONSE_S=""
              READ_FASTEST_RESPONSE_S=""
              READ_STDDEV_RESPONSE_S=""
              WRITE_OPS_PER_S=""
              WRITE_SLOWEST_RESPONSE_S=""
              WRITE_AVERAGE_RESPONSE_S=""
              WRITE_FASTEST_RESPONSE_S=""
              WRITE_STDDEV_RESPONSE_S=""
              for i in $(seq ${REPEAT_COUNT}); do
                echo -n "."
                RAW_QPS=$(podman run --replace --name benchmark --rm --network etcd_bridge pgetcd:latest benchmark txn-mixed "" \
                  --conns=${CONN_CLI_COUNT} --clients=${CONN_CLI_COUNT} \
                  --total=${RUN_COUNT} \
                  --endpoints "http://etcd:${CLIENT_PORT}" \
                  --rw-ratio ${RATIO} --limit ${RANGE_RESULT_LIMIT} \
                  --val-size ${VALUE_SIZE} \
                  2>/dev/null)
                if [ $? -ne 0 ]; then
                  echo "benchmark command failed: $?"
                  quit -1
                fi
                QPS=$(echo -e "${RAW_QPS}" | grep "Requests/sec" | awk "{print \$2}")
                RD_QPS=$(echo -e "${QPS}" | sed -n '1 p')
                WR_QPS=$(echo -e "${QPS}" | sed -n '2 p')
                if [ -z "${RD_QPS}" ]; then
                  echo "error rd: \"${RAW_QPS}\""
                  RD_QPS=0
                fi
                if [ -z "${WR_QPS}" ]; then
                  echo "error wr: \"${RAW_QPS}\""
                  WR_QPS=0
                fi
                SLOWEST=$(echo -e "${RAW_QPS}" | grep "Slowest" | awk "{print \$2}")
                RD_SLOWEST=$(echo -e "${SLOWEST}" | sed -n '1 p')
                WR_SLOWEST=$(echo -e "${SLOWEST}" | sed -n '2 p')
                FASTEST=$(echo -e "${RAW_QPS}" | grep "Fastest" | awk "{print \$2}")
                RD_FASTEST=$(echo -e "${FASTEST}" | sed -n '1 p')
                WR_FASTEST=$(echo -e "${FASTEST}" | sed -n '2 p')
                AVERAGE=$(echo -e "${RAW_QPS}" | grep "Average" | awk "{print \$2}")
                RD_AVERAGE=$(echo -e "${AVERAGE}" | sed -n '1 p')
                WR_AVERAGE=$(echo -e "${AVERAGE}" | sed -n '2 p')
                STD_DEV=$(echo -e "${RAW_QPS}" | grep "Stddev" | awk "{print \$2}")
                RD_STDDEV=$(echo -e "${STD_DEV}" | sed -n '1 p')
                WR_STDDEV=$(echo -e "${STD_DEV}" | sed -n '2 p')

                if [ $i -eq 1 ]; then
                  READ_OPS_PER_S="${RD_QPS}"
                  READ_AVERAGE_RESPONSE_S="${RD_AVERAGE}"
                  READ_SLOWEST_RESPONSE_S="${RD_SLOWEST}"
                  READ_FASTEST_RESPONSE_S="${RD_FASTEST}"
                  READ_STDDEV_RESPONSE_S="${RD_STDDEV}"
                  WRITE_OPS_PER_S="${WR_QPS}"
                  WRITE_AVERAGE_RESPONSE_S="${WR_AVERAGE}"
                  WRITE_SLOWEST_RESPONSE_S="${WR_SLOWEST}"
                  WRITE_FASTEST_RESPONSE_S="${WR_FASTEST}"
                  WRITE_STDDEV_RESPONSE_S="${WR_STDDEV}"
                else 
                READ_OPS_PER_S="${READ_OPS_PER_S},${RD_QPS}"
                READ_AVERAGE_RESPONSE_S="${READ_AVERAGE_RESPONSE_S},${RD_AVERAGE}"
                READ_SLOWEST_RESPONSE_S="${READ_SLOWEST_RESPONSE_S},${RD_SLOWEST}"
                READ_FASTEST_RESPONSE_S="${READ_FASTEST_RESPONSE_S},${RD_FASTEST}"
                READ_STDDEV_RESPONSE_S="${READ_STDDEV_RESPONSE_S},${RD_STDDEV}"
                WRITE_OPS_PER_S="${WRITE_OPS_PER_S},${WR_QPS}"
                WRITE_AVERAGE_RESPONSE_S="${WRITE_AVERAGE_RESPONSE_S},${WR_AVERAGE}"
                WRITE_SLOWEST_RESPONSE_S="${WRITE_SLOWEST_RESPONSE_S},${WR_SLOWEST}"
                WRITE_FASTEST_RESPONSE_S="${WRITE_FASTEST_RESPONSE_S},${WR_FASTEST}"
                WRITE_STDDEV_RESPONSE_S="${WRITE_STDDEV_RESPONSE_S},${WR_STDDEV}"
                fi
              done
              END=$(date +%s)
              DIFF=$((${END} - ${START}))
              echo "took ${DIFF} seconds"
              collect_db_size
              COMPLETED_DB_SIZE="${COLLECTED_DB_SIZE}"
              START=$(date +%s%N)
              compact_etcd_db
              FINAL_COMPACT_MS=$((($(date +%s%N) - $START)/1000000))
              START=$(date +%s%N)
              defrag_etcd_db
              FINAL_DEFRAG_MS=$((($(date +%s%N) - $START)/1000000))
              collect_db_size
              FINAL_DB_SIZE="${COLLECTED_DB_SIZE}"
              cat >>"${OUTPUT_FILE}" <<EOF
${COMMIT},${KEY_SIZE},${KEY_SPACE_SIZE},${BACKEND_SIZE},${RANGE_RESULT_LIMIT},${KV_TYPE_STR},${CMP_ORIOLE_STR},${CMP_WAL_STR},${INIT_MS},${EMPTY_COMPACT_MS},${EMPTY_DEFRAG_MS},${INIT_DB_SIZE},${RATIO},${CONN_CLI_COUNT},${VALUE_SIZE},[${READ_OPS_PER_S}],[${READ_AVERAGE_RESPONSE_S}],[${READ_SLOWEST_RESPONSE_S}],[${READ_FASTEST_RESPONSE_S}],[${READ_STDDEV_RESPONSE_S}],[${WRITE_OPS_PER_S}],[${WRITE_AVERAGE_RESPONSE_S}],[${WRITE_SLOWEST_RESPONSE_S}],[${WRITE_FASTEST_RESPONSE_S}],[${WRITE_STDDEV_RESPONSE_S}],${COMPLETED_DB_SIZE},${FINAL_COMPACT_MS},${FINAL_DEFRAG_MS},${FINAL_DB_SIZE}
EOF
          done
        done
      kill_etcd_server ${CURRENT_ETCD_PID}
      done
    done
  done
done

popd > /dev/null
