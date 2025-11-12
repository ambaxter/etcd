#!/bin/bash

VALUE_SIZE_POWER_RANGE="8 11" CONN_CLI_COUNT_POWER_RANGE="5 9" PG_KV_TYPE_LIST="lbr_now_norm lbr_now_nifs lbr_kid_now_norm lbr_kid_now_nifs" PG_CMP_WAL_LIST="lz4" RATIO_LIST="1/128" ./pg-thunderdome.sh -o
VALUE_SIZE_POWER_RANGE="8 11" PG_KV_TYPE_LIST="lbr_now_norm lbr_now_nifs lbr_kid_now_norm lbr_kid_now_nifs" PG_CMP_WAL_LIST="lz4" RATIO_LIST="128/1" CONN_CLI_COUNT_POWER_RANGE="11 11" ./pg-thunderdome.sh -o
