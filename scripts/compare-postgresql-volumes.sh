#!/usr/bin/env bash

set -euo pipefail

if (( $# < 2 || $# > 3 )); then
    echo "usage: $0 <local-task-id> <replicated-task-id> [result-directory]" >&2
    exit 2
fi

LOCAL_TASK=$1
REPLICATED_TASK=$2
RESULT_ROOT=${3:-${TMPDIR:-/tmp}/mantissa-postgresql-$(date -u +%Y%m%dT%H%M%SZ)}
MANTISSA_BIN=${MANTISSA_BIN:-mantissa}
PGBENCH_RUNS=${PGBENCH_RUNS:-3}
PGBENCH_SCALE=${PGBENCH_SCALE:-10}
PGBENCH_CLIENTS=${PGBENCH_CLIENTS:-16}
PGBENCH_JOBS=${PGBENCH_JOBS:-4}
PGBENCH_SECONDS=${PGBENCH_SECONDS:-60}
COMMAND_TIMEOUT_SECONDS=${COMMAND_TIMEOUT_SECONDS:-1800}

if ! [[ "$PGBENCH_RUNS" =~ ^[1-9][0-9]*$ ]]; then
    echo "PGBENCH_RUNS must be a positive integer" >&2
    exit 2
fi

mkdir -p "$RESULT_ROOT"
SUMMARY="$RESULT_ROOT/summary.tsv"
printf 'storage\trun\tinit_seconds\tbenchmark_seconds\ttps\tlatency_ms\tdata_kib\n' >"$SUMMARY"

# Returns wall time in nanoseconds for one benchmark phase.
now_ns() {
    date +%s%N
}

# Converts two nanosecond timestamps to seconds with millisecond precision.
elapsed_seconds() {
    awk -v start="$1" -v end="$2" 'BEGIN { printf "%.3f", (end - start) / 1000000000 }'
}

# Runs one command inside a benchmark task without attaching stdin.
task_exec() {
    local task_id=$1
    shift
    timeout "${COMMAND_TIMEOUT_SECONDS}s" "$MANTISSA_BIN" tasks exec --no-stdin "$task_id" "$@"
}

# Returns the hostname currently running one task.
task_node() {
    timeout 10s "$MANTISSA_BIN" tasks list --no-trunc |
        awk -v id="$1" 'index($1, id) == 1 { print $9; exit }'
}

# Checks that one task is a usable PostgreSQL benchmark target.
check_task() {
    local task_id=$1
    local storage=$2
    if ! task_exec "$task_id" pg_isready -q -U mantissa -d app; then
        echo "$storage PostgreSQL task is not ready: $task_id" >&2
        exit 1
    fi
}

# Runs one initialization, warm-up, and measured pgbench workload.
run_case() {
    local storage=$1
    local task_id=$2
    local run=$3
    local case_dir="$RESULT_ROOT/$run-$storage"
    local started finished init_seconds benchmark_seconds tps latency data_kib

    mkdir -p "$case_dir"
    echo "Starting $storage run $run"

    started=$(now_ns)
    task_exec "$task_id" pgbench -i -s "$PGBENCH_SCALE" -U mantissa app \
        2>&1 | tee "$case_dir/init.log"
    finished=$(now_ns)
    init_seconds=$(elapsed_seconds "$started" "$finished")

    task_exec "$task_id" pgbench -c 4 -j 2 -T 15 -U mantissa app \
        >"$case_dir/warmup.log" 2>&1

    started=$(now_ns)
    task_exec "$task_id" pgbench \
        -c "$PGBENCH_CLIENTS" \
        -j "$PGBENCH_JOBS" \
        -T "$PGBENCH_SECONDS" \
        -P 10 \
        -r \
        -U mantissa \
        app 2>&1 | tee "$case_dir/pgbench.log"
    finished=$(now_ns)
    benchmark_seconds=$(elapsed_seconds "$started" "$finished")

    tps=$(awk '$1 == "tps" && $2 == "=" { value = $3 } END { print value }' "$case_dir/pgbench.log")
    latency=$(awk '$1 == "latency" && $2 == "average" { value = $4 } END { print value }' "$case_dir/pgbench.log")
    data_kib=$(task_exec "$task_id" du -sk /var/lib/postgresql/data/pgdata | awk '{print $1}')
    if [[ -z "$tps" || -z "$latency" || -z "$data_kib" ]]; then
        echo "Could not read all results for $storage run $run" >&2
        exit 1
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$storage" "$run" "$init_seconds" "$benchmark_seconds" \
        "$tps" "$latency" "$data_kib" | tee -a "$SUMMARY"
}

check_task "$LOCAL_TASK" local
check_task "$REPLICATED_TASK" replicated

LOCAL_NODE=$(task_node "$LOCAL_TASK")
REPLICATED_NODE=$(task_node "$REPLICATED_TASK")
if [[ -z "$LOCAL_NODE" || -z "$REPLICATED_NODE" ]]; then
    echo "Could not find both tasks in 'mantissa tasks list'" >&2
    exit 1
fi
if [[ "$LOCAL_NODE" != "$REPLICATED_NODE" ]]; then
    echo "Both tasks must run on the same node for a fair comparison" >&2
    echo "local=$LOCAL_NODE replicated=$REPLICATED_NODE" >&2
    exit 1
fi

{
    printf 'node=%s\n' "$LOCAL_NODE"
    printf 'local_task=%s\n' "$LOCAL_TASK"
    printf 'replicated_task=%s\n' "$REPLICATED_TASK"
    printf 'runs=%s\n' "$PGBENCH_RUNS"
    printf 'scale=%s\n' "$PGBENCH_SCALE"
    printf 'clients=%s\n' "$PGBENCH_CLIENTS"
    printf 'jobs=%s\n' "$PGBENCH_JOBS"
    printf 'seconds=%s\n' "$PGBENCH_SECONDS"
} >"$RESULT_ROOT/settings.txt"

echo "Comparing PostgreSQL storage on $LOCAL_NODE"
echo "Each run replaces the pgbench tables in both app databases."
for run in $(seq 1 "$PGBENCH_RUNS"); do
    if ((run % 2 == 1)); then
        run_case local "$LOCAL_TASK" "$run"
        run_case replicated "$REPLICATED_TASK" "$run"
    else
        run_case replicated "$REPLICATED_TASK" "$run"
        run_case local "$LOCAL_TASK" "$run"
    fi
done

awk -F '\t' '
    NR > 1 {
        count[$1]++
        init[$1] += $3
        tps[$1] += $5
        latency[$1] += $6
    }
    END {
        print ""
        print "Average results"
        print "storage\tinit_s\ttps\tlatency_ms"
        for (storage in count) {
            printf "%s\t%.3f\t%.2f\t%.3f\n", storage,
                init[storage] / count[storage], tps[storage] / count[storage],
                latency[storage] / count[storage]
        }
        if (count["local"] && count["replicated"]) {
            local_tps = tps["local"] / count["local"]
            replicated_tps = tps["replicated"] / count["replicated"]
            printf "\nreplicated/local TPS: %.2f%%\n", 100 * replicated_tps / local_tps
        }
    }
' "$SUMMARY" | tee "$RESULT_ROOT/averages.txt"

echo "Raw output and summary: $RESULT_ROOT"
