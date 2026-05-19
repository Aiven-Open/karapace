#!/usr/bin/env bash
set -Eeuo pipefail

output_file=${1:?output file is required}
interval_seconds=${2:?interval is required}
shift 2
containers=("$@")
parent_pid=${DOCKER_STATS_PARENT_PID-}

if [[ ${#containers[@]} -eq 0 ]]; then
    echo "At least one container name is required" >&2
    exit 1
fi

table_output_file=${output_file%.csv}.table.log
shutting_down=0

render_table() {
    [[ -f ${output_file} ]] || return 0

    : >"${table_output_file}"

    /usr/bin/awk -F',' '
        function flush_snapshot() {
            if (current_ts == "") {
                return
            }

            sep = sprintf("%144s", "")
            gsub(/ /, "=", sep)
            print sep
            printf "Docker stats snapshot at %s\n", current_ts
            print sep
            printf "%-34s %-8s %-26s %-8s %-22s %-20s %-5s\n", "container", "cpu", "mem_usage", "mem%", "net_io", "block_io", "pids"
            print sep
            printf "%s", snapshot_buffer
            printf "\n"
            snapshot_buffer = ""
        }

        NR == 1 {
            next
        }

        {
            ts = $1
            if (current_ts != "" && ts != current_ts) {
                flush_snapshot()
            }
            current_ts = ts
            snapshot_buffer = snapshot_buffer sprintf("%-34s %-8s %-26s %-8s %-22s %-20s %-5s\n", $2, $3, $4, $5, $6, $7, $8)
        }

        END {
            flush_snapshot()
        }
    ' "${output_file}" >"${table_output_file}"
}

cleanup() {
    if [[ ${shutting_down} == "1" ]]; then
        return
    fi
    shutting_down=1
    render_table
}

handle_signal() {
    cleanup
    exit 0
}

trap cleanup EXIT
trap handle_signal INT TERM

mkdir -p "$(dirname "${output_file}")"
printf 'timestamp,container,cpu_perc,mem_usage,mem_perc,net_io,block_io,pids\n' >"${output_file}"

while true; do
    if [[ -n ${parent_pid} ]] && ! kill -0 "${parent_pid}" 2>/dev/null; then
        exit 0
    fi

    if [[ ! -d $(dirname "${output_file}") ]]; then
        exit 0
    fi

    mapfile -t running_containers < <(docker ps --format '{{.Names}}')
    active_containers=()
    for container in "${containers[@]}"; do
        if printf '%s\n' "${running_containers[@]}" | /usr/bin/grep -Fxq "${container}"; then
            active_containers+=("${container}")
        fi
    done

    if [[ ${#active_containers[@]} -eq 0 ]]; then
        sleep "${interval_seconds}"
        continue
    fi

    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    while IFS= read -r line; do
        [[ -n ${line} ]] || continue
        if [[ ! -d $(dirname "${output_file}") ]]; then
            exit 0
        fi
        printf '%s,%s\n' "${timestamp}" "${line}" >>"${output_file}"
    done < <(
        docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}' "${active_containers[@]}" 2>/dev/null
    )
    sleep "${interval_seconds}"
done
