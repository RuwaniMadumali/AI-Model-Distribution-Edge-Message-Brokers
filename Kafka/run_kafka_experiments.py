import subprocess
import time
import os
import csv

# ============================================================
# CONFIG
# ============================================================

model_files = [
    "model_10MB.bin",
    "model_50MB.bin",
    "model_100MB.bin"
]

device_counts = [1, 3, 5]

approaches = ["full", "metadata"]

# Kafka Docker container name
KAFKA_CONTAINER = "kafka"

# Results file
RESULTS_FILE = "results_kafka.csv"

# Network conditions
network_conditions = {
    "normal": {
        "delay": "0ms",
        "bandwidth": None
    },

    "moderate": {
        "delay": "50ms",
        "bandwidth": "10mbit"
    },

    "constrained": {
        "delay": "100ms",
        "bandwidth": "5mbit"
    }
}


# ============================================================
# CSV INITIALIZATION
# ============================================================

if os.path.exists(RESULTS_FILE):
    os.remove(RESULTS_FILE)

with open(RESULTS_FILE, "w", newline="") as f:

    writer = csv.writer(f)

    writer.writerow([
        "Approach",
        "Network Condition",
        "Configured Delay (ms)",
        "Configured Bandwidth (Mbps)",
        "Latency (ms)",
        "Throughput (Mbps)",
        "Model File",
        "Model Size (MB)",
        "Device Count",
        "Status",
        "Failure Reason"
    ])


# ============================================================
# GET MODEL SIZE
# ============================================================

def get_model_size_mb(model):

    if not os.path.exists(model):
        return None

    size_bytes = os.path.getsize(model)

    return size_bytes / (1024 * 1024)


# ============================================================
# CHECK tc
# ============================================================

def check_tc():

    print("\nChecking tc inside Kafka container...")

    result = subprocess.run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "which",
            "tc"
        ],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:

        print("WARNING: tc was not found inside the Kafka container.")

        print(
            "Network throttling cannot be applied until tc "
            "is installed inside the Kafka container."
        )

        return False

    print("tc found:", result.stdout.strip())

    return True


# ============================================================
# RESET NETWORK
# ============================================================

def reset_network_condition():

    print("\nResetting network condition...")

    subprocess.run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "tc",
            "qdisc",
            "del",
            "dev",
            "eth0",
            "root"
        ],
        capture_output=True,
        text=True
    )

    time.sleep(1)


# ============================================================
# APPLY NETWORK CONDITION
# ============================================================

def apply_network_condition(delay, bandwidth):

    # Always remove previous configuration first
    reset_network_condition()

    # Normal network
    if delay == "0ms" and bandwidth is None:

        print("Network condition: NORMAL")
        print("Delay: 0 ms")
        print("Bandwidth: Unlimited")

        return True

    print("Applying network condition...")
    print("Delay:", delay)

    if bandwidth:
        print("Bandwidth:", bandwidth)
    else:
        print("Bandwidth: Unlimited")

    command = [
        "docker",
        "exec",
        KAFKA_CONTAINER,
        "tc",
        "qdisc",
        "add",
        "dev",
        "eth0",
        "root",
        "netem",
        "delay",
        delay
    ]

    if bandwidth:

        command.extend([
            "rate",
            bandwidth
        ])

    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:

        print("ERROR applying network condition:")
        print(result.stderr)

        return False

    print("Network condition successfully applied.")

    return True


# ============================================================
# START CONSUMERS
# ============================================================

def start_consumers(approach, devices):

    consumers = []

    for i in range(devices):

        print(
            f"Starting consumer {i + 1}/{devices} "
            f"for {approach}"
        )

        if approach == "full":

            p = subprocess.Popen(
                ["python3", "kafka_consumer_full.py"]
            )

        else:

            p = subprocess.Popen(
                ["python3", "kafka_consumer_meta.py"]
            )

        consumers.append(p)

    return consumers


# ============================================================
# STOP CONSUMERS
# ============================================================

def stop_consumers(consumers):

    for p in consumers:

        try:
            p.terminate()

        except Exception:
            pass

    time.sleep(2)


# ============================================================
# RUN PRODUCER
# ============================================================

def run_producer(approach, model):

    start_time = time.perf_counter()

    result = None

    try:

        if approach == "full":

            result = subprocess.run(
                [
                    "python3",
                    "kafka_producer_full.py",
                    model
                ],
                capture_output=True,
                text=True
            )

        else:

            result = subprocess.run(
                [
                    "python3",
                    "kafka_producer_meta.py"
                ],
                capture_output=True,
                text=True
            )

    except Exception as e:

        end_time = time.perf_counter()

        return (
            end_time - start_time,
            False,
            str(e),
            ""
        )

    end_time = time.perf_counter()

    elapsed = end_time - start_time

    output = result.stdout
    error = result.stderr

    success = result.returncode == 0

    return (
        elapsed,
        success,
        error,
        output
    )


# ============================================================
# SAVE RESULT
# ============================================================

def save_result(
    approach,
    condition,
    delay,
    bandwidth,
    latency,
    throughput,
    model,
    model_size,
    devices,
    status,
    failure_reason
):

    if bandwidth is None:

        bandwidth_value = "Unlimited"

    else:

        bandwidth_value = bandwidth.replace("mbit", "")

    with open(
        RESULTS_FILE,
        "a",
        newline=""
    ) as f:

        writer = csv.writer(f)

        writer.writerow([
            approach,
            condition,
            delay.replace("ms", ""),
            bandwidth_value,
            round(latency * 1000, 3)
            if latency is not None else "",
            round(throughput, 3)
            if throughput is not None else "",
            model,
            round(model_size, 2)
            if model_size is not None else "",
            devices,
            status,
            failure_reason
        ])


# ============================================================
# MAIN EXPERIMENT
# ============================================================

print("\n")
print("=" * 70)
print("KAFKA MODEL DISTRIBUTION EXPERIMENT")
print("=" * 70)

tc_available = check_tc()


for approach in approaches:

    print("\n")
    print("=" * 70)
    print(f"TESTING APPROACH: {approach.upper()}")
    print("=" * 70)

    for condition, settings in network_conditions.items():

        print("\n")
        print("-" * 70)
        print(f"NETWORK CONDITION: {condition.upper()}")
        print("-" * 70)

        delay = settings["delay"]
        bandwidth = settings["bandwidth"]

        # ----------------------------------------------------
        # APPLY NETWORK CONDITION
        # ----------------------------------------------------

        if tc_available:

            network_applied = apply_network_condition(
                delay,
                bandwidth
            )

        else:

            network_applied = False

            if condition != "normal":

                print(
                    "WARNING: Network condition was NOT applied."
                )

        for model in model_files:

            model_size = get_model_size_mb(model)

            for devices in device_counts:

                print("\n")
                print("=" * 70)

                print(
                    f"TEST: Kafka | "
                    f"{approach.upper()} | "
                    f"{condition.upper()} | "
                    f"{model} | "
                    f"devices={devices}"
                )

                print("=" * 70)

                # ------------------------------------------------
                # START CONSUMERS
                # ------------------------------------------------

                consumers = start_consumers(
                    approach,
                    devices
                )

                time.sleep(3)

                # ------------------------------------------------
                # RUN PRODUCER
                # ------------------------------------------------

                latency, success, error, output = run_producer(
                    approach,
                    model
                )

                # ------------------------------------------------
                # CALCULATE THROUGHPUT
                # ------------------------------------------------

                throughput = None

                if (
                    success
                    and approach == "full"
                    and model_size is not None
                    and latency > 0
                ):

                    throughput = (
                        model_size
                        * 8
                        / latency
                    )

                # ------------------------------------------------
                # STATUS
                # ------------------------------------------------

                if success:

                    status = "Success"
                    failure_reason = ""

                else:

                    status = "Failed"

                    failure_reason = error.strip()

                    if not failure_reason:

                        failure_reason = (
                            "Producer process failed"
                        )

                # ------------------------------------------------
                # PRINT OUTPUT
                # ------------------------------------------------

                print("\nProducer output:")

                if output:

                    print(output)

                if error:

                    print("\nProducer error:")

                    print(error)

                print("\nExperiment result:")

                print("Status:", status)

                print(
                    "Latency:",
                    round(latency * 1000, 3),
                    "ms"
                )

                if throughput is not None:

                    print(
                        "Throughput:",
                        round(throughput, 3),
                        "Mbps"
                    )

                else:

                    print(
                        "Throughput: N/A"
                    )

                # ------------------------------------------------
                # SAVE RESULT
                # ------------------------------------------------

                save_result(
                    approach,
                    condition,
                    delay,
                    bandwidth,
                    latency,
                    throughput,
                    model,
                    model_size,
                    devices,
                    status,
                    failure_reason
                )

                # ------------------------------------------------
                # STOP CONSUMERS
                # ------------------------------------------------

                stop_consumers(consumers)

                time.sleep(2)


# ============================================================
# RESET NETWORK
# ============================================================

if tc_available:

    reset_network_condition()


print("\n")
print("=" * 70)
print("ALL KAFKA EXPERIMENTS COMPLETED!")
print("=" * 70)

print("\nResults saved to:")

print(os.path.abspath(RESULTS_FILE))
