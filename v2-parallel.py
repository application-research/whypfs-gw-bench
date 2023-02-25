#!/usr/bin/env python3
# Let's run everything in parallel this time

import argparse
import datetime
import os
import subprocess
import time
import sys
import concurrent.futures
from threading import Thread

num_successes = 0

def generate_testfile(thread_num):
    if not args.silent:
        print(f"Generating testfile for thread {thread_num}")
    subprocess.run(["dd", "if=/dev/urandom", f"of=testfile-{thread_num:03}.bin", f"bs={args.blobsize}M", "count=1"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def upload_thread(thread_num, testfile):
    global num_successes
    start_time = time.monotonic()
    result = subprocess.run(
        ["curl", "-X", "POST", "-F", "file=@" + testfile, "localhost:1313/upload"],
        capture_output=True, text=True)
    end_time = time.monotonic()
    transfer_time = end_time - start_time
    if result.returncode == 0:
        output = result.stdout.strip()
        if output.startswith("baf"):
            # We successfully uploaded a file using this thread, record a victory!
            num_successes += 1
            # If we're not in silent mode, output a helpful message.
            if not args.silent:
                print(f"Thread {thread_num}: Upload succeeded, took {transfer_time:.2f} seconds end-to-end")
            return transfer_time
        else:
            print(f"Error: Failed to upload file for thread {thread_num}.")
            print(output)
    else:
        print(f"Error: Failed to run curl command for thread {thread_num}.")
        print(result)
    return None


def run_upload(run_number):
    global num_successes
    num_successes = 0
    threads = []
    slowest_time = float("-inf")
    fastest_time = float("inf")
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
        # Generate test files in parallel
        executor.map(generate_testfile, range(args.threads))
        # Wait for final disk activity to settle
        time.sleep(1)
        # Wait for a file to appear telling this group of threads to run
        while not os.path.exists("/tmp/trigger-" + str(run_number)):
            #print("looking for /mnt/mfs/trigger-" + str(run_number))
            time.sleep(0.04)
        # Record the start time
        start_time = time.monotonic()
        # Upload files and record transfer times
        futures = [executor.submit(upload_thread, i, f"testfile-{i:03}.bin") for i in range(args.threads)]
        for future in concurrent.futures.as_completed(futures):
            transfer_time = future.result()
            if transfer_time is not None:
                if transfer_time > slowest_time:
                    slowest_time = transfer_time
                if transfer_time < fastest_time:
                    fastest_time = transfer_time

    end_time = time.monotonic()
    transfer_time = end_time - start_time
    return transfer_time, slowest_time, fastest_time, num_successes


def stop_gateway():
    if not args.silent:
        print("Stopping WhyPFS Gateway")
    subprocess.run(["sudo", "systemctl", "stop", "whypfs-gateway"])
    subprocess.run(["sudo", "systemctl", "stop", "whypfs-gateway-seaweed"])
    time.sleep(1)
    status = subprocess.run(["sudo", "systemctl", "is-active", "--quiet", "whypfs-gateway"])
    if status.returncode == 0:
        print("Error: Failed to stop whypfs-gateway.")
        exit(1)


def start_gateway():
    if not args.silent:
        print("Starting WhyPFS Gateway")
    if args.label == "MooseFS":
        subprocess.run(["sudo", "systemctl", "start", "whypfs-gateway"])
    else:
        subprocess.run(["sudo", "systemctl", "start", "whypfs-gateway-seaweed"])


def remove_folder():
    if not args.silent:
        print("Removing folder .whypfs")
    if args.label == "MooseFS":
        subprocess.run(["sudo", "rm", "-r", "/mnt/mfs/.whypfs"])
    else:
        subprocess.run(["sudo", "rm", "-r", "/mnt/seaweedfs/.whypfs"])
    


def wait_for_server():
    if not args.silent:
        print("Checking for IPFS Gateway liveness...")
    while True:
        try:
            if not args.silent:
                print("Running check...")
            output = subprocess.check_output(
                ["curl", "-s", "-f", "http://localhost:1313/gw/ipfs/QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o"])
            if output.decode("utf-8").strip() == "hello world":
                if not args.silent:
                    print("\"Hello world\" found, which means IPFS is live. Continuing...")
                break
        except:
            #print("Did not find the string we expected. Trying again...")
            time.sleep(1)
            pass


def print_report(run_number, transfer_time, slowest_time, fastest_time):
    if args.threads == 1:
        total_data = args.blobsize * 1024 * 1024
    else:
        total_data = args.threads * args.blobsize * 1024 * 1024
    if args.threads > 1:
        total_data_success = total_data * (num_successes / args.threads)
    transfer_rate = total_data / transfer_time
    # We're converting from bytes/s to mbps here.
    mbps = transfer_rate / 1024 / 1024 * 8
    # Now let's apply a modifier which is the bandwidth discounting failed threads, but only when we have more than 1 thread
    if args.threads > 1:
        mbps = mbps * (num_successes / args.threads)
    print(f"\n=== Run {run_number} ===")
    if args.threads == 1:
        print(f"Filename: testfile-000.bin")
    else:
        print("Filename: testfile-[threadid].bin")
        print(f"\nWe performed {args.threads} uploads across {args.threads} threads, {num_successes} of which succeeded.")
        print(f"That's a success rate of { (num_successes / args.threads) * 100:.2f}%.")
    print(f"Data transferred: {total_data / 1024 / 1024:.2f} MiB")
    if args.threads > 1:
        print(f"Data successfully transferred: {total_data_success / 1024 / 1024:.2f} MiB")
        print(f"Slowest thread: {slowest_time:.2f} seconds")
        print(f"Fastest thread: {fastest_time:.2f} seconds")
    print(f"Transfer time: {transfer_time:.2f} seconds")
    print(f"Transfer rate: {mbps:.2f} mbps")


def save_report(run_number, transfer_time, slowest_time, fastest_time):
    if args.threads == 1:
        total_data = args.blobsize * 1024 * 1024
    else:
        total_data = args.threads * args.blobsize * 1024 * 1024
    if args.threads > 1:
        total_data_success = total_data * (num_successes / args.threads)
    transfer_rate = total_data / transfer_time
    # We're converting from bytes/s to mbps here.
    mbps = transfer_rate / 1024 / 1024 * 8
    report_run_number = "{:03d}".format(run_number)
    report_file_name = f"report-{report_timestamp}-{args.label}-{report_run_number}.txt"
    report_file = open(report_file_name, 'w')

    if report_file is not None:
        report_file.write(f"\n=== Run {run_number} ===\n")
        
        if args.threads == 1:
            report_file.write(f"Filename: testfile-000.bin")
        else:
            report_file.write("Filename: testfile-[threadid].bin")
            report_file.write(f"\nWe performed {args.threads} uploads across {args.threads} threads, {num_successes} of which succeeded.")
            report_file.write(f"That's a success rate of { (num_successes / args.threads) * 100:.2f}%.")

        report_file.write(f"Data transferred: {total_data / 1024 / 1024:.2f} MiB\n")
        if args.threads > 1:
            report_file.write(f"Data successfully transferred: {total_data_success / 1024 / 1024:.2f} MiB")
            report_file.write(f"Slowest thread: {slowest_time:.2f} seconds")
            report_file.write(f"Fastest thread: {fastest_time:.2f} seconds")
        report_file.write(f"Transfer time: {transfer_time:.2f} seconds\n")
        report_file.write(f"Transfer rate: {mbps:.2f} mbps\n")
        report_file.close()


def run_continuous(num_runs):
    global num_successes_total
    num_successes_total = 0
    best_time = None
    best_speed = None
    slowest_time = None
    slowest_speed = None
    total_time = 0
    total_speed = 0
    for i in range(num_runs):
        print(f"\nRunning test {i + 1}...")
        wait_for_server()
        transfer_time = run_upload(i+1)
        # Grab the num_successes reported by our run of upload threads, and add it to the total number of successes.
        num_successes_total += transfer_time[3]
        print_report(i+1, transfer_time[0], transfer_time[1], transfer_time[2])
        if args.report:
            save_report(i+1, transfer_time[0], transfer_time[1], transfer_time[2])
        total_data = args.threads * args.blobsize * 1024 * 1024
        total_time += transfer_time[0]
        total_speed += total_data / transfer_time[0]
        if best_time is None:
            best_time = transfer_time[0]
            best_speed = total_data / transfer_time[0]
        if transfer_time[0] < best_time:
            best_time = transfer_time[0]
            best_speed = total_data / transfer_time[0]
        if slowest_time is None:
            slowest_time = transfer_time[0]
            slowest_speed = total_data / transfer_time[0]
        if transfer_time[0] > slowest_time:
            slowest_time = transfer_time[0]
            slowest_speed = total_data / transfer_time[0]
    average_time = total_time / num_runs
    average_speed = total_speed / num_runs

    # We calculate how much data transfer occurred by considering how much failed as well.
    overall_data_transferred = (num_successes_total / (num_runs * args.threads)) * (num_runs * args.threads) * args.blobsize * 1024 * 1024
    overall_data_transferred_MiB = overall_data_transferred / (1024 * 1024)

    transfer_rate = overall_data_transferred / total_time
    mbps = transfer_rate / 1024 / 1024 * 8

    # Print final report
    print(f"\n=== Final Report ===")
    print(f"We moved {overall_data_transferred_MiB}MiB in {round(total_time, 3):.2f} seconds")
    print(f"That's a transfer rate of {mbps:.2f} mbps.")
    print(f"\nWe performed {num_runs * args.threads} transfers across {num_runs} total run(s), {num_successes_total} of which succeeded.")
    print(f"That's a success rate of { (num_successes_total / (num_runs * args.threads)) *100:.2f}%.")
    print(f"\nBest run time: {best_time:.2f} seconds")
    print(f"Best run speed: {best_speed / 1024 / 1024 * 8:.2f} mbps")
    print(f"Slowest run time: {slowest_time:.2f} seconds")
    print(f"Slowest run speed: {slowest_speed / 1024 / 1024 * 8:.2f} mbps")
    print(f"Average run time: {average_time:.2f} seconds")
    print(f"Average run speed: {average_speed / 1024 / 1024 * 8:.2f} mbps")

    # Save final report
    if args.report:
        print("Saving final report to disk...")
        report_file_name = f"report-{report_timestamp}-{args.label}-final.txt"
        report_file = open(report_file_name, 'w')
        report_file.write(f"\n=== Final Report ===\n")
        report_file.write(f"We moved {overall_data_transferred_MiB}MiB in {round(total_time, 3):.2f} seconds\n")
        report_file.write(f"That's a transfer rate of {mbps:.2f} mbps.\n")
        report_file.write(f"\nWe performed {num_runs * args.threads} transfers across {num_runs} total run(s), {num_successes_total} of which succeeded.\n")
        report_file.write(f"That's a success rate of { (num_successes_total / (num_runs * args.threads)) *100:.2f}%.\n")
        report_file.write(f"\nBest run time: {best_time:.2f} seconds\n")
        report_file.write(f"Best run speed: {best_speed / 1024 / 1024 * 8:.2f} mbps\n")
        report_file.write(f"Slowest run time: {slowest_time:.2f} seconds\n")
        report_file.write(f"Slowest run speed: {slowest_speed / 1024 / 1024 * 8:.2f} mbps\n")
        report_file.write(f"Average run time: {average_time:.2f} seconds\n")
        report_file.write(f"Average run speed: {average_speed / 1024 / 1024 * 8:.2f} mbps\n")
        report_file.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the performance of MooseFS with WhyPFS gateway.")
    parser.add_argument("-c", "--continuous", metavar="N", type=int, help="run continuously N times", default=1)
    parser.add_argument("-t", "--threads", metavar="T", type=int, help="run parallel threads * T", default=1)
    parser.add_argument("-b", "--blobsize", type=int, help="size of file in MiB to generate", default=50)
    parser.add_argument("-r", "--report", help="produce reports", action=argparse.BooleanOptionalAction)
    parser.add_argument("-s", "--silent", help="run silently - only produce reports", action=argparse.BooleanOptionalAction)
    parser.add_argument("-l", "--label", help="label to use for reports", default="MooseFS")
    args = parser.parse_args()

    report_timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S')

    if args.report is not None:
        report_file = open(args.report, "w")
        sys.stdout = report_file

    run_continuous(args.continuous)

    if args.report is not None:
        sys.stdout = sys.__stdout__
        report_file.close()
