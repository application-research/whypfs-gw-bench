#!/usr/bin/env python3

import argparse
import datetime
import os
import subprocess
import time
import sys

testfile_filename = "why-random-50m"
#testfile_filename = "ubuntu-22.04.1-desktop-amd64.iso"

real_file_size = os.path.getsize(testfile_filename) / 1024 / 1024
num_successes = 0

def stop_gateway():
    if not args.silent:
        print("Stopping WhyPFS Gateway")
    subprocess.run(["sudo", "systemctl", "stop", "whypfs-gateway"])
    time.sleep(1)
    status = subprocess.run(["sudo", "systemctl", "is-active", "--quiet", "whypfs-gateway"])
    if status.returncode == 0:
        print("Error: Failed to stop whypfs-gateway.")
        exit(1)


def start_gateway():
    if not args.silent:
        print("Starting WhyPFS Gateway")
    subprocess.run(["sudo", "systemctl", "start", "whypfs-gateway"])


def remove_folder():
    if not args.silent:
        print("Removing folder /mnt/mfs/.whypfs")
    subprocess.run(["sudo", "rm", "-r", "/mnt/mfs/.whypfs"])


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


def run_upload():
    global num_successes
    start_time = time.monotonic()
    result = subprocess.run(
        ["curl", "-X", "POST", "-F", "file=@" + testfile_filename, "localhost:1313/upload"],
        capture_output=True, text=True)
    end_time = time.monotonic()
    transfer_time = end_time - start_time
    if result.returncode == 0:
        output = result.stdout.strip()
        if output.startswith("baf"):
            num_successes += 1
            return transfer_time
        else:
            print("Error: Failed to upload file.")
    else:
        print("Error: Failed to run curl command.")


def print_report(run_number, transfer_time):
    total_data = real_file_size * 1024 * 1024
    transfer_rate = total_data / transfer_time
    # We're converting from bytes/s to mbps here.
    mbps = transfer_rate / 1024 / 1024 * 8 * 1.049
    print(f"\n=== Run {run_number} ===")
    print(f"Filename: " + testfile_filename)
    print(f"Data transferred: {total_data / 1024 / 1024:.2f} MiB")
    print(f"Transfer time: {transfer_time:.2f} seconds")
    print(f"Transfer rate: {mbps:.2f} mbps")

def save_report(run_number, transfer_time):
    total_data = real_file_size * 1024 * 1024
    transfer_rate = total_data / transfer_time
    mbps = transfer_rate / 1024 / 1024 * 8 * 1.049
    report_run_number = "{:03d}".format(run_number)
    report_file_name = f"report-{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}-MooseFS-1c1t-{report_run_number}.txt"
    report_file = open(report_file_name, 'w')

    if report_file is not None:
        report_file.write(f"\n=== Run {run_number} ===\n")
        print(f"Filename: " + testfile_filename)
        report_file.write(f"Data transferred: {total_data / 1024 / 1024:.2f} MiB\n")
        report_file.write(f"Transfer time: {transfer_time:.2f} seconds\n")
        report_file.write(f"Transfer rate: {mbps:.2f} mbps\n")
        report_file.close()

def run_continuous(num_runs):
    best_time = None
    best_speed = None
    slowest_time = None
    slowest_speed = None
    total_time = 0
    total_speed = 0
    for i in range(num_runs):
        print(f"\nRunning test {i + 1}...")
        stop_gateway()
        remove_folder()
        start_gateway()
        wait_for_server()
        transfer_time = run_upload()
        print_report(i+1, transfer_time)
        if args.report:
            save_report(i+1, transfer_time)
        total_data = real_file_size * 1024 * 1024
        total_time += transfer_time
        total_speed += total_data / transfer_time
        if best_time is None or transfer_time < best_time:
            best_time = transfer_time
            best_speed = total_data / transfer_time
        if slowest_time is None or transfer_time > slowest_time:
            slowest_time = transfer_time
            slowest_speed = total_data / transfer_time
    average_time = total_time / num_runs
    average_speed = total_speed / num_runs

    overall_data_transferred = num_runs * real_file_size * 1024 * 1024
    overall_data_transferred_MiB = overall_data_transferred / (1024 * 1024)

    transfer_rate = overall_data_transferred / total_time
    mbps = transfer_rate / 1024 / 1024 * 8

    print(f"\n=== Final Report ===")
    print(f"We moved {overall_data_transferred_MiB}MiB in {round(total_time, 3):.2f} seconds")
    print(f"That's a transfer rate of {mbps:.2f} mbps.")
    print(f"\nWe performed {num_runs} total runs, {num_successes} of which succeeded.")
    print(f"That's a success rate of {num_successes/num_runs*100:.2f}%.")
    print(f"\nBest time: {best_time:.2f} seconds")
    print(f"Best speed: {best_speed / 1024 / 1024 * 8:.2f} mbps")
    print(f"Slowest time: {slowest_time:.2f} seconds")
    print(f"Slowest speed: {slowest_speed / 1024 / 1024 * 8:.2f} mbps")
    print(f"Average time: {average_time:.2f} seconds")
    print(f"Average speed: {average_speed / 1024 / 1024 * 8:.2f} mbps")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the performance of MooseFS with whyPFS gateway.")
    parser.add_argument("-c", "--continuous", metavar="N", type=int, help="run continuously N times", default=1)
    parser.add_argument("-r", "--report", help="produce reports", action=argparse.BooleanOptionalAction)
    parser.add_argument("-s", "--silent", help="run silently - only produce reports", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    if args.report is not None:
        report_file = open(args.report, "w")
        sys.stdout = report_file

    if args.continuous == 1:
        stop_gateway()
        remove_folder()
        start_gateway()
        wait_for_server()
        transfer_time = run_upload()
        print_report(1, transfer_time)
        if args.report:
            save_report(1, transfer_time)
    else:
        run_continuous(args.continuous)

    if args.report is not None:
        sys.stdout = sys.__stdout__
        report_file.close()
