#!/usr/bin/env python3
# genrate_flows.py
# coding=utf-8
# Description=Generate Synth Netflow records at diffrent samplings
# UNLICENSED - Private
# ALL RIGHTS RESERVED
# © COPYRIGHT 2024 FERMIHDI LIMITED

"""Generate Synth Netflow"""

__copyright__ = "COPYRIGHT 2024 FERMIHDI LIMITED"
__maintainer__ = "FermiHDI Limited"
__credits__ = ["Craig Yamato"]
__license__ = "UNLICENSED/NOLICENSE - Private"
__status__ = "Development"
__version__ = "0.0.1"

import argparse
from datetime import datetime 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="FermiHDI Generate Synth Netflow"
    )
    parser.add_argument(
        "-t",
        "--time",
        type=int,
        default=600,
        help="Time in seconds, defaults to 600 seconds",
    )
    parser.add_argument(
        "-f",
        "--fps",
        type=int,
        default=200000,
        help="The Flow rate to simulate in the data, defaults to 200000",
    )
    parser.add_argument(
        "-d",
        "--device_sampling_rate",
        type=int,
        default=1000,
        help="The n:1 device flow sampling to be emulated, defaults to 1000",
    )
    parser.add_argument(
        "-V",
        action="version",
        version=__version__,
        help="Display the version and exit",
    )
    parser.add_argument(
        "-x",
        "--exit",
        action="store_true",
        default=False,
        help="Auto exit when done"
    )
    args = parser.parse_args()

    from data_gen import HDDataGeneration
    gen = HDDataGeneration()
    
    gen.make_layout()

    start_time = datetime.now()
    flows_made, device_flows_made = gen.load_random_data(
        time=args.time,
        fps=args.fps,
        _device_sampling_rate=args.device_sampling_rate,
        auto_exit=args.exit,
    )
    end = (datetime.now() - start_time)
    minutes = divmod(end.seconds, 60)

    print(f"Done!")
    print(f"Total raw flows made: {flows_made}")
    print(f"Total device sampled flows made: {device_flows_made}")
    print(f"Time Taken: {minutes[0]} minutes, {minutes[1]} seconds")