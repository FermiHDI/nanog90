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
        "-s",
        "--sampling_rate",
        type=int,
        default=1000,
        help="The n:1 flow sampling to be emulated, defaults to 1000",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default="",
        help="The directory where the data files will be written, defaults to current directory",
    )
    parser.add_argument(
        "-ro",
        "--reports_only",
        action=argparse.BooleanOptionalAction,
        type=bool,
        default=False,
        help="Genrate a reports only",
    )
    parser.add_argument(
        "-nr",
        "--no_reports",
        action=argparse.BooleanOptionalAction,
        type=bool,
        default=False,
        help="Do not genrate any reports",
    )
    parser.add_argument(
        "-pr",
        "--peering_report",
        action=argparse.BooleanOptionalAction,
        type=bool,
        default=False,
        help="Genrate perring reports",
    )
    parser.add_argument(
        "--topN",
        type=int,
        default=10,
        help="The length of the Top N elements to use in reports, defualts to 10",
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
    
    data_dir: str = args.output_dir     
    if (len(args.output_dir) > 0 and not args.output_dir.endswith("/")):
        data_dir += "/"
    
    total_start_time = datetime.now()
    
    if not args.reports_only:
        from data_gen import DataGeneration
        gen = DataGeneration()
        
        gen.make_layout()

        start_time = datetime.now()
        flows_made, sampled_flows_made = gen.load_random_data(
            time=args.time,
            fps=args.fps,
            sampling_rate=args.sampling_rate,
            auto_exit=args.exit,
            data_dir=data_dir
        )
        end = (datetime.now() - start_time)
        minutes = divmod(end.seconds, 60)
        
        print(f"Done genrating data")
        print(f"Total raw flows made: {flows_made}")
        print(f"Total sampled flows made: {sampled_flows_made}")
        print(f"Time Taken: {minutes[0]} minutes, {minutes[1]} seconds")
        
    if not args.no_reports:
        from graph import Graphing
        reports = Graphing()
        
        start_time = datetime.now()
        reports.genrate_reports(output_dir=data_dir, peering_report=args.peering_report, topn=args.topN)
        end = (datetime.now() - start_time)
        minutes = divmod(end.seconds, 60)
        
        print(f"Done making reports")
        print(f"Time Taken: {minutes[0]} minutes, {minutes[1]} seconds")
        
    total_end = (datetime.now() - total_start_time)
    total_minutes = divmod(total_end.seconds, 60)
    
    print(f"Done!")
    print(f"Total Time Taken: {total_minutes[0]} minutes, {total_minutes[1]} seconds")