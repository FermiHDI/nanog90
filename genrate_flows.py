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
from typing import (
    List,
    Tuple,
)

from blessed import Terminal
from rich.align import Align
from rich.console import (
    Console,
    Group,
    ConsoleOptions,
    RenderResult,
    RenderableType,
)
from rich.highlighter import ReprHighlighter
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
)
from rich.style import StyleType
from rich.table import Table
from rich.text import Text

class LoggingWindow:
    """An internal renderable used as a Layout logger."""

    highlighter = ReprHighlighter()
    logs: List[str]

    def __init__(self, style: StyleType = "") -> None:
        """Rich Renderable Logging Window.

        Args:
            style (StyleType, optional): Rich style. Defaults to "".
        """
        self.style = style
        self.logs = []

    def append(self, message: RenderableType) -> None:
        """Add new element to be rendered.

        Args:
            message (RenderableType): The Rich renderable to add
        """
        self.logs.append(message)

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        """Call by console.

        Args:
            console (Console): Rich console
            options (ConsoleOptions): Rich console metadata

        Returns:
            RenderResult: Rich rendering info

        Yields:
            Iterator[RenderResult]: Rich rendering info
        """
        height = options.height or options.size.height
        while len(self.logs) > height - 2:
            self.logs.pop(0)
        yield Panel(Group(*self.logs), title="Logs", title_align="left")

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
        default=True,
        help="Genrate perring reports",
    )
    parser.add_argument(
        "--topN",
        type=int,
        default=10,
        help="The length of the Top N elements to use in reports, defualts to 10",
    )
    parser.add_argument(
        "--rich",
        action=argparse.BooleanOptionalAction,
        type=bool,
        default=False,
        help="Use rich UI (Not advisable under docker)",
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
        
    total_flows_to_make = 0
    flows_per_ms = 1
    if not args.reports_only:
        flows_per_ms = args.fps // 1000
        flows_per_ms = flows_per_ms if flows_per_ms > 0 else 1
        total_flows_to_make = (args.time * 1000 * flows_per_ms)
    
    layout = Layout(name="root")
    layout.split(
        Layout(name="header", size=3),
        Layout(name="main", ratio=1),
        Layout(name="footer", size=3),
    )
    layout["main"].split_row(
        Layout(name="body", ratio=2),
        Layout(name="side", minimum_size=20),
    )
    layout["side"].split(
        Layout(name="info"),
        Layout(name="meta", size=6),
        Layout(name="commands"),
    )
    layout["header"].update(
        Panel(Text("FermiHDI Flow Genrator", justify="center"))
    )
    layout["footer"].update(
        Panel(Text("© COPYRIGHT 2024 FERMIHDI LIMITED", justify="center"))
    )
    logging_window = LoggingWindow()
    layout["body"].update(logging_window)
        
    # Setup the info display
    rt_progress = Progress(
        "{task.description}",
        SpinnerColumn(),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        expand=False,
    )
    rt_job_id = rt_progress.add_task("[cyan]Route Table ", total=4)
    
    job_progress = Progress(
        "{task.description}",
        SpinnerColumn(),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        expand=False,
    )
    cd_job_id = job_progress.add_task(
        "[cyan]Generating Data", total=total_flows_to_make
    )
    
    report_gen_progress = Progress(
        "{task.description}",
        SpinnerColumn(),
        BarColumn(),
        TextColumn("{task.completed} of {task.total}"),
        expand=False,
    )
    report_jobs = 15
    if args.peering_report:
        report_jobs = 16
    report_gen_job_id = report_gen_progress.add_task(
        "[cyan]Generating Reports ", total=report_jobs
    )
    
    if args.no_reports:
        layout["meta"].update(
            Panel(
                Align.center(
                    Group(rt_progress, job_progress), vertical="middle"
                )
            )
        )
    
    if args.reports_only:
        layout["meta"].update(
            Panel(
                Align.center(
                    Group(report_gen_progress), vertical="middle"
                )
            )
        )
    
    if not args.no_reports and not args.reports_only:
        layout["meta"].update(
            Panel(
                Align.center(
                    Group(rt_progress, job_progress, report_gen_progress), vertical="middle"
                )
            )
        )

    info_table = Table(box=None)
    info_table.add_column(justify="left", no_wrap=True)
    info_table.add_column(justify="left", no_wrap=True)
    info_table.add_row("Total Flow Records:", f"{total_flows_to_make:n}")
    write_dir="Curent Directory" if len(data_dir) == 0 else data_dir
    info_table.add_row("Writing files to:", f"{write_dir}")
    info_table.add_row("Started at:", f"{datetime.utcnow()}",)
    layout["info"].update(
        Panel(info_table, title="Information")
    )

    command_table = Table(box=None, title="Commands")
    command_table.add_column(justify="left", no_wrap=True)
    command_table.add_column(justify="left", no_wrap=True)
    command_table.add_row("q:", "Exit")
    layout["commands"].update(Panel(command_table))

    if args.rich:
        def log(message: str) -> None:
            """Print a log.

            Args:
                message (str): log message
            """
            logging_window.append(
                Text.assemble(
                    (
                        f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z",
                        "cyan",
                    ),
                    f" {message}",
                )
            )
    else:
        def log(message: str) -> None:
            """Print a log.

            Args:
                message (str): log message
            """
            print(f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z {message}")
            
    total_start_time = datetime.now()
    
    if args.rich:
        try:
            with Live(layout, refresh_per_second=10, screen=True):
                if not args.reports_only:
                    from data_gen import DataGeneration
                    gen = DataGeneration(log=log)

                    start_time = datetime.now()
                    
                    log("Getting ASNs")
                    asn_table = gen.get_asns()
                    rt_progress.update(task_id=rt_job_id, advance=1)
                    log("Selecting ASNs")
                    selected_asns = gen.random_asns(
                        asn_table=asn_table, asns_to_select=1000
                    )
                    rt_progress.update(task_id=rt_job_id, advance=1)
                    log("Building Route Table")
                    gen.make_route_table(asns=selected_asns)
                    rt_progress.update(task_id=rt_job_id, advance=1)
                    log("Building Server Table")
                    gen.build_server_ip_table(
                        from_ip=gen.SERVER_RANGE[0], to_ip=gen.SERVER_RANGE[1]
                    )
                    rt_progress.update(task_id=rt_job_id, advance=1)
                    gen.log("Generating Flow Data")
                    total_flows_made, total_sampled_flows_made = gen.generate_data(
                        flows_to_make=total_flows_to_make,
                        flows_per_ms=flows_per_ms,
                        job_progress=job_progress,
                        job_task=cd_job_id,
                        sampling_rate=args.sampling_rate,
                        data_dir=data_dir,
                    )
                    job_progress.update(task_id=cd_job_id, advance=total_flows_to_make)
                    
                    end = (datetime.now() - start_time)
                    minutes = divmod(end.seconds, 60)
                    
                if not args.no_reports:
                    from graph import Graphing
                    reports = Graphing(output_dir=data_dir, log=log)
                    
                    start_time = datetime.now()
                    reports.genrate_reports(
                        genrate_peering_report=args.peering_report, 
                        topn=args.topN, 
                        report_gen_progress=report_gen_progress, 
                        report_gen_job_id=report_gen_job_id
                    )
                    end = (datetime.now() - start_time)
                    minutes = divmod(end.seconds, 60)
                                    
                total_end = (datetime.now() - total_start_time)
                total_minutes = divmod(total_end.seconds, 60)
                
                info_table.add_row("Completed at:", f"{datetime.utcnow()}")
                info_table.add_row("Elapsed time:", f"{total_minutes[0]} minutes, {total_minutes[1]} seconds")
                
                if not args.exit:
                    term = Terminal()
                    with term.cbreak():
                        val = ""
                        while val not in (
                            "q",
                            "Q",
                        ):
                            val = term.inkey()
                            if val.is_sequence:  # type: ignore
                                if val.name == "KEY_ESCAPE" or val.name == "KEY_BACKSPACE":  # type: ignore
                                    break
                                    
        except KeyboardInterrupt as e:
            log(f"Keyboard interrupt: {e}")
    
    else:
        if not args.reports_only:
            from data_gen import DataGeneration
            gen = DataGeneration(log=log)

            start_time = datetime.now()
            
            log("Getting ASNs")
            asn_table = gen.get_asns()
            log("Selecting ASNs")
            selected_asns = gen.random_asns(
                asn_table=asn_table, asns_to_select=1000
            )
            log("Building Route Table")
            gen.make_route_table(asns=selected_asns)
            log("Building Server Table")
            gen.build_server_ip_table(
                from_ip=gen.SERVER_RANGE[0], to_ip=gen.SERVER_RANGE[1]
            )
            gen.log("Generating Flow Data")
            total_flows_made, total_sampled_flows_made = gen.generate_data(
                flows_to_make=total_flows_to_make,
                flows_per_ms=flows_per_ms,
                job_progress=job_progress,
                job_task=cd_job_id,
                sampling_rate=args.sampling_rate,
                data_dir=data_dir,
            )
            
            end = (datetime.now() - start_time)
            minutes = divmod(end.seconds, 60)
            
        if not args.no_reports:
            from graph import Graphing
            reports = Graphing(output_dir=data_dir, log=log)
            
            start_time = datetime.now()
            reports.genrate_reports(
                genrate_peering_report=args.peering_report, 
                topn=args.topN, 
                report_gen_progress=report_gen_progress, 
                report_gen_job_id=report_gen_job_id
            )
            end = (datetime.now() - start_time)
            minutes = divmod(end.seconds, 60)
                            
        total_end = (datetime.now() - total_start_time)
        total_minutes = divmod(total_end.seconds, 60)
                
    
    print(f"Done!")
    print(f"Total Time Taken: {total_minutes[0]} minutes, {total_minutes[1]} seconds")
    
    if not args.reports_only:
        print(f"Total raw flows made: {total_flows_made}")
        print(f"Total sampled flows made: {total_sampled_flows_made}")
        print(f"Time taken to genrate flows: {minutes[0]} minutes, {minutes[1]} seconds")
        
    if not args.no_reports:
        print(f"Time taken to make reports: {minutes[0]} minutes, {minutes[1]} seconds")