#!/usr/bin/env python3
# graph.py
# coding=utf-8
# Description=Pre generate HD data based on Netflow v5 records
# UNLICENSED - Private
# ALL RIGHTS RESERVED
# © COPYRIGHT 2024 FERMIHDI LIMITED

"""Simple Test of a FermiHDI HD RMM."""

__copyright__ = "COPYRIGHT 2024 FERMIHDI LIMITED"
__maintainer__ = "FermiHDI Limited"
__credits__ = ["Craig Yamato"]
__license__ = "UNLICENSED/NOLICENSE - Private"
__status__ = "Development"
__version__ = "0.0.1"

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio 
import os.path
import errno
from random import randrange, choices
from typing import (
    BinaryIO,
    Dict,
    List,
    Optional,
    TextIO,
    Tuple,
    TypedDict,
)

# Kaleido Engine Setup
pio.kaleido.scope.default_format = "png"

# Poltly Image Setup
fig = go.Figure()
fig.to_image(format="png", engine="kaleido")
# fig.write_image("images/fig1.png")

class Graphing:
    """Genrate graphs showing the effect of sampled flows"""
    raw_flow_csv: str = "raw_flow.csv"
    sampled_flow_csv: str = "sampled_flow.csv"
    output_dir: str = ""
    raw_flow_file_path: str = ""
    sampled_flow_file_path: str = ""
    
    def __init__(self, log, output_dir: str = "") -> None:
        """Init graphing class instance.
        Args:
            log: Logging function
            output_dir (Optional[str]): The directory where graphes should be written. Defaults to curent directory.
        """
        self.log = log
        self.output_dir = output_dir
        self.raw_flow_file_path = f"{self.output_dir}/{self.raw_flow_csv}"
        self.sampled_flow_file_path = f"{self.output_dir}/{self.sampled_flow_csv}"
        pd.options.mode.copy_on_write = True
    
    def ip_int_to_string(self, ip: int) -> str:
        """Convert an IP integer to string format
        Args:
            ip (int): The IP integer
        Returns:
            str: The IP string
        """
        ip_bytes: bytes = ip.to_bytes(4, byteorder="big")
        ip_str: str = f"{ip_bytes[0]}.{ip_bytes[1]}.{ip_bytes[2]}.{ip_bytes[3]}"
        
        return ip_str
    
    def get_unit_size(self, max_unit: int) -> Tuple[int, str]:
        div = 1000
        unit = "bps"
        if max_unit > 1000000000000000:
            div = 1000000000000000000
            unit = "Ebps"
        elif max_unit > 1000000000000:
            div = 1000000000000000
            unit = "Tbps"
        elif max_unit > 1000000000:
            div = 1000000000000
            unit = "Gbps"
        elif max_unit > 1000000:
            div = 1000000000
            unit = "Mbps"
        elif max_unit > 1000:
            div = 1000000
            unit = "kbps"
        return (div, unit)
    
    def check_for_data_files(self) -> bool:
        """Check to see if the data files exist.
        Args:
        Returns:
            bool: True if successful, False otherwise
        """
        rc: bool = True if (len(self.output_dir) == 0) or (os.path.isdir(f"{self.output_dir}")) else False
        if not rc:
            filename = f"{self.output_dir} Directory Not Found"
            
        rc = True if (rc) and (os.path.isfile(f"{self.raw_flow_file_path}")) and (os.path.isfile(f"{self.sampled_flow_file_path}")) else False
        if not rc:
            filename = f"File {self.output_dir}"
            filename += self.sampled_flow_csv if os.path.isfile(f"{self.raw_flow_file_path}") else self.raw_flow_csv
            filename += " Not Found"
        
        if not rc:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), filename)
        
        return rc
    
    def load_raw_flows(self) -> bool:
        """ Load the raw flows CSV file into a pandas dataframe. 
        Args:
        Returns:
            bool: True if successful, False otherwise
        """
        rc: bool = self.check_for_data_files()
        if rc:
            try:
                self.raw_flow_df = pd.read_csv(
                    self.raw_flow_file_path,
                    header=0,
                    usecols=[
                        "timestamp", 
                        "srcaddr", 
                        "dstaddr", 
                        "src_as", 
                        "dst_as", 
                        "dOctets",
                        "last",
                        "first",
                    ]
                )
                if self.raw_flow_df.empty:
                    rc = False
                    raise RuntimeError('CSV is empty')
            except Exception as e:
                self.log(f'There was an error in {self.raw_flow_file_path} input: {e}')
        return rc
    
    def load_sampled_flows(self) -> bool:
        """ Load the sampled flows CSV file into a pandas dataframe. 
        Args:
        Returns:
            bool: True if successful, False otherwise
        """
        rc: bool = self.check_for_data_files()
        if rc:
            try:
                self.sampled_flow_df = pd.read_csv(
                    self.sampled_flow_file_path,
                    header=0,
                    usecols=[
                        "timestamp", 
                        "srcaddr", 
                        "dstaddr", 
                        "src_as", 
                        "dst_as", 
                        "dOctets",
                        "last",
                        "first",
                    ]
                )
                if self.sampled_flow_df.empty:
                    rc = False
                    raise RuntimeError('CSV is empty')
            except Exception as e:
                self.log(f'There was an error in {self.sampled_flow_file_path} input: {e}')
        return rc
        
    def select_random_flows(self) -> Tuple[List[int], List[int]]:
        """Select three random flows from the raw flow dataframe to be used in queries
        args:
        Returns:
            Tuple[List[int], List[int]]: A list of srcaddres and src_as if successful, false otherwise
        """
        index: List[int] = [0] * 3
        addr: List[int] = [0] * 3
        asn: List[int] = [0] * 3
        
        q: pd.Series = self.raw_flow_df.query("src_as < 64000").value_counts("src_as").nlargest(10)
        if q.shape[0] > 3:
            for i in range(0, 3):
                while True:
                    _index: int = randrange(0, q.shape[0])
                    # Check if we alrady have this index in our list
                    if _index not in index:
                        asn[i] = q.index[_index]
                        _q: pd.Series = self.raw_flow_df.query(f"src_as == {q.index[_index]}").value_counts("srcaddr").nlargest(1)
                        addr[i] = _q.index[0].item()
                        break
        else:
            index = False
            
        return (addr, asn)
        
    def agg_df(self, df: pd.DataFrame, query_str: str) -> pd.DataFrame:
        """Get an agraggated dataframe of the raw flow dataframe
        args:
            query_str (str): The query string to apply to the dataframe
        Returns:
            pd.DataFrame: A dataframe of the raw flow dataframe
        """
        agg_df: pd.DataFrame = df.query(query_str)
        agg_df["bps"] = agg_df["dOctets"] / (agg_df["last"] - agg_df["first"])
    
        agg_df["timestamp"] = pd.to_datetime(agg_df["timestamp"], unit="ms")
        agg_df.set_index("timestamp", inplace=True)
    
        agg_df = agg_df.resample("1min").mean().reset_index()
    
        return agg_df
        
    def peering_report(self, df: pd.DataFrame, topn: int) -> pd.DataFrame:
        """Generate a peering report for a dataframe
        Args: 
            df (pd.DataFrame): The dataframe to generate a peering report for
            topn (int): The number of top peers to show
        Returns:
            pd.DataFrame: A dataframe of the peering report
        """
        
        df["bps"] = df["dOctets"] / (df["last"] - df["first"])
        
        src_asns_dOctets_: pd.DataFrame = df[["src_as", "bps"]].query("src_as < 64000")
        src_asns_dOctets_min: pd.DataFrame = src_asns_dOctets_.groupby("src_as").min()
        src_asns_dOctets_max: pd.DataFrame = src_asns_dOctets_.groupby("src_as").max()
        src_asns_dOctets: pd.DataFrame = src_asns_dOctets_.groupby("src_as").mean()
        
        dst_asns_dOctets_: pd.DataFrame = df[["dst_as", "bps"]].query("dst_as < 64000")
        dst_asns_dOctets_min: pd.DataFrame = dst_asns_dOctets_.groupby("dst_as").min()
        dst_asns_dOctets_max: pd.DataFrame = dst_asns_dOctets_.groupby("dst_as").max()
        dst_asns_dOctets: pd.DataFrame = dst_asns_dOctets_.groupby("dst_as").mean()
        
        src_asns_adders: pd.DataFrame = df[["src_as", "srcaddr"]].query("src_as < 64000")
        dst_asns_adders: pd.DataFrame = df[["dst_as", "dstaddr"]].query("dst_as < 64000")
        
        src_asns_dOctets.rename(columns={"src_as": "as", "bps": "src_bps"}, inplace=True)
        src_asns_dOctets_min.rename(columns={"src_as": "as", "bps": "src_bps_min"}, inplace=True)
        src_asns_dOctets_max.rename(columns={"src_as": "as", "bps": "src_bps_max"}, inplace=True)
        
        dst_asns_dOctets.rename(columns={"dst_as": "as", "bps": "dst_bps"}, inplace=True)
        dst_asns_dOctets_min.rename(columns={"dst_as": "as", "bps": "dst_bps_min"}, inplace=True)
        dst_asns_dOctets_max.rename(columns={"dst_as": "as", "bps": "dst_bps_max"}, inplace=True)
        
        src_asns_adders.rename(columns={"src_as": "as", "srcaddr": "address"}, inplace=True)
        dst_asns_adders.rename(columns={"dst_as": "as", "dstaddr": "address"}, inplace=True)
        
        asns_adders: pd.DataFrame = pd.concat([src_asns_adders, dst_asns_adders]).groupby("as").nunique()
        del src_asns_adders, dst_asns_adders
        
        asns: pd.DataFrame = pd.concat([
            src_asns_dOctets, 
            src_asns_dOctets_min, 
            src_asns_dOctets_max, 
            dst_asns_dOctets,
            dst_asns_dOctets_min, 
            dst_asns_dOctets_max, 
            asns_adders, 
        ], join="outer", axis=1)
        
        asns["transit"] = pd.Series(choices(["Transit", "Non-Transit"], weights=[1,1], k=asns.shape[0]), index=asns.index) 
        asns["total_bandwidth"] = asns["src_bps"] + asns["dst_bps"]
        asns.index.name = "as"
        del asns_adders, src_asns_dOctets, dst_asns_dOctets
        
        peering_report: pd.DataFrame = asns.sort_values(by="total_bandwidth", ascending=False).head(topn).reset_index()
        del asns
        peering_report["as"] = peering_report["as"].astype(str)
        
        return peering_report
    
    def save_df_as_line_graph_png(self, df: pd.DataFrame, filename: str, title: str, color: str) -> bool:
        """Save a dataframe as a line graph png image file.
        args:
            df (pd.DataFrame): The dataframe to save as a PNG images
            filename: The name of the file to save the dataframe as
        Returns:
            bool: True if successful, False otherwise
        """
        rc: bool = False
        max_unit: int = df["bps"].max()
        div, unit = self.get_unit_size(max_unit=max_unit)
        df["bps"] = df["bps"] / div
        try:
            fig = px.line(
                df, 
                x="timestamp", 
                y="bps", 
                color=color,
                
                labels={"bps": "Ingress Traffic", "srcaddr": "Source IP", "timestamp": "Time", "src_as": "Source AS"},
            )
            fig.update_traces(textposition='top center')
            fig.update_layout(
                title_text=title,
                showlegend=True,
                yaxis_ticksuffix=f" {unit}",
            )
            
            fig.write_image(filename)
            rc = True
        except Exception as e:
            self.log(f'There was an error in {filename} output: {e}')
        
        return rc
    
    def save_peering_df_as_bubble_chart_png(self, df: pd.DataFrame, filename: str) -> bool:
        """Save a dataframe as a bubble chart png image file.
        args:
            df (pd.DataFrame): The dataframe to save as a PNG images
            filename: The name of the file to save the dataframe as
        Returns:
            bool: True if successful, False otherwise
        """
        rc: bool = False
        max_unit: int = df["src_bps"].max()
        src_div, src_unit = self.get_unit_size(max_unit=max_unit)
        df["src_bps"] = df["src_bps"] / src_div
        df["src_bps_min"] = df["src_bps_min"] / src_div
        df["src_bps_max"] = df["src_bps_max"] / src_div
        
        max_unit = df["dst_bps"].max()
        dst_div, dst_unit = self.get_unit_size(max_unit=max_unit)
        df["dst_bps"] = df["dst_bps"] / dst_div
        df["dst_bps_min"] = df["dst_bps_min"] / src_div
        df["dst_bps_max"] = df["dst_bps_max"] / src_div
        
        try:
            fig = px.scatter(
                df,
                x="src_bps",
                error_x="src_bps_max",
                error_x_minus="src_bps_min",
                log_x=True,
                y="dst_bps",
                error_y="dst_bps_max",
                error_y_minus="dst_bps_min",
                log_y=True,
                trendline="ols",
                size="address",
                color="transit",
                text="as",
                labels={"src_bps": "Ingress Traffic", "dst_bps": "Egress Traffic", "address": "Count of unqiue IPs", "transit": "Transit", "as": "ASN"}, 
                hover_name="as",
                hover_data=["src_bps", "dst_bps", "address"],
                width=1000,
                height=1000,
            )
            fig.update_traces(textposition='top center')
            fig.update_layout(
                title_text=f'Peering Report For Top {df.shape[0]} ASNs - Bubble Size = Unqiue IPs',
                showlegend=True,
                xaxis_ticksuffix=f" {src_unit}",
                yaxis_ticksuffix=f" {dst_unit}",
            )
            
            fig.write_image(filename)
            rc = True
            
        except Exception as e:
            self.log(f'There was an error in {filename} output: {e}')
        
        return rc
    
    def save_df_as_csv(self, df: pd.DataFrame, filename: str) -> bool:
        """Save a dataframe as a csv file.
        args:
            df (pd.DataFrame): The dataframe to save as a CSV images
            filename: The name of the file to save the dataframe as
        Returns:
            bool: True if successful, False otherwise
        """
        rc: bool = False
        try:
            df.index.name = "id"
            df.to_csv(filename)
            rc = True
        except Exception as e:
            self.log(f'There was an error in {filename} output: {e}')
        
        return rc
    
    def genrate_reports(self, genrate_peering_report: bool, report_gen_progress, report_gen_job_id, topn: int = 0) -> bool:
        """Generate reports
        Args:
            genrate_peering_report (bool): Whether or not to generate peering reports
            topn (int): The number of top peers to show
        Returns:
            bool: True if successful, False otherwise
        """
        rc: bool = True
        
        if self.check_for_data_files():
            self.log(f"Loading data files")
            self.load_raw_flows()
            report_gen_progress.update(task_id=report_gen_job_id, advance=1)
            self.load_sampled_flows()
            report_gen_progress.update(task_id=report_gen_job_id, advance=1)
            
            self.log(f"Selecting random flows to be used for queries")
            address_queries, as_queries = self.select_random_flows()
            report_gen_progress.update(task_id=report_gen_job_id, advance=1)
            
            agg_src_as_dfs: List[pd.DataFrame] = [None] * 3
            agg_src_adders_dfs: List[pd.DataFrame] = [None] * 3
            
            self.log(f"Generating reports")            
            for i in range(0, 3):
                ip: str = self.ip_int_to_string(ip=address_queries[i])
                self.log(f"Generating report {i+1} of 3 for IP {ip}")
                agg_src_adders_dfs[i] = self.agg_df(df=self.raw_flow_df, query_str=f"srcaddr == {address_queries[i]}")
                if not self.save_df_as_line_graph_png(
                    df=agg_src_adders_dfs[i], 
                    filename=f"{self.output_dir}line_graph_for_ip_{address_queries[i]}.png", 
                    title=f"Traffic for source IP {ip}", 
                    color="srcaddr"
                ):
                    rc = False
                    break
                if not self.save_df_as_csv(df=agg_src_adders_dfs[i], filename=f"{self.output_dir}ip_{address_queries[i]}.csv"):
                    rc = False
                    break
                report_gen_progress.update(task_id=report_gen_job_id, advance=1)
                                
                self.log(f"Generating report {i+1} of 3 for ASN {as_queries[i]}")
                agg_src_as_dfs[i] = self.agg_df(df=self.raw_flow_df, query_str=f"src_as == {as_queries[i]}")
                if not self.save_df_as_line_graph_png(
                    df=agg_src_as_dfs[i], 
                    filename=f"{self.output_dir}line_graph_for_as_{as_queries[i]}.png", 
                    title=f"Trafic for source ASN {as_queries[i]}", 
                    color="src_as"
                ):
                    rc = False
                    break
                if not self.save_df_as_csv(df=agg_src_as_dfs[i], filename=f"{self.output_dir}as_{as_queries[i]}.csv"):
                    rc = False
                    break
                report_gen_progress.update(task_id=report_gen_job_id, advance=1)
            
            if rc and genrate_peering_report:
                self.log(f"Generating peering reports")
                peering_report_df: pd.DataFrame = self.peering_report(df=self.raw_flow_df, topn=topn)
                if not self.save_peering_df_as_bubble_chart_png(df=peering_report_df, filename=f"{self.output_dir}peering_report.png"):
                    rc = False
                if rc and not self.save_df_as_csv(df=peering_report_df, filename=f"{self.output_dir}peering_report.csv"):
                    rc = False
                report_gen_progress.update(task_id=report_gen_job_id, advance=1)

        return rc
         