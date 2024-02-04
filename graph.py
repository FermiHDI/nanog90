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
from random import randrange
import numpy as np
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

class graphing:
    """Genrate graphs showing the effect of sampled flows"""
    raw_flow_csv: str = "raw_flow.csv"
    sampled_flow_csv: str = "sampled_flows.csv"
    output_dir: str = ""
    raw_flow_file_path: str = ""
    sampled_flow_file_path: str = ""
    
    def __init__(self, output_dir: str = "") -> None:
        """Init graphing class instance.
        Args:
            output_dir (Optional[str]): The directory where graphes should be written. Defaults to curent directory.
        """
        self.output_dir = output_dir
        self.raw_flow_file_path = f"{self.output_dir}/{self.raw_flow_csv}"
        self.sampled_flow_file_path = f"{self.output_dir}/{self.sampled_flow_csv}"
        pd.options.mode.copy_on_write = True
        
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
            filename = "File "
            filename += self.raw_flow_csv if os.path.isfile(f"{self.raw_flow_file_path}") else self.sampled_flow_csv
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
                        "dOctets"
                    ]
                ).set_index("timestamp")
                if self.raw_flow_df.empty:
                    rc = False
                    raise RuntimeError('CSV is empty')
            except Exception as e:
                print(f'There was an error in {self.raw_flow_file_path} input: {e}')
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
                    self.sampled_flow_csv_flow_file_path,
                    header=0,
                    usecols=[
                        "timestamp", 
                        "srcaddr", 
                        "dstaddr", 
                        "src_as", 
                        "dst_as", 
                        "dOctets"
                    ]
                ).set_index("timestamp")
                if self.sampled_flow_df.empty:
                    rc = False
                    raise RuntimeError('CSV is empty')
            except Exception as e:
                print(f'There was an error in {self.sampled_flow_file_path} input: {e}')
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
        
        if self.raw_flow_df.shape[0] > 3:
            q: pd.Series = self.raw_flow_df.query("src_as < 64000").value_counts("src_as").nlargest(10)
            for i in range(0, 3):
                while True:
                    _index: int = randrange(0, q.shape[0])
                    # Check if we alrady have this index in our list
                    if _index not in index:
                        asn[i] = q.index[_index]
                        q = self.raw_flow_df.query(f"src_as == {q.index[_index]}").value_counts("srcaddr").nlargest(10)
                        addr[i] = q.index[_index]
                        break
        else:
            index = False
        
        return (addr, asn)
            
    def bytes_to_megabytes(self, bytes_) -> float:
        """Convert bytes to megabytes.
        args:
            bytes_ (int): The number of bytes to convert
        Returns:
            float: The number of megabytes
        """
        return bytes_ / 1000000.0
        
    def agg_df(self, query_str: str) -> pd.DataFrame:
        """Get an agraggated dataframe of the raw flow dataframe
        args:
            query_str (str): The query string to apply to the dataframe
        Returns:
            pd.DataFrame: A dataframe of the raw flow dataframe
        """
        agg_df: pd.DataFrame = self.raw_flow_df.query(query_str)
    
        agg_df["timestamp"] = pd.to_datetime(agg_df["timestamp"], unit="ms")
        agg_df.set_index("timestamp", inplace=True)
    
        agg_df = agg_df[["dOctets"]].resample("1min").sum().reset_index()
    
        bytes_: pd.Series = agg_df["dOctets"]
        megabytes_: pd.Series = bytes_.apply(self.bytes_to_megabytes)
        agg_df["dOctets"] = megabytes_
    
        del bytes_, megabytes_
    
        return agg_df
        
    def peering_report(self, df: pd.DataFrame, topn: int) -> pd.DataFrame:
        """Generate a peering report for a dataframe
        Args: 
            df (pd.DataFrame): The dataframe to generate a peering report for
            topn (int): The number of top peers to show
        Returns:
            pd.DataFrame: A dataframe of the peering report
        """
        
        # We are cheating here and asuming that all flows took excatly 1 sec
        
        src_asns_dOctets: pd.DataFrame = df[["src_as", "dOctets"]].query("src_as < 64000").groupby("src_as").sum()
        dst_asns_dOctets: pd.DataFrame = df[["dst_as", "dOctets"]].query("dst_as < 64000").groupby("dst_as").sum()
        src_asns_adders: pd.DataFrame = df[["src_as", "srcaddr"]].query("src_as < 64000")
        dst_asns_adders: pd.DataFrame = df[["dst_as", "dstaddr"]].query("dst_as < 64000")
        
        src_asns_dOctets.rename(columns={"src_as": "as", "dOctets": "src_dOctets"}, inplace=True)
        dst_asns_dOctets.rename(columns={"dst_as": "as", "dOctets": "dst_dOctets"}, inplace=True)
        
        src_asns_adders.rename(columns={"src_as": "as", "srcaddr": "address"}, inplace=True)
        dst_asns_adders.rename(columns={"dst_as": "as", "dstaddr": "address"}, inplace=True)
        asns_adders: pd.DataFrame = pd.concat([src_asns_adders, dst_asns_adders]).groupby("as").nunique()
        del src_asns_adders, dst_asns_adders
        
        asns: pd.DataFrame = pd.concat([src_asns_dOctets, dst_asns_dOctets, asns_adders], join="outer", axis=1)
        
        _bytes: pd.Series = asns["src_dOctets"]
        _megabytes: pd.Series = _bytes.apply(self.bytes_to_megabytes)
        asns["src_dOctets"] = _megabytes
        
        _bytes = asns["dst_dOctets"]
        _megabytes: pd.Series = _bytes.apply(self.bytes_to_megabytes)
        asns["dst_dOctets"] = _megabytes
        
        asns["transit"] = np.random.randint(low=0, high=2, size=asns.shape[0])
        asns.index.name = "as"
        
        return asns
    
    def save_df_as_line_graph_png(self, df: pd.DataFrame, filename: str) -> bool:
        """Save a dataframe as a line graph png image file.
        args:
            df (pd.DataFrame): The dataframe to save as a PNG images
            filename: The name of the file to save the dataframe as
        Returns:
            bool: True if successful, False otherwise
        """
        rc: bool = False
        try:
            fig = go.Figure(
                data=[
                    go.Table(
                        header=dict(
                            values=list(df.columns),
                            fill_color='paleturquoise',
                            align='left'
                        ),
                        cells=dict(
                            values=df.values.tolist(),
                            fill_color='lavender',
                            align='left'
                        )
                    )
                ]
            )
            fig.write_image(filename)
            rc = True
        except Exception as e:
            print(f'There was an error in {filename} output: {e}')
        
        return rc
    
    def save_df_as_bubble_chart_png(self, df: pd.DataFrame, filename: str) -> bool:
        """Save a dataframe as a bubble chart png image file.
        args:
            df (pd.DataFrame): The dataframe to save as a PNG images
            filename: The name of the file to save the dataframe as
        Returns:
            bool: True if successful, False otherwise
        """
        rc: bool = False
        try:
            fig = px.scatter(
                df,
                x="src_dOctets",
                y="dst_dOctets",
                size="transit",
                color="transit",
                hover_name="as",
                hover_data=["src_dOctets", "dst_dOctets", "transit"]
            )
            fig.write_image(filename)
            rc = True
        except Exception as e:
            print(f'There was an error in {filename} output: {e}')
        
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
            df.to_csv(filename)
            rc = True
        except Exception as e:
            print(f'There was an error in {filename} output: {e}')
        
        return rc
        