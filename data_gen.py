#!/usr/bin/env python3
# data_gen.py
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

import csv
import gzip
import ipaddress
import os
import uuid
from random import randint, randrange
from typing import (
    Dict,
    List,
    Optional,
    TextIO,
    Tuple,
    TypedDict,
)
from urllib.request import Request, urlopen

from rich.progress import (
    Progress,
)

class FlowRecord(TypedDict):
    """A Typping object for a Flow Record."""

    timestamp: int
    system_id: str
    srcaddr: int
    dstaddr: int
    nexthop: int
    input: int
    output: int
    dPkts: int
    dOctets: int
    first: int
    last: int
    srcport: int
    dstport: int
    tcp_flags: int
    protocol: int
    tos: int
    src_as: int
    dst_as: int
    src_mask: int
    dst_mask: int

class ASNRoute(TypedDict, total=False):
    """A Typing object for a ASN Route's Metadata."""

    subnet_bits: int
    network_address: int
    broadcast_address: int
    next_hop: int
    ip_range_start: int
    ip_range_end: int
    asn: int
    country: str
    as_description: str
    ifindex: int

class NetworkInterface(TypedDict, total=False):
    """A Typing object for a Route's Network Interfaces."""

    ifindex: int
    next_hop: str
    next_hop_b: bytes
    next_hop_i: int

class DataGeneration:
    """Produce flow random based on NetFlow v5."""

    RECORD_LEN = 75
    DEFAULT_PEERING_INTERFACES: List[NetworkInterface] = [
        {"ifindex": 10, "next_hop": "10.0.10.2"},
        {"ifindex": 11, "next_hop": "10.0.20.2"},
        {"ifindex": 12, "next_hop": "10.0.30.2"},
        {"ifindex": 13, "next_hop": "10.0.40.2"},
        {"ifindex": 14, "next_hop": "10.0.0.2"},
    ]
    EPHEMERAL_PORTS: Tuple[int, int] = (49152, 65535)
    IP2ASN_FILE = "ip2asn-v4-u32.tsv"
    IP2ASN_CLEAN_FILE = "ip2asn-v4-u32-clean.tsv"
    IP2ASN_FILE_URL = "https://iptoasn.com/data/ip2asn-v4-u32.tsv.gz"
    INTERNAL_ASN = 65000
    INTERNAL_SUBNET = 24
    INTERNAL_IFINDEX: NetworkInterface = {
        "ifindex": 100,
        "next_hop": "10.1.1.2",
        "next_hop_b": b"\n\x01\x01\x02",
        "next_hop_i": 167837954,
    }
    MAX_JITTER: int = 6
    MAX_LIGHT_PACKET_BYTES: int = 4000000
    MIN_LIGHT_PACKET_BYTES: int = 2000000
    MAX_HEAVY_PACKET_BYTES: int = 20000000
    MIN_HEAVY_PACKET_BYTES: int = 4000000
    SERVER_AS_SOURCE_WEIGHT: int = 85  # Percent that the flow will heavy in the Server to Client direction
    SERVER_LATENCY: int = 15
    SERVER_PORT: List[int] = [443, 80, 22]
    SERVER_RANGE: Tuple[str, str] = ("10.10.10.10", "10.10.10.100")

    route_table: Dict[int, ASNRoute]
    server_list: List[int]

    system_id: str = ""
    
    def __init__(self, log, system_id: Optional[bytes] = None):
        """Init flow_gen class instance.

        If `system_id` is not given then a random 32 Byte ID is generated.
        Args:
            log: Logging function
            system_id (Optional[bytes]): 32 Byte System ID
        """
        self.log = log
        self.first_three = True
        if system_id:
            self.system_id = system_id.hex()
        else:
            self.system_id = "{:<32}".format(uuid.uuid4().hex)

    def get_asns(self) -> List[List[str]]:
        """Get all non DOD Autonomous System Numbers in the Internet.

        This method will load a TSV file of ASNs and scrub them. If the TSV is
        not found it will be downloaded from
        "https://iptoasn.com/data/ip2asn-v4-u32.tsv.gz" as defined in
        `IP2ASN_FILE_URL`.  The Format is:
        range_start, range_end, AS_number, country_code, AS_description

        Returns:
            List[List[str]]: A filtered list of ASNs and their metadata
        """
        zip_file_name: str = self.IP2ASN_FILE_URL.rsplit("/", 1)[-1]
        ip2asn: List[List[str]]
        # Test for clean ASN file
        if os.path.isfile(self.IP2ASN_CLEAN_FILE):
            self.log(
                f"Loading a cleaned ASN list form {self.IP2ASN_CLEAN_FILE}"
            )
            with open(self.IP2ASN_CLEAN_FILE, "r") as fd:
                _reader = csv.reader(fd, delimiter="\t", quotechar='"')
                ip2asn = list(_reader)
            self.log(
                f"Loaded {len(ip2asn)} ASNs from {self.IP2ASN_CLEAN_FILE}"
            )
        else:
            # Test for existing Raw ASN File
            if not os.path.isfile(self.IP2ASN_FILE) and not os.path.isfile(
                zip_file_name
            ):
                self.log(
                    f"{self.IP2ASN_FILE} was not found, downloading it from {self.IP2ASN_FILE_URL}"
                )
                req = Request(
                    self.IP2ASN_FILE_URL,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                zip_resp = urlopen(req)  # nosec: B310  URL is hand coded

                # Download into file, not using a temp file so the raw file is here for auditing
                zip_file = open(zip_file_name, "wb")
                zip_file.write(zip_resp.read())
                zip_file.close()
            if not os.path.isfile(self.IP2ASN_FILE) and os.path.isfile(
                zip_file_name
            ):
                # Unzip file, not using a temp file so that the file can be reused
                self.log(
                    f"Decompressing {self.IP2ASN_FILE} from {zip_file_name}"
                )
                gz = gzip.GzipFile(zip_file_name, "rb")
                tsv_file = open(self.IP2ASN_FILE, "wb")
                tsv_file.write(gz.read())
                tsv_file.close()
                gz.close()
            elif not os.path.isfile(zip_file_name) and not os.path.isfile(
                self.IP2ASN_FILE
            ):
                raise ValueError(
                    f"Unable to find compressed ASN file: {zip_file_name}"
                )
            elif not os.path.isfile(self.IP2ASN_FILE):
                raise ValueError(
                    f"Unable to find ASN file: {self.IP2ASN_FILE}"
                )

            # Load TSV ASN Data
            with open(self.IP2ASN_FILE, "r") as fd:
                _reader = csv.reader(fd, delimiter="\t", quotechar='"')
                ip2asn = list(_reader)

            # Clean the ASN data
            self.log(f"Cleaning route data for {len(ip2asn)} routes")
            dropped: int = 0
            routes_count = len(ip2asn)
            # Loop backwards to filter the list in place and save on memory utilization.
            index = routes_count - 1
            while index > 0:
                if ip2asn[index][4] == ("Not routed"):
                    dropped += 1
                    self.log(f"Dropping ASN {ip2asn[index][2]} - Not routed")
                    del ip2asn[index]
                elif ip2asn[index][4] == ("-Reserved AS-"):
                    dropped += 1
                    self.log(f"Dropping ASN {ip2asn[index][2]} - Reserved AS")
                    del ip2asn[index]
                elif ip2asn[index][4].startswith("DNIC-"):
                    dropped += 1
                    self.log(f"Dropping ASN {ip2asn[index][2]} - DNIC AS")
                    del ip2asn[index]
                elif int(ip2asn[index][2]) > 65535:
                    dropped += 1
                    self.log(f"Dropping ASN {ip2asn[index][2]} - Private AS")
                    del ip2asn[index]
                elif int(ip2asn[index][2]) == 0:
                    dropped += 1
                    self.log(f"Dropping ASN {ip2asn[index][2]} - AS 0")
                    del ip2asn[index]
                elif int(ip2asn[index][1]) < (int(ip2asn[index][0]) + 254):
                    dropped += 1
                    self.log(
                        f"Dropping ASN {ip2asn[index][2]} - Non Routable AS"
                    )
                    del ip2asn[index]
                index -= 1
            self.log(
                f"Filter dropped {dropped} of {routes_count} routes, {routes_count - dropped} routes remaining"
            )
            self.log("Saving Cleaned ASN List")
            with open(self.IP2ASN_CLEAN_FILE, "w") as fd:
                write = csv.writer(fd, delimiter="\t", quotechar='"')
                write.writerows(ip2asn)
        return ip2asn

    def random_asns(
        self, asn_table: List[List[str]], asns_to_select: int
    ) -> Dict[int, ASNRoute]:
        """Randomly select ASNs.

        Args:
            asn_table (List[List[str]]): The ASN table to select ASNs from
            asns_to_select (int): The number of ASNs to select

        Returns:
            Dict[int, ASNRoute]: A new Dict where the ASN is the key and the value is the records
        """
        if len(asn_table) < asns_to_select:
            raise ValueError(
                "More ASNs requested then in the route table provided"
            )
        # Make a subnet table
        subnet_table: List[int] = [
            0
        ] * 25  # The index is the number of bits and the value is the number of IPs, All subnets are between 8 and 24 in size
        for i in range(
            24, 7, -1
        ):  # Netflow v5 is only 32-bit IPv4 addresses so ranges are only between 8 and 24 bits
            subnet_table[i] = 2 ** i
        new_table = {}
        for x in range(asns_to_select):
            i = randrange(0, len(asn_table))  # nosec: B311
            # Find subnet
            subnet = 8
            for sub in range(24, 7, -1):
                # Faster in Python to do a negative then a double boolean
                if (
                    not int(asn_table[i][1]) - int(asn_table[i][0])
                    > subnet_table[sub]
                ):
                    subnet = sub
                    break
            route: ASNRoute = {
                "subnet_bits": subnet,
                "network_address": int(asn_table[i][0]),
                "broadcast_address": int(asn_table[i][1]),
                "ip_range_start": int(asn_table[i][0]) + 2,
                "ip_range_end": int(asn_table[i][1]) - 1,
                "asn": int(asn_table[i][2]),
                "country": asn_table[i][3],
                "as_description": asn_table[i][4],
            }
            new_table[route["asn"]] = route
            self.log(
                f"Selected Route {x + 1} of {asns_to_select} for ASN {asn_table[i][2]}"
            )
            del asn_table[i]
        return new_table

    def make_route_table(
        self,
        asns: Dict[int, ASNRoute],
    ) -> Dict[int, ASNRoute]:
        """Create a new route table.

        This will create a new consoldated super route table using randomely selected interface.

        Args:
            asns (Dict[int, ASNRoute]): The ASNs that the route table will be constructed from

        Returns:
            Dict[int, ASNRoute]: The completed route table
        """
        # Convert each interface's next hop address to an int
        for interface in self.DEFAULT_PEERING_INTERFACES:
            ip_obj: ipaddress.IPv4Address = ipaddress.ip_address(
                interface["next_hop"]
            )
            interface["next_hop_b"] = ip_obj.packed
            interface["next_hop_i"] = int.from_bytes(interface["next_hop_b"], "big")
            
        for asn in asns:
            ifindex = randrange(  # nosec: B311
                0, len(self.DEFAULT_PEERING_INTERFACES)
            )  # nosec: B311
            asns[asn]["next_hop"] = self.DEFAULT_PEERING_INTERFACES[ifindex][
                "next_hop_i"
            ]
            asns[asn]["ifindex"] = interface["ifindex"]
            
        self.route_table = asns
        return asns

    def build_server_ip_table(self, from_ip: str, to_ip: str) -> List[int]:
        """Create a list of server ip address as int.

        Args:
            from_ip (str): The first IP Address in the range
            to_ip (str): The last IP Address in the range

        Returns:
            List[int]: List of servers address as int
        """
        server_ip_table: List[int] = []
        ip_start: int = int.from_bytes(
            ipaddress.ip_address(from_ip).packed,
            byteorder="big",
        )
        ip_end: int = int.from_bytes(
            ipaddress.ip_address(to_ip).packed,
            byteorder="big",
        )
        # Check for invesrsion
        if ip_start > ip_end:
            temp = ip_start
            ip_start = ip_end
            ip_end = temp
            del temp

        i = ip_start
        stop_at = ip_end + 1
        while i < stop_at:
            server_ip_table.append(i)
            i += 1
        self.server_list = server_ip_table
        return server_ip_table

    def random_client(
        self,
    ) -> Tuple[int, int, int, int, int]:
        """Randomly select an IP Address from the Internet.

        Randomly selects an IP Address from the Internet and provides it's
        routing information from the route table provided

        Returns:
            Tuple[int, int, int, int, int]: Selected_IP_Address: int, Next_Hop_Address: int, Subnet: int, ASN, IFIndex
        """
        # Select a random route
        route_index = randrange(0, len(self.route_table))  # nosec: B311
        route: ASNRoute = self.route_table[
            list(self.route_table.keys())[route_index]
        ]
        
        # Select a random IP from the routes range
        ip = randrange(  # nosec: B311
            route["ip_range_start"], route["ip_range_end"]
        )  # nosec: B311
        
        return (
            ip,
            route["next_hop"],
            route["subnet_bits"],
            route["asn"],
            route["ifindex"],
        )

    def random_server(self) -> int:
        """Randomly select a server IP Address.

        Args:
            server_list (_type_): List of server IP Addresses

        Returns:
            int: The selected servers IP Address as int
        """
        ip = self.server_list[
            randrange(0, len(self.server_list))  # nosec: B311
        ]  # nosec: B311
        return ip

    def generate_flow_record(
        self,
        time_index: int,
    ) -> Tuple[FlowRecord, FlowRecord]:
        """Generate flow records for a bidirectional connection.

        Args:
            time_index (int): The time index for the first flow

        Returns:
            Tuple[FlowRecord, FlowRecord]: _description_
        """
        # Generate Client and server
        client = self.random_client()
        server = self.random_server()
        # Randomly Select Transfer Sizes for flow set
        heavy_transfer_size = randint(  # nosec: B311
            self.MIN_HEAVY_PACKET_BYTES, 
            self.MAX_HEAVY_PACKET_BYTES
        )
        light_transfer_size = randint(  # nosec: B311
            self.MIN_LIGHT_PACKET_BYTES, 
            self.MAX_LIGHT_PACKET_BYTES
        )
        # Randomly picked heavy side
        if randrange(0, 100) > self.SERVER_AS_SOURCE_WEIGHT:  # nosec: B311
            client_transfer = heavy_transfer_size
            server_transfer = light_transfer_size
        else:
            client_transfer = light_transfer_size
            server_transfer = heavy_transfer_size
        # Build flow profile
        # L3 Flow size in Bytes
        client_packets: int = (
            client_transfer // 1200
        )
        # L4 Port
        client_port: int = randint(  # nosec: B311
            self.EPHEMERAL_PORTS[0], 
            self.EPHEMERAL_PORTS[1]
        )  
        # L3 Flow size in Bytes
        server_packets: int = (
            server_transfer // 1200
        )
        # Flow start time in milliseconds
        client_start_time: int = time_index - randint(  # nosec: B311
            1, 60000
        )
        # Flow start time in milliseconds
        server_start_time: int = time_index - randint(  # nosec: B311
            1, 60000
        )
        server_port: int = self.SERVER_PORT[randint(0,2)]
        
        # Client Flow Record
        client_flow: FlowRecord = {
            "timestamp": time_index,
            "system_id": self.system_id,
            "srcaddr": client[0],
            "dstaddr": server,
            "nexthop": self.INTERNAL_IFINDEX["next_hop_i"],
            "dPkts": client_packets,
            "dOctets": client_transfer,
            "first": client_start_time,
            "last": time_index,
            "srcport": client_port,
            "dstport": server_port,
            "tcp_flags": 0,
            "protocol": 6,
            "tos": 0,
            "src_as": client[3],
            "dst_as": self.INTERNAL_ASN,
            "src_mask": client[2],
            "dst_mask": self.INTERNAL_SUBNET,
            "input": client[4],
            "output": self.INTERNAL_IFINDEX["ifindex"],
        }
        server_time = (
            time_index
            + self.SERVER_LATENCY
            + randrange(0, self.MAX_JITTER)  # nosec: B311
        )  # nosec: B311
        # Server Flow Record
        server_flow: FlowRecord = {
            "timestamp": server_time,
            "system_id": self.system_id,
            "srcaddr": server,
            "dstaddr": client[0],
            "nexthop": client[1],
            "dPkts": server_packets,
            "dOctets": server_transfer,
            "first": server_start_time,
            "last": time_index,
            "srcport": server_port,
            "dstport": client_port,
            "tcp_flags": 0,
            "protocol": 6,
            "tos": 0,
            "src_as": self.INTERNAL_ASN,
            "dst_as": client[3],
            "src_mask": self.INTERNAL_SUBNET,
            "dst_mask": client[2],
            "input": self.INTERNAL_IFINDEX["ifindex"],
            "output": client[4],
        }
        return (client_flow, server_flow)

    def to_bytes(self, flow_record: FlowRecord) -> bytes:
        """Format a flow record to bytes.

        Args:
            flow_record (FlowRecord): The FlowRecord Object tht should be converted

        Returns:
            bytes: The Netflow v5 formatted in bytes for an HD system
        """
        return (
            flow_record["timestamp"].to_bytes(6, byteorder="big")
            + bytes.fromhex(flow_record["system_id"])
            + flow_record["srcaddr"].to_bytes(4, byteorder="big")
            + flow_record["dstaddr"].to_bytes(4, byteorder="big")
            + flow_record["nexthop"].to_bytes(4, byteorder="big")
            + flow_record["dPkts"].to_bytes(4, byteorder="big")
            + flow_record["dOctets"].to_bytes(4, byteorder="big")
            + flow_record["srcport"].to_bytes(2, byteorder="big")
            + flow_record["dstport"].to_bytes(2, byteorder="big")
            + flow_record["tcp_flags"].to_bytes(1, byteorder="big")
            + flow_record["protocol"].to_bytes(1, byteorder="big")
            + flow_record["tos"].to_bytes(1, byteorder="big")
            + flow_record["src_as"].to_bytes(2, byteorder="big")
            + flow_record["dst_as"].to_bytes(2, byteorder="big")
            + flow_record["src_mask"].to_bytes(4, byteorder="big")
            + flow_record["dst_mask"].to_bytes(4, byteorder="big")
            + flow_record["input"].to_bytes(2, byteorder="big")
            + flow_record["output"].to_bytes(2, byteorder="big")
        )

    def generate_data(
        self,
        flows_to_make: int,
        flows_per_ms: int,
        job_progress: Progress,
        job_task: int,
        sampling_rate: int,
        data_dir: str,
    ) -> Tuple[int, int]:
        """Actual method to make records from synth netflow records.

        Args:
            flows_to_make (int): The total number of flows needed
            flows_per_ms (int): Flow per millisecond
            job_progress (Progress): Dashboard Progress
            job_task (int): Dashboard Progress Job ID
            sampling_rate (int): Sampling rate
            data_dir (str): The directory to write the data files to

        Raises:
            OSError: _description_
        """
        flow_buffer: Dict[int, List[FlowRecord]] = {}
        time_index_ms: int = 60000 # Start 1 min so first flows can have a postive start time
        total_flows_made: int = 0
        total_sampled_flows_made: int = 0
        raw_flows: List[FlowRecord] = []
        fps: int = flows_per_ms * 1000
        fps_after_sampling: int = fps // sampling_rate
        fps_after_sampling = fps_after_sampling if fps_after_sampling > 0 else 1
        segments: int = fps // fps_after_sampling
        
        try:
            # Setup output CSV files
            self.log(f"Creating files {data_dir}raw_flow.csv and {data_dir}sampled_flow.csv")
            csv_raw_file: TextIO = open(f"{data_dir}raw_flow.csv", "w")
            csv_sampled_file: TextIO = open(f"{data_dir}sampled_flow.csv", "w")
            
            current_flow, future_flow = self.generate_flow_record(0)
            flow_csv_keys = list(current_flow.keys())
            
            self.log(f"Writing csv headers to {data_dir}raw_flow.csv and {data_dir}sampled_flow.csv")
            raw_flow_csv_writer = csv.DictWriter(csv_raw_file, delimiter=",", quotechar='"', fieldnames=flow_csv_keys)
            sampled_flow_csv_writer = csv.DictWriter(csv_sampled_file, delimiter=",", quotechar='"', fieldnames=flow_csv_keys)
            raw_flow_csv_writer.writeheader()
            sampled_flow_csv_writer.writeheader()

            flows: List[FlowRecord] = []
            current_flow: FlowRecord = {}
            future_flow: FlowRecord = {}

            # Loop to make each record
            self.log(f"Looping to make flows: {total_flows_made} of {flows_to_make}")
            while total_flows_made < flows_to_make:                    
                # Loop to genrate the number of flow recrods required for this second
                for _ in range(1000):
                    try:
                        # Check if there are any fuure matching flows in the buffer
                        flows = flow_buffer[time_index_ms]
                        del flow_buffer[time_index_ms]  # remove the future flow from the buffer now that it has been added
                    except KeyError:
                        flows = []

                    # Loop to genrate the number of flow recrods required for this millisecond
                    while len(flows) < flows_per_ms:
                        # Genrate a a new flow record and it's future corsponding return flow
                        current_flow, future_flow = self.generate_flow_record(
                            time_index=time_index_ms
                        )
                        flows.append(current_flow)  # add the current flow to the list of flows
                        del current_flow  # free memory
                        try:
                            # Add the future corsponding flow to the future flow buffer
                            flow_buffer[future_flow["timestamp"]].append(
                                future_flow
                            )
                        except KeyError:
                            flow_buffer[future_flow["timestamp"]] = [future_flow]
                        del future_flow  # free memory
                    total_flows_made += len(flows)
                    
                    # Write flows files
                    for flow_record in flows:
                        raw_flow_csv_writer.writerow(flow_record)
                    
                    # Add the flows to a second buffer
                    raw_flows.extend(flows)
                    
                    # Set next loop
                    time_index_ms += 1
                    flows.clear()
                
                start_index: int = 0
                end_index: int = sampling_rate - 1
                for _ in range(segments):
                    random_flow: int = randint(start_index, end_index)
                    random_flow = random_flow if random_flow < len(raw_flows) else len(raw_flows) - 1
                    sampled_flow_csv_writer.writerow(raw_flows[random_flow])
                    start_index = end_index + 1
                    end_index = end_index + sampling_rate - 1
                    total_sampled_flows_made += 1
                raw_flows.clear()
                job_progress.update(
                    task_id=job_task,
                    advance=fps,
                )
                    
        except OSError as e:
            if e.errno == 12:
                raise OSError("Memory could not be allocated for drive size")
        
        finally:
            try:
                csv_raw_file.flush()
                csv_raw_file.close()
                csv_sampled_file.flush()
                csv_sampled_file.close()
            except OSError:
                pass
            del raw_flows
            del flows
        
        return (total_flows_made, total_sampled_flows_made)
