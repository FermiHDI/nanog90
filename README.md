<p align="left">
  <img src="https://github.com/FermiHDI/images/blob/main/logos/FermiHDI%20Logo%20Hz%20-%20Dark.png?raw=true" width="500" alt="logo"/> 
</p>

# NANOG 90
<img src="https://nanog.org/static/svg/logos/nanog_logo_white_text.svg" width="200">

This repo holds non-sensitive source code used at and to perpare for NANOG 90 in Charlotte, NC during the 12-14 of February 2024

## License

UNLICENSED - Private<br/>
ALL RIGHTS RESERVED<br/>
© COPYRIGHT 2024 FERMIHDI LIMITED

## Getting Started

## Running Script In Docker
A docker file is included to help you get up and running as fast as posabule.
```bash
docker build -t fermihdi/nanog90:flowgen .
cd ..
mkdir data
docker run -v ./data:/data --name flowgen fermihdi/nanog90:flowgen
```

## Getting Help
```bash
docker run -it --name flowgen fermihdi/nanog90:flowgen --help

FermiHDI Generate Synth Netflow

options:
  -h, --help            show this help message and exit
  -t TIME, --time TIME  Time in seconds, defaults to 600 seconds
  -f FPS, --fps FPS     The Flow rate to simulate in the data, defaults to 200000
  -s SAMPLING_RATE, --sampling_rate SAMPLING_RATE
                        The n:1 flow sampling to be emulated, defaults to 1000
  -o OUTPUT_DIR, --output_dir OUTPUT_DIR
                        The directory where the data files will be written, defaults to current directory
  -ro, --reports_only, --no-reports_only
                        Genrate a reports only (default: False)
  -nr, --no_reports, --no-no_reports
                        Do not genrate any reports (default: False)
  -pr, --peering_report, --no-peering_report
                        Genrate perring reports (default: True)
  --topN TOPN           The length of the Top N elements to use in reports, defualts to 10
  --rich, --no-rich     Use rich UI (Not advisable under docker) (default: False)
  -V                    Display the version and exit
  -x, --exit            Auto exit when done
```

## Sugested Parameters
```bash
docker run -it -v ./data:/data --name flowgen fermihdi/nanog90:flowgen --rich -t 600 -f 200000 -s 1000 -pr --topN 10 -x
```