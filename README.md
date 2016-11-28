# elblogreader
Quickie to look over most recent log files from AWS ELB

## Use
getlogs.py takes command line parameters, so you can specify 
a loadbalancer, a date and how many files you want to download.
The option --help will give more detail.


## Dependencies
Dependencies are in requirements.txt

## Configuration
* Assumes the machine using it has a role allowing it to read from the relevant bucket
* No arguments are taken -- edit code to fix things :)


