#!/usr/bin/env python

import  os
import  sys
import  time
import  json
from    datetime import datetime, timedelta
from    argparse import ArgumentParser
import  csv

try:
    import boto3
except ImportError:
    print ("AWS batch not supported.")
    sys.exit(2)

version = "1.0"

class AWS_Batch(object):
    def __init__(self, batch_cfg_file):
        self.class_name = self.__class__.__name__
        self.batch_cfg_file = batch_cfg_file
        # read the joson file
        with open(self.batch_cfg_file) as cfgFileHandle:
            self.seq2hla_cfg= json.load(cfgFileHandle)
        # get the job parameters
        self.jobParams = self.seq2hla_cfg["job_parameters"]
        # get the submit options
        self.submitOpts = self.seq2hla_cfg["submit_opts"]
        # get the queue
        self.queue = self.seq2hla_cfg["queue"]
        # get the aws profile
        self.profile = self.seq2hla_cfg["aws_profile"]
        # env
        self.environment = self.submitOpts['environment']
        self.envSlots = self.environment[0]

msgErrPrefix='>>> Error (' + os.path.basename(__file__) +') '
msgInfoPrefix='>>> Info (' + os.path.basename(__file__) +') '
debugPrefix='>>> Debug (' + os.path.basename(__file__) +') '

def pInfo(msg):
    tmsg=time.asctime()
    print msgInfoPrefix+tmsg+": "+msg

def pError(msg):
    tmsg=time.asctime()
    print msgErrPrefix+tmsg+": "+msg

def pDebug(msg):
    if debug:
        tmsg=time.asctime()
        print debugPrefix+tmsg+": "+msg

def Summary(hdr):
    print(hdr)
    print('\tVersion: ' + version)
    print('\tJob name: ' + jobname  )
    print('\tBatch cfg file: ' + cfgfile)
    print('\tBatch job def: ' +  bc.submitOpts['jobdef'])
    print('\tBatch job queue: ' + bc.queue)
    print('\tWork dir: ' + bc.jobParams['wd'])
    print('\tInput map file: ' + bc.jobParams['mf'])
    print('\tNo. rows in map file: ' + str(norows))
    print('\tMemory: ' + str(bc.submitOpts['memory']))
    print('\tEnvironment var ' + bc.envSlots['name'] + ": " + bc.envSlots['value'])
    print('\tNo. of threads/cores: ' + str(bc.submitOpts['vcpus']))
    print('\tDocker command: ' + bc.jobParams['cmd'])
    print('\tMain python script: ' + bc.jobParams['pyscript'])
    print('\tseq2hla script: ' + bc.jobParams['sp'])
    print('\tlog file: ' + bc.jobParams['lf'])
    print('\tAWS profile: ' + bc.profile)
    print('\tDebug: ' + str(debug))
    tbegin=time.asctime()
    print('\tTime: ' + tbegin + "\n")

# defaults
defCfgFile = './bioinf_seq2hla.json'
# command line parser
parser = ArgumentParser( description = "python script submit seq2hla job to aws batch" )
parser.add_argument( "inputfile", nargs = 1, help = "input file mapping dnanexus files" )
parser.add_argument( "-w", "--workdir",
                     help = "Working directory (where download and process are done) [default: cwd]" )
parser.add_argument( "-m", "--memory",
                     help = "Memory per job [default: cfg file]" )
parser.add_argument( "-t", "--threads",
                     help = "Number of threads per job [default: cfg file]" )
parser.add_argument( "-c", "--cfgfile", default = defCfgFile,
                     help = "json batch config file [default: " + defCfgFile + "]" )
parser.add_argument( "-D", "--Debug", action="store_true", default = False,
                     help = "Turn on debug output [default: False]" )
parser.add_argument( "-T", "--test", action="store_true", default = False,
                     help = "Test without submitting job [default: False]" )
parser.add_argument( "-S", "--summary", action="store_true", default = False,
                     help = "Print summary prior to executing [default: False]" )
parser.add_argument( "--version", action="store_true", default = False,
                     help = "Print version of " + __file__ )
args = parser.parse_args()
memory = args.memory
debug = args.Debug
cfgfile = args.cfgfile
inputfile = args.inputfile
inputfile = inputfile[0]
threads = args.threads
test = args.test
workdir = args.workdir
summary = args.summary
jobname = 'seq2hla_' + str(int(time.time()*100))
# version
if args.version:
    print(__file__ + " version: " + version)
    sys.exit()
# verify cfgfile
if not os.path.isfile(cfgfile):
    pError("Config file " + cfgfile + " not found")
    sys.exit(2)
# workdir
if workdir == None:
    workdir = os.path.abspath(os.getenv('PWD'))
else:
    if not os.path.isdir( workdir ):
        pError( "Work directory " + workdir + " does not exist" )
        sys.exit(2)
    workdir = os.path.abspath(workdir)
# instantiate the batch class
bc = AWS_Batch(cfgfile)
# update cfg stuff
if memory != None:
    bc.submitOpts['memory'] = memory
if threads != None:
    bc.submitOpts['vcpus'] = int(threads)

bc.jobParams['wd'] = workdir
# update nslots env
bc.envSlots['value'] = str(bc.submitOpts['vcpus'])

if inputfile != None:
    if not os.path.isfile(inputfile):
        pError("Map input file " + inputfile + " not found")
        sys.exit(2)
    bc.jobParams['mf'] = os.path.abspath(inputfile)
# count lines in file (number of jobs)
maplist = []
with open(inputfile, mode='r') as csv_file:
    for row in csv.reader(csv_file):
        if row:
            maplist.append(row)
norows = len(maplist)


if summary:
    Summary("Summary of " + __file__)

# create the batch client
pDebug('Creating aws batch client')
try:
    session = boto3.Session(profile_name = bc.profile)
    batchC = session.client('batch')
except Exception as e:
    print('boto3 session or client exception ' + str(e))
    sys.exit(2)

if test:
    pInfo('Test mode complete; no actual submitting of job')
else:
    pDebug('job params: \n\t' + str(bc.jobParams))
    subOut = batchC.submit_job(
                   jobName = jobname,
                   jobQueue = bc.queue,
                   arrayProperties = { "size": norows },
                   jobDefinition = bc.submitOpts['jobdef'],
                   parameters = bc.jobParams,
                   containerOverrides = {
                      "vcpus": bc.submitOpts['vcpus'],
                      "memory": bc.submitOpts["memory"],
                      "environment": bc.submitOpts['environment']
                   }
    pInfo('Submitted jobName: ' + subOut['jobName'] + ' jobId: ' + subOut['jobId'])
    )
