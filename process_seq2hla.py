#! /usr/bin/env python
import 	os
import 	dxpy
import  sys
import  os.path
import  os
import  subprocess
from    argparse import ArgumentParser
import  time
import  csv
import  subprocess

version = "2.0"
# def logger class
class Logger(object):
    def __init__(self, logfile):
        self.terminal = sys.stdout
        self.log = open(logfile, "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        #this flush method is needed for python 3 compatibility.
        #this handles the flush command by doing nothing.
        #you might want to specify some extra behavior here.
        self.log.flush()
        pass

    def __del__(self):
        self.write("\n")    # in case we really append next time
        self.log.close()

# def functions
def flush():
    if arrayjob:
        myLogger.flush()
    else:
        sys.stdout.flush()

def parseMapfile(mapfile_a, filerow_a):
    # get the map names based in row number in the map file
    mapinfo = {}
    # get the  number of rows
    pInfo('Reading mapfile ' + mapfile_a)
    maplist = []
    with open(mapfile_a, mode='r') as csv_file:
        for row in csv.reader(csv_file):
            if row:
                maplist.append(row)
    norows = len(maplist)
    mapinfo['norows'] = norows
    # check filerow
    if filerow_a > norows or filerow_a < 1:
        pError('Invalid file row (' + str(filerow_a) +') : 1/' + norows)
        sys.exit(2)
    # get the maprow and check length
    maprow = maplist[filerow_a-1]
    nofields = 5
    if len(maprow) != nofields:
        pError('File row has incorrect number of fields (' + str(len(maprow)) +
               '/' + str(nofields))
        sys.exit(2)
    # get prefix, file 1 names; file 2 names;
    mapinfo['fprefix'] = maprow[0]
    mapinfo['fd1'] = maprow[2]
    mapinfo['fs1'] = maprow[1]
    mapinfo['fd2'] = maprow[4]
    mapinfo['fs2'] = maprow[3]
    return mapinfo

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
    print('\tWork dir: ' + workdir)
    print('\tBatch array job: ' + str(arrayjob))
    print('\tRNA Seq script: ' + seqproc)
    print('\tRNA Seq script threads: ' + str(threads))
    print('\tMapping file: ' + mapfile)
    print('\tRow into map file: ' + str(filerow))
    print('\tMap info: ' + str(mapinfo))
    print('\tNo download: ' + str(nodownload))
    print('\tShow download progress: ' + str(progress))
    print('\tKeep download files: ' + str(keepdownload))
    if log:
        print('\tLog file: ' + fullLog)
    else:
        print('\tLog file: None')
    print('\tDebug: ' + str(debug))
    print('\tNo process: ' + str(noprocess))
    tbegin=time.asctime()
    print('\tTime: ' + tbegin + "\n")

# defaults
defSeqProcess = '/usr/local/seq2hla/seq2HLA.py'
logfile = None
# command line parser
parser = ArgumentParser( description = "python script to download dnanex rna seq files and process" )
parser.add_argument( "mapfile", nargs = 1, help = "file map into dnanexus" )
parser.add_argument( "-w", "--workdir",
                     help = "full path of working directory (where download and process are done) [default: cwd]" )
parser.add_argument( "--seqproc", default = defSeqProcess,
                     help = "RNA Seq processing script [default: " + defSeqProcess + "]" )
parser.add_argument( "--threads",
                     help = "No. of threads in RNA Seq processing script" )
parser.add_argument( "-r", "--filerow",
                     help = "row (1..nrows) into file [required if not arrayjob]" )
parser.add_argument( "--nodownload", action="store_true", default = False,
                     help = "Download only without processing [default: False]" )
parser.add_argument( "--keepdownload", action="store_true", default = False,
                     help = "Keep dnanexus download files [default: delete after processing]" )
parser.add_argument( "-l", "--log", action="store_true", default = False,
                     help = "create log file (interactive only) [default: False]" )
parser.add_argument( "-a", "--arrayjob", action="store_true", default = False,
                     help = "script submitted as a batch arrayjob [default: False]" )
parser.add_argument( "-p", "--progress", action="store_true", default = False,
                     help = "Show progress in downloading file [default: False]" )
parser.add_argument( "-D", "--Debug", action="store_true", default = False,
                     help = "Turn on debug output [default: False]" )
parser.add_argument( "-S", "--summary", action="store_true", default = False,
                     help = "Print summary prior to executing [default: False]" )
parser.add_argument( "--noprocess", action="store_true", default = False,
                     help = "Test without processing [default: False]" )
parser.add_argument( "--version", action="store_true", default = False,
                     help = "Print version of " + __file__ )
args = parser.parse_args()
mapfile = args.mapfile
mapfile = mapfile[0]
debug = args.Debug
filerow = args.filerow
nodownload = args.nodownload
progress = args.progress
log = args.log
arrayjob = args.arrayjob
seqproc = args.seqproc
threads = args.threads
workdir = args.workdir
summary = args.summary
noprocess = args.noprocess
keepdownload = args.keepdownload
# version
if args.version:
    print(__file__ + " version: " + version)
    sys.exit()
# noprocess mapfile exists
if not os.path.isfile(mapfile):
    pError('Map file (' + mapfile + ') does not exist')
    sys.exit(2)
mapfile = os.path.abspath(mapfile)
# workdir
if workdir == None:
    workdir = os.path.abspath(os.getenv('PWD'))
else:
    if not os.path.isdir( workdir ):
        pError( "Work directory " + workdir + " does not exist" )
        sys.exit(2)
    workdir = os.path.abspath(workdir)

if arrayjob:
    # get the batch array index (0 based) which is row number - 1
    echeck = 'AWS_BATCH_JOB_ARRAY_INDEX'
    if echeck in os.environ:
        filerow = int(os.environ[echeck]) + 1
    else:
        pError("Required array job env " + echeck + " not found")
        sys.exit(2)
    # set no. of threads
    echeck = 'NSLOTS'
    if echeck in os.environ:
        threads = int(os.environ[echeck])
    # always log
    log = True
else:
    # filerow is required and an int
    if filerow == None:
        pError('File row number (-r/--filerow) is required')
        sys.exit(2)
    filerow = int(filerow)

# parse the mapfile
pInfo('Reading mapfile ' + mapfile)
mapinfo = parseMapfile(mapfile, filerow)

# create the log file name (full path)
if log :
    fullLog = mapinfo['fprefix'] + ".log"
    fullLog = workdir + "/" + fullLog
# create the trace file name (for arrayjob)
if arrayjob:
    fullTrace = fullLog.replace(".log",".trace")
    myLogger = Logger(fullTrace)
    sys.stderr = sys.stdout = myLogger

# check for seqproc file
if not os.path.isfile(seqproc):
    pError('seq2hla processing  file (' + seqproc + ') does not exist')
    sys.exit(2)
seqproc = os.path.abspath(seqproc)
seqdir = os.path.dirname(seqproc)
if summary:
    Summary("Summary of " + __file__)

fd1 = mapinfo['fd1']
fs1 = workdir + "/" + mapinfo['fs1']
fd2 = mapinfo['fd2']
fs2 = workdir + "/" + mapinfo['fs2']
fprefix = mapinfo['fprefix']
pDebug('Download files: ' + fd1 + "/" + fd2)
pDebug('Saved files: ' + fs1 + "/" + fs2)
pDebug('Prefix: ' + fprefix)
# instantiate dxpy
pInfo('Instantiating dx')
try:
    dx = dxpy
except Exception as e:
    pError('dxpy exception ' + str(e))
    sys.exit(2)

if not nodownload:
    # downloading
    pInfo('Downloading ' + fd1 + ' to ' + fs1)
    dx.download_dxfile(fd1, fs1, show_progress=progress)
    pInfo('Downloading ' + fd2 + ' to ' + fs2)
    dx.download_dxfile(fd2, fs2, show_progress=progress)
else:
    pInfo('Would download ' + fd1 + ' to ' + fs1)
    pInfo('Would download ' + fd2 + ' to ' + fs2)

pfile = seqproc
cmd = pfile + ' -1 ' + fs1 + ' -2 ' + fs2 + " -r " + workdir + "/"+ fprefix
if threads != None:
    cmd = cmd + " -p " + str(threads)
pInfo( "cmd> " + cmd )
flush()
if noprocess:
    pInfo('Not processing; script is exiting.')
    sys.exit(0)
else:
    # verify files to be processed exist
    if not os.path.isfile(fs1):
        pError('File to process (' + fs1 + ') does not exist')
        sys.exit(2)
    if not os.path.isfile(fs2):
        pError('File to process (' + fs2 + ') does not exist')
        sys.exit(2)
    pInfo("Executing popen ...")
    flush()
    # redirect stdout to logile
    if log:
        sout = sys.stdout
        serr = sys.stderr
        flog = open ( fullLog, 'w' )
        sys.stderr = sys.stdout = flog
    # popen
    try:
        process = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, shell=True, cwd=seqdir)
        status = process.wait()
    except Exception as e:
        # redirect stdout back
        if log:
            sys.stdout = sout
            sys.stderr = serr
        pError('Error executing popen' + str(e))
        sys.exit(2)
    # redirect stdout back
    if log:
        sys.stdout = sout
        sys.stderr = serr
    flush()
    # check status
    if status:
        pError( "Error executing " + pfile + ': ' + str(status) )
        sys.exit(2)
    else:
        if not keepdownload:
            pInfo("Removing downloaded files (" + fs1 + " and " + fs2 )
            os.remove(fs1)
            os.remove(fs2)
        pInfo( "Executing " + pfile + " completed without errors.")
        sys.exit(0)
