import sys
from generate_header import *
import pandas as pd
from pandas import DataFrame
import math

numIteration = 5

def generateLassenGpfsExtremePoints(baseDir='/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePoints'):
    numJob = 0
    df = pd.read_csv(baseDir + '/extreme-points.csv')
    for iteration in range(5):
        for index, row in df.iterrows():
            system = 'lassen-gpfs'
            numBB = 0
            point = row['point']
            api = row['api(-a)']
            blockSize = row['blockSize(-b)']
            collective = row['collective(-c)']
            fsync = row['fsync(-e)']
            useExistingTestFile = row['useExistingTestFile(-E)']
            filePerProc = row['filePerProc(-F)']
            intraTestBarriers = row['intraTestBarriers(-g)']
            setAlignment = row['setAlignment(-J)']
            keepFile = row['keepFile(-k)']
            memoryPerNode = row['memoryPerNode(-M)']
            noFill = row['noFill(-n)']
            preallocate = row['preallocate(-p)']
            readFile = row['readFile(-r)']
            segment = row['segment(-s)']
            transferSize = row['transferSize(-t)']
            uniqueDir = row['uniqueDir(-u)']
            useFileView = row['useFileView(-V)']
            writeFile = row['writeFile(-w)']
            fsyncPerWrite = row['fsyncPerWrite(-Y)']
            numProc = row['numProc']
            numNodes = row['numNode']

            jobName = 'iteration' + str(iteration + 1) + '.' + point + '.' + str(system) + '.' + str(numBB) + '.' + str(api) + '.' + str(blockSize) + '.' + str(collective) + '.' + str(fsync) \
                + '.' + str(useExistingTestFile) + '.' + str(filePerProc) + '.' + str(intraTestBarriers) + '.' + str(setAlignment) \
                + '.' + str(keepFile) + '.' + str(memoryPerNode) + '.' + str(noFill) + '.' + str(preallocate) + '.' + str(readFile) \
                + '.' + str(segment) + '.' + str(transferSize) + '.' + str(uniqueDir) + '.' + str(useFileView) + '.' + str(writeFile) \
                + '.' + str(fsyncPerWrite) + '.' + str(numProc)

            filename = baseDir + '/jobs/' + jobName + '.sh'

            blockSizeByte = toByte(blockSize)
            wtime = 1 + 2 ** math.ceil((math.log(blockSizeByte / 1024, 32)))
            wtime = math.ceil(wtime * (1 + numNodes / 8))

            generateLassenHeader(filename, numNodes, wtime, jobName, jobName)
            
            with open(filename, "a") as script:
                script.write("export NUM_PROC=" + str(numProc) + "\n")
                script.write("export NUM_NODES=" + str(numNodes) + "\n")
                script.write("export FILE_SYSTEM=" + system + "\n")
                script.write("export NUM_BB_SERVERS=" + str(numBB) + "\n")
                script.write("\n")

                script.write("export JOB_HOME=/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePoints\n")
                script.write("export BIN_PATH=/g/g92/xu23/gpfs1/local/ior-3.3.0/bin\n")
                script.write("export RECORDER_PATH=/g/g92/xu23/gpfs1/local/recorder-2.2\n")
                script.write("export DARSHAN_PATH=/g/g92/xu23/gpfs1/local/darshan-3.3.1\n")
                script.write("export PROFILES=/p/gpfs1/xu23/research/io-experiments/ior/extremePoints/profiles\n")     
                script.write("\n")     
                
                script.write("export API=" + str(api) + "\n")     
                script.write("export BLOCK_SIZE=" + str(blockSize) + "\n")     
                script.write("export SET_ALIGNMENT=" + str(setAlignment) + "\n")
                script.write("export MEMORY_PER_NODE=" + str(memoryPerNode) + "\n")
                script.write("export SEGMENT=" + str(segment) + "\n")     
                script.write("export TRANSFER_SIZE=" + str(transferSize) + "\n")     
                script.write("\n")

                script.write("mkdir -p /g/g92/xu23/gpfs1/research/io-experiments/ior/extremePoints/app-output-files/" + jobName + "\n")
                script.write("cp /g/g92/xu23/gpfs1/research/io-experiments/ior/ior-util.py /g/g92/xu23/gpfs1/research/io-experiments/ior/extremePoints/app-output-files/" + jobName + "\n")
                script.write("cd /g/g92/xu23/gpfs1/research/io-experiments/ior/extremePoints/app-output-files/" + jobName + "\n")
                script.write("\n")

                script.write("export RECORDER_TRACES_DIR=/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePoints/app-output-files/" + jobName + "\n")


                execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/ior -a ${API} -b ${BLOCK_SIZE} -J ${SET_ALIGNMENT} -M ${MEMORY_PER_NODE} -s ${SEGMENT} -t ${TRANSFER_SIZE}"
                
                if collective == 1:
                    execution += " -c"
                if fsync == 1:
                    execution += " -e"
                if filePerProc == 1:
                    execution += " -F"
                if intraTestBarriers == 1:
                    execution += " -g"
                if noFill == 1:
                    execution += " -n"
                if preallocate == 1:
                    execution += " -p"
                if uniqueDir == 1:
                    execution += " -u"
                if useFileView == 1:
                    execution += " -V"
                if fsyncPerWrite == 1:
                    execution += " -Y"

                redirect = " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"
                    
                script.write("printf \"start\\n\" > $JOB_HOME/app-stdio/" + jobName + ".txt\n")                

                script.write("printf \"\\nwrite scratch\\n\"" + redirect)
                execution1 = execution + " -w -k -o testFile" + redirect
                if row['write-scratch']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution1)
                else:
                    script.write("jsrun " + execution1)

                script.write("printf \"\\nWAW\\n\"" + redirect)
                execution2 = execution + " -w -E -k -o testFile" + redirect
                if row['write-after-write']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution2)
                else:
                    script.write("jsrun " + execution2)

                script.write("printf \"\\nRAW\\n\"" + redirect)
                execution3 = execution + " -r -k -o testFile" + redirect
                if row['read-after-write']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution3)
                else:
                    script.write("jsrun " + execution3)
                
                script.write("python3 ior-util.py rename renamed " + str(uniqueDir) + "\n")

                script.write("printf \"\\nread renamed\\n\"" + redirect)
                execution4 = ''
                if uniqueDir == 1:
                    execution4 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution4 = execution + " -r -k -o renamed-testFile"
                execution4 += redirect
                if row['read-renamed']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution4)
                else:
                    script.write("jsrun " + execution4)

                script.write("printf \"\\nRAR\\n\"" + redirect)
                execution5 = ''
                if uniqueDir == 1:
                    execution5 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution5 = execution + " -r -k -o renamed-testFile"
                execution5 += redirect
                if row['read-after-read']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution5)
                else:
                    script.write("jsrun " + execution5)

                script.write("printf \"\\nWAR\\n\"" + redirect)
                execution6 = ''
                if uniqueDir == 1:
                    execution6 = execution + " -w -E -o trenamed-estFile"
                else:
                    execution6 = execution + " -w -E -o renamed-testFile"
                execution6 += redirect
                if row['write-after-read']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution6)
                else:
                    script.write("jsrun " + execution6)

                if uniqueDir == 1:
                    script.write("rm t* -r\n")

                script.write("python3 ./ior-util.py prepareFileGpfs " + str(uniqueDir) + " " + str(filePerProc) + " " + str(numProc) + " touchedTestFile" + "\n")

                script.write("printf \"\\nwrite touched file\\n\"" + redirect)
                execution7 = execution + " -w -E -o touchedTestFile" + redirect
                if row['write-touched-file']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution7)
                else:
                    script.write("jsrun " + execution7)

                if uniqueDir == 1:
                    script.write("rm t* -r\n")

                script.write("\n")

                script.write("mv $RECORDER_TRACES_DIR/recorder-logs $PROFILES/" + jobName + ".recorder-log\n")

                # script.write("export MY_DARSHAN_LOG_DIR=$PROFILES\n")
                # script.write("export DXT_ENABLE_IO_TRACE=1\n")
                # script.write("jsrun -E LD_PRELOAD=${DARSHAN_PATH}/lib/libdarshan.so " + execution)
                # script.write("\n")

                numJob += 1
    print(numJob)
    return numJob

def generateLassenBBExtremePoints(baseDir='/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePoints'):
    numJob = 0
    df = pd.read_csv(baseDir + '/extreme-points.csv')
    for iteration in range(5):
        for index, row in df.iterrows():
            system = 'lassen-bb'
            numBB = 1
            point = row['point']
            api = row['api(-a)']
            blockSize = row['blockSize(-b)']
            collective = row['collective(-c)']
            fsync = row['fsync(-e)']
            useExistingTestFile = row['useExistingTestFile(-E)']
            filePerProc = row['filePerProc(-F)']
            intraTestBarriers = row['intraTestBarriers(-g)']
            setAlignment = row['setAlignment(-J)']
            keepFile = row['keepFile(-k)']
            memoryPerNode = row['memoryPerNode(-M)']
            noFill = row['noFill(-n)']
            preallocate = row['preallocate(-p)']
            readFile = row['readFile(-r)']
            segment = row['segment(-s)']
            transferSize = row['transferSize(-t)']
            uniqueDir = row['uniqueDir(-u)']
            useFileView = row['useFileView(-V)']
            writeFile = row['writeFile(-w)']
            fsyncPerWrite = row['fsyncPerWrite(-Y)']
            numProc = row['numProc']
            numNodes = row['numNode']

            jobName = 'iteration' + str(iteration + 1) + '.' + point + '.' + str(system) + '.' + str(numBB) + '.' + str(api) + '.' + str(blockSize) + '.' + str(collective) + '.' + str(fsync) \
                + '.' + str(useExistingTestFile) + '.' + str(filePerProc) + '.' + str(intraTestBarriers) + '.' + str(setAlignment) \
                + '.' + str(keepFile) + '.' + str(memoryPerNode) + '.' + str(noFill) + '.' + str(preallocate) + '.' + str(readFile) \
                + '.' + str(segment) + '.' + str(transferSize) + '.' + str(uniqueDir) + '.' + str(useFileView) + '.' + str(writeFile) \
                + '.' + str(fsyncPerWrite) + '.' + str(numProc)

            filename = baseDir + '/jobs/' + jobName + '.sh'

            blockSizeByte = toByte(blockSize)
            wtime = 1 + 2 ** math.ceil((math.log(blockSizeByte / 1024, 32)))
            wtime = math.ceil(wtime * (1 + numNodes / 8))

            generateLassenHeader(filename, numNodes, wtime, jobName, jobName)
                
            with open(filename, "a") as script:
                script.write("export NUM_PROC=" + str(numProc) + "\n")
                script.write("export NUM_NODES=" + str(numNodes) + "\n")
                script.write("export FILE_SYSTEM=" + system + "\n")
                script.write("export NUM_BB_SERVERS=" + str(numBB) + "\n")
                script.write("\n")

                script.write("export JOB_HOME=/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePoints\n")
                script.write("export BIN_PATH=/g/g92/xu23/gpfs1/local/ior-3.3.0/bin\n")
                script.write("export RECORDER_PATH=/g/g92/xu23/gpfs1/local/recorder-2.2\n")
                script.write("export DARSHAN_PATH=/g/g92/xu23/gpfs1/local/darshan-3.3.1\n")
                script.write("export PROFILES=/p/gpfs1/xu23/research/io-experiments/ior/extremePoints/profiles\n")     
                script.write("\n")     
                
                script.write("export API=" + str(api) + "\n")     
                script.write("export BLOCK_SIZE=" + str(blockSize) + "\n")     
                script.write("export SET_ALIGNMENT=" + str(setAlignment) + "\n")
                script.write("export MEMORY_PER_NODE=" + str(memoryPerNode) + "\n")
                script.write("export SEGMENT=" + str(segment) + "\n")     
                script.write("export TRANSFER_SIZE=" + str(transferSize) + "\n")     
                script.write("\n")

                script.write("mkdir -p /g/g92/xu23/gpfs1/research/io-experiments/ior/extremePoints/app-output-files/" + jobName + "\n")
                script.write("jsrun -r 1 -n ${NUM_NODES} mkdir -p $BBPATH/" + jobName + "\n")
                script.write("jsrun -r 1 -n ${NUM_NODES} cp /g/g92/xu23/gpfs1/research/io-experiments/ior/ior-util.py $BBPATH/" + jobName + "\n")
                script.write("cd $BBPATH/" + jobName + "\n")
                script.write("\n")

                script.write("export RECORDER_TRACES_DIR=/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePoints/app-output-files/" + jobName + "\n")

                execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/ior -a ${API} -b ${BLOCK_SIZE} -J ${SET_ALIGNMENT} -M ${MEMORY_PER_NODE} -s ${SEGMENT} -t ${TRANSFER_SIZE}"
                
                if collective == 1:
                    execution += " -c"
                if fsync == 1:
                    execution += " -e"
                if filePerProc == 1:
                    execution += " -F"
                if intraTestBarriers == 1:
                    execution += " -g"
                if noFill == 1:
                    execution += " -n"
                if preallocate == 1:
                    execution += " -p"
                if uniqueDir == 1:
                    execution += " -u"
                if useFileView == 1:
                    execution += " -V"
                if fsyncPerWrite == 1:
                    execution += " -Y"

                redirect = " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"
                    
                script.write("printf \"start\\n\" > $JOB_HOME/app-stdio/" + jobName + ".txt\n")                

                script.write("printf \"\\nwrite scratch\\n\"" + redirect)
                execution1 = execution + " -w -k -o testFile" + redirect
                if row['write-scratch']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution1)
                else:
                    script.write("jsrun " + execution1)

                script.write("printf \"\\nWAW\\n\"" + redirect)
                execution2 = execution + " -w -E -k -o testFile" + redirect
                if row['write-after-write']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution2)
                else:
                    script.write("jsrun " + execution2)

                script.write("printf \"\\nRAW\\n\"" + redirect)
                execution3 = execution + " -r -k -o testFile" + redirect
                if row['read-after-write']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution3)
                else:
                    script.write("jsrun " + execution3)
                
                script.write("jsrun -r 1 -n ${NUM_NODES} python3 ior-util.py rename renamed " + str(uniqueDir) + "\n")

                script.write("printf \"\\nread renamed\\n\"" + redirect)
                execution4 = ''
                if uniqueDir == 1:
                    execution4 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution4 = execution + " -r -k -o renamed-testFile"
                execution4 += redirect
                if row['read-renamed']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution4)
                else:
                    script.write("jsrun " + execution4)

                script.write("printf \"\\nRAR\\n\"" + redirect)
                execution5 = ''
                if uniqueDir == 1:
                    execution5 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution5 = execution + " -r -k -o renamed-testFile"
                execution5 += redirect
                if row['read-after-read']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution5)
                else:
                    script.write("jsrun " + execution5)

                script.write("printf \"\\nWAR\\n\"" + redirect)
                execution6 = ''
                if uniqueDir == 1:
                    execution6 = execution + " -w -E -o trenamed-estFile"
                else:
                    execution6 = execution + " -w -E -o renamed-testFile"
                execution6 += redirect
                if row['write-after-read']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution6)
                else:
                    script.write("jsrun " + execution6)

                if uniqueDir == 1:
                    script.write("rm t* -r\n")

                script.write("jsrun -r 32 -n ${NUM_PROC} python3 ./ior-util.py prepareFileBB " + str(uniqueDir) + " touchedTestFile" + "\n")

                script.write("printf \"\\nwrite touched file\\n\"" + redirect)
                execution7 = execution + " -w -E -o touchedTestFile" + redirect
                if row['write-touched-file']:
                    script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution7)
                else:
                    script.write("jsrun " + execution7)

                if uniqueDir == 1:
                    script.write("rm t* -r\n")

                script.write("\n")

                script.write("mv $RECORDER_TRACES_DIR/recorder-logs $PROFILES/" + jobName + ".recorder-log\n")
                
                # script.write("export MY_DARSHAN_LOG_DIR=$PROFILES\n")
                # script.write("export DXT_ENABLE_IO_TRACE=1\n")
                # script.write("jsrun -E LD_PRELOAD=${DARSHAN_PATH}/lib/libdarshan.so " + execution)
                # script.write("\n")

                numJob += 1
    print(numJob)
    return numJob

def generateLassenGpfsExtremePointsNoRecorder(baseDir='/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePointsNoRecorder'):
    numJob = 0
    df = pd.read_csv(baseDir + '/extreme-points.csv')
    for iteration in range(5):
        for index, row in df.iterrows():
            system = 'lassen-gpfs'
            numBB = 0
            point = row['point']
            api = row['api(-a)']
            blockSize = row['blockSize(-b)']
            collective = row['collective(-c)']
            fsync = row['fsync(-e)']
            useExistingTestFile = row['useExistingTestFile(-E)']
            filePerProc = row['filePerProc(-F)']
            intraTestBarriers = row['intraTestBarriers(-g)']
            setAlignment = row['setAlignment(-J)']
            keepFile = row['keepFile(-k)']
            memoryPerNode = row['memoryPerNode(-M)']
            noFill = row['noFill(-n)']
            preallocate = row['preallocate(-p)']
            readFile = row['readFile(-r)']
            segment = row['segment(-s)']
            transferSize = row['transferSize(-t)']
            uniqueDir = row['uniqueDir(-u)']
            useFileView = row['useFileView(-V)']
            writeFile = row['writeFile(-w)']
            fsyncPerWrite = row['fsyncPerWrite(-Y)']
            numProc = row['numProc']
            numNodes = row['numNode']

            jobName = 'iteration' + str(iteration + 1) + '.' + point + '.' + str(system) + '.' + str(numBB) + '.' + str(api) + '.' + str(blockSize) + '.' + str(collective) + '.' + str(fsync) \
                + '.' + str(useExistingTestFile) + '.' + str(filePerProc) + '.' + str(intraTestBarriers) + '.' + str(setAlignment) \
                + '.' + str(keepFile) + '.' + str(memoryPerNode) + '.' + str(noFill) + '.' + str(preallocate) + '.' + str(readFile) \
                + '.' + str(segment) + '.' + str(transferSize) + '.' + str(uniqueDir) + '.' + str(useFileView) + '.' + str(writeFile) \
                + '.' + str(fsyncPerWrite) + '.' + str(numProc)

            filename = baseDir + '/jobs/' + jobName + '.sh'

            blockSizeByte = toByte(blockSize)
            wtime = 1 + 2 ** math.ceil((math.log(blockSizeByte / 1024, 32)))
            wtime = math.ceil(wtime * (1 + numNodes / 8))

            generateLassenHeader(filename, numNodes, wtime, jobName, jobName)
            
            with open(filename, "a") as script:
                script.write("export NUM_PROC=" + str(numProc) + "\n")
                script.write("export NUM_NODES=" + str(numNodes) + "\n")
                script.write("export FILE_SYSTEM=" + system + "\n")
                script.write("export NUM_BB_SERVERS=" + str(numBB) + "\n")
                script.write("\n")

                script.write("export JOB_HOME=/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePointsNoRecorder\n")
                script.write("export BIN_PATH=/g/g92/xu23/gpfs1/local/ior-3.3.0/bin\n")
                script.write("export RECORDER_PATH=/g/g92/xu23/gpfs1/local/recorder-2.2\n")
                script.write("export DARSHAN_PATH=/g/g92/xu23/gpfs1/local/darshan-3.3.1\n")
                script.write("export PROFILES=/p/gpfs1/xu23/research/io-experiments/ior/extremePointsNoRecorder/profiles\n")     
                script.write("\n")     
                
                script.write("export API=" + str(api) + "\n")     
                script.write("export BLOCK_SIZE=" + str(blockSize) + "\n")     
                script.write("export SET_ALIGNMENT=" + str(setAlignment) + "\n")
                script.write("export MEMORY_PER_NODE=" + str(memoryPerNode) + "\n")
                script.write("export SEGMENT=" + str(segment) + "\n")     
                script.write("export TRANSFER_SIZE=" + str(transferSize) + "\n")     
                script.write("\n")

                script.write("mkdir -p /g/g92/xu23/gpfs1/research/io-experiments/ior/extremePointsNoRecorder/app-output-files/" + jobName + "\n")
                script.write("cp /g/g92/xu23/gpfs1/research/io-experiments/ior/ior-util.py /g/g92/xu23/gpfs1/research/io-experiments/ior/extremePointsNoRecorder/app-output-files/" + jobName + "\n")
                script.write("cd /g/g92/xu23/gpfs1/research/io-experiments/ior/extremePointsNoRecorder/app-output-files/" + jobName + "\n")
                script.write("\n")

                script.write("export RECORDER_TRACES_DIR=/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePointsNoRecorder/app-output-files/" + jobName + "\n")


                execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/ior -a ${API} -b ${BLOCK_SIZE} -J ${SET_ALIGNMENT} -M ${MEMORY_PER_NODE} -s ${SEGMENT} -t ${TRANSFER_SIZE}"
                
                if collective == 1:
                    execution += " -c"
                if fsync == 1:
                    execution += " -e"
                if filePerProc == 1:
                    execution += " -F"
                if intraTestBarriers == 1:
                    execution += " -g"
                if noFill == 1:
                    execution += " -n"
                if preallocate == 1:
                    execution += " -p"
                if uniqueDir == 1:
                    execution += " -u"
                if useFileView == 1:
                    execution += " -V"
                if fsyncPerWrite == 1:
                    execution += " -Y"

                redirect = " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"
                    
                script.write("printf \"start\\n\" > $JOB_HOME/app-stdio/" + jobName + ".txt\n")                

                script.write("printf \"\\nwrite scratch\\n\"" + redirect)
                execution1 = execution + " -w -k -o testFile" + redirect
                script.write("jsrun " + execution1)

                script.write("printf \"\\nWAW\\n\"" + redirect)
                execution2 = execution + " -w -E -k -o testFile" + redirect
                script.write("jsrun " + execution2)

                script.write("printf \"\\nRAW\\n\"" + redirect)
                execution3 = execution + " -r -k -o testFile" + redirect
                script.write("jsrun " + execution3)
                
                script.write("python3 ior-util.py rename renamed " + str(uniqueDir) + "\n")

                script.write("printf \"\\nread renamed\\n\"" + redirect)
                execution4 = ''
                if uniqueDir == 1:
                    execution4 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution4 = execution + " -r -k -o renamed-testFile"
                execution4 += redirect
                script.write("jsrun " + execution4)

                script.write("printf \"\\nRAR\\n\"" + redirect)
                execution5 = ''
                if uniqueDir == 1:
                    execution5 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution5 = execution + " -r -k -o renamed-testFile"
                execution5 += redirect
                script.write("jsrun " + execution5)

                script.write("printf \"\\nWAR\\n\"" + redirect)
                execution6 = ''
                if uniqueDir == 1:
                    execution6 = execution + " -w -E -o trenamed-estFile"
                else:
                    execution6 = execution + " -w -E -o renamed-testFile"
                execution6 += redirect
                script.write("jsrun " + execution6)

                if uniqueDir == 1:
                    script.write("rm t* -r\n")

                script.write("python3 ./ior-util.py prepareFileGpfs " + str(uniqueDir) + " " + str(filePerProc) + " " + str(numProc) + " touchedTestFile" + "\n")

                script.write("printf \"\\nwrite touched file\\n\"" + redirect)
                execution7 = execution + " -w -E -o touchedTestFile" + redirect
                script.write("jsrun " + execution7)

                if uniqueDir == 1:
                    script.write("rm t* -r\n")

                script.write("\n")


                # script.write("export MY_DARSHAN_LOG_DIR=$PROFILES\n")
                # script.write("export DXT_ENABLE_IO_TRACE=1\n")
                # script.write("jsrun -E LD_PRELOAD=${DARSHAN_PATH}/lib/libdarshan.so " + execution)
                # script.write("\n")

                numJob += 1
    print(numJob)
    return numJob

def generateLassenBBExtremePointsNoRecorder(baseDir='/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePointsNoRecorder'):
    numJob = 0
    df = pd.read_csv(baseDir + '/extreme-points.csv')
    for iteration in range(5):
        for index, row in df.iterrows():
            system = 'lassen-bb'
            numBB = 1
            point = row['point']
            api = row['api(-a)']
            blockSize = row['blockSize(-b)']
            collective = row['collective(-c)']
            fsync = row['fsync(-e)']
            useExistingTestFile = row['useExistingTestFile(-E)']
            filePerProc = row['filePerProc(-F)']
            intraTestBarriers = row['intraTestBarriers(-g)']
            setAlignment = row['setAlignment(-J)']
            keepFile = row['keepFile(-k)']
            memoryPerNode = row['memoryPerNode(-M)']
            noFill = row['noFill(-n)']
            preallocate = row['preallocate(-p)']
            readFile = row['readFile(-r)']
            segment = row['segment(-s)']
            transferSize = row['transferSize(-t)']
            uniqueDir = row['uniqueDir(-u)']
            useFileView = row['useFileView(-V)']
            writeFile = row['writeFile(-w)']
            fsyncPerWrite = row['fsyncPerWrite(-Y)']
            numProc = row['numProc']
            numNodes = row['numNode']

            jobName = 'iteration' + str(iteration + 1) + '.' + point + '.' + str(system) + '.' + str(numBB) + '.' + str(api) + '.' + str(blockSize) + '.' + str(collective) + '.' + str(fsync) \
                + '.' + str(useExistingTestFile) + '.' + str(filePerProc) + '.' + str(intraTestBarriers) + '.' + str(setAlignment) \
                + '.' + str(keepFile) + '.' + str(memoryPerNode) + '.' + str(noFill) + '.' + str(preallocate) + '.' + str(readFile) \
                + '.' + str(segment) + '.' + str(transferSize) + '.' + str(uniqueDir) + '.' + str(useFileView) + '.' + str(writeFile) \
                + '.' + str(fsyncPerWrite) + '.' + str(numProc)

            filename = baseDir + '/jobs/' + jobName + '.sh'

            blockSizeByte = toByte(blockSize)
            wtime = 1 + 2 ** math.ceil((math.log(blockSizeByte / 1024, 32)))
            wtime = math.ceil(wtime * (1 + numNodes / 8))

            generateLassenHeader(filename, numNodes, wtime, jobName, jobName)
                
            with open(filename, "a") as script:
                script.write("export NUM_PROC=" + str(numProc) + "\n")
                script.write("export NUM_NODES=" + str(numNodes) + "\n")
                script.write("export FILE_SYSTEM=" + system + "\n")
                script.write("export NUM_BB_SERVERS=" + str(numBB) + "\n")
                script.write("\n")

                script.write("export JOB_HOME=/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePointsNoRecorder\n")
                script.write("export BIN_PATH=/g/g92/xu23/gpfs1/local/ior-3.3.0/bin\n")
                script.write("export RECORDER_PATH=/g/g92/xu23/gpfs1/local/recorder-2.2\n")
                script.write("export DARSHAN_PATH=/g/g92/xu23/gpfs1/local/darshan-3.3.1\n")
                script.write("export PROFILES=/p/gpfs1/xu23/research/io-experiments/ior/extremePointsNoRecorder/profiles\n")     
                script.write("\n")     
                
                script.write("export API=" + str(api) + "\n")     
                script.write("export BLOCK_SIZE=" + str(blockSize) + "\n")     
                script.write("export SET_ALIGNMENT=" + str(setAlignment) + "\n")
                script.write("export MEMORY_PER_NODE=" + str(memoryPerNode) + "\n")
                script.write("export SEGMENT=" + str(segment) + "\n")     
                script.write("export TRANSFER_SIZE=" + str(transferSize) + "\n")     
                script.write("\n")

                script.write("mkdir -p /g/g92/xu23/gpfs1/research/io-experiments/ior/extremePointsNoRecorder/app-output-files/" + jobName + "\n")
                script.write("jsrun -r 1 -n ${NUM_NODES} mkdir -p $BBPATH/" + jobName + "\n")
                script.write("jsrun -r 1 -n ${NUM_NODES} cp /g/g92/xu23/gpfs1/research/io-experiments/ior/ior-util.py $BBPATH/" + jobName + "\n")
                script.write("cd $BBPATH/" + jobName + "\n")
                script.write("\n")

                script.write("export RECORDER_TRACES_DIR=/g/g92/xu23/gpfs1/research/io-experiments/ior/extremePointsNoRecorder/app-output-files/" + jobName + "\n")

                execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/ior -a ${API} -b ${BLOCK_SIZE} -J ${SET_ALIGNMENT} -M ${MEMORY_PER_NODE} -s ${SEGMENT} -t ${TRANSFER_SIZE}"
                
                if collective == 1:
                    execution += " -c"
                if fsync == 1:
                    execution += " -e"
                if filePerProc == 1:
                    execution += " -F"
                if intraTestBarriers == 1:
                    execution += " -g"
                if noFill == 1:
                    execution += " -n"
                if preallocate == 1:
                    execution += " -p"
                if uniqueDir == 1:
                    execution += " -u"
                if useFileView == 1:
                    execution += " -V"
                if fsyncPerWrite == 1:
                    execution += " -Y"

                redirect = " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"
                    
                script.write("printf \"start\\n\" > $JOB_HOME/app-stdio/" + jobName + ".txt\n")                

                script.write("printf \"\\nwrite scratch\\n\"" + redirect)
                execution1 = execution + " -w -k -o testFile" + redirect
                script.write("jsrun " + execution1)

                script.write("printf \"\\nWAW\\n\"" + redirect)
                execution2 = execution + " -w -E -k -o testFile" + redirect
                script.write("jsrun " + execution2)

                script.write("printf \"\\nRAW\\n\"" + redirect)
                execution3 = execution + " -r -k -o testFile" + redirect
                script.write("jsrun " + execution3)
                
                script.write("jsrun -r 1 -n ${NUM_NODES} python3 ior-util.py rename renamed " + str(uniqueDir) + "\n")

                script.write("printf \"\\nread renamed\\n\"" + redirect)
                execution4 = ''
                if uniqueDir == 1:
                    execution4 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution4 = execution + " -r -k -o renamed-testFile"
                execution4 += redirect
                script.write("jsrun " + execution4)

                script.write("printf \"\\nRAR\\n\"" + redirect)
                execution5 = ''
                if uniqueDir == 1:
                    execution5 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution5 = execution + " -r -k -o renamed-testFile"
                execution5 += redirect
                script.write("jsrun " + execution5)

                script.write("printf \"\\nWAR\\n\"" + redirect)
                execution6 = ''
                if uniqueDir == 1:
                    execution6 = execution + " -w -E -o trenamed-estFile"
                else:
                    execution6 = execution + " -w -E -o renamed-testFile"
                execution6 += redirect
                script.write("jsrun " + execution6)

                if uniqueDir == 1:
                    script.write("rm t* -r\n")

                script.write("jsrun -r 32 -n ${NUM_PROC} python3 ./ior-util.py prepareFileBB " + str(uniqueDir) + " touchedTestFile" + "\n")

                script.write("printf \"\\nwrite touched file\\n\"" + redirect)
                execution7 = execution + " -w -E -o touchedTestFile" + redirect
                script.write("jsrun " + execution7)

                if uniqueDir == 1:
                    script.write("rm t* -r\n")

                script.write("\n")

                
                # script.write("export MY_DARSHAN_LOG_DIR=$PROFILES\n")
                # script.write("export DXT_ENABLE_IO_TRACE=1\n")
                # script.write("jsrun -E LD_PRELOAD=${DARSHAN_PATH}/lib/libdarshan.so " + execution)
                # script.write("\n")

                numJob += 1
    print(numJob)
    return numJob


def generateLassenGpfsIOR(scriptPath='/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/jobs'):
    numJob = 0
    df = generateIOROptions()
    df = df[df['system'] == 'lassen-gpfs'].copy()
    df = df[df['numProc'] == 64].copy()
    # df = df.head(2).copy()
    for iteration in range(numIteration):
        for index, row in df.iterrows():
            system = row['system']
            numBB = row['numBB']
            if system != 'cori-bb' and numBB != 1:
                continue
            if system == 'lassen-gpfs' or system == 'summit-gpfs' or system == 'cori-lustre':
                numBB = 0
            api = row['api(-a)']
            blockSize = row['blockSize(-b)']
            collective = row['collective(-c)']
            if api == 'POSIX' and collective == 1:
                continue
            fsync = row['fsync(-e)']
            if api != 'POSIX' and fsync == 1:
                continue
            useExistingTestFile = row['useExistingTestFile(-E)']
            filePerProc = row['filePerProc(-F)']
            if (system == 'lassen-bb' or system == 'summit-bb') and filePerProc == 0:
                continue
            intraTestBarriers = row['intraTestBarriers(-g)']
            setAlignment = row['setAlignment(-J)']
            if api != 'HDF5' and setAlignment != 1:
                continue
            keepFile = row['keepFile(-k)']
            memoryPerNode = row['memoryPerNode(-M)']
            noFill = row['noFill(-n)']
            if api != 'HDF5' and noFill == 1:
                continue
            preallocate = row['preallocate(-p)']
            if api != 'MPIIO' and preallocate != 0:
                continue
            readFile = row['readFile(-r)']
            segment = row['segment(-s)']
            transferSize = row['transferSize(-t)']
            uniqueDir = row['uniqueDir(-u)']
            if uniqueDir == 1 and filePerProc != 1:
                continue
            useFileView = row['useFileView(-V)']
            if api != 'MPIIO' and useFileView != 0:
                continue
            writeFile = row['writeFile(-w)']
            fsyncPerWrite = row['fsyncPerWrite(-Y)']
            if api != 'POSIX' and fsyncPerWrite == 1:
                continue
            numProc = row['numProc']
            numNodes = row['numNodes']

            jobName = 'iteration' + str(iteration + 1) + '.' \
                + str(system) + '.' + str(numBB) + '.' + str(api) + '.' + str(blockSize) + '.' + str(collective) + '.' + str(fsync) \
                + '.' + str(useExistingTestFile) + '.' + str(filePerProc) + '.' + str(intraTestBarriers) + '.' + str(setAlignment) \
                + '.' + str(keepFile) + '.' + str(memoryPerNode) + '.' + str(noFill) + '.' + str(preallocate) + '.' + str(readFile) \
                + '.' + str(segment) + '.' + str(transferSize) + '.' + str(uniqueDir) + '.' + str(useFileView) + '.' + str(writeFile) \
                + '.' + str(fsyncPerWrite) + '.' + str(numProc)

            filename = scriptPath + '/' + jobName + '.sh'

            blockSizeByte = toByte(blockSize)
            wtime = 1 + 2 ** math.ceil((math.log(blockSizeByte / 1024, 32)))
            wtime = math.ceil(wtime * (1 + numNodes / 8))

            generateLassenHeader(filename, numNodes, wtime, jobName, jobName)
            
            with open(filename, "a") as script:
                script.write("export NUM_PROC=" + str(numProc) + "\n")
                script.write("export NUM_NODES=" + str(numNodes) + "\n")
                script.write("export FILE_SYSTEM=" + system + "\n")
                script.write("export NUM_BB_SERVERS=" + str(numBB) + "\n")
                script.write("\n")

                script.write("export JOB_HOME=/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs\n")
                script.write("export BIN_PATH=/g/g92/xu23/gpfs1/local/ior-3.3.0/bin\n")
                script.write("export RECORDER_PATH=/g/g92/xu23/gpfs1/local/recorder-2.2\n")
                script.write("export DARSHAN_PATH=/g/g92/xu23/gpfs1/local/darshan-3.3.1\n")
                script.write("export PROFILES=/p/gpfs1/xu23/research/io-experiments/ior/gpfs/profiles\n")     
                script.write("\n")     
                
                script.write("export API=" + str(api) + "\n")     
                script.write("export BLOCK_SIZE=" + str(blockSize) + "\n")     
                script.write("export SET_ALIGNMENT=" + str(setAlignment) + "\n")
                script.write("export MEMORY_PER_NODE=" + str(memoryPerNode) + "\n")
                script.write("export SEGMENT=" + str(segment) + "\n")     
                script.write("export TRANSFER_SIZE=" + str(transferSize) + "\n")     
                script.write("\n")

                script.write("mkdir -p /g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/app-output-files/" + jobName + "\n")
                script.write("cp /g/g92/xu23/gpfs1/research/io-experiments/ior/ior-util.py /g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/app-output-files/" + jobName + "\n")
                script.write("cd /g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/app-output-files/" + jobName + "\n")
                script.write("\n")

                execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/ior -a ${API} -b ${BLOCK_SIZE} -J ${SET_ALIGNMENT} -M ${MEMORY_PER_NODE} -s ${SEGMENT} -t ${TRANSFER_SIZE}"
                
                if collective == 1:
                    execution += " -c"
                if fsync == 1:
                    execution += " -e"
                if filePerProc == 1:
                    execution += " -F"
                if intraTestBarriers == 1:
                    execution += " -g"
                if noFill == 1:
                    execution += " -n"
                if preallocate == 1:
                    execution += " -p"
                if uniqueDir == 1:
                    execution += " -u"
                if useFileView == 1:
                    execution += " -V"
                if fsyncPerWrite == 1:
                    execution += " -Y"

                redirect = " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"
                    
                script.write("printf \"start\\n\" > $JOB_HOME/app-stdio/" + jobName + ".txt\n")

                script.write("printf \"\\nwrite scratch\\n\"" + redirect)
                execution1 = execution + " -w -k -o testFile" + redirect
                script.write("jsrun " + execution1)

                script.write("printf \"\\nWAW\\n\"" + redirect)
                execution2 = execution + " -w -E -k -o testFile" + redirect
                script.write("jsrun " + execution2)

                script.write("printf \"\\nRAW\\n\"" + redirect)
                execution3 = execution + " -r -k -o testFile" + redirect
                script.write("jsrun " + execution3)
                
                script.write("python3 ior-util.py rename renamed " + str(uniqueDir) + "\n")

                script.write("printf \"\\nread renamed\\n\"" + redirect)
                execution4 = ''
                if uniqueDir == 1:
                    execution4 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution4 = execution + " -r -k -o renamed-testFile"
                execution4 += redirect
                script.write("jsrun " + execution4)

                script.write("printf \"\\nRAR\\n\"" + redirect)
                execution5 = ''
                if uniqueDir == 1:
                    execution5 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution5 = execution + " -r -k -o renamed-testFile"
                execution5 += redirect
                script.write("jsrun " + execution5)

                script.write("printf \"\\nWAR\\n\"" + redirect)
                execution6 = ''
                if uniqueDir == 1:
                    execution6 = execution + " -w -E -o trenamed-estFile"
                else:
                    execution6 = execution + " -w -E -o renamed-testFile"
                execution6 += redirect
                script.write("jsrun " + execution6)

                if uniqueDir == 1:
                    script.write("rm t* -r\n")

                script.write("python3 ./ior-util.py prepareFileGpfs " + str(uniqueDir) + " " + str(filePerProc) + " " + str(numProc) + " touchedTestFile" + "\n")

                script.write("printf \"\\nwrite touched file\\n\"" + redirect)
                execution7 = execution + " -w -E -o touchedTestFile" + redirect
                script.write("jsrun " + execution7)

                if uniqueDir == 1:
                    script.write("rm t* -r\n")

                script.write("\n")

                # script.write("export RECORDER_TRACES_DIR=$PROFILES\n")
                # script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution)
                # script.write("\n")
                
                # script.write("export MY_DARSHAN_LOG_DIR=$PROFILES\n")
                # script.write("export DXT_ENABLE_IO_TRACE=1\n")
                # script.write("jsrun -E LD_PRELOAD=${DARSHAN_PATH}/lib/libdarshan.so " + execution)
                # script.write("\n")

                numJob += 1
    print(numJob)
    return numJob

def generateLassenBBIOR(scriptPath='/g/g92/xu23/gpfs1/research/io-experiments/ior/bb/jobs'):
    numJob = 0
    df = generateIOROptions()
    df = df[df['system'] == 'lassen-bb'].copy()
    df = df[df['numProc'] == 64].copy()
    # df = df.head(10).copy()
    for iteration in range(numIteration):
        for index, row in df.iterrows():
            system = row['system']
            numBB = row['numBB']
            if system != 'cori-bb' and numBB != 1:
                continue
            if system == 'lassen-gpfs' or system == 'summit-gpfs' or system == 'cori-lustre':
                numBB = 0
            api = row['api(-a)']
            blockSize = row['blockSize(-b)']
            collective = row['collective(-c)']
            if api == 'POSIX' and collective == 1:
                continue
            fsync = row['fsync(-e)']
            if api != 'POSIX' and fsync == 1:
                continue
            useExistingTestFile = row['useExistingTestFile(-E)']
            filePerProc = row['filePerProc(-F)']
            if (system == 'lassen-bb' or system == 'summit-bb') and filePerProc == 0:
                continue
            intraTestBarriers = row['intraTestBarriers(-g)']
            setAlignment = row['setAlignment(-J)']
            if api != 'HDF5' and setAlignment != 1:
                continue
            keepFile = row['keepFile(-k)']
            memoryPerNode = row['memoryPerNode(-M)']
            noFill = row['noFill(-n)']
            if api != 'HDF5' and noFill == 1:
                continue
            preallocate = row['preallocate(-p)']
            if api != 'MPIIO' and preallocate != 0:
                continue
            readFile = row['readFile(-r)']
            segment = row['segment(-s)']
            transferSize = row['transferSize(-t)']
            uniqueDir = row['uniqueDir(-u)']
            if uniqueDir == 1 and filePerProc != 1:
                continue
            useFileView = row['useFileView(-V)']
            if api != 'MPIIO' and useFileView != 0:
                continue
            writeFile = row['writeFile(-w)']
            fsyncPerWrite = row['fsyncPerWrite(-Y)']
            if api != 'POSIX' and fsyncPerWrite == 1:
                continue
            numProc = row['numProc']
            numNodes = row['numNodes']

            jobName = 'iteration' + str(iteration + 1) + '.' \
                + str(system) + '.' + str(numBB) + '.' + str(api) + '.' + str(blockSize) + '.' + str(collective) + '.' + str(fsync) \
                + '.' + str(useExistingTestFile) + '.' + str(filePerProc) + '.' + str(intraTestBarriers) + '.' + str(setAlignment) \
                + '.' + str(keepFile) + '.' + str(memoryPerNode) + '.' + str(noFill) + '.' + str(preallocate) + '.' + str(readFile) \
                + '.' + str(segment) + '.' + str(transferSize) + '.' + str(uniqueDir) + '.' + str(useFileView) + '.' + str(writeFile) \
                + '.' + str(fsyncPerWrite) + '.' + str(numProc)

            filename = scriptPath + '/' + jobName + '.sh'

            blockSizeByte = toByte(blockSize)
            wtime = 1 + 2 ** math.ceil((math.log(blockSizeByte / 1024, 32)))
            wtime = math.ceil(wtime * (1 + numNodes / 8))

            generateLassenHeader(filename, numNodes, wtime, jobName, jobName)
            
            with open(filename, "a") as script:
                script.write("export NUM_PROC=" + str(numProc) + "\n")
                script.write("export NUM_NODES=" + str(numNodes) + "\n")
                script.write("export FILE_SYSTEM=" + system + "\n")
                script.write("export NUM_BB_SERVERS=" + str(numBB) + "\n")
                script.write("\n")

                script.write("export JOB_HOME=/g/g92/xu23/gpfs1/research/io-experiments/ior/bb\n")
                script.write("export BIN_PATH=/g/g92/xu23/gpfs1/local/ior-3.3.0/bin\n")
                script.write("export RECORDER_PATH=/g/g92/xu23/gpfs1/local/recorder-2.2\n")
                script.write("export DARSHAN_PATH=/g/g92/xu23/gpfs1/local/darshan-3.3.1\n")
                script.write("export PROFILES=/p/gpfs1/xu23/research/io-experiments/ior/bb/profiles\n")     
                script.write("\n")     
                
                script.write("export API=" + str(api) + "\n")     
                script.write("export BLOCK_SIZE=" + str(blockSize) + "\n")     
                script.write("export SET_ALIGNMENT=" + str(setAlignment) + "\n")
                script.write("export MEMORY_PER_NODE=" + str(memoryPerNode) + "\n")
                script.write("export SEGMENT=" + str(segment) + "\n")     
                script.write("export TRANSFER_SIZE=" + str(transferSize) + "\n")     
                script.write("\n")

                script.write("jsrun -r 1 -n ${NUM_NODES} mkdir -p $BBPATH/" + jobName + "\n")
                script.write("jsrun -r 1 -n ${NUM_NODES} cp /g/g92/xu23/gpfs1/research/io-experiments/ior/ior-util.py $BBPATH/" + jobName + "\n")
                script.write("cd $BBPATH/" + jobName + "\n")
                script.write("\n")

                execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/ior -a ${API} -b ${BLOCK_SIZE} -J ${SET_ALIGNMENT} -M ${MEMORY_PER_NODE} -s ${SEGMENT} -t ${TRANSFER_SIZE}"
                
                if collective == 1:
                    execution += " -c"
                if fsync == 1:
                    execution += " -e"
                if filePerProc == 1:
                    execution += " -F"
                if intraTestBarriers == 1:
                    execution += " -g"
                if noFill == 1:
                    execution += " -n"
                if preallocate == 1:
                    execution += " -p"
                if uniqueDir == 1:
                    execution += " -u"
                if useFileView == 1:
                    execution += " -V"
                if fsyncPerWrite == 1:
                    execution += " -Y"

                redirect = " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"
                    
                script.write("printf \"start\\n\" > $JOB_HOME/app-stdio/" + jobName + ".txt\n")

                script.write("printf \"\\nwrite scratch\\n\"" + redirect)
                execution1 = execution + " -w -k -o testFile" + redirect
                script.write("jsrun " + execution1)

                script.write("printf \"\\nWAW\\n\"" + redirect)
                execution2 = execution + " -w -E -k -o testFile" + redirect
                script.write("jsrun " + execution2)

                script.write("printf \"\\nRAW\\n\"" + redirect)
                execution3 = execution + " -r -k -o testFile" + redirect
                script.write("jsrun " + execution3)
                
                script.write("jsrun -r 1 -n ${NUM_NODES} python3 ior-util.py rename renamed " + str(uniqueDir) + "\n")

                script.write("printf \"\\nread renamed\\n\"" + redirect)
                execution4 = ''
                if uniqueDir == 1:
                    execution4 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution4 = execution + " -r -k -o renamed-testFile"
                execution4 += redirect
                script.write("jsrun " + execution4)

                script.write("printf \"\\nRAR\\n\"" + redirect)
                execution5 = ''
                if uniqueDir == 1:
                    execution5 = execution + " -r -k -o trenamed-estFile"
                else:
                    execution5 = execution + " -r -k -o renamed-testFile"
                execution5 += redirect
                script.write("jsrun " + execution5)

                script.write("printf \"\\nWAR\\n\"" + redirect)
                execution6 = ''
                if uniqueDir == 1:
                    execution6 = execution + " -w -E -o trenamed-estFile"
                else:
                    execution6 = execution + " -w -E -o renamed-testFile"
                execution6 += redirect
                script.write("jsrun " + execution6)

                # if uniqueDir == 1:
                #     script.write("rm t* -r\n")

                script.write("jsrun -r 32 -n ${NUM_PROC} python3 ./ior-util.py prepareFileBB " + str(uniqueDir) + " touchedTestFile" + "\n")

                script.write("printf \"\\nwrite touched file\\n\"" + redirect)
                execution7 = execution + " -w -E -o touchedTestFile" + redirect
                script.write("jsrun " + execution7)

                # if uniqueDir == 1:
                #     script.write("rm t* -r\n")

                script.write("\n")

                # script.write("export RECORDER_TRACES_DIR=$PROFILES\n")
                # script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution)
                # script.write("\n")
                
                # script.write("export MY_DARSHAN_LOG_DIR=$PROFILES\n")
                # script.write("export DXT_ENABLE_IO_TRACE=1\n")
                # script.write("jsrun -E LD_PRELOAD=${DARSHAN_PATH}/lib/libdarshan.so " + execution)
                # script.write("\n")

                numJob += 1
    print(numJob)
    return numJob

def generateIOROptions():
    system = DataFrame({'system': ['lassen-gpfs', 'lassen-bb', 'summit-gpfs', 'summit-bb', 'cori-lustre', 'cori-bb'], 'key': [0] * 6})
    numBB = DataFrame({'numBB': [1, 4, 16, 64, 256], 'key': [0] * 5})
    api = DataFrame({'api(-a)': ['POSIX', 'MPIIO'], 'key': [0] * 2})
    collective = DataFrame({'collective(-c)': [0, 1], 'key': [0] * 2})
    fsync = DataFrame({'fsync(-e)': [0, 1], 'key': [0] * 2})
    useExistingTestFile = DataFrame({'useExistingTestFile(-E)': [0], 'key': [0]})
    filePerProc = DataFrame({'filePerProc(-F)': [0, 1], 'key': [0] * 2})
    intraTestBarriers = DataFrame({'intraTestBarriers(-g)': [0], 'key': [0]})
    setAlignment = DataFrame({'setAlignment(-J)': [1], 'key': [0]})
    keepFile = DataFrame({'keepFile(-k)': [0], 'key': [0]})
    memoryPerNode = DataFrame({'memoryPerNode(-M)': [0], 'key': [0]})
    noFill = DataFrame({'noFill(-n)': [0, 1], 'key': [0] * 2})
    preallocate = DataFrame({'preallocate(-p)': [0, 1], 'key': [0] * 2})
    readFile = DataFrame({'readFile(-r)': [1], 'key': [0]})
    segment = DataFrame({'segment(-s)': [1], 'key': [0]})
    transferSize = DataFrame({'transferSize(-t)': ['1k', '32k', '1m', '32m', '1g'], 'blockSize(-b)': ['1k', '32k', '1m', '32m', '1g'], 'key': [0] * 5})
    uniqueDir = DataFrame({'uniqueDir(-u)': [0, 1], 'key': [0] * 2})
    useFileView = DataFrame({'useFileView(-V)': [0, 1], 'key': [0] * 2})
    writeFile = DataFrame({'writeFile(-w)': [1], 'key': [0]})
    fsyncPerWrite = DataFrame({'fsyncPerWrite(-Y)': [0, 1], 'key': [0] * 2})
    numProc = DataFrame({'numProc': [64, 128, 256, 512], 'numNodes': [2, 4, 8, 16], 'key': [0] * 4})

    df = system.merge(numBB, how='outer',  on='key')
    df = df.merge(api, how='outer',  on='key')
    df= df.merge(collective, how='outer',  on='key')
    df= df.merge(fsync, how='outer',  on='key')
    df= df.merge(useExistingTestFile, how='outer',  on='key')
    df= df.merge(filePerProc, how='outer',  on='key')
    df= df.merge(intraTestBarriers, how='outer',  on='key')
    df= df.merge(setAlignment, how='outer',  on='key')
    df= df.merge(keepFile, how='outer',  on='key')
    df= df.merge(memoryPerNode, how='outer',  on='key')
    df= df.merge(noFill, how='outer',  on='key')
    df= df.merge(preallocate, how='outer',  on='key')
    df= df.merge(readFile, how='outer',  on='key')
    df= df.merge(segment, how='outer',  on='key')
    df= df.merge(uniqueDir, how='outer',  on='key')
    df= df.merge(useFileView, how='outer',  on='key')
    df= df.merge(writeFile, how='outer',  on='key')
    df= df.merge(fsyncPerWrite, how='outer',  on='key')
    df = df.merge(transferSize, how='outer',  on='key')
    df= df.merge(numProc, how='outer',  on='key')
    return df

def toByte(size):
    if type(size) == int:
        return size
    unit = size[-1]
    if unit == 'k':
        return int(size[:-1]) * 1024
    elif unit == 'm':
        return int(size[:-1]) * 1024 * 1024
    elif unit == 'g':
        return int(size[:-1]) * 1024 * 1024 * 1024
    else:
        return int(size)

if __name__ == '__main__':
    pass
    system = sys.argv[1]
    if system == 'gpfs':
        generateLassenGpfsIOR()
    elif system == 'bb':
        generateLassenBBIOR()
    elif system == 'gpfs-extreme':
        generateLassenGpfsExtremePoints()
    elif system == 'bb-extreme':
        generateLassenBBExtremePoints()
    elif system == 'gpfs-extreme-no-recorder':
        generateLassenGpfsExtremePointsNoRecorder()
    elif system == 'bb-extreme-no-recorder':
        generateLassenBBExtremePointsNoRecorder()
    