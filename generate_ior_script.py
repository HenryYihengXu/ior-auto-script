import sys
from generate_header import *
import pandas as pd
from pandas import DataFrame
import math

numIteration = 5

def generateLassenGpfsIOR(scriptPath='/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/jobs'):
    previousJobName = ''
    numJob = 0
    df = generateIOROptions()
    df = df[df['system'] == 'lassen-gpfs'].copy()
    # df1 = df[(df['useExistingTestFile(-E)'] == 1) & (df['filePerProc(-F)'] == 1) & (df['uniqueDir(-u)'] == 1)].head(3).copy()
    # df2 = df[(df['useExistingTestFile(-E)'] == 1) & (df['filePerProc(-F)'] == 0) & (df['uniqueDir(-u)'] == 0)].head(3).copy()
    # df3 = df[(df['useExistingTestFile(-E)'] == 1) & (df['filePerProc(-F)'] == 1) & (df['uniqueDir(-u)'] == 0)].head(3).copy()
    # df = pd.concat([df1, df2, df3])
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
        writeFile = row['writeFile(-w)']
        fsyncPerWrite = row['fsyncPerWrite(-Y)']
        if api != 'POSIX' and fsyncPerWrite == 1:
            continue
        numProc = row['numProc']
        numNodes = row['numNodes']

        jobName = str(system) + '.' + str(numBB) + '.' + str(api) + '.' + str(blockSize) + '.' + str(collective) + '.' + str(fsync) \
            + '.' + str(useExistingTestFile) + '.' + str(filePerProc) + '.' + str(intraTestBarriers) + '.' + str(setAlignment) \
            + '.' + str(keepFile) + '.' + str(memoryPerNode) + '.' + str(noFill) + '.' + str(preallocate) + '.' + str(readFile) \
            + '.' + str(segment) + '.' + str(transferSize) + '.' + str(uniqueDir) + '.' + str(useFileView) + '.' + str(writeFile) \
            + '.' + str(fsyncPerWrite) + '.' + str(numProc)

        filename = scriptPath + '/' + jobName + '.sh'

        # if numProc == 512:
        #     if previousJobName != '':
        #         generateLassenHeader(filename, numNodes, numNodes * 4, jobName, jobName, '\"ended(\"' + previousJobName + '\")\"')
        #     else:
        #         generateLassenHeader(filename, numNodes, numNodes * 4, jobName, jobName)
        #     previousJobName = jobName
        # else:
        #     generateLassenHeader(filename, numNodes, numNodes * 4, jobName, jobName)
        blockSizeByte = toByte(blockSize)
        wtime = 4 + 2 ** math.ceil((math.log(blockSizeByte / 1024, 32)))
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
            
            script.write("for ITERATION in")
            for i in range(1, numIteration + 1):
                script.write(" " + str(i))
            script.write("\n")
            script.write("do\n")

            script.write("printf \"\\nIteration ${ITERATION}\\n\"" + redirect)

            script.write("printf \"\\nwrite scratch\\n\"" + redirect)
            execution1 = execution + " -w -k -o testFile${ITERATION}" + redirect
            script.write("    jsrun " + execution1)

            script.write("printf \"\\nWAW\\n\"" + redirect)
            execution2 = execution + " -w -E -k -o testFile${ITERATION}" + redirect
            script.write("    jsrun " + execution2)

            script.write("printf \"\\nRAW\\n\"" + redirect)
            execution3 = execution + " -r -k -o testFile${ITERATION}" + redirect
            script.write("    jsrun " + execution3)
            
            script.write("    python3 ior-util.py rename renamed " + str(uniqueDir) + "\n")

            script.write("printf \"\\nread renamed\\n\"" + redirect)
            execution4 = ''
            if uniqueDir == 1:
                execution4 = execution + " -r -k -o trenamed-estFile${ITERATION}"
            else:
                execution4 = execution + " -r -k -o renamed-testFile${ITERATION}"
            execution4 += redirect
            script.write("    jsrun " + execution4)

            script.write("printf \"\\nRAR\\n\"" + redirect)
            execution5 = ''
            if uniqueDir == 1:
                execution5 = execution + " -r -k -o trenamed-estFile${ITERATION}"
            else:
                execution5 = execution + " -r -k -o renamed-testFile${ITERATION}"
            execution5 += redirect
            script.write("    jsrun " + execution5)

            script.write("printf \"\\nWAR\\n\"" + redirect)
            execution6 = ''
            if uniqueDir == 1:
                execution6 = execution + " -w -E -o trenamed-estFile${ITERATION}"
            else:
                execution6 = execution + " -w -E -o renamed-testFile${ITERATION}"
            execution6 += redirect
            script.write("    jsrun " + execution6)

            if uniqueDir == 1:
                script.write("rm t* -r\n")

            script.write("python3 ./ior-util.py prepareFileGpfs " + str(uniqueDir) + " " + str(filePerProc) + " " + str(numProc) + " touchedTestFile${ITERATION}" + "\n")

            script.write("printf \"\\nwrite touched file\\n\"" + redirect)
            execution7 = execution + " -w -E -o touchedTestFile${ITERATION}" + redirect
            script.write("    jsrun " + execution7)

            if uniqueDir == 1:
                script.write("rm t* -r\n")

            script.write("done\n")
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
    previousJobName = ''
    numJob = 0
    df = generateIOROptions()
    df = df[df['system'] == 'lassen-bb'].copy()
    print(df.shape)
    df = df.head(100).copy()
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
        writeFile = row['writeFile(-w)']
        fsyncPerWrite = row['fsyncPerWrite(-Y)']
        numProc = row['numProc']
        numNodes = row['numNodes']

        jobName = str(system) + '.' + str(numBB) + '.' + str(api) + '.' + str(blockSize) + '.' + str(collective) + '.' + str(fsync) \
            + '.' + str(useExistingTestFile) + '.' + str(filePerProc) + '.' + str(intraTestBarriers) + '.' + str(setAlignment) \
            + '.' + str(1) + '.' + str(memoryPerNode) + '.' + str(noFill) + '.' + str(preallocate) + '.' + str(readFile) \
            + '.' + str(segment) + '.' + str(transferSize) + '.' + str(uniqueDir) + '.' + str(useFileView) + '.' + str(writeFile) \
            + '.' + str(fsyncPerWrite) + '.' + str(numProc)

        filename = scriptPath + '/' + jobName + '.sh'

        if numProc == 512:
            if previousJobName != '':
                generateLassenHeader(filename, numNodes, numNodes * 4, jobName, jobName, '\"ended(\"' + previousJobName + '\")\"')
            else:
                generateLassenHeader(filename, numNodes, numNodes * 4, jobName, jobName)
            previousJobName = jobName
        else:
            generateLassenHeader(filename, numNodes, numNodes * 4, jobName, jobName)
        
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

            script.write("cd $BBPATH\n")
            script.write("\n")

            execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/ior -a ${API} -b ${BLOCK_SIZE} -J ${SET_ALIGNMENT} -M ${MEMORY_PER_NODE} -s ${SEGMENT} -t ${TRANSFER_SIZE}"
            if collective == 1:
                execution += " -c"
            if fsync == 1:
                execution += " -e"
            if useExistingTestFile == 1:
                execution += " -E"
            if filePerProc == 1:
                execution += " -F"
            if intraTestBarriers == 1:
                execution += " -g"
            if keepFile == 1:
                execution += " -k"
            if noFill == 1:
                execution += " -n"
            if preallocate == 1:
                execution += " -p"
            if readFile == 1:
                execution += " -r"
            if uniqueDir == 1:
                execution += " -u"
            if useFileView == 1:
                execution += " -V"
            if writeFile == 1:
                execution += " -w"
            if fsyncPerWrite == 1:
                execution += " -Y"
            
            # execution += " -o $JOB_HOME/app-output-files/" + jobName + ".testFile"
            execution += " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"

            script.write("for ITERATION in")
            for i in range(1, numIteration + 1):
                script.write(" " + str(i))
            script.write("\n")
            script.write("do\n")
            script.write("    jsrun " + execution)
            script.write("done\n")
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
    useFileView = DataFrame({'useFileView(-V)': [0], 'key': [0]})
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
    generateLassenGpfsIOR()
    # generateLassenBBIOR()
    