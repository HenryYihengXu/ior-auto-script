from fileinput import filename
import sys
from generate_header import *
import pandas as pd
from pandas import DataFrame
import subprocess
import math

numIteration = 5

def generateLassenGpfsHaccio(scriptPath='/g/g92/xu23/gpfs1/research/io-experiments/haccio/gpfs/jobs'):
    numJob = 0
    df = generateHaccioOptions()
    df = df[df['system'] == 'lassen-gpfs'].copy()
    # print(df)
    # df = df[df['numProc'] == 64].copy()
    # df = df.head(2).copy()
    for iteration in range(numIteration):
        for index, row in df.iterrows():
            system = row['system']
            numBB = row['numBB']
            if system != 'cori-bb' and numBB != 1:
                continue
            if system == 'lassen-gpfs' or system == 'summit-gpfs' or system == 'cori-lustre':
                numBB = 0
            scaling = row['scaling']
            numProc = row['numProc']
            numNodes = row['numNodes']
            numParticles = 0
            if scaling == 'strong-scaling':
                numParticles = math.ceil(524288 / numProc * 64)
            else:
                numParticles = 131072

            jobName = 'iteration' + str(iteration + 1) + '.' \
                + str(system) + '.' + str(numBB) + '.' + scaling + '.' + str(numParticles) + '.' \
                + str(numProc)

            filename = scriptPath + "/" + jobName + ".sh"

            wtime = math.ceil(numParticles / 1024 / 32)
            wtime = math.ceil(wtime * (1 + numNodes / 8))
            
            generateLassenHeader(filename, numNodes, wtime, jobName, jobName)
            
            with open(filename, "a") as script:
                script.write("export NUM_PROC=" + str(numProc) + "\n")
                script.write("export NUM_NODES=" + str(numNodes) + "\n")
                script.write("export FILE_SYSTEM=" + "lassen-gpfs" + "\n")
                script.write("export NUM_BB_SERVERS=" + str(0) + "\n")
                script.write("\n")

                script.write("export JOB_HOME=/g/g92/xu23/gpfs1/research/io-experiments/haccio/gpfs\n")
                script.write("export BIN_PATH=/g/g92/xu23/gpfs1/apps/hacc-io-modified\n")
                script.write("export RECORDER_PATH=/g/g92/xu23/gpfs1/local/recorder-2.2\n")
                script.write("export DARSHAN_PATH=/g/g92/xu23/gpfs1/local/darshan-3.3.1\n")
                script.write("export PROFILES=/p/gpfs1/xu23/research/io-experiments/haccio/gpfs/profiles\n")     
                script.write("\n")     
                
                script.write("export NUM_PARTICLES=" + str(numParticles) + "\n")        
                script.write("\n")

                script.write("mkdir -p /g/g92/xu23/gpfs1/research/io-experiments/haccio/gpfs/app-output-files/" + jobName + "\n")
                script.write("cd /g/g92/xu23/gpfs1/research/io-experiments/haccio/gpfs/app-output-files/" + jobName + "\n")
                script.write("\n")

                execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/hacc_io ${NUM_PARTICLES} ./" + jobName
                
                execution += " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"

                script.write("jsrun " + execution)
                script.write("rm ./" + jobName + "-Part*\n")
                script.write("mv recorder-logs " + jobName + ".recorder-log\n")
                script.write("\n")
                
                numJob += 1
    
    for index, row in df.iterrows():
        system = row['system']
        numBB = row['numBB']
        if system != 'cori-bb' and numBB != 1:
            continue
        if system == 'lassen-gpfs' or system == 'summit-gpfs' or system == 'cori-lustre':
            numBB = 0
        scaling = row['scaling']
        numProc = row['numProc']
        numNodes = row['numNodes']
        numParticles = 0
        if scaling == 'strong-scaling':
            numParticles = math.ceil(524288 / numProc * 64)
        else:
            numParticles = 131072

        jobName = str(system) + '.' + str(numBB) + '.' + scaling + '.' + str(numParticles) + '.' \
            + str(numProc) + '.recorder'

        filename = scriptPath + "/" + jobName + ".sh"

        wtime = math.ceil(numParticles / 1024 / 32)
        wtime = math.ceil(wtime * (1 + numNodes / 8))
        
        generateLassenHeader(filename, numNodes, wtime, jobName, jobName)
        
        with open(filename, "a") as script:
            script.write("export NUM_PROC=" + str(numProc) + "\n")
            script.write("export NUM_NODES=" + str(numNodes) + "\n")
            script.write("export FILE_SYSTEM=" + "lassen-gpfs" + "\n")
            script.write("export NUM_BB_SERVERS=" + str(0) + "\n")
            script.write("\n")

            script.write("export JOB_HOME=/g/g92/xu23/gpfs1/research/io-experiments/haccio/gpfs\n")
            script.write("export BIN_PATH=/g/g92/xu23/gpfs1/apps/hacc-io-modified\n")
            script.write("export RECORDER_PATH=/g/g92/xu23/gpfs1/local/recorder-2.2\n")
            script.write("export DARSHAN_PATH=/g/g92/xu23/gpfs1/local/darshan-3.3.1\n")
            script.write("export PROFILES=/p/gpfs1/xu23/research/io-experiments/haccio/gpfs/profiles\n")     
            script.write("\n")     
            
            script.write("export NUM_PARTICLES=" + str(numParticles) + "\n")        
            script.write("\n")

            script.write("mkdir -p /g/g92/xu23/gpfs1/research/io-experiments/haccio/gpfs/app-output-files/" + jobName + "\n")
            script.write("cd /g/g92/xu23/gpfs1/research/io-experiments/haccio/gpfs/app-output-files/" + jobName + "\n")
            script.write("\n")

            execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/hacc_io ${NUM_PARTICLES} ./" + jobName
            
            # execution += " -o $JOB_HOME/app-output-files/" + jobName + ".testFile"
            execution += " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"

            script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution)
            script.write("rm ./" + jobName + "-Part*\n")
            script.write("mv recorder-logs " + jobName + ".recorder-log\n")
            script.write("\n")
            
            # script.write("export MY_DARSHAN_LOG_DIR=$PROFILES\n")
            # script.write("export DXT_ENABLE_IO_TRACE=1\n")
            # script.write("jsrun -E LD_PRELOAD=${DARSHAN_PATH}/lib/libdarshan.so " + execution)
            # script.write("\n")
            numJob += 1

    print(numJob)

def generateLassenBBHaccio(scriptPath='/g/g92/xu23/gpfs1/research/io-experiments/haccio/bb/jobs'):
    numJob = 0
    df = generateHaccioOptions()
    df = df[df['system'] == 'lassen-bb'].copy()
    # df = df[df['numProc'] == 64].copy()
    # df = df.head(2).copy()
    for iteration in range(numIteration):
        for index, row in df.iterrows():
            system = row['system']
            numBB = row['numBB']
            if system != 'cori-bb' and numBB != 1:
                continue
            if system == 'lassen-gpfs' or system == 'summit-gpfs' or system == 'cori-lustre':
                numBB = 0
            scaling = row['scaling']
            numProc = row['numProc']
            numNodes = row['numNodes']
            numParticles = 0
            if scaling == 'strong-scaling':
                numParticles = math.ceil(524288 / numProc * 64)
            else:
                numParticles = 131072

            jobName = 'iteration' + str(iteration + 1) + '.' \
                + str(system) + '.' + str(numBB) + '.' + scaling + '.' + str(numParticles) + '.' \
                + str(numProc)

            filename = scriptPath + "/" + jobName + ".sh"

            wtime = math.ceil(numParticles / 1024 / 32)
            wtime = math.ceil(wtime * (1 + numNodes / 8))
            
            generateLassenHeader(filename, numNodes, wtime, jobName, jobName)
            
            with open(filename, "a") as script:
                script.write("export NUM_PROC=" + str(numProc) + "\n")
                script.write("export NUM_NODES=" + str(numNodes) + "\n")
                script.write("export FILE_SYSTEM=" + "lassen-bb" + "\n")
                script.write("export NUM_BB_SERVERS=" + str(0) + "\n")
                script.write("\n")

                script.write("export JOB_HOME=/g/g92/xu23/gpfs1/research/io-experiments/haccio/bb\n")
                script.write("export BIN_PATH=/g/g92/xu23/gpfs1/apps/hacc-io-modified\n")
                script.write("export RECORDER_PATH=/g/g92/xu23/gpfs1/local/recorder-2.2\n")
                script.write("export DARSHAN_PATH=/g/g92/xu23/gpfs1/local/darshan-3.3.1\n")
                script.write("export PROFILES=/p/gpfs1/xu23/research/io-experiments/haccio/bb/profiles\n")     
                script.write("\n")     
                
                script.write("export NUM_PARTICLES=" + str(numParticles) + "\n")        
                script.write("\n")

                script.write("mkdir -p /g/g92/xu23/gpfs1/research/io-experiments/haccio/bb/app-output-files/" + jobName + "\n")
                script.write("jsrun -r1 -n${NUM_NODES} mkdir -p $BBPATH/" + jobName + "\n")
                script.write("cd $BBPATH/" + jobName + "\n")
                script.write("\n")

                execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/hacc_io ${NUM_PARTICLES} ./" + jobName
                
                execution += " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"

                script.write("export RECORDER_TRACES_DIR=/g/g92/xu23/gpfs1/research/io-experiments/haccio/bb/app-output-files/" + jobName + "\n")
                script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution)
                # script.write("rm ./" + jobName + "-Part*\n")
                # script.write("mv recorder-logs " + jobName + ".recorder-log\n")
                script.write("\n")
                
                # script.write("export MY_DARSHAN_LOG_DIR=$PROFILES\n")
                # script.write("export DXT_ENABLE_IO_TRACE=1\n")
                # script.write("jsrun -E LD_PRELOAD=${DARSHAN_PATH}/lib/libdarshan.so " + execution)
                # script.write("\n")
                numJob += 1
    
    for index, row in df.iterrows():
        system = row['system']
        numBB = row['numBB']
        if system != 'cori-bb' and numBB != 1:
            continue
        if system == 'lassen-gpfs' or system == 'summit-gpfs' or system == 'cori-lustre':
            numBB = 0
        scaling = row['scaling']
        numProc = row['numProc']
        numNodes = row['numNodes']
        numParticles = 0
        if scaling == 'strong-scaling':
            numParticles = math.ceil(524288 / numProc * 64)
        else:
            numParticles = 131072

        jobName = str(system) + '.' + str(numBB) + '.' + scaling + '.' + str(numParticles) + '.' \
            + str(numProc) + '.recorder'

        filename = scriptPath + "/" + jobName + ".sh"

        wtime = math.ceil(numParticles / 1024 / 32)
        wtime = math.ceil(wtime * (1 + numNodes / 8))
        
        generateLassenHeader(filename, numNodes, wtime, jobName, jobName)
        
        with open(filename, "a") as script:
            script.write("export NUM_PROC=" + str(numProc) + "\n")
            script.write("export NUM_NODES=" + str(numNodes) + "\n")
            script.write("export FILE_SYSTEM=" + "lassen-bb" + "\n")
            script.write("export NUM_BB_SERVERS=" + str(0) + "\n")
            script.write("\n")

            script.write("export JOB_HOME=/g/g92/xu23/gpfs1/research/io-experiments/haccio/bb\n")
            script.write("export BIN_PATH=/g/g92/xu23/gpfs1/apps/hacc-io-modified\n")
            script.write("export RECORDER_PATH=/g/g92/xu23/gpfs1/local/recorder-2.2\n")
            script.write("export DARSHAN_PATH=/g/g92/xu23/gpfs1/local/darshan-3.3.1\n")
            script.write("export PROFILES=/p/gpfs1/xu23/research/io-experiments/haccio/bb/profiles\n")     
            script.write("\n")     
            
            script.write("export NUM_PARTICLES=" + str(numParticles) + "\n")        
            script.write("\n")

            script.write("mkdir -p /g/g92/xu23/gpfs1/research/io-experiments/haccio/bb/app-output-files/" + jobName + "\n")
            script.write("jsrun -r1 -n${NUM_NODES} mkdir -p $BBPATH/" + jobName + "\n")
            script.write("cd $BBPATH/" + jobName + "\n")
            script.write("\n")

            execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/hacc_io ${NUM_PARTICLES} ./" + jobName
                
            execution += " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"

            script.write("export RECORDER_TRACES_DIR=/g/g92/xu23/gpfs1/research/io-experiments/haccio/bb/app-output-files/" + jobName + "\n")
            script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution)
            # script.write("rm ./" + jobName + "-Part*\n")
            # script.write("mv recorder-logs " + jobName + ".recorder-log\n")
            script.write("\n")
            
            # script.write("export MY_DARSHAN_LOG_DIR=$PROFILES\n")
            # script.write("export DXT_ENABLE_IO_TRACE=1\n")
            # script.write("jsrun -E LD_PRELOAD=${DARSHAN_PATH}/lib/libdarshan.so " + execution)
            # script.write("\n")
            numJob += 1

    print(numJob)

def generateHaccioOptions():
    system = DataFrame({'system': ['lassen-gpfs', 'lassen-bb', 'summit-gpfs', 'summit-bb', 'cori-lustre', 'cori-bb'], 'key': [0] * 6})
    numBB = DataFrame({'numBB': [1, 4, 16, 64, 256], 'key': [0] * 5})
    scaling = DataFrame({'scaling': ['strong-scaling', 'weak-scaling'], 'key': [0] * 2})
    numProc = DataFrame({'numProc': [64, 128, 256, 512], 'numNodes': [2, 4, 8, 16], 'key': [0] * 4})

    df = system.merge(numBB, how='outer',  on='key')
    df = df.merge(scaling, how='outer',  on='key')
    df= df.merge(numProc, how='outer',  on='key')
    return df

if __name__ == '__main__':
    pass
    system = sys.argv[1]
    if system == 'lassen-gpfs':
        generateLassenGpfsHaccio()
    elif system == 'lassen-bb':
        generateLassenBBHaccio()
    