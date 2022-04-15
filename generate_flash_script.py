from fileinput import filename
import sys
from generate_header import *
import pandas as pd
from pandas import DataFrame
import subprocess

numIteration = 5

def generateLassenGpfsFlash(scriptPath):
    
    for numProc, iGridSize, jGridSize in zip(
        [64, 128, 256, 512, 64, 128, 256, 512], 
        [1024, 1024, 1024, 1024, 512, 1024, 1024, 2048], 
        [1024, 1024, 1024, 1024, 512, 512, 1024, 1024]):
        numNodes = numProc // 32

        jobName = "lassen-gpfs." + str(iGridSize) + "." + str(jGridSize) + \
            "." + str(numProc)

        subprocess.run(['mkdir', "/g/g92/xu23/gpfs1/research/io-experiments/flash/gpfs/app-output-files/" + jobName])
        
        filename = scriptPath + "/" + jobName + ".sh"
        
        generateLassenHeader(filename, numNodes, numNodes * 4, jobName, jobName)
        
        with open(filename, "a") as script:
            script.write("export NUM_PROC=" + str(numProc) + "\n")
            script.write("export NUM_NODES=" + str(numNodes) + "\n")
            script.write("export FILE_SYSTEM=" + "lassen-gpfs" + "\n")
            script.write("export NUM_BB_SERVERS=" + str(0) + "\n")
            script.write("\n")

            script.write("export JOB_HOME=/g/g92/xu23/gpfs1/research/io-experiments/flash/gpfs\n")
            script.write("export BIN_PATH=/g/g92/xu23/gpfs1/apps/FLASH4.6.2-2d.ug.nofbs/object\n")
            script.write("export RECORDER_PATH=/g/g92/xu23/gpfs1/local/recorder-2.2\n")
            script.write("export DARSHAN_PATH=/g/g92/xu23/gpfs1/local/darshan-3.3.1\n")
            script.write("export PROFILES=/p/gpfs1/xu23/research/io-experiments/flash/gpfs/profiles\n")     
            script.write("\n")     
            
            script.write("export IGRIDSIZE=" + str(iGridSize) + "\n")     
            script.write("export JGRIDSIZE=" + str(jGridSize) + "\n")     
            script.write("\n")

            script.write("cd /g/g92/xu23/gpfs1/research/io-experiments/flash/gpfs/app-output-files/" + jobName + "\n")
            script.write("\n")

            execution = "-r 32 -n ${NUM_PROC} ${BIN_PATH}/flash4"
            
            # execution += " -o $JOB_HOME/app-output-files/" + jobName + ".testFile"
            execution += " &>> $JOB_HOME/app-stdio/" + jobName + ".txt\n"

            # script.write("for ITERATION in")
            # for i in range(1, numIteration + 1):
            #     script.write(" " + str(i))
            # script.write("\n")
            # script.write("do\n")
            # script.write("    jsrun " + execution)
            # script.write("done\n")
            # script.write("\n")

            # script.write("export RECORDER_TRACES_DIR=$PROFILES\n")
            script.write("jsrun -E LD_PRELOAD=${RECORDER_PATH}/lib/librecorder.so " + execution)
            script.write("\n")
            
            # script.write("export MY_DARSHAN_LOG_DIR=$PROFILES\n")
            # script.write("export DXT_ENABLE_IO_TRACE=1\n")
            # script.write("jsrun -E LD_PRELOAD=${DARSHAN_PATH}/lib/libdarshan.so " + execution)
            # script.write("\n")

if __name__ == '__main__':
    pass
    # generateLassen()
    