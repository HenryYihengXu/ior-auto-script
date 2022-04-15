import os

def generateLassenHeader(scriptPath, nnodes, WTime, jobName, baseFileName, dependency=None):
    with open(scriptPath, 'w+') as script:
        script.write("#!/bin/bash\n")
        script.write("\n")
        script.write("### LSF syntax\n")
        script.write("#BSUB -nnodes " + str(nnodes) + "\n")
        script.write("#BSUB -W " + str(WTime) + "\n")
        script.write("#BSUB -e " + baseFileName + ".err\n")
        script.write("#BSUB -o " + baseFileName + ".out\n")
        script.write("#BSUB -J " + jobName + "\n")
        script.write("#BSUB -q pbatch\n")
        if dependency != None:
            script.write("#BSUB -w " + dependency + "\n")
        script.write("\n")
        
def generateSummitHeader(scriptPath, nnodes, WTime, jobName, baseFileName, isBB):
    with open(scriptPath, 'w+') as script:
        script.write("#!/bin/bash\n")
        script.write("\n")
        script.write("### LSF syntax\n")
        script.write("#BSUB -P csc452\n")
        script.write("#BSUB -nnodes " + str(nnodes) + "\n")
        script.write("#BSUB -W " + str(WTime) + "\n")
        script.write("#BSUB -e " + baseFileName + ".err\n")
        script.write("#BSUB -o " + baseFileName + ".out\n")
        script.write("#BSUB -J " + jobName + "\n")
        script.write("#BSUB -q pbatch\n")
        
        if isBB:
            script.write("#BSUB -alloc_flags NVME\n")

        script.write("\n")

def generateSummitHeader(scriptPath, nnodes, WTime, jobName, baseFileName, isBB, BBCapacity, BBAccessMode="striped"):
    with open(scriptPath, 'w+') as script:
        script.write("#!/bin/bash\n")
        script.write("\n")
        script.write("#SBATCH --qos=regular\n")
        script.write("#SBATCH --nodes " + str(nnodes) + "\n")
        script.write("#SBATCH --time " + str(WTime) + "\n")
        script.write("#SBATCH -e " + baseFileName + ".err\n")
        script.write("#SBATCH -o " + baseFileName + ".out\n")
        script.write("#SBATCH -J " + jobName + "\n")
        
        if isBB:
            script.write("##DW jobdw capacity=" + BBCapacity + " access_mode=" + BBAccessMode + " type=scratch\n")

        script.write("\n")
