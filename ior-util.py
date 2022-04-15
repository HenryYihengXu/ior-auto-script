import glob
from re import sub
import subprocess
import sys
from mpi4py import MPI


def rename(name, uniqueDir):
    if uniqueDir == 1:
        dirnames = glob.glob("./*")
        for dirname in dirnames:
            dirname = dirname.split('/')[-1]
            if dirname == 'ior-util.py':
                continue
            filenames = glob.glob('./' + dirname + '/*')
            for filename in filenames:
                filename = filename.split('/')[-1]
                if filename == 'ior-util.py':
                    continue
                subprocess.run('mv ./' + dirname + '/' + filename + ' ./' + dirname + '/' + name + '-' + filename.split('-')[-1], shell=True)
            
            # subprocess.run('mv ./' + dirname + ' ' + './renamed-' + dirname, shell=True)   
    else:
        filenames = glob.glob("./*")
        for filename in filenames:
            print(filename)
            filename = filename.split('/')[-1]
            if filename == 'ior-util.py':
                continue
            subprocess.run('mv ./' + filename + ' ' + './' + name + '-' + filename.split('-')[-1], shell=True)

def prepareFileGpfs(uniqueDir, filePerProcess, numProc, name):
    if uniqueDir == 1:
        for i in range(numProc):
            subprocess.run('mkdir t' + str(i), shell=True)
            if i < 10:
                subprocess.run('touch ' + name[0] + str(i) + '/' + name[1:] + '.0000000' + str(i), shell=True)
            elif i >= 10 and i < 100:
                subprocess.run('touch ' + name[0] + str(i) + '/' + name[1:] + '.000000' + str(i), shell=True)
            elif i >= 100 and i < 1000:
                subprocess.run('touch ' + name[0] + str(i) + '/' + name[1:] + '.00000' + str(i), shell=True)
            else:
                subprocess.run('touch ' + name[0] + str(i) + '/' + name[1:] + '.0000' + str(i), shell=True)
    else:
        if filePerProcess == 1:
            for i in range(numProc):
                if i < 10:
                    subprocess.run('touch ' + name + '.0000000' + str(i), shell=True)
                elif i >= 10 and i < 100:
                    subprocess.run('touch ' + name + '.000000' + str(i), shell=True)
                elif i >= 100 and i < 1000:
                    subprocess.run('touch ' + name + '.00000' + str(i), shell=True)
                else:
                    subprocess.run('touch ' + name + '.0000' + str(i), shell=True)
        else:
            subprocess.run('touch ' + name, shell=True)

def prepareFileBB(uniqueDir, name):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if uniqueDir == 1:
        subprocess.run('mkdir t' + str(rank), shell=True)
        if rank < 10:
            subprocess.run('touch ' + name[0] + '/' + name[1:] + '.0000000' + str(rank), shell=True)
        elif rank >= 10 and rank < 100:
            subprocess.run('touch ' + name[0] + '/' + name[1:] + '.000000' + str(rank), shell=True)
        elif rank >= 100 and rank < 1000:
            subprocess.run('touch ' + name[0] + '/' + name[1:] + '.00000' + str(rank), shell=True)
        else:
            subprocess.run('touch ' + name[0] + '/' + name[1:] + '.0000' + str(rank), shell=True)
    else:
        if rank < 10:
            subprocess.run('touch ' + name + '.0000000' + str(rank), shell=True)
        elif rank >= 10 and rank < 100:
            subprocess.run('touch ' + name + '.000000' + str(rank), shell=True)
        elif rank >= 100 and rank < 1000:
            subprocess.run('touch ' + name + '.00000' + str(rank), shell=True)
        else:
            subprocess.run('touch ' + name + '.0000' + str(rank), shell=True)


if __name__ == '__main__':
    pass
    function = sys.argv[1]
    if function == 'rename':
        rename(sys.argv[2], int(sys.argv[3]))
    elif function == 'prepareFileGpfs':
        prepareFileGpfs(int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5])
    elif function == 'prepareFileBB':
        prepareFileBB(int(sys.argv[2]), sys.argv[3])
    else:
        print("No such function\n")
