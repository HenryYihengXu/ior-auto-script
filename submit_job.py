import glob
import subprocess
import sys
import math
import time

def submitLassenGpfsJob(dir='/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/jobs'):
    count = 0
    filenames = glob.glob(dir + "/*.sh")
    dependency = ""
    for filename in filenames:
        print(filename)
        numProc = int(filename.split('.')[-2])
        if numProc == 256 or numProc == 512:
            if dependency != "":
                print('depends on ' + dependency)
                subprocess.run('bsub -w \"ended(\"' + dependency + "\")\" " + filename, shell=True)
            else:
                subprocess.run(['bsub', filename])
            dependency = filename.split('/')[-1][:-3]
        else:
            subprocess.run(['bsub', filename])

        count += 1
        if count % 30 == 0:
            time.sleep(15 * 60)
    print('job submitted: ' + str(count))
    return count

def submitLassenGpfsFailedJob(filename='/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/app-stdio/failedJobs'):
    count = 0
    with open(filename) as file:
        failedJobs = file.readlines()[0].split(',')
        for job in failedJobs:
            #  print(job)
            if job == '':
                continue
            subprocess.run(['rm', '-r', '/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/app-output-files/' + job[:-3]])
            subprocess.run(['rm', '/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/app-stdio/' + job[:-3] + '.txt'])
            subprocess.run(['rm', '/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/jobs/' + job[:-3] + '.err'])
            subprocess.run(['rm', '/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/jobs/' + job[:-3] + '.out'])
            subprocess.run(['bsub', job])
            count += 1
            if count % 30 == 0:
                time.sleep(15 * 60)
    print('job submitted: ' + str(count))
    return count

def submitLassenBBJob(dir='/g/g92/xu23/gpfs1/research/io-experiments/ior/bb/jobs'):
    filenames = glob.glob(dir + "/*.sh")
    # print(filenames)
    count = 0
    with open(dir + '/submitted_jobs', 'w+') as file:
        for filename in filenames:
            configs = filename.split('.')
            # print(configs)
            blockSize = toByte(configs[3])
            segment = configs[15]
            storage = math.ceil(blockSize * int(segment) * 32 / (1024 * 1024 * 1024)) + 1
            subprocess.run('bsub -stage storage=' + storage + ' < ' + filename, shell=True)
            print('bsub -stage storage=' + str(storage) + ' < ' + filename)
            # subprocess.run(['ls', '-a'])
            file.write(filename + '\n')
            count += 1
            if count % 50 == 0:
                time.sleep(1800)
    print('job submitted: ' + str(count))
    return count

def submitLassenBBFailedJob(filename='/g/g92/xu23/gpfs1/research/io-experiments/ior/bb/app-stdio/failedJobs'):
    count = 0
    with open(filename) as file:
        failedJobs = file.readlines()[0].split(',')
        for job in failedJobs:
            if job == '':
                continue
            # print(job)
            configs = job.split('.')
            blockSize = toByte(configs[3])
            segment = configs[15]
            storage = math.ceil(blockSize * int(segment) * 32 / (1024 * 1024 * 1024)) + 1

            # subprocess.run(['rm', '-r', '/g/g92/xu23/gpfs1/research/io-experiments/ior/bb/app-output-files/' + job[:-3]])
            subprocess.run(['rm', '/g/g92/xu23/gpfs1/research/io-experiments/ior/bb/app-stdio/' + job[:-3] + '.txt'])
            subprocess.run(['rm', '/g/g92/xu23/gpfs1/research/io-experiments/ior/bb/jobs/' + job[:-3] + '.err'])
            subprocess.run(['rm', '/g/g92/xu23/gpfs1/research/io-experiments/ior/bb/jobs/' + job[:-3] + '.out'])
            subprocess.run('bsub -stage storage=' + str(storage) + ' < ' + job, shell=True)
            print('bsub -stage storage=' + str(storage) + ' < ' + job)
            count += 1
            if count % 50 == 0:
                time.sleep(1800)
    print('job submitted: ' + str(count))
    return count

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

# def toGB(size):
#     unit = size[-1]
#     if unit == 'k':
#         return math.ceil(int(size[:-1]) / (1024 * 1024) + 0.5)
#     elif unit == 'm':
#         return math.ceil(int(size[:-1]) / 1024 + 0.5)
#     elif unit == 'g':
#         return math.ceil(int(size[:-1]) + 0.5)
#     else:
#         return math.ceil(int(size) / (1024 * 1024 * 1024) + 0.5)


def submittedJobs(dir):
    filenames = glob.glob(dir)
    filenames = [x[:-4] + '.sh' for x in filenames]
    return filenames


if __name__ == '__main__':
    pass
    submitLassenGpfsJob()
    # submitLassenBBJob()
    # submitLassenGpfsFailedJob()
    # submitLassenBBFailedJob()
