import re
import numpy as np
import pandas as pd
import sys
import glob

def main():
    parse_ior_output()
    aggregateIteration()

def parse_ior_output():
    numFailed = 0
    dic = {
        'status': [],
        'system': [],
        'lassen-gpfs': [],
        'lassen-bb' : [],
        'summit-gpfs': [],
        'summit-bb': [],
        'cori-lustre': [],
        'cori-bb': [],
        'numBB': [],
        'api(-a)': [],
        'POSIX': [],
        'MPIIO': [],
        'HDF5': [],
        'blockSize(-b)': [],
        'collective(-c)': [],
        'fsync(-e)': [],
        'useExistingTestFile(-E)': [],
        'filePerProc(-F)': [],
        'intraTestBarriers(-g)': [],
        'setAlignment(-J)': [],
        'keepFile(-k)': [],
        'memoryPerNode(-M)': [],
        'noFill(-n)': [],
        'preallocate(-p)': [],
        'readFile(-r)': [],
        'segment(-s)': [],
        'transferSize(-t)': [],
        'uniqueDir(-u)': [],
        'useFileView(-V)': [],
        'writeFile(-w)': [],
        'fsyncPerWrite(-Y)': [],
        'numProc': [],
        'numNode': [],
        'iteration': [],
        'access': [],
        'write-scratch': [],
        'write-touched-file': [],
        'read-renamed': [],
        'write-after-write': [],
        'read-after-write': [],
        'write-after-read': [],
        'read-after-read': [],
        'bandwidth(MB/s)': [],
        'IOPS': [],
        'latency(s)': [],
        'openTime(s)': [],
        'wr/rdTime(s)': [],
        'closeTime(s)': [],
        'time(s)': [],
    }

    jobs = glob.glob('/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/jobs/*.sh')
    jobs = list(map(lambda x: x.split('/')[-1], jobs))
    outputs = glob.glob('/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/app-stdio/*.txt')
    outputs = list(map(lambda x: x.split('/')[-1], outputs))
    
    for job in jobs:
        output = job[:-3] + '.txt'
        configs = job.split('.')
        
        if output not in outputs:
            with open('/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/app-stdio/failedJobs', 'a') as failedJobs:
                    failedJobs.write(job + ',')
            numFailed += 1

            for iteration in [1, 2, 3, 4, 5]:
                # write from scratch
                dic['iteration'].append(iteration)
                appendConfigsFailed(dic, configs)
                dic['writeFile(-w)'].append('1')
                dic['readFile(-r)'].append('0')
                dic['useExistingTestFile(-E)'].append(0)
                dic['access'].append('write')
                dic['write-scratch'].append(1)
                dic['write-touched-file'].append(0)
                dic['read-renamed'].append(0)
                dic['write-after-write'].append(0)
                dic['read-after-write'].append(0)
                dic['write-after-read'].append(0)
                dic['read-after-read'].append(0)

                # write after write on the same file
                dic['iteration'].append(iteration)
                appendConfigsFailed(dic, configs)
                dic['writeFile(-w)'].append('1')
                dic['readFile(-r)'].append('0')
                dic['useExistingTestFile(-E)'].append(1)
                dic['access'].append('write')
                dic['write-scratch'].append(0)
                dic['write-touched-file'].append(0)
                dic['read-renamed'].append(0)
                dic['write-after-write'].append(1)
                dic['read-after-write'].append(0)
                dic['write-after-read'].append(0)
                dic['read-after-read'].append(0)

                # read after write on the same file
                dic['iteration'].append(iteration)
                appendConfigsFailed(dic, configs)
                dic['writeFile(-w)'].append('0')
                dic['readFile(-r)'].append('1')
                dic['useExistingTestFile(-E)'].append(0)
                dic['access'].append('read')
                dic['write-scratch'].append(0)
                dic['write-touched-file'].append(0)
                dic['read-renamed'].append(0)
                dic['write-after-write'].append(0)
                dic['read-after-write'].append(1)
                dic['write-after-read'].append(0)
                dic['read-after-read'].append(0)

                # read renamed file
                dic['iteration'].append(iteration)
                appendConfigsFailed(dic, configs)
                dic['writeFile(-w)'].append('0')
                dic['readFile(-r)'].append('1')
                dic['useExistingTestFile(-E)'].append(0)
                dic['access'].append('read')
                dic['write-scratch'].append(0)
                dic['write-touched-file'].append(0)
                dic['read-renamed'].append(1)
                dic['write-after-write'].append(0)
                dic['read-after-write'].append(0)
                dic['write-after-read'].append(0)
                dic['read-after-read'].append(0)

                # read after read
                dic['iteration'].append(iteration)
                appendConfigsFailed(dic, configs)
                dic['writeFile(-w)'].append('0')
                dic['readFile(-r)'].append('1')
                dic['useExistingTestFile(-E)'].append(0)
                dic['access'].append('read')
                dic['write-scratch'].append(0)
                dic['write-touched-file'].append(0)
                dic['read-renamed'].append(0)
                dic['write-after-write'].append(0)
                dic['read-after-write'].append(0)
                dic['write-after-read'].append(0)
                dic['read-after-read'].append(1)

                # write after read
                dic['iteration'].append(iteration)
                appendConfigsFailed(dic, configs)
                dic['writeFile(-w)'].append('1')
                dic['readFile(-r)'].append('0')
                dic['useExistingTestFile(-E)'].append(1)
                dic['access'].append('write')
                dic['write-scratch'].append(0)
                dic['write-touched-file'].append(0)
                dic['read-renamed'].append(0)
                dic['write-after-write'].append(0)
                dic['read-after-write'].append(0)
                dic['write-after-read'].append(1)
                dic['read-after-read'].append(0)

                # write touched file
                dic['iteration'].append(iteration)
                appendConfigsFailed(dic, configs)
                dic['writeFile(-w)'].append('1')
                dic['readFile(-r)'].append('0')
                dic['useExistingTestFile(-E)'].append(1)
                dic['access'].append('write')
                dic['write-scratch'].append(0)
                dic['write-touched-file'].append(1)
                dic['read-renamed'].append(0)
                dic['write-after-write'].append(0)
                dic['read-after-write'].append(0)
                dic['write-after-read'].append(0)
                dic['read-after-read'].append(0)

        else:
            with open(output) as f:
                lines = f.readlines()
                count = 0

                for line in lines:
                    pos = re.search("(write|read)", line)
                    if pos and pos.start() == 0:
                        result = line.split()
                        if len(result) != 11:
                            continue
                        appendConfigs(dic, configs)
                        dic['status'].append('finished')
                        dic['iteration'].append(int(count // 7) + 1)
                        dic['access'].append(result[0])
                        dic['bandwidth(MB/s)'].append(float(result[1]))
                        dic['IOPS'].append(float(result[2]))
                        dic['latency(s)'].append(float(result[3]))
                        dic['openTime(s)'].append(float(result[6]))
                        dic['wr/rdTime(s)'].append(float(result[7]))
                        dic['closeTime(s)'].append(float(result[8]))
                        dic['time(s)'].append(float(result[9]))

                        if count % 7 == 0: # write scratch
                            dic['writeFile(-w)'].append('1')
                            dic['readFile(-r)'].append('0')
                            dic['useExistingTestFile(-E)'].append(0)
                            dic['write-scratch'].append(1)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(0)
                        elif count % 7 == 1: # write after write
                            dic['writeFile(-w)'].append('1')
                            dic['readFile(-r)'].append('0')
                            dic['useExistingTestFile(-E)'].append(1)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(1)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(0)
                        elif count % 7 == 2: # read after write
                            dic['writeFile(-w)'].append('0')
                            dic['readFile(-r)'].append('1')
                            dic['useExistingTestFile(-E)'].append(0)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(1)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(0)
                        elif count % 7 == 3: # read renamed
                            dic['writeFile(-w)'].append('0')
                            dic['readFile(-r)'].append('1')
                            dic['useExistingTestFile(-E)'].append(0)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(1)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(0)
                        elif count % 7 == 4: # read after read
                            dic['writeFile(-w)'].append('0')
                            dic['readFile(-r)'].append('1')
                            dic['useExistingTestFile(-E)'].append(0)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(1)
                        elif count % 7 == 5: # write after read
                            dic['writeFile(-w)'].append('1')
                            dic['readFile(-r)'].append('0')
                            dic['useExistingTestFile(-E)'].append(1)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(1)
                            dic['read-after-read'].append(0)
                        elif count % 7 == 6: # write touched file
                            dic['writeFile(-w)'].append('1')
                            dic['readFile(-r)'].append('0')
                            dic['useExistingTestFile(-E)'].append(1)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(1)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(0)

                        count += 1
                        
                if count < 35:
                    with open('/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/app-stdio/failedJobs', 'a') as failedJobs:
                        failedJobs.write(job + ',')
                    numFailed += 1

                    for idx in range(count, 35):
                        appendConfigsFailed(dic, configs)
                        dic['iteration'].append(idx // 7 + 1)
                        
                        if idx % 7 == 0: # write scratch
                            dic['access'].append('write')
                            dic['writeFile(-w)'].append('1')
                            dic['readFile(-r)'].append('0')
                            dic['useExistingTestFile(-E)'].append(0)
                            dic['write-scratch'].append(1)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(0)
                        elif idx % 7 == 1: # write after write
                            dic['access'].append('write')
                            dic['writeFile(-w)'].append('1')
                            dic['readFile(-r)'].append('0')
                            dic['useExistingTestFile(-E)'].append(1)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(1)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(0)
                        elif idx % 7 == 2: # read after write
                            dic['access'].append('read')
                            dic['writeFile(-w)'].append('0')
                            dic['readFile(-r)'].append('1')
                            dic['useExistingTestFile(-E)'].append(0)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(1)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(0)
                        elif idx % 7 == 3: # read renamed
                            dic['access'].append('read')
                            dic['writeFile(-w)'].append('0')
                            dic['readFile(-r)'].append('1')
                            dic['useExistingTestFile(-E)'].append(0)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(1)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(0)
                        elif idx % 7 == 4: # read after read
                            dic['access'].append('read')
                            dic['writeFile(-w)'].append('0')
                            dic['readFile(-r)'].append('1')
                            dic['useExistingTestFile(-E)'].append(0)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(1)
                        elif idx % 7 == 5: # write after read
                            dic['access'].append('write')
                            dic['writeFile(-w)'].append('1')
                            dic['readFile(-r)'].append('0')
                            dic['useExistingTestFile(-E)'].append(1)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(0)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(1)
                            dic['read-after-read'].append(0)
                        elif idx % 7 == 6: # write touched file
                            dic['access'].append('write')
                            dic['writeFile(-w)'].append('1')
                            dic['readFile(-r)'].append('0')
                            dic['useExistingTestFile(-E)'].append(1)
                            dic['write-scratch'].append(0)
                            dic['write-touched-file'].append(1)
                            dic['read-renamed'].append(0)
                            dic['write-after-write'].append(0)
                            dic['read-after-write'].append(0)
                            dic['write-after-read'].append(0)
                            dic['read-after-read'].append(0)

    for key, value in dic.items():
        print(key + ": " + str(len(value)))
    data = pd.DataFrame.from_dict(dic)
    data.to_csv('/g/g92/xu23/gpfs1/research/io-experiments/ior/gpfs/app-stdio/lassen-gpfs-ior-result.csv', index=False)
    print("failed jobs: " + str(numFailed))
    return data

def appendConfigs(dic, configs):
    system = configs[0]
    dic['system'].append(system)
    if system == 'lassen-gpfs':
        dic['lassen-gpfs'].append(1)
        dic['lassen-bb'].append(0)
        dic['summit-gpfs'].append(0)
        dic['summit-bb'].append(0)
        dic['cori-lustre'].append(0)
        dic['cori-bb'].append(0)
    elif system == 'lassen-bb':
        dic['lassen-gpfs'].append(0)
        dic['lassen-bb'].append(1)
        dic['summit-gpfs'].append(0)
        dic['summit-bb'].append(0)
        dic['cori-lustre'].append(0)
        dic['cori-bb'].append(0)
    elif system == 'summit-gpfs':
        dic['lassen-gpfs'].append(0)
        dic['lassen-bb'].append(0)
        dic['summit-gpfs'].append(1)
        dic['summit-bb'].append(0)
        dic['cori-lustre'].append(0)
        dic['cori-bb'].append(0)
    elif system == 'summit-bb':
        dic['lassen-gpfs'].append(0)
        dic['lassen-bb'].append(0)
        dic['summit-gpfs'].append(0)
        dic['summit-bb'].append(1)
        dic['cori-lustre'].append(0)
        dic['cori-bb'].append(0)
    elif system == 'cori-lustre':
        dic['lassen-gpfs'].append(0)
        dic['lassen-bb'].append(0)
        dic['summit-gpfs'].append(0)
        dic['summit-bb'].append(0)
        dic['cori-lustre'].append(1)
        dic['cori-bb'].append(0)
    elif system == 'summit-bb':
        dic['lassen-gpfs'].append(0)
        dic['lassen-bb'].append(0)
        dic['summit-gpfs'].append(0)
        dic['summit-bb'].append(0)
        dic['cori-lustre'].append(0)
        dic['cori-bb'].append(1)

    dic['numBB'].append(int(configs[1]))

    api = configs[2]
    dic['api(-a)'].append(api)
    if api == 'POSIX':
        dic['POSIX'].append(1)
        dic['MPIIO'].append(0)
        dic['HDF5'].append(0)
    elif api == 'MPIIO':
        dic['POSIX'].append(0)
        dic['MPIIO'].append(1)
        dic['HDF5'].append(0)
    elif api == 'HDF5':
        dic['POSIX'].append(0)
        dic['MPIIO'].append(0)
        dic['HDF5'].append(1)

    dic['blockSize(-b)'].append(toByte(configs[3]))
    dic['collective(-c)'].append(int(configs[4]))
    dic['fsync(-e)'].append(int(configs[5]))
    # dic['useExistingTestFile(-E)'].append(int(configs[6]))
    dic['filePerProc(-F)'].append(int(configs[7]))
    dic['intraTestBarriers(-g)'].append(int(configs[8]))
    dic['setAlignment(-J)'].append(toByte(configs[9]))
    dic['keepFile(-k)'].append(int(configs[10]))
    dic['memoryPerNode(-M)'].append(percentageToFloat(configs[11]))
    dic['noFill(-n)'].append(int(configs[12]))
    dic['preallocate(-p)'].append(int(configs[13]))
    # dic['readFile(-r)'].append(int(configs[14]))
    dic['segment(-s)'].append(int(configs[15]))
    dic['transferSize(-t)'].append(toByte(configs[16]))
    dic['uniqueDir(-u)'].append(int(configs[17]))
    dic['useFileView(-V)'].append(int(configs[18]))
    # dic['writeFile(-w)'].append(int(configs[19]))
    dic['fsyncPerWrite(-Y)'].append(int(configs[20]))
    dic['numProc'].append(int(configs[21]))
    dic['numNode'].append(int(configs[21]) // 32)

def appendConfigsFailed(dic, configs):
    appendConfigs(dic, configs)
    dic['status'].append('failed')
    dic['bandwidth(MB/s)'].append(0)
    dic['IOPS'].append(0)
    dic['latency(s)'].append(float('inf'))
    dic['openTime(s)'].append(float('inf'))
    dic['wr/rdTime(s)'].append(float('inf'))
    dic['closeTime(s)'].append(float('inf'))
    dic['time(s)'].append(float('inf'))


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

def percentageToFloat(percentage):
    if percentage[-1] != '%':
        return float(percentage)
    else:
        return 0.01 * float(percentage[:-1])

def aggregateIteration():
    df = pd.read_csv('/Users/henryxu/Desktop/io-project/lassen-gpfs-ior-result.csv')
    df = df.groupby([
        'system',
        'lassen-gpfs',
        'lassen-bb',
        'summit-gpfs',
        'summit-bb',
        'cori-lustre',
        'cori-bb',
        'numBB',
        'api(-a)',
        'POSIX',
        'MPIIO',
        'HDF5',
        'blockSize(-b)',
        'collective(-c)',
        'fsync(-e)',
        'useExistingTestFile(-E)',
        'filePerProc(-F)',
        'intraTestBarriers(-g)',
        'setAlignment(-J)',
        'keepFile(-k)',
        'memoryPerNode(-M)',
        'noFill(-n)',
        'preallocate(-p)',
        'readFile(-r)',
        'segment(-s)',
        'transferSize(-t)',
        'uniqueDir(-u)',
        'useFileView(-V)',
        'writeFile(-w)',
        'fsyncPerWrite(-Y)',
        'numProc',
        'numNode',
        'access',
        'write-scratch',
        'write-touched-file',
        'read-renamed',
        'write-after-write',
        'read-after-write',
        'write-after-read',
        'read-after-read',
    ]).agg({
        'status': lambda x: x.iloc[-1], 
        'bandwidth(MB/s)': {np.mean, np.min, np.max},
        'IOPS': {np.mean, np.min, np.max},
        'latency(s)': {np.mean, np.min, np.max},
        'openTime(s)': {np.mean, np.min, np.max},
        'wr/rdTime(s)': {np.mean, np.min, np.max},
        'closeTime(s)': {np.mean, np.min, np.max},
        'time(s)': {np.mean, np.min, np.max},
    })
    df.reset_index(inplace=True)
    df.droplevel(0, axis=1)
    df.columns = [
        'system',
        'lassen-gpfs',
        'lassen-bb',
        'summit-gpfs',
        'summit-bb',
        'cori-lustre',
        'cori-bb',
        'numBB',
        'api(-a)',
        'POSIX',
        'MPIIO',
        'HDF5',
        'blockSize(-b)',
        'collective(-c)',
        'fsync(-e)',
        'useExistingTestFile(-E)',
        'filePerProc(-F)',
        'intraTestBarriers(-g)',
        'setAlignment(-J)',
        'keepFile(-k)',
        'memoryPerNode(-M)',
        'noFill(-n)',
        'preallocate(-p)',
        'readFile(-r)',
        'segment(-s)',
        'transferSize(-t)',
        'uniqueDir(-u)',
        'useFileView(-V)',
        'writeFile(-w)',
        'fsyncPerWrite(-Y)',
        'numProc',
        'numNode',
        'access',
        'write-scratch',
        'write-touched-file',
        'read-renamed',
        'write-after-write',
        'read-after-write',
        'write-after-read',
        'read-after-read',
        'status',
        'max bandwidth(MB/s)',
        'min bandwidth(MB/s)',
        'avg bandwidth(MB/s)',
        'max IOPS',
        'min IOPS',
        'avg IOPS',
        'max latency(s)',
        'avg latency(s)',
        'min latency(s)',
        'max openTime(s)',
        'min openTime(s)',
        'avg openTime(s)',
        'max wr/rdTime(s)',
        'min wr/rdTime(s)',
        'avg wr/rdTime(s)',
        'max closeTime(s)',
        'min closeTime(s)',
        'avg closeTime(s)',
        'max time(s)',
        'min time(s)',
        'avg time(s)',
    ]
    df.sort_values(by=[
        'system',
        'numBB',
        'api(-a)',
        'POSIX',
        'MPIIO',
        'HDF5',
        'blockSize(-b)',
        'collective(-c)',
        'fsync(-e)',
        # 'useExistingTestFile(-E)',
        'filePerProc(-F)',
        'intraTestBarriers(-g)',
        'setAlignment(-J)',
        'keepFile(-k)',
        'memoryPerNode(-M)',
        'noFill(-n)',
        'preallocate(-p)',
        # 'readFile(-r)',
        'segment(-s)',
        'transferSize(-t)',
        'uniqueDir(-u)',
        'useFileView(-V)',
        # 'writeFile(-w)',
        'fsyncPerWrite(-Y)',
        'numProc',
        'numNode',
    ],
    inplace=True)
    df.to_csv('/Users/henryxu/Desktop/io-project/lassen-gpfs-ior-result-aggregate-iteration.csv', index=False)
    s = pd.Series("########", df.columns)
    grp = np.arange(len(df)) // 7
    df = df.groupby(grp, group_keys=False).apply(lambda x: x.append(s, ignore_index=True)).reset_index(drop=True)
    df.to_csv('/Users/henryxu/Desktop/io-project/lassen-gpfs-ior-result-aggregate-iteration-split.csv', index=False)
    return df

def addGoodBadAvg(metric='avg bandwidth(MB/s)'):
    df = pd.read_csv('/Users/henryxu/Desktop/io-project/lassen-gpfs-ior-result-aggregate-iteration.csv')
    df1 = df[df['status'].str.contains('finished')].copy()

    maxMetric = df[metric].max()
    avgMetric = df1[metric].mean()
    minMetric = df[metric].min()
    # stdMetric = df[metric].std()
    print("max " + metric + ": " + str(maxMetric))
    print("min " + metric + ": " + str(minMetric))
    print("avg " + metric + ": " + str(avgMetric))
    
    def isGood(x, metric, avgMetric):
        if x[metric] >= avgMetric:
            return 'good'
        else:
            return 'bad'

    df['good/bad'] = df.apply(lambda row: isGood(row, metric, avgMetric), axis=1)
    df.to_csv('/Users/henryxu/Desktop/io-project/lassen-gpfs-ior-result-aggregate-iteration-good-bad-avg.csv', index=False)
    
    s = pd.Series("########", df.columns)
    grp = np.arange(len(df)) // 7
    df = df.groupby(grp, group_keys=False).apply(lambda x: x.append(s, ignore_index=True)).reset_index(drop=True)
    df.to_csv('/Users/henryxu/Desktop/io-project/lassen-gpfs-ior-result-aggregate-iteration-good-bad-avg-split.csv', index=False)
    return df

def addGoodBadMedian(metric='avg bandwidth(MB/s)'):
    df = pd.read_csv('/Users/henryxu/Desktop/io-project/lassen-gpfs-ior-result-aggregate-iteration.csv')
    df1 = df[df['status'].str.contains('finished')].copy()

    maxMetric = df[metric].max()
    medianMetric = df1[metric].median()
    minMetric = df[metric].min()
    # stdMetric = df[metric].std()
    print("max " + metric + ": " + str(maxMetric))
    print("min " + metric + ": " + str(minMetric))
    print("avg " + metric + ": " + str(medianMetric))
    
    def isGood(x, metric, medianMetric):
        if x[metric] >= medianMetric:
            return 'good'
        else:
            return 'bad'

    df['good/bad'] = df.apply(lambda row: isGood(row, metric, medianMetric), axis=1)
    df.to_csv('/Users/henryxu/Desktop/io-project/lassen-gpfs-ior-result-aggregate-iteration-good-bad-median.csv', index=False)
    
    s = pd.Series("########", df.columns)
    grp = np.arange(len(df)) // 7
    df = df.groupby(grp, group_keys=False).apply(lambda x: x.append(s, ignore_index=True)).reset_index(drop=True)
    df.to_csv('/Users/henryxu/Desktop/io-project/lassen-gpfs-ior-result-aggregate-iteration-good-bad-median-split.csv', index=False)
    return df

def normalize(file='/Users/henryxu/Desktop/io-project/lassen-gpfs-ior-result-aggregate-iteration-good-bad-median.csv'):
    df = pd.read_csv(file)
    normalized_df = (df - df.min()) / (df.max() - df.min())
    df.to_csv(file[:-4] + '-normalized.csv', index=False)
    return df

if __name__ == '__main__':
    pass
    # main()
