import os
import pandas
import numpy as np
import multiprocessing as mp
from brainsmash.mapgen.base import Base

def brainsmash_pipeline(dat, dist, indx, outdir, outnm='', n_iter=1000,start_at=0):
    vec = dat.iloc[:,indx].values
    base = Base(x=vec,D=dist)
    surrogates = base(n=n_iter)
    fnm = os.path.join(outdir,'%s_%s-%s'%(outnm,indx,start_at))
    np.savez_compressed(fnm,surrogates)

data = pandas.read_pickle('/home/jvogel44/PLS_GXP/input/BrainSmash_input.pk')
pcs = data['PCs']
dist = data['dist']
outdir = '/home/jvogel44/scratch/PLS/BrainSmash_PC'

jobs = []
for indx in range(pcs.shape[1]):
    for start in range(0,100,10):
        p = mp.Process(target = brainsmash_pipeline,
                       args = (pcs,dist,indx,outdir,'PC',10,start))

    jobs.append(p)
    p.start()


