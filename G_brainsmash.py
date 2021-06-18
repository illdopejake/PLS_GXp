import os
import pandas
import numpy as np
import multiprocessing as mp
from brainsmash.mapgen.base import Base

def brainsmash_pipeline(dat, dist, indx, outdir, outnm='', n_iter=1000):
    vec = dat.loc[:,indx].values
    base = Base(x=vec,D=dist)
    surrogates = base(n=n_iter)
    fnm = os.path.join(outdir,'%s_%s'%(outnm,indx))
    np.savez_compressed(fnm,surrogates)

data = pandas.read_pickle('/home/jvogel44/PLS_GXP/input/BrainSmash_input.pk')
gxp = data['gxp']
dist = data['dist']
outdir = '/home/jvogel44/scratch/PLS/BrainSmash_G'

jobs = []
for indx in gxp.columns:
    p = mp.Process(target = brainsmash_pipeline,
                   args = (gxp,dist,indx,outdir,'G',100))

    jobs.append(p)
    p.start()


