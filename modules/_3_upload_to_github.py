"""

    """

import shutil
from pathlib import Path

from githubdata import GitHubDataRepo
from persiantools.jdatetime import JalaliDateTime

from main import fpn
from main import gdu

def clone_target_repo() :
    gdt = GitHubDataRepo(gdu.nom_price_st)
    gdt.clone_overwrite()
    return gdt

def replace_old_data_with_new(gdt) :
    gdt.data_fp.unlink()

    tjd = JalaliDateTime.now().strftime('%Y-%m-%d')
    fp = gdt.local_path / f'{tjd}.prq'

    shutil.copy(fpn.t1_1 , fp)

def push_to_github(gdt) :
    msg = 'Updated by ' + gdu.slf
    gdt.commit_and_push(msg , branch = 'main')

def main() :
    pass

    ##
    gdt = clone_target_repo()

    ##
    replace_old_data_with_new(gdt)

    ##
    push_to_github(gdt)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
