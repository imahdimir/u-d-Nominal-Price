"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.classs import return_not_special_variables_of_class
from mirutil.df import assert_no_duplicated_rows_in_df_cols_subset
from mirutil.df import save_df_as_prq
from namespace_mahdimir.tse import DNomPriceCol

from main import c
from main import check_all_vals_are_notna
from main import fpn
from main import gdu

def check_complete_backward_compatibility(df_old , df_new) :
    dfo = df_old
    dfn = df_new

    _mrg_cols = [c.ftic , c.d]
    dfo1 = dfo.merge(dfn , on = _mrg_cols , how = 'left' , indicator = True)

    assert dfo1['_merge'].eq('both').all() , 'Not backward compatible!'

def concat_new_and_old_data(dfo , dfn) :
    df = pd.concat([dfn , dfo])

    df = df.drop_duplicates()

    return df

def check_no_new_col_is_added(df) :
    cs = df.columns.to_list()
    cs1 = list(return_not_special_variables_of_class(DNomPriceCol).values())

    assert cs == cs1 , 'New cols are added'

def main() :
    pass

    ##
    gdt = GitHubDataRepo(gdu.nom_price_st)
    dfo = gdt.read_data()

    ##
    dfn = pd.read_parquet(fpn.t1_1)

    ##
    check_complete_backward_compatibility(dfo , dfn)

    ##
    df = concat_new_and_old_data(dfo , dfn)

    ##
    check_all_vals_are_notna(df)

    ##
    check_no_new_col_is_added(df)

    ##
    assert_no_duplicated_rows_in_df_cols_subset(df , [c.ftic , c.d])

    ##
    df = df.sort_values(by = c.d , ascending = False)

    ##
    save_df_as_prq(df , fpn.t2)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


def test() :
    pass

    ##

    ##
