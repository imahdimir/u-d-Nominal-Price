"""

    """

from pathlib import Path

import pandas as pd
from mirutil.df import reorder_df_cols_as_a_class_values
from mirutil.df import save_df_as_prq
from mirutil.jdate import make_jdate_col_fr_str_date_col_in_a_df
from namespace_mahdimir.tse import DNomPriceCol

from main import c
from main import check_all_vals_are_notna
from main import cn
from main import fpn

# namespace     %%%%%%%%%%%%%%%
cd = DNomPriceCol()

def split_all_rows(dft) :
    df = pd.DataFrame()

    for indx , row in dft.iterrows() :
        print(indx , ' : ' , row[c.ftic])

        df1 = split_into_df_cols(row[cn.rspt])

        df1[c.ftic] = row[c.ftic]
        df1[c.tse_id] = row[c.tse_id]

        df = pd.concat([df , df1])

        # break

    return df

def split_into_df_cols(res_text) :
    df = pd.DataFrame(res_text.split(';'))
    return df[0].str.split('@' , expand = True)

def rename_cols(df) :
    cns = {
            '0' : c.d ,

            '1' : cd.nhi ,
            '2' : cd.nlo ,
            '3' : cd.nclos ,
            '4' : cd.nlst ,
            '5' : cd.nopn ,
            '6' : cd.nystrd ,

            '7' : cd.val ,
            '8' : cd.vol ,
            '9' : cd.trd_count ,
            }

    return df.rename(columns = cns)

def remove_nan_rows(df) :
    cols = df.columns.difference([c.ftic , c.tse_id , c.d])

    msk = df[cols].isna().any(axis = 1)
    msk &= df[c.d].eq('')

    df = df[~ msk]

    return df

def drop_duplicates_except_tse_id(df) :
    """
    firms TSETMC id might be changed during time, but the firm is not changed.
    """
    return df.drop_duplicates(subset = df.columns.difference([c.tse_id]))

def drop_duplicates_on_ftic_and_date(df) :
    """
    some firms have two obs on a single day

    especially those that their id has changed have this issue on the
        changing day.

    I remove these days. maybe I should not. to be analyzed later.
    """
    return df.drop_duplicates(subset = [c.ftic , c.d] , keep = False)

def main() :
    pass

    ##
    dft = pd.read_parquet(fpn.t0)

    ##
    df = split_all_rows(dft)

    ##
    def manual() :
        pass

        ##

        # for manual run because it is time-consuming
        save_df_as_prq(df , fpn.t1_0)

        ##
        df = pd.read_parquet(fpn.t1_0)

    ##
    df = rename_cols(df)

    ##
    df = remove_nan_rows(df)

    ##
    df = df.drop_duplicates()

    ##
    check_all_vals_are_notna(df)

    ##
    _fu = make_jdate_col_fr_str_date_col_in_a_df
    df = _fu(df , c.d , c.jd , date_fmt = '%Y%m%d')

    ##
    df = drop_duplicates_except_tse_id(df)

    ##
    df = drop_duplicates_on_ftic_and_date(df)

    ##
    df = reorder_df_cols_as_a_class_values(df , DNomPriceCol)

    ##
    save_df_as_prq(df , fpn.t1_1)

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


def test() :
    pass

    ##

    ##

##
