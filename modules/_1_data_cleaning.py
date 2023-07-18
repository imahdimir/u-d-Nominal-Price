"""

    """

from pathlib import Path

import pandas as pd
from mirutil.df import reorder_df_cols_as_a_class_values
from mirutil.df import save_df_as_prq
from namespace_mahdimir.tse import DNomPriceCol
from persiantools.jdatetime import JalaliDateTime

from main import c
from main import cn
from main import fpn

# namespace     %%%%%%%%%%%%%%%
cdn = DNomPriceCol()

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

            '1' : cdn.nhi ,
            '2' : cdn.nlo ,
            '3' : cdn.nclos ,
            '4' : cdn.nlst ,
            '5' : cdn.nopn ,
            '6' : cdn.nystrd ,

            '7' : cdn.val ,
            '8' : cdn.vol ,
            '9' : cdn.trd_count ,
            }

    return df.rename(columns = cns)

def remove_nan_rows(df) :
    cols = df.columns.difference([c.ftic , c.tse_id , c.d])

    msk = df[cols].isna().any(axis = 1)
    msk &= df[c.d].eq('')

    df = df[~ msk]

    return df

def check_all_vals_are_notna(df) :
    msk = df.isna().any(axis = 1)
    df1 = df[msk]

    assert df1.empty , 'there are some nan values'

def check_date_vals_and_make_jdate(df) :
    df[c.d] = pd.to_datetime(df[c.d] , format = '%Y%m%d')

    df[c.jd] = df[c.d].apply(JalaliDateTime.to_jalali)

    df[c.jd] = df[c.jd].apply(lambda x : x.strftime('%Y-%m-%d'))
    df[c.d] = df[c.d].apply(lambda x : x.strftime('%Y-%m-%d'))

    return df

def drop_duplicates_except_tse_id(df) :
    """
    firms TSETMC id might changed during time, but the firm is not changed.
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
    df = check_date_vals_and_make_jdate(df)

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


if False :
    pass

    ##

    def test() :
        pass

        ##
        vars(DNomPriceCol)

        ##
        def return_not_special_variables_of_class(cls) :
            return {x : y for x , y in cls.__dict__.items() if
                    not x.startswith('_')}

        ##
        return_not_special_variables_of_class(DNomPriceCol)

        ##
        type(DNomPriceCol())

        ##
        def reorder_df_cols_as_class_values(df , cls) :
            return df[return_not_special_variables_of_class(cls).values()]

    ##
