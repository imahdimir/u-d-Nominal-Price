"""

    """

import time
from pathlib import Path

import pandas as pd
import requests
from githubdata import GitHubDataRepo as GDR
from mirutil.async_req import get_resps_async_sync
from mirutil.const import Const as MConst
from mirutil.df import save_df_as_prq
from mirutil.utils import ret_clusters_indices

from main import c
from main import cn
from main import fpn
from main import gdu

mk = MConst()

class Const :
    # nominal prices url format
    url = 'http://members.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={}&Top=999999&A={}'

def get_all_firm_ids() :
    """ get all Firms' TSETMC ids """
    gdr = GDR(gdu.firm_ids_s)
    df = gdr.read_data()
    df = df.astype('string')
    return df

def make_nominal_price_url(firmticker_id) :
    k = Const()
    return k.url.format(firmticker_id , 1)

def filter_to_get_items(df) :
    msk = df[cn.rspt].isna()
    msk |= df[cn.rspt].eq('')

    df = df[msk]
    print('empty ones count:' , len(df))

    return df

def get_all_data(df , test_mode = True) :
    _df = filter_to_get_items(df)

    cls = ret_clusters_indices(_df , 50)

    for se in cls :
        si = se[0]
        ei = se[1]
        print(se)

        inds = _df.iloc[si : ei].index
        print(inds)

        urls = df.loc[inds , cn.url]

        rs = get_resps_async_sync(urls)

        df = add_resps_to_df(df , inds , rs)

        if test_mode :
            break

        time.sleep(2)

    return df

def read_a_single_response(resp) :
    rg = resp
    if rg.r.status == 200 :
        rc = rg.cont
        rt = rc.decode('utf-8')
        return rt

def add_resps_to_df(df , inds , resps) :
    cols = {
            cn.rspt : read_a_single_response ,
            }

    for ky , vl in cols.items() :
        df.loc[inds , ky] = [vl(x) for x in resps]

    msk = df.loc[inds , cn.rspt].notna()
    print('new data count: ' , len(msk[msk]))

    return df

def get_all_data_with_retry(df) :
    """ get all data in number of loops and not test mode """
    try :

        for i in range(10) :
            print('Loop numebr: ' , i)

            df = get_all_data(df , test_mode = False)

    except KeyboardInterrupt :
        print('KeyboardInterrupt')

    finally :
        return df

def main() :
    pass

    ##

    # get all firm ids
    df = get_all_firm_ids()

    ##

    # make a col for url
    df[cn.url] = df[c.tse_id].apply(make_nominal_price_url)

    ##

    # make a col for response text
    df[cn.rspt] = None

    ##

    # get all data async and with retry
    df = get_all_data_with_retry(df)

    ##

    # drop url col
    df = df.drop(columns = [cn.url])

    ##

    # save temp data without index
    save_df_as_prq(df , fpn.t0)

    ##

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

        # single stock
        url = make_nominal_price_url('18401147983387689')
        r = requests.get(url , headers = mk.headers)
        x = r.text

        ##

        # test mode
        df = get_all_data(df)

        ##

        x = df.loc[3 , cn.rspt]
        x

        ##
        type(x)

        ##
        # byte to str
        x = x.decode('utf-8')
        x

        ##
