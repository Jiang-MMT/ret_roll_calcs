import pandas as pd
from pandas.tseries.offsets import DateOffset, MonthBegin
from glob import glob
from datetime import date

# file_name = 'CL_master.csv'
def process_file():
    for file_name in glob('*.csv'):
        print "starting to process {}".format(file_name)
        process(file_name)


def process(file_name):
    df = pd.read_csv(file_name, usecols=[0,5,6,7,12],parse_dates={'trade_date':[1,2,3]}, keep_date_col=True)
    # add 'contract_date' column on the master dataframe
    dfc = df[['trade_date', 'symbol']].groupby('symbol').tail(1).rename(columns={'trade_date':'contract_date'})
    spotTD = dfc['contract_date'].diff(periods=1).max()
    df = df.merge(dfc, how='left', on='symbol')
    df.drop(['tradingDay_Day'],axis=1, inplace=True)
    # extract month begin trading data for each symbol
    # add a date diff column
    dfmb = df.groupby(['symbol', 'tradingYear', 'tradingMonth']).head(1)
    dfmb = dfmb[dfmb.trade_date.dt.day < 10]
    dfmb['ddiff'] = dfmb['contract_date'] - dfmb['trade_date']
    # discard more than 1 year data
    # discard trading date with first entry is not spot date
    dfmb = dfmb.groupby(['tradingYear', 'tradingMonth']).filter(lambda x: x['ddiff'].iloc[0] <= spotTD)
    imask = dfmb.groupby(['tradingYear', 'tradingMonth']).apply(lambda x: x['contract_date'] <= x['contract_date'].iloc[0] + pd.Timedelta('385 days'))
    idx = imask[imask].index.get_level_values(2)
    dfmb = dfmb.loc[idx]
    # find the first group with a length greater than 12
    #calculate number of contract with a year
    num_contract = dfmb.groupby(['tradingYear', 'tradingMonth']).size().mode()[0]
    trade_date_df = dfmb.groupby(['tradingYear', 'tradingMonth']).filter(lambda x: len(x) < num_contract)
    if not trade_date_df.empty:
        first_trade_date = trade_date_df.tail(1).trade_date.values[0]
        dfmb = dfmb[dfmb.trade_date > first_trade_date]
    df_spot = dfmb.groupby(['tradingYear', 'tradingMonth']).nth(0).rename(columns={'close':'spot'})
    df_f2m = dfmb.groupby(['tradingYear', 'tradingMonth']).nth(1).rename(columns={'close':'f2m'})
    df_f1y = dfmb.groupby(['tradingYear', 'tradingMonth']).nth(-1).rename(columns={'close':'f1y'})
    rts = pd.concat([df_spot[['trade_date','spot']], df_f2m['f2m'], df_f1y['f1y']], axis=1)
    rts = rts.reset_index(drop=True)
    rts = rts.sort_values('trade_date')
    print 'Calculting.....'
    rts['s'] = rts['spot'].shift(-12)
    rts['ss'] = rts['spot'].shift(-1)
    rts['ret1yl'] = rts['s'] - rts['f1y']
    rts['diff'] = rts['ss'] - rts['f2m']
    rts['roll1y'] = rts['diff'].rolling(window=12).sum().shift(-11)
    rts.dropna(how='any', inplace=True)
    rts = rts.reset_index(drop=True).sort_values('trade_date')
    rts = rts.drop(columns=['diff', 's', 'ss'])
    rts.to_csv('{}_roll_ret_{}.csv'.format(file_name[:2],date.today().strftime("%b_%d_%Y")), index=False)
    print 'File {} is successfully processed!'.format(file_name)

if __name__ == '__main__':
    process_file()
    # process(file_name)
