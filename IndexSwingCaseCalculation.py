import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta


class SwingCalculation():
    def __init__(self):
        self.df = pd.read_feather('D:\CashAPINewDataMergingandAutomation\CashOHLC.feather')
        self.df = self.df[self.df["sectorname"] != 'ETF']
        
        print(self.df['TradeDate'].max())
        self.UpPerValue = 50
        self.DownPerValue = 30
        self.index = 'NIFTY'
        self.NIFTYdf = self.df[self.df['nsesymbol'] == self.index].copy()
        self.NIFTYdf = self.NIFTYdf.sort_values(by='TradeDate')
        self.NIFTYdf['TradeDate'] = pd.to_datetime(self.NIFTYdf['TradeDate'])

        print(self.NIFTYdf['TradeDate'].max())
        self.df['TradeDate'] = pd.to_datetime(self.df['TradeDate']).dt.date
        threshold = 20
        
    def get_monthly_rolling_highs(self, df_dates):
        ohlc_df = self.df.copy()
        df_dates['IndexDate'] = pd.to_datetime(df_dates['IndexDate'], dayfirst=True)
        ohlc_df['TradeDate'] = pd.to_datetime(ohlc_df['TradeDate'], dayfirst=True)

        results = []

        for i in range(len(df_dates)):
            start_date = df_dates.loc[i, 'IndexDate'] + relativedelta(days=1)
            print(pd.to_datetime(df_dates.loc[i, 'IndexDate']).date(), datetime.now().strftime("%H:%M:%S"))

            if i == len(df_dates) - 1:
                end_date = ohlc_df['TradeDate'].max()
            else:
                end_date = df_dates.loc[i + 1, 'IndexDate']

            current_month_end = df_dates.loc[i, 'IndexDate'] + pd.offsets.MonthEnd(0)
            future_month_ends = pd.date_range(
                start=current_month_end + relativedelta(days=1), 
                end=end_date, 
                freq='M'
            )

            month_ends = [current_month_end] + list(future_month_ends)

            for month_end in month_ends:
                mask = (ohlc_df['TradeDate'] >= df_dates.loc[i, 'IndexDate']) & (ohlc_df['TradeDate'] <= month_end)
                period_data = ohlc_df.loc[mask]

                if not period_data.empty:
                    grouped = (
                        period_data.groupby('co_code').apply(
                            lambda x: pd.Series({
                                'High': x['High'].max(),
                                'MaxHighDate': x.loc[x['High'].idxmax(), 'TradeDate'],
                                'Low': x['Low'].min(),
                                'MinLowDate': x.loc[x['Low'].idxmin(), 'TradeDate'],
                                'nsesymbol': x.loc[x.index[0], 'nsesymbol'],
                                'mcaptype': x.loc[x.index[0], 'mcaptype'],
                                'sectorname': x.loc[x.index[0], 'sectorname'],
                                'industryname': x.loc[x.index[0], 'industryname'],
                                'FirstDate': x['TradeDate'].min(),
                                'FirstOpen': x.loc[x[x['TradeDate'] == x['TradeDate'].min()].index[0], 'Open'],
                                'FirstHigh': x.loc[x[x['TradeDate'] == x['TradeDate'].min()].index[0], 'High'],
                                'FirstLow': x.loc[x[x['TradeDate'] == x['TradeDate'].min()].index[0], 'Low'],
                                'FirstClose': x.loc[x[x['TradeDate'] == x['TradeDate'].min()].index[0], 'Close'],
                                'LastTradeDate': x['TradeDate'].max(),
                                'LastOpen': x.loc[x[x['TradeDate'] == x['TradeDate'].max()].index[0], 'Open'],
                                'LastHigh': x.loc[x[x['TradeDate'] == x['TradeDate'].max()].index[0], 'High'],
                                'LastLow': x.loc[x[x['TradeDate'] == x['TradeDate'].max()].index[0], 'Low'],
                                'LastClose': x.loc[x[x['TradeDate'] == x['TradeDate'].max()].index[0], 'Close']
                            })
                        ).reset_index()
                    )

                    grouped['StkPercChange'] = (((grouped['LastClose'] - grouped['FirstClose']) / grouped['FirstClose']) * 100).round(2)

                    for _, row in grouped.iterrows():
                        results.append({
                            'IndexStartDate': df_dates.loc[i, 'IndexDate'],
                            'IndexEndDate': end_date,
                            'MonthEndDate': month_end,
                            'co_code': row['co_code'],
                            'Symbol': row['nsesymbol'],
                            'FirstDate': row['FirstDate'],
                            'FirstOpen': row['FirstOpen'],
                            'FirstHigh': row['FirstHigh'],
                            'FirstLow': row['FirstLow'],
                            'FirstClose': row['FirstClose'],
                            'LastDate': row['LastTradeDate'],
                            'LastOpen': row['LastOpen'],
                            'LastHigh': row['LastHigh'],
                            'LastLow': row['LastLow'],
                            'LastClose': row['LastClose'],
                            'MaxHigh': row['High'],
                            'MaxHighDate': row['MaxHighDate'],
                            'MinLow': row['Low'],
                            'MinLowDate': row['MinLowDate'],
                            'StkPercChange': row['StkPercChange'],
                            'mcaptype': row['mcaptype'],
                            'sectorname': row['sectorname'],
                            'industryname': row['industryname'],
                        })

        result_df = pd.DataFrame(results)

        sector_group = result_df.groupby(['sectorname', 'IndexStartDate', 'MonthEndDate'])['StkPercChange'].mean().reset_index()
        sector_group.rename(columns={'StkPercChange': 'SectorAvgPercChange'}, inplace=True)
        result_df = result_df.merge(sector_group, on=['sectorname', 'IndexStartDate', 'MonthEndDate'], how='left')

        # Mcap avg
        mcap_group = result_df.groupby(['mcaptype', 'IndexStartDate', 'MonthEndDate'])['StkPercChange'].mean().reset_index()
        mcap_group.rename(columns={'StkPercChange': 'McapAvgPercChange'}, inplace=True)
        result_df = result_df.merge(mcap_group, on=['mcaptype', 'IndexStartDate', 'MonthEndDate'], how='left')

        # Industry avg
        industry_group = result_df.groupby(['industryname', 'IndexStartDate', 'MonthEndDate'])['StkPercChange'].mean().reset_index()
        industry_group.rename(columns={'StkPercChange': 'IndustryAvgPercChange'}, inplace=True)
        result_df = result_df.merge(industry_group, on=['industryname', 'IndexStartDate', 'MonthEndDate'], how='left')

      
        merged_df = result_df.merge(
            df_dates[['Index','IndexDate', 'IndexPrice', 'IndexClose', 'Type', 'PercentChange', 'Days Difference',]],
            left_on='IndexStartDate',
            right_on='IndexDate',
            how='left'
        )

        return merged_df

   
    def format_time_difference(self,days):
        """Convert days to a clean 'years months' format."""
        if pd.isna(days):
            return None
        years = int(days // 365)  
        months = int((days % 365) // 30)  

        if years > 0 and months > 0:
            return f"{years} year {months} month"
        elif years > 0:
            return f"{years} year"
        elif months > 0:
            return f"{months} month"
        else:
            return f"{int(days)} days"

    # def find_tops_and_bottoms_ToptoTop(self,df):
    #     # df = df.copy().reset_index(drop=True)
    #     df = df.copy()
    #     print(df)
    #     tops = []
    #     bottoms = []
    #     highest_high = 0
    #     last_top = 0
    #     top_date = None
    #     drop_detected = False
    #     last_date = df['TradeDate'].iloc[-1]

    #     for index, row in df.iterrows():
    #         current_high = row['High']
    #         current_low = row['Low']
    #         current_close = row['Close']
    #         current_date = row['TradeDate']

    #         # **Step 1: Detect New Highest High**
    #         if current_high > highest_high:
    #             highest_high = current_high
    #             top_date = current_date
    #             top_close = current_close
    #             drop_detected = False

    #         # **Step 2: Detect 20% Drop from High**
    #         if highest_high > 0 and current_low <= highest_high * 0.8:
    #             if highest_high > last_top:
    #                 tops.append((highest_high, top_date, "Top", top_close))

    #                 # **Find Bottom Between Two Tops**
    #                 if len(tops) > 1:
    #                     prev_top_date = tops[-2][1]
    #                     df_between = df[(df['TradeDate'] > prev_top_date) & (df['TradeDate'] < top_date)]
    #                     if not df_between.empty:
    #                         bottom_row = df_between.loc[df_between['Low'].idxmin()]
    #                         bottoms.append((bottom_row['Low'], bottom_row['TradeDate'], "Bottom", bottom_row['Close']))

    #                 last_top = highest_high
    #                 highest_high = 0
    #                 drop_detected = True

    #         # **Step 3: Detect Breakout Above Last Top**
    #         if drop_detected and current_high > last_top:
    #             highest_high = current_high
    #             top_date = current_date
    #             top_close = current_close
    #             drop_detected = False

    #         # **Step 4: If Last Row, Ensure Last Top and Bottom are Recorded**
    #         if current_date == last_date and highest_high > last_top:
    #             tops.append((highest_high, top_date, "Top", top_close))

    #             # **Find Bottom for Last Top**
    #             if len(tops) > 1:
    #                 prev_top_date = tops[-2][1]
    #                 df_between = df[(df['TradeDate'] > prev_top_date) & (df['TradeDate'] <= top_date)]
    #                 if not df_between.empty:
    #                     bottom_row = df_between.loc[df_between['Low'].idxmin()]
    #                     bottoms.append((bottom_row['Low'], bottom_row['TradeDate'], "Bottom", bottom_row['Close']))

    #     # **Step 5: Last Top → Bottom on 2025-03-04**
    #     if tops:
    #         last_top_value, last_top_date, _, last_top_close  = tops[-1]  
            
    #         # **Find Low of 2025-03-04**
    #         bottom_2025_low = df[df['TradeDate'] == '2025-03-04']['Low'].values
    #         bottom_2025_close = df[df['TradeDate'] == '2025-03-04']['Close'].values
    #         if len(bottom_2025_low) > 0:
    #             bottoms.append((bottom_2025_low[0], pd.to_datetime('2025-03-04'), "Bottom" , bottom_2025_close[0]))

    #     # Convert to DataFrame & Merge Tops and Bottoms
    #     tops_df = pd.DataFrame(tops, columns=['IndexPrice', 'IndexDate', "Type", "IndexClose"])
    #     bottoms_df = pd.DataFrame(bottoms, columns=['IndexPrice', 'IndexDate', "Type", "IndexClose"])
    #     final_df = pd.concat([tops_df, bottoms_df]).reset_index(drop=True)

    #     final_df['IndexDate'] = pd.to_datetime(final_df['IndexDate'])

    #     final_df = final_df.sort_values(by="IndexDate").reset_index(drop=True)

    #     final_df['PercentChange'] = round(final_df['IndexPrice'].pct_change() * 100, 2) 

    #     final_df['Days Difference'] = final_df['IndexDate'].diff().dt.days

    #     final_df.insert(final_df.columns.get_loc('Days Difference') + 1, 'Time Difference String',
    #                     final_df['Days Difference'].apply(self.format_time_difference))

    #     final_df['Top to Top Days Only'] = None
    #     last_top_date = None

    #     for i in range(len(final_df)):
    #         if final_df.loc[i, "Type"] == "Top":
    #             if last_top_date is not None:
    #                 final_df.loc[i, "Top to Top Days Only"] = (final_df.loc[i, "IndexDate"] - last_top_date).days
    #             last_top_date = final_df.loc[i, "IndexDate"]

    #     final_df.insert(final_df.columns.get_loc('Top to Top Days Only') + 1, 'Top to Top Time String',
    #                     final_df['Top to Top Days Only'].apply(self.format_time_difference))
        
    #     final_df['Index'] = self.index

    #     return final_df
    

    def find_tops_and_bottoms_ToptoTop(self, df):
        df = df.copy()
        tops = []
        bottoms = []
        highest_high = 0
        last_top = 0
        top_date = None
        drop_detected = False
        last_date = df['TradeDate'].iloc[-1]

        for index, row in df.iterrows():
            current_high = row['High']
            current_low = row['Low']
            current_close = row['Close']
            current_date = row['TradeDate']

            # Detect New Highest High
            if current_high > highest_high:
                highest_high = current_high
                top_date = current_date
                top_close = current_close
                drop_detected = False

            # Detect 20% Drop from High
            if highest_high > 0 and current_low <= highest_high * 0.8:
                if highest_high > last_top:
                    tops.append((highest_high, top_date, "Top", top_close))

                    # Find Bottom Between Two Tops
                    if len(tops) > 1:
                        prev_top_date = tops[-2][1]
                        df_between = df[(df['TradeDate'] > prev_top_date) & (df['TradeDate'] < top_date)]
                        if not df_between.empty:
                            bottom_row = df_between.loc[df_between['Low'].idxmin()]
                            bottoms.append((bottom_row['Low'], bottom_row['TradeDate'], "Bottom", bottom_row['Close']))

                    last_top = highest_high
                    highest_high = 0
                    drop_detected = True

            # Detect Breakout Above Last Top
            if drop_detected and current_high > last_top:
                highest_high = current_high
                top_date = current_date
                top_close = current_close
                drop_detected = False

            # If Last Row, Ensure Last Top and Bottom are Recorded
            if current_date == last_date and highest_high > last_top:
                tops.append((highest_high, top_date, "Top", top_close))

                # Find Bottom for Last Top
                if len(tops) > 1:
                    prev_top_date = tops[-2][1]
                    df_between = df[(df['TradeDate'] > prev_top_date) & (df['TradeDate'] <= top_date)]
                    if not df_between.empty:
                        bottom_row = df_between.loc[df_between['Low'].idxmin()]
                        bottoms.append((bottom_row['Low'], bottom_row['TradeDate'], "Bottom", bottom_row['Close']))

        # **Step 5: Compare Last Date with 2025-03-04**
        if not df[df['TradeDate'] == '2025-03-04'].empty:
            bottom_2025_low = df[df['TradeDate'] == '2025-03-04']['Low'].values[0]
            bottom_2025_close = df[df['TradeDate'] == '2025-03-04']['Close'].values[0]

            last_date_high = df['High'].iloc[-1]
            last_date_low = df['Low'].iloc[-1]
            last_date_close = df['Close'].iloc[-1]
            last_date_trade = df['TradeDate'].iloc[-1]

            if last_date_low < bottom_2025_low:
                # Make last date a Bottom
                bottoms.append((last_date_low, last_date_trade, "Bottom", last_date_close))
            elif last_date_high > bottom_2025_low:
                # Make 2025-03-04 a Bottom and last date a Top
                
                bottoms.append((bottom_2025_low, pd.to_datetime('2025-03-04'), "Bottom", bottom_2025_close))
                tops.append((last_date_high, last_date_trade, "Top", last_date_close))

        # Convert to DataFrame & Merge Tops and Bottoms
        tops_df = pd.DataFrame(tops, columns=['IndexPrice', 'IndexDate', "Type", "IndexClose"])
        bottoms_df = pd.DataFrame(bottoms, columns=['IndexPrice', 'IndexDate', "Type", "IndexClose"])
        final_df = pd.concat([tops_df, bottoms_df]).reset_index(drop=True)

        final_df['IndexDate'] = pd.to_datetime(final_df['IndexDate'])
        final_df = final_df.sort_values(by="IndexDate").reset_index(drop=True)
        final_df['PercentChange'] = round(final_df['IndexPrice'].pct_change() * 100, 2)
        final_df['Days Difference'] = final_df['IndexDate'].diff().dt.days

        final_df.insert(final_df.columns.get_loc('Days Difference') + 1, 'Time Difference String',
                        final_df['Days Difference'].apply(self.format_time_difference))

        final_df['Top to Top Days Only'] = None
        last_top_date = None

        for i in range(len(final_df)):
            if final_df.loc[i, "Type"] == "Top":
                if last_top_date is not None:
                    final_df.loc[i, "Top to Top Days Only"] = (final_df.loc[i, "IndexDate"] - last_top_date).days
                last_top_date = final_df.loc[i, "IndexDate"]

        final_df.insert(final_df.columns.get_loc('Top to Top Days Only') + 1, 'Top to Top Time String',
                        final_df['Top to Top Days Only'].apply(self.format_time_difference))
        
        final_df['Index'] = self.index

        return final_df




        
    def InitialFuncRS(self, swing_df):
        df = self.df.copy()
                
        df["TradeDate"] = pd.to_datetime(df["TradeDate"])
        df = df.sort_values(['co_code', 'TradeDate'])
        df = df.set_index('TradeDate')

        df['1WeekHigh'] = df.groupby('co_code')['High'].rolling('7D').max().reset_index(level=0, drop=True)
        df["1WeekHighChange%"] = ((df["High"] - df['1WeekHigh']) / df['1WeekHigh']) * 100
        df["1WeekHighComRank"] = df.groupby("TradeDate")[f"{'1WeekHigh'}Change%"].rank(method="dense", ascending=False)
        df = df.sort_values(by=["co_code", "TradeDate"])
        
        df['1MonthHigh'] = df.groupby('co_code')['High'].rolling('30D').max().reset_index(level=0, drop=True)
        df["1MonthHighChange%"] = ((df["High"] - df['1MonthHigh']) / df['1MonthHigh']) * 100
        df["1MonthHighComRank"] = df.groupby("TradeDate")[f"{'1MonthHigh'}Change%"].rank(method="dense", ascending=False)
        df = df.sort_values(by=["co_code", "TradeDate"])
        
        df['3MonthHigh'] = df.groupby('co_code')['High'].rolling('90D').max().reset_index(level=0, drop=True)
        df["3MonthHighChange%"] = ((df["High"] - df['3MonthHigh']) / df['3MonthHigh']) * 100
        df["3MonthHighComRank"] = df.groupby("TradeDate")[f"{'3MonthHigh'}Change%"].rank(method="dense", ascending=False)
        df = df.sort_values(by=["co_code", "TradeDate"])
        
        df['6MonthHigh'] = df.groupby('co_code')['High'].rolling('180D').max().reset_index(level=0, drop=True)
        df["6MonthHighChange%"] = ((df["High"] - df['6MonthHigh']) / df['6MonthHigh']) * 100
        df["6MonthHighComRank"] = df.groupby("TradeDate")[f"{'6MonthHigh'}Change%"].rank(method="dense", ascending=False)
        df = df.sort_values(by=["co_code", "TradeDate"])
        
        df['1YearHigh'] = df.groupby('co_code')['High'].rolling('365D').max().reset_index(level=0, drop=True)
        df["1YearHighChange%"] = ((df["High"] - df['1YearHigh']) / df['1YearHigh']) * 100
        df["1YearHighComRank"] = df.groupby("TradeDate")[f"{'1YearHigh'}Change%"].rank(method="dense", ascending=False)
        df = df.sort_values(by=["co_code", "TradeDate"])
        
        df['2YearHigh'] = df.groupby('co_code')['High'].rolling('730D').max().reset_index(level=0, drop=True)
        df["2YearHighChange%"] = ((df["High"] - df['2YearHigh']) / df['1YearHigh']) * 100
        df["2YearHighComRank"] = df.groupby("TradeDate")[f"{'2YearHigh'}Change%"].rank(method="dense", ascending=False)
        df = df.sort_values(by=["co_code", "TradeDate"])
        
        df['3YearHigh'] = df.groupby('co_code')['High'].rolling('1095D').max().reset_index(level=0, drop=True)
        df["3YearHighChange%"] = ((df["High"] - df['3YearHigh']) / df['1YearHigh']) * 100
        df["3YearHighComRank"] = df.groupby("TradeDate")[f"{'3YearHigh'}Change%"].rank(method="dense", ascending=False)
        df = df.sort_values(by=["co_code", "TradeDate"])
        df = df.reset_index()
        df.to_feather("CashOHLCPerChange.feather")
        df.to_csv("CashOHLCPerChange.csv")
        
        swing_df['IndexDate'] = pd.to_datetime(swing_df['IndexDate']).dt.date
        df['TradeDate'] = pd.to_datetime(df['TradeDate']).dt.date

        filtered_df = df[df['TradeDate'].isin(swing_df['IndexDate'])]

        merged_df = pd.merge(filtered_df, swing_df, how='left', left_on='TradeDate', right_on='Date')

        merged_df.drop(columns=['Date'], inplace=True)
        merged_df.to_csv(f"Stock_RS{self.index}Case2.csv")
        return merged_df
        
   
        
    def find_tops_and_bottoms(self, df):
        df = df.sort_values(by='TradeDate').reset_index(drop=True)
        top_bottom = []
        highest_top = df['High'].iloc[0]
        lowest_bottom = df['Low'].iloc[0]
        top_date = df['TradeDate'].iloc[0]
        bottom_date = df['TradeDate'].iloc[0]
        tracking = ''
        found = False

        for i in range(1, len(df)):
            row = df.iloc[i]
            high = row['High']
            low = row['Low']
            date = row['TradeDate']

            if tracking == '' and not found:
                if high > highest_top:
                    highest_top = high
                    top_date = date
                if low < lowest_bottom:
                    lowest_bottom = low
                    bottom_date = date
                up_percent = ((highest_top - lowest_bottom) / lowest_bottom) * 100
                down_percent = ((highest_top - low) / highest_top) * 100

                if up_percent >= 20:
                    tracking = 'Top'
                    top_date = date
                    found = True
                elif down_percent >= 20:
                    tracking = 'Bottom'
                    bottom_date = date
                    found = True

            if tracking == 'Top':
                if high > highest_top:
                    highest_top = high
                    top_date = date
                lowest_bottom = df.loc[df['TradeDate'].between(top_date, date), 'Low'].min()
                down_percent = ((highest_top - lowest_bottom) / highest_top) * 100

                if down_percent >= 20:
                    top_bottom.append({
                        'Type': 'Top', 
                        'Price': highest_top, 
                        'Date': top_date, 
                        'PercentChange': round(((highest_top - top_bottom[-1]['Price']) / top_bottom[-1]['Price']) * 100, 2) if len(top_bottom) > 0 else np.nan
                    })
                    tracking = 'Bottom'
                    bottom_date = date
                    lowest_bottom = low

            elif tracking == 'Bottom':
                if low < lowest_bottom:
                    lowest_bottom = low
                    bottom_date = date
                highest_top = df.loc[df['TradeDate'].between(bottom_date, date), 'High'].max()
                up_percent = ((highest_top - lowest_bottom) / lowest_bottom) * 100

                if up_percent >= 20:
                    top_bottom.append({
                        'Type': 'Bottom', 
                        'Price': lowest_bottom, 
                        'Date': bottom_date, 
                        'PercentChange': round(((lowest_bottom - top_bottom[-1]['Price']) / top_bottom[-1]['Price']) * 100, 2) if len(top_bottom) > 0 else np.nan
                    })
                    tracking = 'Top'
                    top_date = date
                    highest_top = high

       

        # Last Row Condition
        last_price = df.iloc[-1]['Close']
        if tracking == 'Top' and len(top_bottom) > 0:
            percent_change = round(((highest_top - top_bottom[-1]['Price']) / top_bottom[-1]['Price']) * 100, 2)
            top_bottom.append({
                'Type': 'Top', 
                'Price': highest_top, 
                'Date': top_date, 
                'PercentChange': percent_change
            })
        elif tracking == 'Bottom' and len(top_bottom) > 0:
            percent_change = round(((top_bottom[-1]['Price'] - last_price) / top_bottom[-1]['Price']) * 100, 2)
            top_bottom.append({
                'Type': 'Bottom', 
                'Price': lowest_bottom, 
                'Date': bottom_date, 
                'PercentChange': percent_change
            })
            
         # Manually Set Bottom on 4 March 2025 at the End
        if not df[df['TradeDate'] == pd.Timestamp('2025-03-04')].empty:
            bottom_row = df[df['TradeDate'] == pd.Timestamp('2025-03-04')].iloc[0]
            lowest_bottom = bottom_row['Low']
            bottom_date = bottom_row['TradeDate']
            top_bottom.append({
                'Type': 'Bottom', 
                'Price': lowest_bottom, 
                'Date': bottom_date, 
                'PercentChange': round(((lowest_bottom - top_bottom[-1]['Price']) / top_bottom[-1]['Price']) * 100, 2) if len(top_bottom) > 0 else np.nan
            })

        top_bottom = pd.DataFrame(top_bottom)
        return top_bottom

    def Case1calculate_close_to_close_change(self, swing_df):
        stock_df = self.df.copy()
        
        swing_df['IndexDate'] = pd.to_datetime(swing_df['IndexDate']).dt.date

        result_list = []
        count = 0

        for co_code in stock_df['co_code'].unique():
            count += 1
            # if count >= 5:
            #     break
            
            stock_data = stock_df[stock_df['co_code'] == co_code].copy()
            stock_data['TradeDate'] = pd.to_datetime(stock_data['TradeDate']).dt.date
            stock_data = stock_data.sort_values(by='TradeDate')
            # print(count, stock_data['Symbol'].iloc[0])
            
            for i in range(len(swing_df) - 1):
                start_date = swing_df.iloc[i]['IndexDate']
                end_date = swing_df.iloc[i + 1]['IndexDate']
                swing_change = swing_df.iloc[i + 1]['PercentChange']
                swing_index = swing_df.iloc[i + 1]['Index']
                close_index = swing_df.iloc[i + 1]['IndexClose']
                swingtype = swing_df.iloc[i + 1]['Type']
                swingdaydiff = swing_df.iloc[i + 1]['Days Difference']


                mask = (stock_data['TradeDate'] >= start_date) & (stock_data['TradeDate'] <= end_date)
                filtered_data = stock_data[mask]
                if not filtered_data.empty:
                    first_close = filtered_data.iloc[0]['Close']
                    last_close = filtered_data.iloc[-1]['Close']
                    percent_change = round(((last_close - first_close) / first_close) * 100, 2)
                    result_list.append({
                    'Co_Code': co_code,
                    'Symbol': filtered_data.iloc[0]['nsesymbol'],
                    'IdxStart_Date': start_date,
                    'IdxEnd_Date': end_date,
                    'StockChange': percent_change,
                    'IndexChange': swing_change,
                    'StkSDate':filtered_data.iloc[0]['TradeDate'],
                    'FirstClose':first_close,
                    'StkEDate':filtered_data.iloc[-1]['TradeDate'],
                    'LastClose':last_close,
                    'Index':swing_index,
                    'IndexDate':end_date,
                    'TypeTorB':swingtype,
                    'IndexDaysDifference':swingdaydiff,
                    'IndexClose':close_index
                    
                    })
                else:
                    percent_change = None

                # Append Result List ko hamesha bahar append karo
                

        result_df = pd.DataFrame(result_list)
        result_df['Index'] = self.index
        return result_df
    
    
    def Case2generate_trade_dates(self, DF):
        DF['Date'] = pd.to_datetime(DF['Date'])
        self.df['TradeDate'] = pd.to_datetime(self.df['TradeDate'])

        days_list = [7, 30, 90, 180, 365]
        final_rows = []

        for index, row in DF.iterrows():
            current_date = row['Date']

            # Extract current date data from self.df
            current_data = self.df[self.df['TradeDate'] == current_date].copy()
            if current_data.empty:
                continue  # Skip if no data for current date

            # Store current close price for percentage calculation
            current_data['IndexDateClose'] = current_data['Close']

            # Create a 0-day entry (Same as current date data with Pct_Change = 0)
            current_data['IndexDate'] = current_date
            current_data['DaysType'] = '0 days'
            current_data['Pct_Change'] = 0
            final_rows.append(current_data)

            for i in range(len(days_list)):
                days = days_list[i]
                future_date = current_date + pd.Timedelta(days=days)
                past_date = current_date - pd.Timedelta(days=days)

                upper_bound = (current_date + pd.Timedelta(days=days_list[i + 1])) if i + 1 < len(days_list) else None
                lower_bound = (current_date - pd.Timedelta(days=days_list[i + 1])) if i + 1 < len(days_list) else None

                # Filter for next days
                if upper_bound:
                    future_rows = self.df[(self.df['TradeDate'] >= future_date) & (self.df['TradeDate'] < upper_bound)].copy()
                else:
                    future_rows = self.df[self.df['TradeDate'] >= future_date].copy()
                
                if not future_rows.empty:
                    min_future_date = future_rows['TradeDate'].min()
                    future_rows = future_rows[future_rows['TradeDate'] == min_future_date].copy()
                    future_rows['IndexDate'] = current_date
                    future_rows['DaysType'] = f'Next {days} days'
                    future_rows = future_rows.merge(
                        current_data[['co_code', 'IndexDateClose']], on='co_code', how='left'
                    )
                    future_rows['Pct_Change'] = ((future_rows['Close'] - future_rows['IndexDateClose']) / 
                                                future_rows['IndexDateClose']) * 100
                    final_rows.append(future_rows)

                # Filter for previous days
                if lower_bound:
                    past_rows = self.df[(self.df['TradeDate'] <= past_date) & (self.df['TradeDate'] > lower_bound)].copy()
                else:
                    past_rows = self.df[self.df['TradeDate'] <= past_date].copy()
                
                if not past_rows.empty:
                    max_past_date = past_rows['TradeDate'].max()
                    past_rows = past_rows[past_rows['TradeDate'] == max_past_date].copy()
                    past_rows['IndexDate'] = current_date
                    past_rows['DaysType'] = f'Prev {days} days'
                    past_rows = past_rows.merge(
                        current_data[['co_code', 'IndexDateClose']], on='co_code', how='left'
                    )
                    past_rows['Pct_Change'] = ((past_rows['IndexDateClose'] - past_rows['Close']) / 
                                                past_rows['Close']) * 100
                    final_rows.append(past_rows)

        if final_rows:
            final_df = pd.concat(final_rows, ignore_index=True)
            final_df = final_df.merge(DF, left_on='IndexDate', right_on='Date', how='left')
            final_df.to_csv(f"Stock_RS{self.index}PrevandNextCase2.csv")
            
            # Ensure Close column remains intact (IndexDateClose is only used for calculation)
            final_df.drop(columns=['IndexDateClose'], inplace=True)
            return final_df  # Return the final DataFrame

        return pd.DataFrame()  # Return an empty DataFrame if no data is found

   
   
    
    def Case3_find_allstocks_tops_and_bottoms(self):
        stock_df = self.df.copy()
        count = 0
        all_top_bottom = []

        for co_code in stock_df['co_code'].unique():
            count += 1
            # if count >= 5:
            #     break
            
            df = stock_df[stock_df['co_code'] == co_code].copy()
            df['TradeDate'] = pd.to_datetime(df['TradeDate']).dt.date       
            print(count, df['nsesymbol'].iloc[0], co_code)
            df = df.sort_values(by='TradeDate').reset_index(drop=True)
            top_bottom = []
            highest_top = df['High'].iloc[0]
            lowest_bottom = df['Low'].iloc[0]
            top_date = df['TradeDate'].iloc[0]
            bottom_date = df['TradeDate'].iloc[0]
            tracking = ''
            found = False
        

            for i in range(1, len(df)):
                row = df.iloc[i]
                high = row['High']
                low = row['Low']
                date = row['TradeDate']

                # Initial Tracking Decision
                if tracking == '' and not found:
                    if high > highest_top:
                        highest_top = high
                        top_date = date
                    if low < lowest_bottom:
                        lowest_bottom = low
                        bottom_date = date
                    up_percent = ((highest_top - lowest_bottom) / lowest_bottom) * 100
                    down_percent = ((highest_top - low) / highest_top) * 100

                    if up_percent >= 20:
                        tracking = 'Top'
                        top_date = date
                        found = True
                    elif down_percent >= 20:
                        tracking = 'Bottom'
                        bottom_date = date
                        found = True

                if tracking == 'Top':
                    if high > highest_top:
                        highest_top = high
                        top_date = date
                    lowest_bottom = df.loc[df['TradeDate'].between(top_date, date), 'Low'].min()
                    down_percent = ((highest_top - lowest_bottom) / highest_top) * 100

                    if down_percent >= 20:
                        top_bottom.append({
                            'co_code': co_code,
                            'Type': 'Top', 
                            'Price': highest_top, 
                            'Date': top_date, 
                            'PercentChange': round(((highest_top - top_bottom[-1]['Price']) / top_bottom[-1]['Price']) * 100, 2) if len(top_bottom) > 0 else np.nan
                        })
                        tracking = 'Bottom'
                        bottom_date = date
                        lowest_bottom = low

                elif tracking == 'Bottom':
                    if low < lowest_bottom:
                        lowest_bottom = low
                        bottom_date = date
                    highest_top = df.loc[df['TradeDate'].between(bottom_date, date), 'High'].max()
                    up_percent = ((highest_top - lowest_bottom) / lowest_bottom) * 100

                    if up_percent >= 20:
                        top_bottom.append({
                            'co_code': co_code,
                            'Type': 'Bottom', 
                            'Price': lowest_bottom, 
                            'Date': bottom_date, 
                            'PercentChange': round(((lowest_bottom - top_bottom[-1]['Price']) / top_bottom[-1]['Price']) * 100, 2) if len(top_bottom) > 0 else np.nan
                        })
                        tracking = 'Top'
                        top_date = date
                        highest_top = high

            # Last Row Condition
            last_price = df.iloc[-1]['Close']
            if tracking == 'Top' and len(top_bottom) > 0:
                percent_change = round(((highest_top - top_bottom[-1]['Price']) / top_bottom[-1]['Price']) * 100, 2)
                top_bottom.append({
                    'co_code': co_code,
                    'Type': 'Top', 
                    'Price': highest_top, 
                    'Date': top_date, 
                    'PercentChange': percent_change
                })
            elif tracking == 'Bottom' and len(top_bottom) > 0:
                percent_change = round(((top_bottom[-1]['Price'] - last_price) / top_bottom[-1]['Price']) * 100, 2)
                top_bottom.append({
                    'co_code': co_code,
                    'Type': 'Bottom', 
                    'Price': lowest_bottom, 
                    'Date': bottom_date, 
                    'PercentChange': percent_change
                })
            if top_bottom:
                top_bottom_df = pd.DataFrame(top_bottom)
                top_bottom_df = pd.merge(top_bottom_df, stock_df, how='left', left_on=['co_code', 'Date'], right_on=['co_code', 'TradeDate'])
                all_top_bottom.append(top_bottom_df)
        return pd.concat(all_top_bottom, ignore_index=True) if all_top_bottom else pd.DataFrame()
    
    
    


    def main(self):
        
        swings_df = self.find_tops_and_bottoms_ToptoTop(self.NIFTYdf)
        swings_df.to_csv(f'D:\CashIndexSwings\StreamlitFiles\SwingPoints{self.index}.csv', index=False)
        # swings_df.to_csv(f'SwingPoints{self.index}.csv', index=False)
        print("Swing Points Calculation Completed")
        print(swings_df)
        
        Stock_Perc = self.Case1calculate_close_to_close_change(swings_df)
        Stock_Perc.to_csv(f"D:\CashIndexSwings\StreamlitFiles\Stock_Perc{self.index}closetocloseCase1.csv")
        # Stock_Perc.to_csv(f"Stock_Perc{self.index}closetocloseCase1.csv")
        print(Stock_Perc)
        
        # MonthlyHigh = self.get_monthly_rolling_highs(swings_df)
    
        # MonthlyHigh.to_csv(f"D:\CashIndexSwings\StreamlitFiles\MonthlyHighToptoBottom{self.index}Case2.csv")
        # "D:\CashIndexSwings\StreamlitFiles\Stock_PercNIFTYclosetocloseCase1.csv"
        
        
        # swings_df = self.find_tops_and_bottoms(self.NIFTYdf)
        # swings_df.to_csv('SwingPoints.csv', index=False)
        # print("Swing Points Calculation Completed")
        # print(swings_df)
        
        
        
        # Stock_FDates = self.Case2generate_trade_dates(swings_df)
        # # Stock_FDates.to_csv(f"Stock_FDates{self.index}Case2.csv")
        # print(Stock_FDates)
        
        # allswings_df = self.Case3_find_allstocks_tops_and_bottoms()
        # allswings_df.to_csv('AllStocksSwingPointsCase3.csv', index=False)
        # print(allswings_df)
        # print("ALL Swing Points Calculation Completed")
        # Stock_RS = self.InitialFuncRS()
        
TP = SwingCalculation()
TP.main()
