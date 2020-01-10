"""
This program reads ITCH 5.0 data, calculates VWAPs for each stock in every trading hour.
While reading and parsing the data, the VWAPs are calculated hourly after trading starts.

-- Input: unzipped ITCH 5.0 daily file
-- Output: csv file containing hourly VWAPs for each stock


Author: Yiyao Zhou
Date: 10/04/2019
"""


import pandas as pd
import os
import numpy as np
import datetime
import string
import math
import csv
import logging


"""
The following functions (parse_*) are used to parse ITCH 5.0 binary data and update the input dictionaries. 

input:
-- Dictionary "system" stores system event message type and timestamp, which was used to define trading start and end.

-- Dictionary "order" stores all order details, including timestamp, reference number, stock ticker, shares and stock price. 
Reference number is the key. The main use of this dictionary is to audit the stock tickers and prices. 

-- Dictionary "execute" stores all order execution details, including timestamp, reference number, stock ticker, executed shares,
executed price, match number and printable. Match number is the key. The stock ticker and executed price can be populated by
referring to "order" using the reference number.

-- Dictionary "avg" stores all the stock tickers and VWAPs for every trading hour. trading hour count is the key.
"""
def parse_system_event_message(f, order, execute, system):
    data = f.read(11)
    stock_locate = int.from_bytes(data[:1], byteorder='big', signed=False)
    tracking_num = int.from_bytes(data[2:4], byteorder='big', signed=False)
    timestamp = int.from_bytes(data[4:10], byteorder='big', signed=False)
    event_code = data[10:11]
    logging.info('%s %s; %s %s', 'System Message:', event_code, 'Timestamp:', timestamp)
    system[event_code] = {
        'stock_locate': stock_locate,
        'tracking_num': tracking_num,
        'timestamp': timestamp,
        'event_code': event_code}
    return timestamp
    
def parse_stock_dictionary(f, order, execute, system):
    data = f.read(38)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)

def parse_stock_trading_action(f, order, execute, system):
    data = f.read(24)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)

def parse_reg_sho_short_sale_price(f, order, execute, system):
    data = f.read(19)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)
    
def parse_market_participant_position(f, order, execute, system):
    data = f.read(25)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)
    
def parse_mwcb_decline_level_message(f, order, execute, system):
    data = f.read(34)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)
    
def parse_mwcb_status_message(f, order, execute, system):
    data = f.read(11)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)

def parse_ipo_quoting_period_update(f, order, execute, system):
    data = f.read(27)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)
    
def parse_limit_up_down_auction_collar(f, order, execute, system):
    data = f.read(34)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)

def parse_operational_halt(f, order, execute, system):
    data = f.read(20)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)

def parse_add_order_no_mpid_attribution(f, order, execute, system):
    data = f.read(35)
    timestamp = int.from_bytes(data[4:10], byteorder='big', signed=False)
    reference = int.from_bytes(data[10:18], byteorder='big', signed=False)
    buysell = data[18:19]
    shares = int.from_bytes(data[19:23], byteorder='big', signed=False)
    stock = str(data[23:31], 'ascii').strip()
    price = int.from_bytes(data[31:35], byteorder='big', signed=False)
    order[reference] = {
        'timestamp': timestamp,
        'reference': reference,
        'buysell': buysell,
        'shares': shares,
        'stock': stock,
        'price': price}
    return timestamp
    
def parse_add_order_with_mpid_attribution(f, order, execute, system):
    data = f.read(39)
    timestamp = int.from_bytes(data[4:10], byteorder='big', signed=False)
    reference = int.from_bytes(data[10:18], byteorder='big', signed=False)
    buysell = data[18:19]
    shares = int.from_bytes(data[19:23], byteorder='big', signed=False)
    stock = str(data[23:31], 'ascii').strip()
    price = int.from_bytes(data[31:35], byteorder='big', signed=False)
    order[reference] = {
        'timestamp': timestamp,
        'reference': reference,
        'buysell': buysell,
        'shares': shares,
        'stock': stock,
        'price': price}
    return timestamp

def parse_order_executed_message(f, order, execute, system):
    data = f.read(30)
    timestamp = int.from_bytes(data[4:10], byteorder='big', signed=False)
    reference = int.from_bytes(data[10:18], byteorder='big', signed=False)
    executed_shares = int.from_bytes(data[18:22], byteorder='big', signed=False)
    match_number = int.from_bytes(data[22:30], byteorder='big', signed=False)
    execute[match_number] = {
        'timestamp': timestamp,
        'reference': reference,
        'executed_shares': executed_shares,
        'stock': order[reference]['stock'],
        'executed_price': order[reference]['price'],
        'match_number': match_number,
        'printable': 'Y'}
    return timestamp

def parse_order_executed_with_price_message(f, order, execute, system):
    data = f.read(35)
    timestamp = int.from_bytes(data[4:10], byteorder='big', signed=False)
    reference = int.from_bytes(data[10:18], byteorder='big', signed=False)
    executed_shares = int.from_bytes(data[18:22], byteorder='big', signed=False)
    match_number = int.from_bytes(data[22:30], byteorder='big', signed=False)
    printable = str(data[30:31], 'ascii').strip()
    executed_price = int.from_bytes(data[31:35], byteorder='big', signed=False)
    execute[match_number] = {
        'timestamp': timestamp,
        'reference': reference,
        'executed_shares': executed_shares,
        'stock': order[reference]['stock'],
        'executed_price': executed_price,
        'match_number': match_number,
        'printable': printable}
    return timestamp
    
def parse_order_cancel_message(f, order, execute, system):
    data = f.read(22)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)

def parse_order_delete_message(f, order, execute, system):
    data = f.read(18)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)
    
def parse_order_replace_message(f, order, execute, system):
    data = f.read(34)
    timestamp = int.from_bytes(data[4:10], byteorder='big', signed=False)
    old_reference = int.from_bytes(data[10:18], byteorder='big', signed=False)
    new_reference = int.from_bytes(data[18:26], byteorder='big', signed=False)
    shares = int.from_bytes(data[26:30], byteorder='big', signed=False)
    price = int.from_bytes(data[30:34], byteorder='big', signed=False)
    order[new_reference] = {
        'timestamp': timestamp,
        'reference': new_reference,
        'buysell': order[old_reference]['buysell'],
        'shares': shares,
        'stock': order[old_reference]['stock'],
        'price': price}   
    return timestamp
    
def parse_non_cross_trade_message(f, order, execute, system):
    data = f.read(43)
    timestamp = int.from_bytes(data[4:10], byteorder='big', signed=False)
    reference = int.from_bytes(data[10:18], byteorder='big', signed=False)
    buysell = data[18:19]
    shares = int.from_bytes(data[19:23], byteorder='big', signed=False)
    stock = str(data[23:31], 'ascii').strip()
    price = int.from_bytes(data[31:35], byteorder='big', signed=False)
    match_number = int.from_bytes(data[35:43], byteorder='big', signed=False)
    execute[match_number] = {
        'timestamp': timestamp,
        'reference': reference,
        'executed_shares': shares,
        'stock': stock,
        'executed_price': price,
        'match_number': match_number,
        'printable': 'Y'}
    return timestamp
        
def parse_cross_trade_message(f, order, execute, system):
    data = f.read(39)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)
    
def parse_broken_trade_execution_message(f, order, execute, system):
    data = f.read(18)
    timestamp = int.from_bytes(data[4:10], byteorder='big', signed=False)
    match_number = int.from_bytes(data[10:18], byteorder='big', signed=False)
    execute[match_number]['executed_shares'] = 0
    return timestamp

def parse_noii_message(f, order, execute, system):
    data = f.read(49)
    return int.from_bytes(data[4:10], byteorder='big', signed=False)

def handling_funcs():
    global handling_funcs
    handling_funcs = {
    b'S' : parse_system_event_message             ,
    b'R' : parse_stock_dictionary                 ,
    b'H' : parse_stock_trading_action             ,
    b'Y' : parse_reg_sho_short_sale_price         ,
    b'L' : parse_market_participant_position      ,
    b'V' : parse_mwcb_decline_level_message       ,
    b'W' : parse_mwcb_status_message              ,
    b'K' : parse_ipo_quoting_period_update        ,
    b'J' : parse_limit_up_down_auction_collar     ,
    b'h' : parse_operational_halt                 ,
    b'A' : parse_add_order_no_mpid_attribution    ,
    b'F' : parse_add_order_with_mpid_attribution  ,
    b'E' : parse_order_executed_message           ,
    b'C' : parse_order_executed_with_price_message,
    b'X' : parse_order_cancel_message             ,
    b'D' : parse_order_delete_message             ,
    b'U' : parse_order_replace_message            ,
    b'P' : parse_non_cross_trade_message          ,
    b'Q' : parse_cross_trade_message              ,
    b'B' : parse_broken_trade_execution_message   ,
    b'I' : parse_noii_message                     ,
}
    return handling_funcs


"""
calculate_weighted_avg calculates the VWAP for each stock during a certain time range (start_time, end_time].

input:
-- Dictionary "execute" stores all order execution details, including timestamp, reference number, stock ticker, executed shares,
executed price, match number and printable. Match number is the key. The stock ticker and executed price can be populated by
referring to "order" using the reference number.

-- Integer start_time and end_time: nanoseconds representing the timestamp for the time range.

output:
-- A dictionary storing the VWAP for each stock with the stock code as the key.
"""
def calculate_weighted_avg(execute, start_time, end_time):
    price_volume = {}
    for key in execute:
        timestamp = execute[key]['timestamp']
        stock = execute[key]['stock']
        volume = execute[key]['executed_shares']
        price = execute[key]['executed_price']
        if (timestamp >= start_time) and (timestamp < end_time) and (execute[key]['printable'] == 'Y'):
            if stock not in price_volume.keys():
                price_volume[stock] = {'volume': volume, 'volpri': price * volume}
            else:
                cur_volume = price_volume[stock]['volume']
                cur_volpri = price_volume[stock]['volpri']
                price_volume[stock] = {'volume': volume + cur_volume, 'volpri': price * volume + cur_volpri}
    return price_volume

"""
get_hourly_VWAP parses the input file and returns the VWAP for all the stocks in each trading hour.

input:
-- String file: the absolute path to the input file.

output:
-- A dictionary storing the VWAP of stocks in each trading hour, with the trading hour as the key.
"""
def get_hourly_VWAP(file_path):
    avg = {}
    handling_funcs()
    with open(file_path, "rb") as f:
        order = {}
        execute = {}
        system = {}

        start_timestamp = 0
        close_timestamp = 0
        count_hour_delta = 0
        hour_nanoseconds = 3600 * 1e9

        while f.read(2):
            # Entries are separated by two zero bytes.
            timestamp = handling_funcs[f.read(1)](f, order, execute, system)       
            # Initialize the start_timestamp
            if start_timestamp == 0:
                try:
                    start_timestamp = system[b'Q']['timestamp']
                    logging.info('Trading Starts')
                except:
                    pass

            # General calculation
            if (timestamp - start_timestamp > hour_nanoseconds * (count_hour_delta + 1)) and (start_timestamp != 0):
                hour_avg = calculate_weighted_avg(execute, start_timestamp + count_hour_delta * hour_nanoseconds, timestamp)
                avg[count_hour_delta] = {stock: hour_avg[stock]['volpri']/hour_avg[stock]['volume']/10000 for stock in hour_avg}
                count_hour_delta += 1
                logging.info('%s %s %s', 'VWAP for trading hour', count_hour_delta, 'was calculated.')


            # Trade close calculation
            if close_timestamp == 0:
                try:
                    close_timestamp = system[b'M']['timestamp']
                    logging.info('Trading Ends')
                    hour_avg = calculate_weighted_avg(execute, start_timestamp + count_hour_delta * hour_nanoseconds, close_timestamp)
                    avg[count_hour_delta] = {stock: hour_avg[stock]['volpri']/hour_avg[stock]['volume']/10000 for stock in hour_avg}
                    logging.info('VWAP for trading close was calculated.')
                    logging.info('VWAP calculation done.')
                    break
                except:
                    pass
    return avg


"""
output_combine_df writes the VWAP in CSV format.

input:
-- Dictionary avg stores the VWAP of stocks in each trading hour, with the trading hour as the key.

output:
-- A CSV file.
"""
def output_combine_df(avg, filename):
    for i in avg:
        temp_df = pd.DataFrame.from_dict(avg[i], orient='index').rename(columns={0: "VWAP{0}".format(i+1)}).sort_index()
        if i == 0:
            df = temp_df
        else:
            df = df.join(temp_df, how = "outer")
    df.to_csv(filename)


if __name__ == "__main__":
    file_path = input("Please enter the path of the unzipped file, example C://Users//ITCH//01302019.NASDAQ_ITCH50  ")
    logging.getLogger().setLevel(logging.INFO)	
    avg = get_hourly_VWAP(file_path)
    output_combine_df(avg, "{0}.csv".format(file_path))
    logging.info('%s %s%s', 'The results were stored in', file_path, '.csv')








