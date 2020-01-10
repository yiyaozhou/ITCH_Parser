# ITCH Parser
Nasdaq TotalView-ITCH is a direct data feed product offered by The Nasdaq Stock Market, LLC.  
The documentation can be found here: http://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHspecification.pdf

My parser reads ITCH 5.0 data, calculates VWAPs for each stock in every trading hour(including the market close).
While reading and parsing the data, the VWAPs are calculated hourly after trading starts.

-- Input: unzipped ITCH 5.0 daily file  
-- Output: csv file containing hourly VWAPs for each stock


## Functions Explanation:

input:  
-- Dictionary "system" stores system event message type and timestamp, which was used to define trading start and end.

-- Dictionary "order" stores all order details, including timestamp, reference number, stock ticker, shares and stock price. 
Reference number is the key. The main use of this dictionary is to audit the stock tickers and prices. 

-- Dictionary "execute" stores all order execution details, including timestamp, reference number, stock ticker, executed shares,
executed price, match number and printable. Match number is the key. The stock ticker and executed price can be populated by
referring to "order" using the reference number.

-- Dictionary "avg" stores all the stock tickers and VWAPs for every trading hour. trading hour count is the key.


## calculate_weighted_avg 
calculate_weighted_avg calculates the VWAP for each stock during a certain time range (start_time, end_time].

input:    
-- Dictionary "execute" stores all order execution details, including timestamp, reference number, stock ticker, executed shares,
executed price, match number and printable. Match number is the key. The stock ticker and executed price can be populated by
referring to "order" using the reference number.

-- Integer start_time and end_time: nanoseconds representing the timestamp for the time range.

output:   
-- A dictionary storing the VWAP for each stock with the stock code as the key.
