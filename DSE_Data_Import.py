import requests
import ssl
import lxml.html as lh
import pandas as pd
import pyodbc as db

ssl._create_default_https_context = ssl._create_unverified_context
connection = db.connect('Driver={SQL Server};'
                      'Server=DESKTOP-9V2IKH8;'
                      'Database=Source;'
                      'Trusted_Connection=yes;')

url='https://www.dsebd.org/latest_share_price_all.php'#Create a handle, page, to handle the contents of the website
tables = pd.read_html(url) # Returns list of all tables on page
table = tables[0].values.tolist()

rownumber = 0
no = []
trdcode = []
ltp = []
high = []
low = []
closep = []
ycp = []
change = []
trade = []
value = []
volume = []

for each_row in table:
    data = table[rownumber][0]
    no.append(data)
    data = table[rownumber][1]
    trdcode.append(data)
    data = table[rownumber][2]
    ltp.append(data)
    data = table[rownumber][3]
    high.append(data)
    data = table[rownumber][4]
    low.append(data)
    data = table[rownumber][5]
    closep.append(data)
    data = table[rownumber][6]
    ycp.append(data)
    data = table[rownumber][7]
    change.append(data)
    data = table[rownumber][8]
    trade.append(data)
    data = table[rownumber][9]
    value.append(data)
    data = table[rownumber][10]
    volume.append(data)
    rownumber += 1

no = no[1:]
trdcode = trdcode[1:]
ltp = ltp[1:]
high = high[1:]
low = low[1:]
closep = closep[1:]
ycp = ycp[1:]
change = change[1:]
trade = trade[1:]
value = value[1:]
volume = volume[1:]

cursor = connection.cursor()

rows = []
for i in range(0, len(trdcode)):
    data = (trdcode[i], ltp[i], high[i], low[i], closep[i], ycp[i], change[i], trade[i], value[i], volume[i])
    rows.append(data)

query = """INSERT INTO test(TRADINGCODE, LTP, HIGH, LOW, CLOSEP, YCP, CHANGE, TRADE, VALUE, VOLUME)
        VALUES (?,?,?,?,?,?,?,?,?,?)"""

truncatequery = "TRUNCATE TABLE TEST"
cursor.execute(truncatequery)

cursor.executemany(query, rows)
connection.commit()

connection.close()