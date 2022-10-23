import requests
import bs4
import re
import datetime
from datetime import *
import tqdm

browserheader = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"}

def detect_os():
    import platform
    return platform.system()

def changeip():    
    os = detect_os()
    if os == 'Windows':
        import winsound
        frequency = 2500
        duration = 200
        winsound.Beep(frequency, duration)
        print("\n")
        input("After you've changed IP adress, Press Enter to continue...")
    elif os == 'Linux':
        from toripchanger import TorIpChanger
        frequency = 2500
        duration = 200
        changer = TorIpChanger(tor_password= '********', tor_port= 9051, local_http_proxy= 'http://127.0.0.1:8118', new_ip_max_attempts= 10)
        print(changer.get_new_ip())
        time.sleep(5)


def generateurl(pagenumber: int) -> str:
    if pagenumber == 1:
        url = "https://bitinfocharts.com/top-100-richest-bitcoin-addresses.html"
    else:
        url = "https://bitinfocharts.com/top-100-richest-bitcoin-addresses-{}.html".format(
            pagenumber)
    return url


def scrapewallets(url: str) -> list:
    html = requests.get(url, headers=browserheader)
    soup = bs4.BeautifulSoup(html.content, "html.parser")
    rows = soup.body.find_all("tr")[-100:]
    if len(rows) == 100:
        return rows
    else:
        changeip()
        return scrapewallets(url)


def savedata(row: bs4.BeautifulSoup, connection, cursor) -> None:
    rank = int(row.find_all("td")[0].text)
    wholeaddress = row.find_all("td")[1].text
    multisig = re.findall("\d+-of-\d+", wholeaddress)
    address = re.findall(".+?(?= |wallet)", wholeaddress)
    walletname = re.findall("(?<=wallet: ).*", wholeaddress)
    if len(multisig) != 0:
        multisig = multisig[0]
    else:
        multisig = ""
    if len(walletname) != 0:
        walletname = walletname[0]
    else:
        walletname = ""
    if len(address) != 0:
        address = address[0]
    else:
        address = wholeaddress
    address = address.replace(".", "")
    balance = row.find_all("td")[2].text
    try:
        floatbalance = float(re.findall(
            ".+?(?= BTC)", balance)[0].replace(",", ""))
    except:
        floatbalance = 0
    firstin = row.find_all("td")[4].text
    try:
        firstin = datetime.strptime(
            firstin, "%Y-%m-%d %X UTC").timestamp()*1000
    except:
        firstin = 0
    lastin = row.find_all("td")[5].text
    try:
        lastin = datetime.strptime(
            lastin, "%Y-%m-%d %X UTC").timestamp()*1000
    except:
        lastin = 0
    firstout = row.find_all("td")[7].text
    try:
        firstout = datetime.strptime(
            firstout, "%Y-%m-%d %X UTC").timestamp()*1000
    except:
        firstout = 0
    lastout = row.find_all("td")[8].text
    try:
        lastout = datetime.strptime(
            lastout, "%Y-%m-%d %X UTC").timestamp()*1000
    except:
        lastout = 0
    if row.find_all("td")[6].text == "":
        ins = 0
    else:
        ins = int(row.find_all("td")[6].text)
    if row.find_all("td")[9].text == "":
        outs = 0
    else:
        outs = int(row.find_all("td")[9].text)
    walleturl = "https://bitinfocharts.com/bitcoin/address/" + address
    existingrow = cursor.execute(
        "SELECT * FROM public.wallets WHERE address = %s", (address,)).fetchall()
    if len(existingrow) == 1:
        previousbestrank = int(existingrow[0][2])
        previoustopbalance = existingrow[0][7]
        if rank < previousbestrank:
            bestrank = rank
        else:
            bestrank = previousbestrank
        if floatbalance > previoustopbalance:
            floattopbalance = floatbalance
        else:
            floattopbalance = previoustopbalance
        cursor.execute("UPDATE public.wallets SET rank = %s, bestrank = %s, balance_BTC = %s, topbalance_BTC = %s, firstin = %s, lastin = %s, firstout = %s, lastout = %s, ins = %s, outs = %s, updated = TRUE WHERE address = %s", (
            rank, bestrank, floatbalance, floattopbalance, firstin, lastin, firstout, lastout, ins, outs, address))
    else:
        bestrank = rank
        floattopbalance = floatbalance
        cursor.execute("INSERT INTO public.wallets VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, TRUE, FALSE, 0)",
                       (walleturl, rank, bestrank, address, walletname, multisig, floatbalance, floattopbalance, firstin, lastin, firstout, lastout, ins, outs))
    connection.commit()


def updatewallets(connection, cursor):
    cursor.execute("UPDATE public.wallets SET updated = FALSE")
    for pagenumber in tqdm.tqdm(range(1, 101), desc="Updating Wallets"):
        url = generateurl(pagenumber)
        rows = scrapewallets(url)
        for row in rows:
            savedata(row, connection, cursor)

    if cursor.execute("SELECT COUNT(*) FROM public.wallets WHERE updated = TRUE").fetchall()[0][0] == 10000:
        cursor.execute(
            "UPDATE public.wallets SET partial = TRUE, rank = 29999 WHERE updated = FALSE")


def walletstable(cursor) -> list:
    wallets = cursor.execute(
        "SELECT * FROM public.wallets ORDER BY rank").fetchall()
    return wallets


def countsavedtxs(walletaddress: str, cursor) -> int:
    numberofsavedtxs = cursor.execute(
        "SELECT COUNT(*) FROM public.transactions WHERE address = %s", (walletaddress,)).fetchall()[0][0]
    return numberofsavedtxs


def countcurrenttxs(row: list) -> int:
    numberofcurrenttxs = row[12] + row[13]
    return numberofcurrenttxs


def activedays(row: list) -> int:
    nowtimestamp = datetime.now().timestamp() * 1000
    firstin = row[8]
    activedays = (datetime.fromtimestamp(nowtimestamp/1000) -
                  datetime.fromtimestamp(firstin/1000)).days + 1
    return activedays


def eligible(row: list, cursor) -> bool:
    currenttxs = countcurrenttxs(row)
    if currenttxs / activedays(row) < 50 and countsavedtxs(row, cursor) < currenttxs:
        return True
    else:
        return False


def generatewalleturl(row: list) -> str:
    url = row[0]
    return url + "-full"


def scrapetxs(url: str) -> list:
    txs = []
    try:
        html = requests.get(url, headers=browserheader)
        if html.reason == 'OK':
            soup = bs4.BeautifulSoup(html.content, "html.parser")
            txs = soup.find_all("tr", attrs={"class": "trb"})
            return txs
        else:
            changeip()
            return scrapetxs(url)
    except:
        changeip()
        return scrapetxs(url)


def get_values(tx) -> tuple:
    cols = tx.find_all("td")
    blocknumber = cols[0].a.text
    time = cols[1].text
    time = datetime.strptime(
        time, "%Y-%m-%d %X UTC").timestamp()*1000
    btcamount = cols[2].span.text
    btcamount = float(re.findall(
        "^.+(?= BTC)", btcamount)[0].replace(",", ""))
    btcbalance = cols[3].text
    btcbalance = float(re.findall(
        "^.+(?= BTC)", btcbalance)[0].replace(",", ""))
    usdbalance = cols[4].text
    try:
        usdbalance = float(re.findall("^.+(?= @)", usdbalance)
                           [0].replace(",", "").replace("$", ""))
    except:
        usdbalance = 0
    usdprofit = cols[5].text
    try:
        usdprofit = float(usdprofit.replace(
            ",", "").replace("$", ""))
    except:
        usdprofit = 0

    return blocknumber, time, btcamount, btcbalance, usdbalance, usdprofit


def savetxs(walletaddress: str, txs: list, connection, cursor, runtime) -> None:
    for tx in txs:
        blocknumber, time, btcamount, btcbalance, usdbalance, usdprofit = get_values(
            tx)
        if time < runtime:
            try:
                cursor.execute("INSERT INTO public.transactions VALUES (%s,%s,%s,%s,%s,%s,%s)", (
                    walletaddress, blocknumber, time, btcamount, btcbalance, usdbalance, usdprofit))
                connection.commit()
            except:
                cursor.execute("ROLLBACK")
                break


def updatetxs(wallets, connection, cursor, runtime):
    for wallet in tqdm.tqdm(wallets, desc="Updating Transactions"):
        if eligible(wallet, cursor):
            url = generatewalleturl(wallet)
            txs = scrapetxs(url)
            walletaddress = wallet[3]
            savetxs(walletaddress, txs, connection, cursor, runtime)
