# Comment: The class returns data from free public api on the Coronavirus (https://covid19api.com)
# Version: 1.0 (2021-04-17)
# Author: Leszek Szeląg (leszek@szelag24.eu)


import time
import datetime
import requests
import json
import awoc  # a-world-of-countries
import pycountry
import countrygroups
import sqlite3
from sys import stdout


class GetApiCovid19:
    def __init__(self):
        self.__api = "https://api.covid19api.com"
        self.__date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        self.__date_top = self.__date
        self.__date_end = self.__date
        self.__country = "poland"
        self.__countries = []
        self.__iso2_list = []
        self.__timeout = 1
        self.__scope = 5
        self.__db_name = "getapicovid19.db"
        self.__countriesCreate()
        self.__dbCreate()

    def __countriesCreate(self):
        self.__countries = []
        __world = awoc.AWOC()
        for continent in __world.get_continents():
            for country in __world.get_countries_data_of(continent["Continent Name"]):
                self.__countries.append([country["ISO2"], continent["Continent Code"].upper()])
        return

    def __dbCreate(self):
        __con = self.dbOpen()
        if __con is not None:
            __con.execute("CREATE TABLE IF NOT EXISTS countries (country TEXT PRIMARY KEY, iso2 TEXT NOT NULL UNIQUE )")
            __con.execute("CREATE TABLE IF NOT EXISTS covid19 ("
                          "country   TEXT     NOT NULL,"
                          "date      TEXT     NOT NULL,"
                          "confirmed INTEGER  NOT NULL,"
                          "deaths    INTEGER  NOT NULL,"
                          "recovered INTEGER  NOT NULL,"
                          "active    INTEGER  NOT NULL,"
                          "PRIMARY KEY (country, date) )")
            __con.commit()
            __con.close()
        return

    def setDateTop(self, date_top):
        self.__date_top = date_top
        return

    def setDateEnd(self, date_end):
        self.__date_end = date_end
        return

    def setCountry(self, count):
        self.__country = count
        return

    def setCountryEU(self):
        self.__iso2_list = []
        __error = ["GB"]  # GB is not European Union
        for count in countrygroups.EUROPEAN_UNION:
            __iso2 = pycountry.countries.get(alpha_3=count).alpha_2
            if not __iso2 in __error:
                self.__iso2_list.append(__iso2)
        return

    def setContinent(self, cont=None):
        self.__iso2_list = []
        if cont is not None:
            for count in self.__countries:
                if count[1] == cont:
                    self.__iso2_list.append(count[0])
        return

    def setIso2list(self, iso2_list):
        self.__iso2_list = iso2_list
        return

    def getDateTop(self):
        return self.__date_top

    def getDateEnd(self):
        return self.__date_end

    def getCountry(self):
        return self.__country

    def getIso2list(self):
        return self.__iso2_list

    def getCountries(self):
        __value = []
        try:
            __link = self.__api + "/countries"
            for i in range(self.__scope):
                __rqs = requests.get(__link)
                if __rqs.status_code == 200:
                    __data = json.loads(__rqs.text)
                    for item in __data:
                        if len(self.__iso2_list) == 0 or item["ISO2"] in self.__iso2_list:
                            __value.append((item["Slug"], item["ISO2"]))
                    __value.sort()
                    break
                else:
                    time.sleep(self.__timeout)
        except:
            __value = []
        return __value

    def getValue(self):
        __value = {}
        if self.__date_top <= self.__date_end <= self.__date:
            __modes = ["Confirmed", "Deaths", "Recovered", "Active"]
            try:
                __link = self.__api + "/country/" + self.__country \
                         + "?from=" + self.__date_top + "T00:00:00Z&to=" + self.__date_end + "T23:59:59Z"
                for i in range(self.__scope):
                    __rqs = requests.get(__link)
                    if __rqs.status_code == 200:
                        __data = json.loads(__rqs.text)
                        for day in __data:
                            __temp = []
                            for mode in __modes:
                                __temp.append(day[mode])
                            __value[day["Date"][:10]] = __temp
                        break
                    else:
                        time.sleep(self.__timeout)
            except:
                __value = {}
        return __value

    def dbOpen(self):
        __con = None
        try:
            __con = sqlite3.connect(self.__db_name)
        except:
            __con = None
        return __con

    def setCountriesDb(self, countries=None):
        if countries is not None:
            __con = self.dbOpen()
            if __con is not None:
                __cur = __con.cursor()
                __cur.execute("SELECT * FROM countries")
                __rows = __cur.fetchall()
                if len(__rows) < len(countries):
                    __con.execute("DELETE FROM countries")
                    __con.executemany("INSERT INTO countries(country,iso2) VALUES (?,?)", countries)
                    __con.commit()
                __cur.close()
                __con.close()
        return

    def getCountriesDb(self):
        __value = []
        __con = self.dbOpen()
        if __con is not None:
            __cur = __con.cursor()
            __cur.execute("SELECT country,iso2 FROM countries ORDER BY country")
            __rows = __cur.fetchall()
            for item in __rows:
                if len(self.__iso2_list) == 0 or item[1] in self.__iso2_list:
                    __value.append((item[0], item[1]))
            __cur.close()
            __con.close()
        return __value

    def setValueDb(self, conn=None, value=None):
        if conn is not None and value is not None:
            conn.execute("DELETE FROM covid19 WHERE country = '" + self.__country + "' and date >= '"
                         + self.__date_top + "' and date <= '" + self.__date_end + "'")
            for item in value:
                __temp = [self.__country, item, value[item][0], value[item][1], value[item][2], value[item][3]]
                conn.execute("INSERT INTO covid19 (country,date,confirmed,deaths,recovered,active)"
                             " VALUES (?,?,?,?,?,?)", __temp)
        return

    def getValueDb(self):
        __value = {}
        __con = self.dbOpen()
        if __con is not None:
            __cur = __con.cursor()
            __cur.execute("SELECT date,confirmed,deaths,recovered,active FROM covid19 WHERE country  = '"
                          + self.__country + "' and date >= '" + self.__date_top + "' and date <= '" + self.__date_end
                          + "' ORDER BY date")
            __rows = __cur.fetchall()
            for item in __rows:
                __value[item[0]] = [item[1], item[2], item[3], item[4]]
            __cur.close()
            __con.close()
        return __value

    def getStartDateDb(self):
        __date_start = ""
        __con = self.dbOpen()
        if __con is not None:
            __cur = __con.cursor()
            __cur.execute("SELECT date FROM covid19 ORDER BY date LIMIT 1")
            __row = __cur.fetchone()
            if __row is not None and len(__row) > 0:
                __date_start = __row[0]
            __cur.close()
            __con.close()
        return __date_start

    def getStopDateDb(self):
        __date_start = ""
        __con = self.dbOpen()
        if __con is not None:
            __cur = __con.cursor()
            __cur.execute("SELECT date FROM covid19 ORDER BY date DESC LIMIT 1")
            __row = __cur.fetchone()
            if __row is not None and len(__row) > 0:
                __date_start = __row[0]
            __cur.close()
            __con.close()
        return __date_start

    def getMinDateDb(self, min_date=None):
        __date_top = "2020-01-01"
        __con = self.dbOpen()
        if __con is not None:
            __cur = __con.cursor()
            __cur.execute("SELECT date FROM covid19 ORDER BY date DESC LIMIT 1")
            __row = __cur.fetchone()
            if __row is not None and len(__row) > 0:
                __date_top = (datetime.datetime.strptime(__row[0], "%Y-%m-%d")
                              + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                if min_date is not None:
                    __date_top = max(__date_top, min_date)
            __cur.close()
            __con.close()
        return __date_top

    def loadApiToDb(self, min_date=None, echo=False):
        __countries = self.getCountries()
        if len(__countries) > 0:
            self.setCountriesDb(__countries)
            __date_top = self.__date_top
            __date_end = self.__date_end
            __country = self.__country
            self.__date_top = self.getMinDateDb(min_date)
            self.__date_end = (datetime.datetime.strptime(self.__date_top, "%Y-%m-%d")
                               + datetime.timedelta(days=60)).strftime("%Y-%m-%d")
            if self.__date_end > self.__date:
                self.__date_end = self.__date
            __con = self.dbOpen()
            if __con is not None:
                if echo:
                    pp = PrintProgress(text="Download " + self.__date_top + " -> " + self.__date_end + " ",
                                       maxi=len(__countries))
                    pp.showStart()
                for count in __countries:
                    self.setCountry(count[0])
                    __value = self.getValue()
                    if len(__value) > 0:
                        self.setValueDb(__con, __value)
                    if echo:
                        pp.showMe()
                __con.commit()
                if echo:
                    pp.showEnd()
                __con.close()
            self.__date_top = __date_top
            self.__date_end = __date_end
            self.__country = __country
        return

    def eraseData(self):
        __con = self.dbOpen()
        if __con is not None:
            __con.execute("DELETE FROM covid19")
            __con.commit()
            __con.close()
        return


class PrintProgress:
    def __init__(self, text="", max_len=50, maxi=None):
        self.__text = text
        self.__max_len = max_len
        self.__maxi = maxi
        self.__sign_a = "="
        self.__sign_b = "-"
        self.__start = "["
        self.__stop = "]"
        self.__pos = 0

    def showStart(self):
        self.__pos = 0
        self.showMe()
        return

    def showMe(self):
        if self.__maxi is not None and self.__pos <= self.__maxi:
            __cur = int(round(self.__pos * 100 / self.__maxi, 0))
            __bar_len = int(round(__cur * self.__max_len / 100, 0))
            __bar = "\r" + self.__text + self.__start \
                    + (self.__sign_a * __bar_len) + (self.__sign_b * (self.__max_len - __bar_len)) \
                    + self.__stop + str(__cur).rjust(4) + "%"
            if self.__pos == self.__maxi:
                __bar = __bar + "\n"
            self.__pos += 1
            stdout.write(__bar)
            stdout.flush()
        return

    def showEnd(self):
        if self.__pos < self.__maxi:
            self.__pos = self.__maxi
            self.showMe()
        return


# Test class
if __name__ == "__main__":
    covid = GetApiCovid19()

    # covid.eraseData()  # Delete all data from covid19 in db to reload

    print("Connect to api...")
    covid.loadApiToDb(min_date="2021-04-01", echo=True)  # Load from a specific date to today from api to db
    while covid.getMinDateDb() <= covid.getDateEnd():
        tmp_date = covid.getMinDateDb()
        covid.loadApiToDb(echo=True)  # Load data in 60 days increments from api to db
        if covid.getMinDateDb() == tmp_date:
            break
    print("Done")

    continent = None  # Filtering read countries
    # continent = "AF"  # Africa
    # continent = "AN"  # Antarctica
    # continent = "AS"  # Asia
    # continent = "EU"  # Europe
    # continent = "NA"  # North America
    # continent = "OC"  # Oceania
    # continent = "SA"  # South America
    # continent = "UE"  # European Union
    if continent == "UE":
        covid.setCountryEU()
    else:
        covid.setContinent(continent)

    # Filtering Poland and Spain (iso2)
    # covid.setIso2list(["PL", "ES"])

    # Enter the last available data in db
    covid_api = covid.getValueDb()
    if len(covid_api) == 0:
        date_filter = covid.getStopDateDb()
        covid.setDateTop(date_filter)
        covid.setDateEnd(date_filter)

    # Print report 1
    s1 = 35
    s2 = 15
    s3 = 98
    print("-" * s3)
    print(("Country (covid-19) in " + covid.getDateEnd()).ljust(s1 + 3) + "Confirmed".rjust(s2) + "Deaths".rjust(s2)
          + "Recovered".rjust(s2) + "Active".rjust(s2))
    print("-" * s3)
    for country in covid.getCountriesDb():
        covid.setCountry(country[0])
        covid_api = covid.getValueDb()
        for key in covid_api:
            line = covid.getCountry().upper().rjust(s1) + " : "
            for i in range(4):
                line += str(covid_api[key][i]).rjust(s2)
            print(line)
    print("-" * s3)
    print("Date range in db: " + covid.getStartDateDb() + " -> " + covid.getStopDateDb())
    print()

    # Print report 2
    s1 = 10
    s2 = 15
    s3 = 70
    covid.setIso2list([])  # Clear filtering continent
    covid.setDateTop("2021-04-01")
    covid.setDateEnd("2021-04-16")
    covid.setCountry("poland")
    data = covid.getValueDb()
    print("Covid-19 in " + covid.getCountry().upper() + ", " + covid.getDateTop() + " -> " + covid.getDateEnd())
    print("-" * s3)
    print("Date".ljust(s1) + "Confirmed".rjust(s2) + "Deaths".rjust(s2) + "Recovered".rjust(s2) + "Active".rjust(s2))
    print("-" * s3)
    for key in data:
        line = key
        for i in range(4):
            line += str(data[key][i]).rjust(s2)
        print(line)
    print("-" * s3)
    print()
