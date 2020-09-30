import io
from io import BytesIO
try:
    import pandas as pd
except:
    print("No library named pandas, please run \"pip install pandas\" in your command line")
    quit()
try:
    import numpy as np
except:
    print("No library named numpy, please run \"pip install numpy\" in your command line")
    quit()
try:
    from googletrans import Translator
except:
    print("No library named googletrans, please run \"pip install googletrans\" in your command line")
    quit()
import http.server
import socketserver
import urllib.parse as urlparse
from urllib.request import urlopen
import numpy as np
import json
from pytz import timezone   
from datetime import datetime
translator = Translator()

#class defines object containing city bunch from a given file
class GeoData(object):
    
#class constructor
    def __init__(self, file_name, encoding):
        f = io.open(file_name, "r", encoding = encoding)
        text = f.read()
        data = bytes(text, encoding)
        df = pd.read_csv(BytesIO(data), encoding=encoding, delimiter="\t", header=None, dtype = str)
        df.columns = ["geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude", "feature class", "feature code",
                       "country code", "cc2", "admin1 code", "admin2 code", "admin3 code", "admin4 code", "population", "elevation",
                       "dem", "timezone", "modification date"]
        self.df = df.loc[df['feature class'] == 'P'] #choosing only cities, villages as stated in the task
        self.df = df
        self.num = len(df)
        self.names = np.array(df['asciiname'], dtype = str)
        return
    
#method finds city information from geonameid
    def find_by_id(self, id):
        df = self.df
        row = df.loc[df['geonameid'] == id]
        return row.to_json(orient = 'records')
    
#method finds city information from name
    def find_by_name(self, name):
        df = self.df
        row = df.loc[df['asciiname'] == name]
        return row.to_json(orient = 'records')
     
#method finds list of cities from given page number
    def find_by_page(self, num_cities, num_page):
        index = int(num_cities * (num_page - 1))
        return self.df[index:index+num_cities].to_json(orient = 'records')

#choose city with most population
    def choose_one(self, df):
        index = np.argmax(np.array(df['population'], dtype = int), axis = 0)
        return df[index:index+1]
    
#method returns all cities starting with given string
    def helper(self, name_part):
        #arr = np.array(self.df['names'], dtype = str)
        res = list()
        for city in self.names:
            if(city.find(name_part) != -1):
                res.append(city)
        return json.dumps(res)

#method finds difference between two given timezones
    def tz_diff(self, tz1, tz2):
        date = datetime.now()
        date = pd.to_datetime(date)
        return (max(tz1.localize(date),tz2.localize(date).astimezone(tz1)) - 
            min(tz1.localize(date),tz2.localize(date).astimezone(tz1)))\
            .seconds/3600  

#method finds 2 cities by russian name
    def find_by_ru_name(self, name_1, name_2):
        n1_en = translator.translate(name_1, dest="en").text
        n2_en = translator.translate(name_2, dest="en").text
        if (n1_en == "Snt. Petersburg"):
            n1_en = "Saint Petersburg"
        if (n2_en == "Snt. Petersburg"):
            n2_en = "Saint Petersburg"
        #print(choosen2_en)
        row_1 = self.choose_one(self.df.loc[self.df['asciiname'] == n1_en])
        row_2 = self.choose_one(self.df.loc[self.df['asciiname'] == n2_en])
        #l = np.argmax(np.array([row_1['latitude'][0], row_2['latitude'][0]], dtype = float))
        d = dict()
        if float(row_1['latitude']) > float(row_2['latitude']):
            d['northernmost'] = name_1
        else:
            d['northernmost'] = name_2
        d['timezones_equal'] = np.array(row_1['timezone'])[0] == np.array(row_2['timezone'])[0]
        t1 = np.array(row_1['timezone'])[0]
        t2 = np.array(row_2['timezone'])[0]
        d['timezone_diff'] = self.tz_diff(timezone(t1), timezone(t2))
        res = dict()
        cities = pd.concat([row_1,row_2], axis = 0)
        res['cities data'] = cities.to_json(orient = 'records')
        res['compare'] = d
        return json.dumps(res)

PORT = 8000
df1 = GeoData("RU.txt", "utf-8")

#handler for server requests
class MyHandler(http.server.BaseHTTPRequestHandler):
    
    def get_info(self,id):
        return df1.find_by_id(id)
    
    def get_page(self, num_cities, num_page):
        return df1.find_by_page(num_cities, num_page)
    
    def get_info_rus(self, name_1, name_2):
        return df1.find_by_ru_name(name_1, name_2)

    def helper(self, name_part):
        return df1.helper(name_part)
    
    def do_GET(self):
        print("Just received a GET request")
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        qs = {}
        path = self.path
        if '?' in path:
            path, tmp = path.split('?', 1)
            qs = urlparse.parse_qs(tmp)
        print(path, qs)
        out = 'Unknown request'
        if (path == '/get_info'):
            try:
                out = self.get_info(qs['id'][0])
            except:
                out = "Wrong request"
        if (path == '/get_page'):
            try:
                out = self.get_page(int(qs['num_cities'][0]), int(qs['num_page'][0]))
            except:
                out = "Wrong request"
        if (path == '/get_info_rus'):
            try:
                out = self.get_info_rus(qs['name_1'][0], qs['name_2'][0])
            except:
                out = "Wrong request"
        if (path == '/helper'):
            try:
                out = self.helper(qs['name_part'][0])
            except:
                out = "Wrong request"
        self.wfile.write(out.encode())
        return  

Handler = MyHandler
  
    
if __name__ =="__main__":
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print("serving at port", PORT)
        httpd.serve_forever()

