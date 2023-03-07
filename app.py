from datetime import datetime
from flask import Flask, request, render_template
from flask import Flask
import pymssql
import redis
import pandas as pd
import math
from geopy.distance import geodesic
from time import time
app = Flask(__name__)

server='assmt-3.database.windows.net'
db='assignment-3'
user='rootsql'
password='Salla@2022@'
connection = pymssql.connect(server,user,password,"assignment-3")
cursor = connection.cursor()
redis_name='redis-3.redis.cache.windows.net'
redis_password='6QUpz6Rh4BpSpHiKOt5xj8AJg8WsEG7YgAzCaGUYm0w='
redis_server=redis.StrictRedis(host=redis_name,port=6380,password=redis_password, ssl=True)

#THIS FUNCION WILL BE CALLED WHEN REDIS CAHCE IS BEING USED AND FOR FIRST TIME QUERY WILL HIT THE SQL DATABASE AND AFTER THAT GET DATA FROM REDIS CAHCE ON CLOUD
def query1(input2):
    print(input2)
    i = 25
    tic = time()
    while i != 0:
        if redis_server.exists(input2) == 1:
            res = redis_server.get(input2)#.decode("utf-8") #STORING THE DATA USING SAME KEY AS INPUT IN REDIS CACHE
            data = eval(res)
            print(data)
            i-=1
        else:
            sql1 = f"SELECT TOP {input2} * FROM earthquake ORDER BY mag DESC"
            cursor.execute(sql1)
            data = cursor.fetchall()
            redis_server.set(input2,str(data)) #SETTING THE VALUE AND KEY IN REDIS CACHE
            i-=1
    toc = time()
    qtime = toc-tic
    print(qtime)
    return data,qtime

#THIS FUNCTION WILL BE CALLED WHEN DIRECTLLY CALLING THE AZURE SQL DATABASE
def old_query(input1):
    i = 25
    tic = time()
    print(i)
    while i!= 0:
        sql1 = f"SELECT TOP {input1} * FROM datas ORDER BY mag DESC"
        cursor.execute(sql1)
        data = cursor.fetchall()
        i-=1
        print(i)
    toc = time()
    qtime = toc- tic
    print(qtime)
    return data, qtime

def query2(date_from,date_to):
    date_query = date_from+date_to
    # ptint()
    print(date_query)
    if redis_server.exists(date_query) == 1:
        print("If loop date")
        tic = time()
        res = redis_server.get(date_query)
        toc = time()
        data = eval(res)
        print(data)
        qtime = toc-tic
        return data,qtime
    else:
        print("Else loop date")
        tic = time()
        sql2 = f"SELECT * from datas where time between '{date_from}' and '{date_to}' and mag > 3.0"
        cursor.execute(sql2)
        toc = time()
        data = cursor.fetchall()
        print(data)
        qtime = toc - tic
        redis_server.set(date_query,str(data))
        return data,qtime


@app.route('/', methods=['GET','POST'])
def index():
    data = None
    qtime = 0
    if request.method == "POST":
        # print(float(request.form['to_mag']))
        # date_from=request.form['date_from']
        # date_to= request.form['date_to']
        # data, qtime = query2(date_from,date_to)
        # print(data.)
        # print(date_from,date_to)
        # input1=request.form['input1']
        # input2=request.form['input2']
        from_mag=float(request.form['from_mag'])
        to_mag=float(request.form['to_mag'])
        print(to_mag,from_mag)
        # if input1:
        #     data, qtime = old_query(input1)
        # elif input2:
        #     data, qtime = query1(input2)
        
            # tic = time()
            # sql2 = f"SELECT * from earthquake where time between '{date_from}' and '{date_to}' and mag > 3.0"
            # c1.execute(sql2)
            # toc = time()
            # data = c1.fetchall()
            # qtime = toc - tic
        if request.form.get('action1') == 'getData':
            tic = time()
            sql3 = f"select count(*) from earthquake"
            sql4 = f"select top 1 place from earthquake order by mag desc"
            cursor.execute(sql3)
            data1 = cursor.fetchall()
            cursor.execute(sql4)
            toc = time()
            data2 = cursor.fetchall()
            data = data1 + data2
            qtime = toc - tic
        elif from_mag and to_mag:
            mag=from_mag+to_mag
            if redis_server.exists(mag) == 1:
                print("If loop date")
                tic = time()
                res = redis_server.get(mag)
                toc = time()
                data = eval(res)
                print(data)
                qtime = toc-tic
                return data,qtime 
            else:
                print(from_mag)
                from_mag = float(request.form['from_mag'])
                to_mag = float(request.form['to_mag'])
                place = request.form['place']
                start = from_mag
                data = []
                tic = time()
                sql10 = f"SELECT time,latitude,longitude,mag,place FROM datas WHERE MAG BETWEEN {from_mag} AND {to_mag} AND place LIKE '%{place}%'"
                cursor.execute(sql10)
                toc = time()
                qtime = toc - tic
                data = cursor.fetchall()
        elif request.form.get('lat') and request.form.get('long'):
            lat = float(request.form['lat'])
            long = float(request.form['long'])
            result = []
            magnitude = []
            dest = None
            tic = time()
            sql6 = f"SELECT latitude, longitude, place, mag FROM earthquake where time > '2022-02-09' AND time < '2022-02-24'"
            cursor.execute(sql6)
            toc = time()
            data = cursor.fetchall()
            start = cursor.fetchall()
            for n in start:
                dist = geodesic(dest, n[:2]).kilometers
                print(dist)
                if dist<500:
                    result.append((n[0],n[1],n[2],n[3],dist))
                    magnitude.append((n[3]))
                mag = max(magnitude)
                for m in result:
                    if m[3] == mag:
                        data.append((m[0],m[1],m[2],m[3],m[4]))
            qtime = toc - tic
        print(211)
        print(data,qtime)
    return render_template("index.html",data=data,qtime=qtime)

if __name__ == '__main__':
    app.config['SESSION_TYPE'] = 'filesystem'
    app.run()