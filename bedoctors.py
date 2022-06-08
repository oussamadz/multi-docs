#!/usr/bin/python3

import pandas as pd 
import requests as rq
from bs4 import BeautifulSoup as bs
import xlsxwriter
import threading 
import queue
que = queue.Queue()
page = 0
url = "https://www.bedoctors.be/en/do/search?lang=en&latitude=&longitude=&key=&specialtyID=&cityZip=&page="
name = []
profession = []
address = []
zipp = []
city = []
phone = []

def work(nm1, qu):
    while not qu.empty():
        global name
        global profession
        global address
        global zipp
        global city
        global phone 
        link = qu.get()
        print(f"{nm1} : {link}")
        doc = rq.get(link)
        doc = bs(doc.text, 'html.parser')
        docl = doc.find('ul', class_="listing")
        lis = docl.find_all('li')
        nam = prof = add = phon = "N/A"
        for li in lis:
            if li.find('h2') != None:
                nam = li.text
                name.append(nam)
                continue
            if li.find('i', class_='fa-user-md') != None:
                prof = li.text
                profession.append(prof)
                continue
            if li.find('i', class_='fa-map-marker-alt') != None:
                add = li.text
                address.append(add)
                try:
                    zipp.append(add.split(',')[1].partition(' ')[2].split(' ')[0])
                    city.append(add.split(',')[1].partition(' ')[2].partition(' ')[2])
                except:
                    zipp.append("N/A")
                    city.append("N/A")
                continue
            if li.find('i', class_='fa-phone-volume') != None and 'Call' not in li.text and "--- -- -- --" not in li.text:
                phon = li.text
                phone.append(phon)
                continue

while page <=6149: #6149
    pg = rq.get(url + str(page))
    sp = bs(pg.text, 'html.parser')
    container = sp.find('table', id="listings")
    items = container.find_all('a')
    for it in items:
        link = "https://www.bedoctors.be" + it.get('href')
        que.put(link)
    threads = []
    for i in range(3): #thread count
        nm = f"thread_0{i+1}"
        t = threading.Thread(target=work, name=nm, args=(nm, que))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    page+=1
    print(f"page: {page}/6150")

df = pd.DataFrame({'name':name, "profession": profession, "address": address, "zip": zipp, "city": city, "phone": phone})
writer = pd.ExcelWriter("bgdocs.xlsx", engine="xlsxwriter")
df.to_excel(writer, sheet_name="sheet_1")
writer.save()
writer.close()
