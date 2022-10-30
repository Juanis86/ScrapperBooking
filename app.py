from functools import reduce
import time
import re
import difflib
import pandas as pd
import urllib3 as urllib
from bs4 import BeautifulSoup
from selenium import webdriver 
from selenium.webdriver.common.by import By
import sys
import pathlib

path = (pathlib.Path(__file__).parent.absolute()).parent
options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
driver = webdriver.Chrome(options=options, executable_path=f'{path}/comp/chromedriver')




def get_hotels_links(region):
    url='https://www.booking.com'
    driver.get(url)
    time.sleep(3)
    links=[]
    driver.find_element(by=By.XPATH, value='//input[@class="ce45093752"]').send_keys(region)
    time.sleep(1)
    driver.find_element(by=By.XPATH, value='//button[@type="submit"]').click()
    n=1
    while n>0:
        time.sleep(1.5)
        try:           
            block= driver.find_elements_by_xpath('//a[@data-testid="title-link"]')
            for b in block:
                link= b.get_attribute('href')  
                name= link.split('/')[5].split('.')[0]
                country=link.split('/')[4]
                type_= link.split('/')[3]
                links.append({'type': type_,'name': name,'country':country, 'link':link})
                print(name)
            n=n+1
            print(n)
            driver.find_element(by=By.XPATH, value=f'//button[@aria-label=" {n}"]').click()
        except:
            n=0
    db= pd.DataFrame(links)
    db.to_csv(f'{path.parent.parent}/datasets/DB_links_hotels_{region}.csv')
    return(db)


def chunk_data(iterable_data, len_of_chunk):
    """ 
    Se divide la data en partes para poder trabajarla
    arg: iterable: lista de datos obtenida
         len_of_chunk: cantidad de partes en las que se dividira la lista
    retunr: Retorna la lista dividida en partes
    """
    try:
        for i in range(0, len(iterable_data), len_of_chunk):
            yield iterable_data[i:i +len_of_chunk]

    except:
        print('error')

def get_reviews(data):
    url=data['rev_link_template']
    start_time = time.time()
    pag_rev=[]
    req = urllib.request.Request(url[0], headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'})
    raw=urllib.request.urlopen(req).read().decode("utf-8")
    soup= BeautifulSoup(raw,'lxml')
    blocks= raw.split('div class="c-review-block"')
    for r in blocks:
        name_html= re.findall('<span class="bui-avatar-block__title">[\w]*</span>', r)
        names=[(x.split('>')[1]).split('<')[0] for x in name_html]
        country_html=re.findall('</span>[\w]*</span>', r)
        countries=[(x.split('>')[1]).split('<')[0] for x in country_html]
        t_type_html=re.findall('<div class="bui-list__body">\n\
    [\w]*\n\
    </div>', r)
        t_type=[(x.split('>')[1]).split('<')[0].replace('\n', '') for x in t_type_html]
        period_html= re.findall('<span class="c-review-block__date">\n\
    [\w]* [0-9]*\n\
    </span>', r)
        period=[(x.split('>')[1]).split('<')[0].replace('\n', '') for x in period_html]
        nights= re.findall('[0-9]* night',r)
        score_html=re.findall('<div class="bui-review-score__badge" aria-label="Scored  "> [0-9]* </div>',r)
        score=[(x.split('> ')[1]).split(' <')[0].replace('\n', '') for x in score_html]
        review=soup.find('span', class_="c-review__body")
        pag_rev.append({'hotel':data['hotel'], 'name': names,'country': countries, 't_type': t_type, 'period': period, 'nights': nights, 'score': score, 'review': review })
        pd.to_csv(f'C:/Users/Jsolchaga86/Desktop/Nomade/Hotels_app/database/review')
        print(f'Done: {time.time() - start_time}')
    return pag_rev
 


def get_reviews_onepage(driver, hotel):
    try:
        name=  (driver.find_element(by=By.XPATH, value='.//span[@class="bui-avatar-block__title"]').text) 
    except:
        name= 'NULL'
    try:
        country=(driver.find_element(by=By.XPATH, value='.//span[@class="bui-avatar-block__subtitle"]').text)
    except:
        country= 'NULL'
    try:
        room_type=(driver.find_element(by=By.XPATH, value='.//a[@class="c-review-block__room-link"]').text)
    except:
        room_type= 'NULL'
    try:
        nights=(driver.find_element(by=By.XPATH, value='.//div[@class="bui-list__body"]').text)
    except:
        nights= 'NULL'
    try:
        period= (driver.find_element(by=By.XPATH, value='.//span[@class="c-review-block__date"]').text)
    except:
        period= 'NULL'
    try:
        t_type=(driver.find_element(by=By.XPATH, value='.//div[@class="bui-list__body"]').text)
    except:
        t_type= 'NULL'
    try:
        review_init= driver.find_elements_by_xpath('.//span[@class="c-review__body"]')
    except:
        review_init= 'NULL'
    try:
        score= driver.find_elements_by_xpath('.//div[aria-label="Scored  "]')
    except:
        score= 'NULL'
    try:
        rev_rev_tot=""
        for rev_rev in review_init:
            rev_rev_tot= (rev_rev.text +".  "+rev_rev_tot+". ")
            review= (rev_rev_tot)
    except:
        reviews_user= 'NULL'
    return dict(zip(('hotel', 'name','country','t_type','period','nights','score','review'),
        (hotel, name,country,t_type,period,nights,score,review)))


def get_lis_custom_links(last_number, possition, link):
    link=[list(link), last_number, possition]
    list_of_links= replace_character(link)
    return list_of_links


def replace_character(link):
    link= link['link']
    try:
        new_links=[]
        for i in range(1,int(link)):
            link_f="".join(link[0])
            link= link_f.replace(f'rows=[0-9]*', f'rows={i}0')
            new_links.append(link)
        return new_links
    except:
        return None

def get_reviews_links(link):
        name= link['name']
        country= link['country']
        link= link['link']
        print(f'getting reviews links for {name}')
        start_time = time.time()
        print('paso_1')
        driver.get(link)
        print(link)
        time.sleep(1)
        print('paso_2')
        driver.find_element(by=By.XPATH, value='//a[@rel="reviews"]').click()
        print('paso_3')
        time.sleep(4)    
        try:
            links_rev_pages= driver.find_elements_by_xpath('.//div[@class="bui-pagination__item "]/a')
            print('paso_4')
            print(links_rev_pages)
            link_1=links_rev_pages[0].get_attribute('href')
            print('paso_5')
            link_2=links_rev_pages[1].get_attribute('href')
            print('paso_6')
            print('searching variable region')
            for i,s in enumerate(difflib.ndiff(link_1, link_2)):
                if s[0]==' ': continue
                possition=s
            last_number=links_rev_pages[-1].get_attribute('data-page-number')
            new_links=get_lis_custom_links (last_number, possition, link)
            print(f'Done {time.time() - start_time}')
            return {'country': country, 'name': name, 'page_link': link, 'rev_link_template': new_links, 'variable_possition':possition}
        except:
            return get_reviews_onepage(driver, name)
    
    
def mapper(links_db):
    try:
        list_links=list(map(get_reviews_links, links_db))
        data_hotel=list(map(get_reviews, list_links))
        return data_hotel
    except:
        return

def reducer(maped_data_1, maped_data_2):
    data_f= maped_data_1 + maped_data_2
    return data_f


def main(links_db):
    start_time = time.time()
    print('spliting data')
    chunks=chunk_data(links_db, 50)
    print(f'time: {time.time() - start_time}')
    print('mapping')
    maped_data=list(map(mapper, chunks))
    print(f'time: {time.time() - start_time}')
    f_maped_data=filter(None, maped_data)
    print('reduccing_data')
    result=list(reduce(reducer, f_maped_data))
    print(f'time: {time.time() - start_time}')
    return result



if __name__=='__main__':
    start_time = time.time()
    get_hotels_links('argentina')
    links_db=pd.read_csv(f'{path.parent.parent}/datasets/DB_links_hotels_argentina.csv')
    data_dict = []
    print(links_db)
    for index, row in links_db[['name', 'country', 'link']].iterrows():
        data_dict.append({
                'name': row['name'],
                'country': row['country'],
                'link': row['link'],
                })
    main(data_dict)
    print(time.time() - start_time)

      
