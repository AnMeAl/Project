import requests
from bs4 import BeautifulSoup

response = requests.get("https://realty.yandex.ru/moskva/snyat/kvartira/odnokomnatnaya/")
soup = BeautifulSoup(response.text, 'html.parser')
ol_elems = soup.find('ol', class_='OffersSerp__list')

flats = []
for li in ol_elems.find_all('li'):
    div = li.find('div')
    flat = {}
    main = div.find('div', class_='OffersSerpItem__generalInfo').find('div', class_='OffersSerpItem__generalInfoInnerContainer')
    main_data = main.find('a').find('span').find('span').get_text(strip=True).encode('latin1').decode('utf-8')
    flat['Площадь'] = float(main_data[:2])
    flat['Количество комнат'] = int(main_data[8])
    flat['Этаж'] = int(main_data[31:33])
    flat['Количество этажей в доме'] = int(main_data[42:44])
    
    price = div.find('div', class_='OfferPriceLabel__priceWithTrend--1_AZI').find('div').find('span')
    price = price.get_text().encode('latin1').decode('utf-8').replace('\xa0', '')
    flat['Цена'] = float(price)
    
    location_metro = main.find('div', class_='OffersSerpItem__location').find('div', class_='SnippetLocation__container--33SEA OfferSnippetLocation__locationContainer--3G-NW')
    metro = location_metro.find('div', class_='MetroWithTime').find('span').find('span', class_='MetroWithTime__metro').find('span', class_='MetroWithTime__title').find('span')
    flat['Метро'] = metro.get_text(strip=True).encode('latin1').decode('utf-8')
    
    location = main.find('div', class_='OffersSerpItem__location').find('div', class_='OfferSnippetLocation__address--3vqLw')
    #adress = location.find('a').get_text(strip=True).encode('latin1').decode('utf-8')
    flat['Адрес'] = location.get_text(strip=True).encode("latin1").decode("utf-8")
    
    description = div.find('div', class_='OffersSerpItem__generalInfo').find('p', class_='OffersSerpItem__description')
    flat['Описание'] = description.get_text(strip=True).encode('latin1').decode('utf-8')

    flats.append(flat)