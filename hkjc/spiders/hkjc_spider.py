import scrapy
import re
import csv
import logging
from scrapy_splash import SplashRequest
from scrapy.loader import ItemLoader

import hkjc.items as items

script = """
function main(splash)
  splash:init_cookies(splash.args.cookies)
  assert(splash:go{
    splash.args.url,
    headers=splash.args.headers,
    http_method=splash.args.http_method,
    body=splash.args.body,
    })
  assert(splash:wait(10))

  local entries = splash:history()
  local last_response = entries[#entries].response
  return {
    url = splash:url(),
    headers = last_response.headers,
    http_status = last_response.status,
    cookies = splash:get_cookies(),
    html = splash:html(),
  }
end
"""

class hkjc_race_spider(scrapy.Spider):
    name = "hkjc-race"
    allowed_domains = ["racing.hkjc.com"]   

    def start_requests(self):

        start_url = []
        with open("./hkjc/hkjc_racing_fixtures.csv", mode="r") as csvfile:
            race_fixtures = csv.reader(csvfile)
            next(race_fixtures, None)
            for fixture in race_fixtures:
                url = "https://racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx?RaceDate="+fixture[0]
                start_url.append(url)
        
        for url in start_url:
            yield SplashRequest(url, self.parse_links,
                endpoint='execute',
                cache_args=['lua_source'],
                args={'lua_source': script},
                headers={'X-My-Header': 'value'},
            )

    def parse_links(self, response):
        race_links = response.xpath('//div[@class="f_clear top_races"]/table/tbody/tr/td/a[not(@class)]//@href').extract()
        race_links.append(response.url)
        for link in set(race_links):
            url = response.urljoin(link)
            yield SplashRequest(url, self.parse_race,
                endpoint='execute',
                cache_args=['lua_source'],
                args={'lua_source': script},
                headers={'X-My-Header': 'value'},
                dont_filter=True
            )
    
    def parse_race(self, response):
        race_meeting = response.xpath('//span[@class="f_fl f_fs13"]/text()').extract()
        if race_meeting:  
            race_head = [x.strip() for x in race_meeting[0].split('\xa0')]

            race_info = response.xpath('//div[@class="race_tab"]//td/text()').extract()

            for row in response.xpath('//div[@class="performance"]/table/tbody//tr'):
                r = ItemLoader(item=items.raceItem(), response=response)
                
                r.add_value('date', race_head[1])
                r.add_value('venue', race_head[3])
                r.add_value('race_no', race_info[0].split(' ')[1])
                r.add_value('race_id', re.search(r"\((.*?)\)", race_info[0]).group(1))
                r.add_value('race_type', race_info[1])
                r.add_value('name', race_info[4])
                r.add_value('prize', race_info[7])
                r.add_value('going', race_info[3])
                r.add_value('course', race_info[6])
                r.add_value('placing', ' '.join(row.xpath('td[1]//text()').extract()).strip())
                r.add_value('horse_no', ' '.join(row.xpath('td[2]//text()').extract()).strip())
                r.add_value('horse', ' '.join(row.xpath('td[3]//text()').extract()).strip())
                r.add_value('horse_url', ' '.join(row.xpath('td[3]/a/@href').extract()).strip())
                r.add_value('jockey', ' '.join(row.xpath('td[4]//text()').extract()).strip())
                r.add_value('trainer', ' '.join(row.xpath('td[5]//text()').extract()).strip())
                r.add_value('actual_wt', ' '.join(row.xpath('td[6]//text()').extract()).strip())
                r.add_value('declared_wt', ' '.join(row.xpath('td[7]//text()').extract()).strip())
                r.add_value('draw', ' '.join(row.xpath('td[8]//text()').extract()).strip())
                r.add_value('lbw', ' '.join(row.xpath('td[9]//text()').extract()).strip())
                r.add_value('running_position', row.xpath('td[10]//text()').re(r'\d+'))
                r.add_value('finish_time', ' '.join(row.xpath('td[11]//text()').extract()).strip())
                r.add_value('win_odds', ' '.join(row.xpath('td[12]//text()').extract()).strip())

                yield r.load_item()
            
            pool = ""

            for row in response.xpath('//div[@class="dividend_tab f_clear"]/table/tbody//tr'):
                if row.xpath('td[2]/a/@href').extract():
                    continue
                
                d = ItemLoader(item=items.dividendItem(), response=response)
                
                d.add_value('date', race_head[1])
                d.add_value('venue', race_head[3])
                d.add_value('race_no', race_info[0].split(' ')[1])
                d.add_value('race_id', re.search(r"\((.*?)\)", race_info[0]).group(1))
                
                row_len = len(row.xpath('td//text()').extract())
                if row_len >= 3:
                    pool = ' '.join(row.xpath('td[1]//text()').extract()).strip()
                    d.add_value('pool', pool)
                    d.add_value('winning_combo', ' '.join(row.xpath('td[2]//text()').extract()).strip())
                    d.add_value('dividend', ' '.join(row.xpath('td[3]//text()').extract()).strip())
                else:
                    d.add_value('pool', pool)
                    d.add_value('winning_combo', ' '.join(row.xpath('td[1]//text()').extract()).strip())
                    d.add_value('dividend', ' '.join(row.xpath('td[2]//text()').extract()).strip())

                yield d.load_item()
        else:
            logging.info("No information for url %s", response.url)

class hkjc_sectional_spider(scrapy.Spider):
    name = "hkjc-sectional"
    allowed_domains = ["racing.hkjc.com"]    

    def start_requests(self):

        start_url = []
        with open("./hkjc_racing_fixtures.csv", mode="r") as csvfile:
            
            race_fixtures = csv.reader(csvfile)
            next(race_fixtures, None)
            for fixture in race_fixtures:
                url = "https://racing.hkjc.com/racing/information/English/Racing/DisplaySectionalTime.aspx?RaceDate="+fixture[0]
                start_url.append(url)

        race_fixtures = [['03/09/2016','ST'],['07/09/2016','HV']]

        for fixture in race_fixtures:
            url = "https://racing.hkjc.com/racing/information/English/Racing/DisplaySectionalTime.aspx?RaceDate="+fixture[0]
            start_url.append(url)

        for url in start_url:
            yield SplashRequest(url, self.parse_links,
                endpoint='execute',
                cache_args=['lua_source'],
                args={'lua_source': script},
                headers={'X-My-Header': 'value'},
            )

    def parse_links(self, response):
        race_links = response.xpath('//div[@class="commTitlePic f_clear"]/table/tbody/tr/td[2]/table/tbody/tr/td[@style]/a/@href').extract()
        race_links.append(response.url)
        for link in set(race_links):
            url = response.urljoin(link)
            yield SplashRequest(url, self.parse_sectional,
                endpoint='execute',
                cache_args=['lua_source'],
                args={'lua_source': script},
                headers={'X-My-Header': 'value'},
                dont_filter=True
            )
    
    def parse_sectional(self, response):
        race_meeting = response.xpath('//div[@class="search"]/p/span[@class="f_fl"]//text()').extract()

        if race_meeting:    
            race_head = re.split(', |:',race_meeting[0])
            race_no = response.xpath('//body/div/div/div/p[1]/text()').re(r'\d+')
            race_time = response.xpath('//div[@class="Race f_clear"]/table/tbody/tr[1]//text()').extract()
            sectional_time = response.xpath('//div[@class="Race f_clear"]/table/tbody/tr[2]//text()').extract()
            
            for row in response.xpath('//table[@class="table_bd f_tac race_table"]/tbody/tr'):

                s = ItemLoader(item=items.sectionalItem(), response=response)
                
                s.add_value('date', race_head[1].strip())
                s.add_value('venue', race_head[2].strip())
                s.add_value('race_no', ' '.join(race_no).strip())
                s.add_value('race_time', [i for i in race_time if re.search(r'\d', i)])
                s.add_value('sectional_time', [i for i in sectional_time if re.search(r'\d', i)])

                s.add_value('finishing_order', ' '.join(row.xpath('td[1]//text()').extract()).strip())
                s.add_value('horse_no', ' '.join(row.xpath('td[2]//text()').extract()).strip())
                s.add_value('horse', ' '.join(row.xpath('td[3]//text()').extract()).strip())
                
                for i in range(1,7):
                    td_pos = "td["+str(i+3)+"]"
                    s.add_value("sec"+str(i)+"_position", ' '.join(row.xpath(td_pos+'/p/span//text()').extract()).strip())
                    s.add_value("sec"+str(i)+"_lbw", ' '.join(row.xpath(td_pos+'/p/i//text()').extract()).strip())
                    s.add_value("sec"+str(i)+"_time", ' '.join(row.xpath(td_pos+'/p[2]//text()').extract()).strip())

                s.add_value('finish_time', ' '.join(row.xpath('td[10]//text()').extract()).strip())
                

                yield s.load_item()
        else:
            logging.info("No information for url %s", response.url)

class hkjc_horse_spider(scrapy.Spider):
    name = "hkjc-horse"
    allowed_domains = ["racing.hkjc.com"]    

    def start_requests(self):

        horse_url = []

        with open("./hkjc-races2.csv", mode="r") as csvfile:         
            races = csv.reader(csvfile)
            next(races, None)
            for race in races:
                if len(race[12].strip()) > 10:
                    url = "https://racing.hkjc.com"+race[12].strip()+"&Option=1"
                    horse_url.append(url)

        start_url = set(horse_url)

        ## rerun selection of failed cases due to scrapy timeout
        # with open("./horse_rerun.csv", mode="r") as csvfile:
        #     horses = csv.reader(csvfile)
        #     for horse in horses:
        #         start_url.append(horse[0])

        for url in start_url:
            yield SplashRequest(url, self.parse_horse,
                endpoint='execute',
                cache_args=['lua_source'],
                args={'lua_source': script},
                headers={'X-My-Header': 'value'},
            )
    
    def parse_horse(self, response):
        horse_heading = response.xpath('//span[@class="title_text"]//text()').extract()

        if horse_heading:    
            retired_match = re.search(r' \(Retired\)', horse_heading[0])
            if retired_match:
                retired = '1'
                horse_name = horse_heading[0][:retired_match.span()[0]]
            else:
                retired = '0'
                horse_name = horse_heading[0]
            
            hi = ItemLoader(item=items.horseInfoItem(), response=response)
                
            id_match = re.search(r'HorseId=', response.url).span()[1]
            horse_id = response.url[id_match:id_match+12]    
            hi.add_value('horse_id', horse_id)
            hi.add_value('horse', horse_name)
            hi.add_value('retired', retired)

            info_table = response.xpath('//table[@class="horseProfile"]/tbody/tr')
            
            url_dict = {'Rating/Wt/Placing':'rating_url', 'Performance by Distance':'distance_url',
                        'Trackwork Records':'trackwork_url', 'Veterinary Records':'veterinary_url',
                        'Movement Records':'movement_url', 'Overseas formrecords':'overseas_url',
                        'Pedigree':'pedigree_url'}
            
            for bullet in info_table.xpath('td[1]/table/tbody/tr[2]/td[2]/ul//li/a'):
                if bullet.xpath('text()').extract()[0] in url_dict:
                    hi.add_value(url_dict[bullet.xpath('text()').extract()[0]], ' '.join(bullet.xpath('@href').extract()).strip())
            
            info_dict = {'Country of Origin / Age':'country_origin', 'Country of Origin':'country_origin', 'Colour / Sex':'colour_sex',
                        'Import Type':'import_type', 'Total Stakes*': 'total_stakes',
                        'No. of 1-2-3-Starts*':'total_123_starts', 'Current Stable Location':'current_stable',
                        'Trainer':'trainer', 'Owner':'owner', 'Current Rating':'last_rating', 'Last Rating': 'last_rating',
                        'Sire':'sire', 'Dam':'dam', 'Dam\'s Sire':'dams_sire'}
            
            for table in info_table.xpath('td/table/tbody'):
                if not table.xpath('tr[1]/td[@class="subsubheader"]'):
                    for row in table.xpath('tr'):
                        if row.xpath('td[1]/text()').extract()[0] == "Same Sire":
                            same_sire = [i for i in row.xpath('td[3]//text()').extract() if not re.search(r'\n', i)]
                            hi.add_value('same_sire', ', '.join(same_sire).strip())
                        elif row.xpath('td[1]/text()').extract()[0] in info_dict:
                            hi.add_value(info_dict[row.xpath('td[1]//text()').extract()[0]], ' '.join(row.xpath('td[3]//text()').extract()).strip())

            yield hi.load_item()
            
            season = ''

            for row in response.xpath('//table[@class="bigborder"]/tbody/tr'):
                if row.xpath('td[@class="hsubheader"]'):
                    continue
                
                if row.xpath('td[@colspan="19"]'):
                    season = ' '.join(row.xpath('td/span//text()').extract()).strip()
                    continue
                
                hf = ItemLoader(item=items.horseFormItem(), response=response)
                
                hf.add_value('horse_id', horse_id)
                hf.add_value('horse', horse_name)

                hf.add_value('season', season)
                hf.add_value('race_id', ' '.join(row.xpath('td[1]/a//text()').extract()).strip())
                hf.add_value('race_url', ' '.join(row.xpath('td[1]/a/@href').extract()).strip())
                hf.add_value('placing', ' '.join(row.xpath('td[2]//text()').extract()).strip())
                hf.add_value('date', ' '.join(row.xpath('td[3]//text()').extract()).strip())
                hf.add_value('course', ' '.join(row.xpath('td[4]//text()').extract()).strip())
                hf.add_value('distance', ' '.join(row.xpath('td[5]//text()').extract()).strip())
                hf.add_value('going', ' '.join(row.xpath('td[6]//text()').extract()).strip())
                hf.add_value('race_class', ' '.join(row.xpath('td[7]//text()').extract()).strip())
                hf.add_value('draw', ' '.join(row.xpath('td[8]//text()').extract()).strip())
                hf.add_value('rating', ' '.join(row.xpath('td[9]//text()').extract()).strip())
                hf.add_value('trainer', ' '.join(row.xpath('td[10]//text()').extract()).strip())
                hf.add_value('jockey', ' '.join(row.xpath('td[11]//text()').extract()).strip())
                hf.add_value('lbw', ' '.join(row.xpath('td[12]//text()').extract()).strip())
                hf.add_value('win_odds', ' '.join(row.xpath('td[13]//text()').extract()).strip())
                hf.add_value('actual_wt', ' '.join(row.xpath('td[14]//text()').extract()).strip())
                hf.add_value('running_position', ' '.join(row.xpath('td[15]//text()').extract()).strip())
                hf.add_value('finish_time', ' '.join(row.xpath('td[16]//text()').extract()).strip())
                hf.add_value('declared_wt', ' '.join(row.xpath('td[17]//text()').extract()).strip())
                hf.add_value('gear', ' '.join(row.xpath('td[18]//text()').extract()).strip())

                yield hf.load_item()
        
        else:
            logging.info("No information for url %s", response.url)