import requests
from bs4 import BeautifulSoup
import regex as re
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import pandas as pd
import os
from tqdm import tqdm
from constant import *
import time

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    , 'accept-language': 'en-US'
}
max_retries = 3
retries = 0


class Transfer:
    def __init__(self):

        self.countries = pd.DataFrame()
        self.saison = pd.DataFrame()
        self.leagues = pd.DataFrame()
        self.club_league = pd.DataFrame()
        self.league_details_saison = pd.DataFrame()
        self.person = pd.DataFrame()
        self.rejected_country_url = pd.DataFrame()
        self.club_season = pd.DataFrame()
        self.manager = pd.DataFrame()
        self.club_details = pd.DataFrame()
        self.club_awards = pd.DataFrame()
        self.scraped_club_url = pd.DataFrame()
        self.scraped_club_at_season_url = pd.DataFrame()

        self.geturls = pd.DataFrame()
        self.geturls2 = pd.DataFrame()
        self.player_trasfor_data = pd.DataFrame()
        self.player_details = pd.DataFrame()
        self.player_details_stats = pd.DataFrame()
        self.player_awards = pd.DataFrame()

        self.list_of_name_database = ['countries', 'saison', 'leagues', 'club_league',
                                      'league_details_saison', 'person', 'rejected_country_url',
                                      'club_season', 'manager', 'club_details', 'club_awards',
                                      'scraped_club_url', 'scraped_club_at_season_url',
                                      'geturls', 'geturls2', 'player_trasfor_data', 'player_details',
                                      'player_details_stats', 'player_awards']

        self.list_of_database = [self.countries, self.saison, self.leagues, self.club_league,
                                 self.league_details_saison, self.person, self.rejected_country_url,
                                 self.club_season, self.manager, self.club_details, self.club_awards,
                                 self.scraped_club_url, self.scraped_club_at_season_url, self.geturls, self.geturls2,
                                 self.player_trasfor_data, self.player_details, self.player_details_stats,
                                 self.player_awards]

        self.base_URL = 'https://www.transfermarkt.com'
        self.path_output = './data'

    def create_output_dir(self):
        os.makedirs(self.path_output)

    def load_csv(self, database):
        if not os.path.exists(self.path_output):
            self.create_output_dir()

        database_name = database + '.csv'
        database_path = os.path.join(self.path_output, database_name)

        if not os.path.exists(database_path):
            columns = globals()['columns_list_of_' + database]
            df = pd.DataFrame(columns=columns)
            df.to_csv(database_path, index=False)
        print(database)
        loaded_data = pd.read_csv(database_path)
        setattr(self, database, loaded_data)

    def save_to_csv(self, database):
        database_name = database + '.csv'
        database_path = os.path.join(self.path_output, database_name)

        df = getattr(self, database)

        df.to_csv(database_path, index=False)

    def scrape_countries_page(self, url):
        max_retries = 3
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(url, headers=HEADERS)
                if response.status_code == 200:

                    sop = BeautifulSoup(response.content, 'html.parser')
                    class_team = sop.find(class_="clearer relevante-wettbewerbe-auflistung")
                    if class_team is not None:
                        name = class_team.find('li').text.strip()
                        if name is not None:
                            id_c = url.split('/')[-1]

                            self.countries.loc[len(self.countries)] = [id_c, name, url]
                            self.save_to_csv('countries')
                            print(f"Added : {name} to countries", end='\r')
                    break

                elif response.status_code == 500:
                    self.rejected_country_url.loc[len(self.rejected_country_url)] = [url]

                    self.save_to_csv('rejected_country_url')


            except requests.exceptions.RequestException:
                retries += 1

        if retries == max_retries:
            print(f"Failed to retrieve data from URL in get countries details : {url}")

    def get_country_id_name_url(self):

        urls = self.create_list_of_countries()

        if len(urls) == 0:
            print('all countries crawled !')
        else:
            print('loading countries :')
            if urls is not None:
                with ThreadPoolExecutor(max_workers=500) as executor:
                    _ = [executor.submit(self.scrape_countries_page, url) for url in
                         tqdm(urls, desc="Scraping countries "
                                         "id name url "
                                         "Progress")]

    def create_list_of_countries(self):
        urls = []
        ### if you want all country uncomment this
        # for i in range(1, 500):
        #     url = self.base_URL + '/wettbewerbe/national/wettbewerbe/' + str(i)
        #     urls.append(url)

        ### just england,farnce,spain,germany,italy
        for i in [189, 40, 75, 50, 157]:
            url = self.base_URL + '/wettbewerbe/national/wettbewerbe/' + str(i)
            urls.append(url)

        parsed_url = self.countries['URL'].to_list()
        rejected_url = self.rejected_country_url['URL'].to_list()
        urls = list(set(urls).difference(parsed_url))
        urls = set(urls) - set(rejected_url)

        return urls

    def create_saison(self):
        for year in range(2015, 2022):
            self.saison.loc[len(self.saison)] = [year]

        self.save_to_csv('saison')

    def scrape_countries_leagues_page(self, url):
        max_retries = 3
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(url, headers=HEADERS)
                if response.status_code == 200:
                    sop = BeautifulSoup(response.content, 'html.parser')

                    table = sop.find('table', class_='items')
                    if table is not None:
                        row = table.find('tbody').find_all('tr')
                        country_id = int(url.split('/')[-5])
                        saison = int(url.split('/')[-3])
                        for idx, r in enumerate(row):
                            if 'Tier' in r.text:

                                list_top_scorer = row[idx + 1].find_all('td')[6].find_all('a')
                                league_name = row[idx + 1].find(class_='hauptlink').text.strip()
                                url_league = \
                                    row[idx + 1].find(class_='hauptlink').find('a').attrs['href'].split('saison_id')[0]
                                url_league = self.base_URL + url_league

                                w_f_g = row[idx + 1].find_all(class_='zentriert')
                                if w_f_g[0].find('a') is not None:
                                    winner = w_f_g[0].find('a').attrs['title']
                                else:
                                    winner = None

                                foreigners = float(w_f_g[1].text.replace('%', '').strip())
                                goal_per_match = float(w_f_g[2].text.strip())
                                if url_league in self.leagues['URL'].values:
                                    league_id = self.leagues[self.leagues['URL'] == url_league]['league_id'].values[0]

                                else:
                                    league_id = len(self.leagues) + 1
                                    self.leagues.loc[len(self.leagues)] = [league_id, url_league, league_name,
                                                                           country_id]
                                    self.save_to_csv('leagues')

                                for scorer in list_top_scorer:
                                    top_scorer = scorer.text
                                    top_scorer_id = int(scorer.attrs['href'].split('/')[-1])
                                    top_scorer_url = scorer.attrs['href']

                                    self.league_details_saison.loc[len(self.league_details_saison)] = [league_id,
                                                                                                       url_league,
                                                                                                       country_id,
                                                                                                       saison,
                                                                                                       winner,
                                                                                                       top_scorer_id,
                                                                                                       foreigners,
                                                                                                       goal_per_match]
                                    self.save_to_csv('league_details_saison')
                                    self.person.loc[len(self.person)] = [top_scorer_id, top_scorer, top_scorer_url]

                                    print(
                                        f"Added {league_name} in {country_id} seison : {saison} and top scorer {top_scorer}",
                                        end='\r')
                                    self.save_to_csv('person')
                    else:
                        print('in this url not found table:' + str(url))

                    break
                else:
                    retries += 1


            except requests.exceptions.RequestException:
                retries += 1

        if retries == max_retries:
            print(f"Failed to retrieve data from URL: {url}")

    def create_list_of_url_countries_leagues(self):
        url_co = self.countries['URL']
        ses = self.saison['saison']
        url_all_season_with_details = [url + '/saison_id/' + str(var) + '/plus/1' for url in url_co for var in ses]

        countri_id = self.league_details_saison['country_id']
        saison_id = self.league_details_saison['saison']
        temp = self.base_URL + '/wettbewerbe/national/wettbewerbe/'
        parsed_url = [temp + str(countri) + '/saison_id/' + str(saison) + '/plus/1' for countri, saison in
                      zip(countri_id, saison_id)]
        url_all_season_with_details = set(url_all_season_with_details) - set(parsed_url)

        return url_all_season_with_details

    def get_feature_of_leagues(self):

        urls = self.create_list_of_url_countries_leagues()
        if len(urls) == 0:
            print('all leagues crawled !')
        else:
            print('loading leagues :')
            if urls is not None:
                with ThreadPoolExecutor(max_workers=500) as executor:
                    _ = [executor.submit(self.scrape_countries_leagues_page, url) for url in
                         tqdm(urls, desc="Scraping feature of leagues Progress")]

    def create_list_of_leagues_clubs(self):
        url_l = self.league_details_saison['URL']
        ses = self.league_details_saison['saison']
        urls = [url + 'plus/?saison_id=' + str(season) for url, season in zip(url_l, ses)]

        parsed_league = self.club_league['league_URL']
        parsed_saison = self.club_league['saison']
        parsed_url = [league + 'plus/?saison_id=' + str(saison) for league in parsed_league for saison
                      in parsed_saison]
        urls = set(urls) - set(parsed_url)

        return urls

    def scrape_clubs_leagues_page(self, url):
        max_retries = 3
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(url, headers=HEADERS)

                sop = BeautifulSoup(response.content, 'html.parser')

                table = sop.find('table', class_='items')
                if table is not None:
                    rows = table.find('tbody').find_all('tr')
                    for idx, row in enumerate(rows):
                        temp = row.find(class_='hauptlink no-border-links')
                        club_name = temp.text
                        club_url = temp.find('a').attrs['href'].split('saison_id')[0]
                        club_url = self.base_URL + club_url

                        club_id = club_url.split('/')[-2]
                        league_url = url.split('plus/?saison_id=')[0]
                        league_name = league_url.split('/')[3]
                        saison = url.split('=')[1]
                        league_id = self.leagues[self.leagues['URL'] == league_url]['league_id'].values[0]
                        if league_id == ' ' or league_id == '':
                            print(league_url)

                        self.club_league.loc[len(self.club_league)] = [league_id, club_id, club_name, league_url,
                                                                       club_url, saison]
                        self.save_to_csv('club_league')
                        print(f"Added {club_name} in {league_name} saison {saison}", end='\r')

                    break

            except requests.exceptions.RequestException:
                retries += 1
                # time.sleep(1)

    def get_clubs_leagues(self):

        urls = self.create_list_of_leagues_clubs()

        if len(urls) == 0:
            print('all clubs in leagues crawled !')
        else:
            print('loading clubs in leagues :')
            if urls is not None:
                with ThreadPoolExecutor(max_workers=500) as executor:
                    _ = [executor.submit(self.scrape_clubs_leagues_page, url) for url in
                         tqdm(urls, desc="Scraping clubs leagues Progress")]

    def get_all_data(self):

        for database in self.list_of_name_database:
            self.load_csv(database)

        if self.saison.empty:
            self.create_saison()

        self.get_country_id_name_url()

        self.get_feature_of_leagues()
        self.get_clubs_leagues()

        self.crawl_club_link()
        self.crawl_club_at_season_link()

        self.call_person()

    def person1(self, urls):

        df = pd.read_csv('./data/leagues.csv')
        leagues = set(df['name'])

        text = urls.split("/")
        url = [text[1], text[4]]
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

        href = 'https://www.transfermarkt.com/{}/leistungsdatendetails/spieler/{}/saison//verein/0/liga/0/wettbewerb//pos/0/trainer_id/0/plus/1'

        response = requests.get(href.format(url[0], url[1]), headers=headers)
        soup = BeautifulSoup(response.content, features='html.parser')
        try:
            awards = soup.find('div', attrs={'class': 'data-header__badge-container'})
            awards = awards.find_all('a')

            for award in awards:
                self.player_awards.loc[len(self.player_awards)] = [url[1], award['title'], award.text.strip()]
                self.save_to_csv('player_awards')
        except:
            # raise
            pass
        table = soup.find_all('tbody')
        try:
            head = soup.find('span', attrs={'class': 'icons_sprite icon-ohnegegentor-table-header'})['title']
            head = 1
        except:
            head = 0

        row = table[1].find_all('tr')
        for i in row:
            # if head == 0:
            season = '20' + i.find('td', attrs={'class': 'zentriert'}).text[:2]
            league = i.find('td', attrs={'class': 'hauptlink no-border-links'}).text
            if season in ['2020', '2021', '2019', '2018', '2017', '2016', '2015'] and league in leagues:
                club = i.find('td', attrs={'class': 'hauptlink no-border-rechts zentriert'}).find('a')['title']
                other = i.find_all('td', attrs={'class': 'zentriert'})
                squad = other[2].text
                appearances = other[3].text
                ppg = other[4].text
                goals = other[5].text

                if head == 0:
                    assists = other[6].text
                    own_goal = other[7].text
                    substitutions_on = other[8].text
                    substitutions_off = other[9].text
                    yellow_cards = other[10].text
                    second_yellow_cards = other[11].text
                    red_cards = other[12].text
                    penalty_goals = other[13].text

                    minutes_per_goal = str(i.find_all('td', attrs={'class': 'rechts'})[0].text)
                    minutes_played = str(i.find_all('td', attrs={'class': 'rechts'})[1].text)
                    goals_conceded = '-'
                    clean_sheets = '-'

                    if minutes_per_goal[0].isdigit():
                        minutes_per_goal = re.sub('\.', '', minutes_per_goal)
                        minutes_per_goal = int(minutes_per_goal[:-1])


                else:

                    own_goal = other[6].text
                    substitutions_on = other[7].text
                    substitutions_off = other[8].text
                    yellow_cards = other[9].text
                    second_yellow_cards = other[10].text
                    red_cards = other[11].text
                    goals_conceded = other[12].text
                    clean_sheets = other[13].text
                    minutes_played = str(i.find('td', attrs={'class': 'rechts'}).text)
                    minutes_per_goal = '-'
                    assists = '-'
                    penalty_goals = '-'

                if minutes_played[0].isdigit():
                    minutes_played = re.sub('\.', '', minutes_played)
                    minutes_played = int(minutes_played[:-1])

                if '-' in url[0]:
                    name = re.sub('-', ' ', url[0])
                else:
                    name = url[0]

                self.player_details_stats.loc[len(self.player_details_stats)] = [url[1], name, season, league, club,
                                                                                 squad,
                                                                                 appearances, ppg, goals,
                                                                                 own_goal, assists, substitutions_on,
                                                                                 substitutions_off,
                                                                                 yellow_cards, second_yellow_cards,
                                                                                 red_cards,
                                                                                 penalty_goals,
                                                                                 goals_conceded, clean_sheets,
                                                                                 minutes_per_goal, minutes_played]
                self.save_to_csv('player_details_stats')

                print(f"Added {name} in {season} to player details stats", end='\r')
                self.geturls.loc[len(self.geturls)] = urls
                self.save_to_csv('geturls')

    def person_detail(self, url):

        text = url.split("/")
        text = [text[1], text[4]]

        href = 'https://www.transfermarkt.co.uk/{}/profil/spieler/{}'.format(text[0], text[1])

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        response = requests.get(href, headers={'User-Agent': 'Mozilla/5.0'})
        # try:
        soup = BeautifulSoup(response.content, 'html.parser')
        if response.status_code == 200:

            my_spans = soup.find('div', attrs={'class': 'info-table info-table--right-space'}).find_all('span')
            my_text = [span.text.strip() for span in my_spans]

            if '.com' in href:
                try:
                    index = my_text.index("Name in home country:")
                    name = my_text[index + 1]
                except:
                    name = '-'
                try:
                    index = my_text.index("Date of birth:")
                    date = my_text[index + 1]
                except:
                    date = '-'
                try:
                    index = my_text.index("Place of birth:")
                    pob = my_text[index + 1]
                except:
                    pob = '-'
                try:
                    index = my_text.index("Age:")
                    age = my_text[index + 1]
                except:
                    age = '-'
                try:
                    index = my_text.index("Height:")
                    height = my_text[index + 1]
                except:
                    height = '-'
                try:
                    index = my_text.index("Citizenship:")
                    citizenship = my_text[index + 1]
                except:
                    citizenship = '-'
                try:
                    index = my_text.index("Position:")
                    position = my_text[index + 1]
                except:
                    position = '-'
                try:
                    index = my_text.index("Foot:")
                    foot = my_text[index + 1]
                except:
                    foot = '-'
                try:
                    index = my_text.index("Player agent:")
                    player_agent = my_text[index + 1]
                except:
                    player_agent = '-'
                try:
                    index = my_text.index("Current club:")
                    current_club = my_text[index + 1]
                except:
                    current_club = '-'
                try:
                    index = my_text.index("Joined:")
                    joined = my_text[index + 1]
                except:
                    joined = '-'
                try:
                    index = my_text.index("Contract expires:")
                    contract_expires = my_text[index + 1]
                except:
                    contract_expires = '-'
                try:
                    index = my_text.index("Date of last contract extension:")
                    date_of_last_contract = my_text[index + 1]
                except:
                    date_of_last_contract = '-'
                try:
                    index = my_text.index("Outfitter:")
                    outfitter = my_text[index + 1]
                except:
                    outfitter = '-'

            if '.de' in href:

                try:
                    index = my_text.index("Name im Heimatland:")
                    name = my_text[index + 1]
                except:
                    name = '-'
                try:
                    index = my_text.index("Geburtsdatum:")
                    date = my_text[index + 1]
                except:
                    date = '-'
                try:
                    index = my_text.index("Geburtsort:")
                    pob = my_text[index + 1]
                except:
                    pob = '-'
                try:
                    index = my_text.index("Alter:")
                    age = my_text[index + 1]
                except:
                    age = '-'
                try:
                    index = my_text.index("Größe:")
                    height = my_text[index + 1]
                except:
                    height = '-'
                try:
                    index = my_text.index("Nationalität:")
                    citizenship = my_text[index + 1]
                except:
                    citizenship = '-'
                try:
                    index = my_text.index("Position:")
                    position = my_text[index + 1]
                except:
                    position = '-'
                try:
                    index = my_text.index("Fuß:")
                    foot = my_text[index + 1]
                except:
                    foot = '-'
                try:
                    index = my_text.index("Spielerberater:")
                    player_agent = my_text[index + 1]
                except:
                    player_agent = '-'
                try:
                    index = my_text.index("Aktueller Verein:")
                    current_club = my_text[index + 1]
                except:
                    current_club = '-'
                try:
                    index = my_text.index("Im Team seit:")
                    joined = my_text[index + 1]
                except:
                    joined = '-'
                try:
                    index = my_text.index("Vertrag bis:")
                    contract_expires = my_text[index + 1]
                except:
                    contract_expires = '-'
                try:
                    index = my_text.index("Vertragsoption:")
                    date_of_last_contract = my_text[index + 1]
                except:
                    date_of_last_contract = '-'
                try:
                    index = my_text.index("Ausrüster:")
                    outfitter = my_text[index + 1]
                except:
                    outfitter = '-'

            self.player_details.loc[len(self.player_details)] = [text[1], date, pob, age, height, citizenship, position,
                                                                 foot,
                                                                 player_agent,
                                                                 current_club, joined, contract_expires,
                                                                 date_of_last_contract,
                                                                 outfitter]

            self.save_to_csv('player_details')
            print(f"Added {name} in  to player details", end='\r')
            divs = soup.find_all('div', class_="box viewport-tracking")
            trs = divs[1].find_all('div', class_='grid tm-player-transfer-history-grid')
            for tr in trs:
                person_id = url.split('/')[6]

                fasl = tr.find('div', class_='grid__cell grid__cell--center tm-player-transfer-history-grid__season')
                season = '20' + fasl.text.strip()[:2]

                tarikh = tr.find('div', class_='grid__cell grid__cell--center tm-player-transfer-history-grid__date')
                date = tarikh.text.strip()

                bashgaha = tr.find_all('a', class_='tm-player-transfer-history-grid__club-link')
                left = bashgaha[0]['href'].split()[-1]
                join = bashgaha[1]['href'].split()[-1]

                hads = tr.find('div',
                               class_='grid__cell grid__cell--center tm-player-transfer-history-grid__market-value')
                mv = hads.text.strip()

                pool = tr.find('div', class_='grid__cell grid__cell--center tm-player-transfer-history-grid__fee')
                fee = pool.text.strip()

                self.player_trasfor_data.loc[len(self.player_trasfor_data)] = [person_id, season, date, left, join, mv,
                                                                               fee]
                self.save_to_csv('player_trasfor_data')

            self.geturls2.loc[len(self.geturls2)] = url
            self.save_to_csv('geturls2')

    def call_person(self):

        geturls_set = set(self.geturls['URL'])
        ddf = pd.read_csv('./date/person.csv')
        urls = set(ddf['URL']) - geturls_set

        geturls_set2 = set(self.geturls2['URL'])
        ddf2 = pd.read_csv('./date/person.csv')
        urls2 = set(ddf2['URL']) - geturls_set2

        with ThreadPoolExecutor(max_workers=500) as executor:
            _ = [executor.submit(self.person1, url) for url in tqdm(urls, desc="Scraping person Progress")]
            __ = [executor.submit(self.person_detail, url) for url in
                  tqdm(urls2, desc="Scraping person details Progress")]
            co_ = as_completed(__)
            co = as_completed(_)

    def scrap_club_page_at_season(self, club_url, season):

        club_id = club_url.split('/')[-2]
        new_club_url = club_url + f"?saison_id={season}"
        req_failed_count = 0
        while req_failed_count < 3:
            try:
                response = requests.get(new_club_url, headers=HEADERS, timeout=10)
                if response.status_code != 200:
                    raise Exception(f"status code is {response.status_code}")

                soup = BeautifulSoup(response.content, 'html.parser')

                manager = soup.find(class_='container-main').find('a')
                manager_id = manager.get('href').split('/')[-1]
                manager_name = manager.get('title')
                self.manager.loc[len(self.manager)] = [manager_id, manager_name]
                self.save_to_csv('manager')

                try:
                    income = soup.find(class_='transfer-record__total--positive').text.strip()
                    if income.isdigit():
                        income = float(income)
                    else:
                        coef = income[-1]
                        income = float(income[1:-1])
                        if coef == 'k':
                            income = income * 1000
                        elif coef == 'm':
                            income = income * 1000000
                        else:
                            income = income * 1000000000

                except:
                    income = '-'

                try:
                    expenditure = soup.find(class_='transfer-record__total--negative').text.strip()
                    if expenditure.isdigit():
                        expenditure = float(expenditure)
                    else:
                        coef = expenditure[-1]
                        expenditure = float(expenditure[1:-1])
                        if coef == 'k':
                            expenditure = expenditure * 1000
                        elif coef == 'm':
                            expenditure = expenditure * 1000000
                        else:
                            expenditure = expenditure * 1000000000

                except:
                    expenditure = '-'

                for item in soup.select('.posrela'):
                    url = item.find(class_='hauptlink').find('a').get('href')
                    person_id = url.split('/')[-1]
                    person_name = url.split('/')[1].replace('-', ' ').title()
                    person_URL = url

                self.club_season.loc[len(self.club_season)] = [club_id, season, manager_id, income, expenditure]
                self.save_to_csv('club_season')
                self.person.loc[len(self.person)] = [person_id, person_name, person_URL]
                self.save_to_csv('person')
                self.scraped_club_at_season_url.loc[len(self.scraped_club_at_season_url)] = [club_url, season]
                self.save_to_csv('scraped_club_at_season_url')

            except Exception as e:
                print(f"scrap_club_page_at_season function with {new_club_url} input: " + str(e))
                req_failed_count += 1

    def crawl_club_at_season_link(self):
        new_url = set()
        print(len(self.club_league))
        for i in range(len(self.club_league)):
            new_url.add((self.club_league.iloc[i]['club_URL'], self.club_league.iloc[i]['saison']))

        scraped_url = set()
        for i in range(len(self.scraped_club_at_season_url)):
            scraped_url.add((self.scraped_club_at_season_url.iloc[i]['club_URL'],
                             self.scraped_club_at_season_url.iloc[i]['season']))

        new_url = new_url - scraped_url
        print(len(new_url))
        print("start crawling club at season links:")
        if len(new_url) == 0:
            print('all club at season crawled !')
        else:
            print('loading club at season :')
            with ThreadPoolExecutor(max_workers=500) as executor:
                _ = (executor.submit(self.scrap_club_page_at_season, url[0], url[1]) for url in
                     tqdm(new_url, desc="Scraping feature of clubs at season Progress"))

    def scrap_club_page(self, club_url):
        req_failed_count = 0
        while req_failed_count < 3:
            try:
                response = requests.get(club_url, headers=HEADERS, timeout=10)
                if response.status_code != 200:
                    raise Exception(f"status code is {response.status_code}")

                soup = BeautifulSoup(response.content, 'html.parser')

                club_id = club_url.split('/')[-2]
                club_name = soup.find(class_='data-header__headline-wrapper--oswald').text.strip()
                try:
                    club_Founded = soup.find(class_='info-table--equal-space').find(
                        itemprop="foundingDate").text
                except:
                    club_Founded = '-'

                try:
                    club_Website = soup.find(class_='info-table--equal-space').find(itemprop="url").find(
                        'a').get('href')
                except:
                    club_Website = '-'

                try:
                    club_Stadium = '-'
                    for item in soup.select('.data-header__details > ul > li'):
                        if 'Stadium' in item.text:
                            club_Stadium = item.find('span').find('a').text
                except:
                    club_Stadium = '-'

                for award in soup.select('.data-header__badge-container > .data-header__success-data'):
                    award_title = award.get('title')
                    number = int(award.find('span').text.strip())

                self.club_details.loc[len(self.club_details)] = [club_id, club_name, club_Founded, club_Website,
                                                                 club_Stadium]
                self.save_to_csv('club_details')
                self.club_awards.loc[len(self.club_awards)] = [club_id, award_title, number]
                self.save_to_csv('club_awards')
                self.scraped_club_url.loc[len(self.scraped_club_url)] = [club_url]
                self.save_to_csv('scraped_club_url')

            except Exception as e:
                print(f"scrap_club_page function with {club_url} input: " + str(e))
                req_failed_count += 1

    def crawl_club_link(self):
        new_url = set()
        print(len(self.club_league))
        for i in range(len(self.club_league)):
            new_url.add(self.club_league.iloc[i]['club_URL'])

        scraped_url = set()
        for i in range(len(self.scraped_club_url)):
            scraped_url.add(self.scraped_club_url.iloc[i]['club_URL'])

        new_url = new_url - scraped_url
        print(len(new_url))
        print("start crawling club links:")
        if len(new_url) == 0:
            print('all club crawled !')
        else:
            print('loading club:')
            with ThreadPoolExecutor(max_workers=500) as executor:
                _ = (executor.submit(self.scrap_club_page, url) for url in
                     tqdm(new_url, desc="Scraping feature of clubs Progress"))


if __name__ == '__main__':
    site = Transfer()
    site.get_all_data()
