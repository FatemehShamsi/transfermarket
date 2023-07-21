from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, text, Date, Float
from sqlalchemy.orm import relationship
from database_eng import *
import pandas as pd
import numpy as np
import os

Base = declarative_base()


class Countries(Base):
    __tablename__ = 'countries'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    Url = Column(String(255))
    leagues = relationship('Leagues', primaryjoin="Countries.id == foreign(Leagues.id)")


class Leagues(Base):
    __tablename__ = 'leagues'
    league_id = Column(Integer, primary_key=True)
    name = Column(String(255))
    country_id = Column(Integer, ForeignKey('countries.id'), nullable=False)
    club_league = relationship('ClubLeague', primaryjoin="Leagues.id == foreign(ClubLeague.league_id)")
    league_details = relationship('LeaguesDetailsSeason', primaryjoin="Leagues.league_id == foreign("
                                                                      "LeaguesDetailsSeason.league_id)")
    player = relationship('Player', primaryjoin="Leagues.league_id == foreign(Player.league_id)")


class Clubs(Base):
    __tablename__ = 'clubs'
    club_id = Column(Integer, primary_key=True)
    name = Column(String(255))
    founded = Column(Date)
    website = Column(String(255))
    stadium = Column(String(255))
    club_league = relationship('ClubLeague', primaryjoin="Clubs.club_id == foreign(ClubLeague.club_id)")
    player = relationship('Player', primaryjoin="Clubs.club_id == foreign(Player.club_id)")
    person_detail = relationship('Person_detail', primaryjoin="Clubs.club_id == foreign(Person_detail.current_club)")
    players_transfor_join = relationship('Players_transfor',
                                         primaryjoin="Clubs.club_id == foreign(Players_transfor.join)")
    clubsseason = relationship('ClubsSeason',
                               primaryjoin="Clubs.club_id == foreign(ClubsSeason.club_id)")
    clubs_award = relationship('Clubs_award',
                               primaryjoin="Clubs.club_id == foreign(Clubs_award.club_id)")


class ClubsSeason(Base):
    __tablename__ = 'clubsseason'
    id = Column(Integer, primary_key=True)
    club_id = Column(Integer, ForeignKey('clubs.club_id'), nullable=False)
    manager_id = Column(Integer, ForeignKey('person.id'), nullable=False)
    season_id = Column(Integer, ForeignKey('seasons.saison'), nullable=False)
    income = Column(String(255))
    expenditure = Column(String(255))


class Person(Base):
    __tablename__ = 'person'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    league_details = relationship('LeaguesDetailsSeason',
                                  primaryjoin="Person.id == foreign(LeaguesDetailsSeason.top_scorer)")
    player = relationship('Player',
                          primaryjoin="Person.id == foreign(Player.person_id)")
    person_detail = relationship('Person_detail',
                                 primaryjoin="Person.id == foreign(Person_detail.person_id)")
    person_awards = relationship('Person_awards',
                                 primaryjoin="Person.id == foreign(Person_awards.person_id)")
    players_transfor = relationship('Players_transfor',
                                    primaryjoin="Person.id == foreign(Players_transfor.person_id)")

    ClubsSeason = relationship('ClubsSeason',
                               primaryjoin="Person.id == foreign(ClubsSeason.manager_id)")


class LeaguesDetailsSeason(Base):
    __tablename__ = 'leagues details season'
    id = Column(Integer, primary_key=True)
    league_id = Column(Integer, ForeignKey('leagues.league_id'), nullable=False)
    saison = Column(Integer, ForeignKey('seasons.saison'), nullable=False)
    winner = Column(String(255))
    top_scorer = Column(Integer, ForeignKey('person.id'), nullable=False)
    goals_per_match = Column(Float)
    foreigners = Column(Float)


class Seasons(Base):
    __tablename__ = 'seasons'
    saison = Column(Integer, primary_key=True)
    club_league = relationship('ClubLeague', primaryjoin="Seasons.saison == foreign(ClubLeague.club_id)")
    league_details = relationship('LeaguesDetailsSeason',
                                  primaryjoin="Seasons.saison == foreign(LeaguesDetailsSeason.saison)")
    player = relationship('Player',
                          primaryjoin="Seasons.saison == foreign(Player.season)")
    players_transfor = relationship('Players_transfor',
                                    primaryjoin="Seasons.saison == foreign(Players_transfor.season)")
    ClubsSeason = relationship('ClubsSeason',
                               primaryjoin="Seasons.saison == foreign(ClubsSeason.season_id)")


class ClubLeague(Base):
    __tablename__ = 'club_leagues'
    clubleage_id = Column(Integer, primary_key=True)
    club_id = Column(Integer, ForeignKey('clubs.club_id'), nullable=False)
    league_id = Column(Integer, ForeignKey('leagues.league_id'), nullable=False)
    saison = Column(Integer, ForeignKey('seasons.saison'), nullable=False)


class Player(Base):
    __tablename__ = 'player'
    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey('person.id'), nullable=False)
    season = Column(Integer, ForeignKey('seasons.saison'), nullable=False)
    # league_id = Column(Integer, ForeignKey('leagues.league_id'), nullable=False)
    # club_id = Column(Integer, ForeignKey('clubs.club_id'), nullable=False)
    league = Column(String(255))
    club = Column(String(255))
    squad = Column(Integer, nullable=False)
    appearances = Column(Integer, nullable=True)
    ppg = Column(Float, nullable=True)
    goals = Column(Integer, nullable=True)
    own_goal = Column(Integer, nullable=True)
    assists = Column(Integer, nullable=False)
    substitutions_on = Column(Integer, nullable=True)
    substitutions_off = Column(Integer, nullable=True)
    yellow_cards = Column(Integer, nullable=True)
    second_yellow_cards = Column(Integer, nullable=True)
    red_cards = Column(Integer, nullable=True)
    penalty_goals = Column(Integer, nullable=True)
    goals_conceded = Column(Integer, nullable=True)
    clean_sheets = Column(Integer, nullable=True)
    minutes_per_goal = Column(Integer, nullable=True)
    minutes_played = Column(Integer, nullable=True)
    market_value = Column(String(255))


class Person_detail(Base):
    __tablename__ = 'person_detail'
    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey('person.id'), nullable=False)
    date = Column(Date, nullable=True)
    Pob = Column(String(255), nullable=True)
    Height = Column(Integer, nullable=True)
    Citizenship = Column(String(255), nullable=True)
    position = Column(String(255), nullable=True)
    Foot = Column(String(128), nullable=True)
    player_agent = Column(String(255), nullable=True)
    # current_club = Column(Integer, ForeignKey('clubs.club_id'), nullable=True)
    current_club = Column(String(255), nullable=True)
    Joined = Column(Date, nullable=True)
    Contract_expires = Column(Date, nullable=True)
    Social_Media = Column(String(255), nullable=True)


class Person_awards(Base):
    __tablename__ = 'person_awards'
    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey('person.id'), nullable=False)
    title = Column(String(255), nullable=True)
    count = Column(Integer, nullable=True)


class Players_transfor(Base):
    __tablename__ = 'players_transfor'
    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey('person.id'), nullable=False)
    season = Column(Integer, ForeignKey('seasons.saison'), nullable=False)
    date = Column(Date, nullable=True)
    join = Column(String(128))
    left = Column(String(128))
    mv = Column(String(128), nullable=True)
    fee = Column(String(255), nullable=True)


class Clubs_award(Base):
    __tablename__ = 'club_awards'
    id = Column(Integer, primary_key=True)
    club_id = Column(Integer, ForeignKey('clubs.club_id'), nullable=False)
    award_title = Column(String(255))
    number = Column(Integer)


class AddToTable:
    def __init__(self):
        self.connection = create_table('fatemeh', '', 'TransferMarket')

    def addCountries(self):
        csvCountries = pd.read_csv('data\countries.csv')
        csvCountries.to_sql('countries', con=self.connection, if_exists='append', index=False)

    def addLeagues(self):
        csvLeagues = pd.read_csv('data\leagues.csv')
        csvLeagues.drop(['URL'], axis=1, inplace=True)
        csvLeagues.to_sql('leagues', con=self.connection, if_exists='append', index=False)

    def addSeasons(self):
        csvSeason = pd.read_csv('data\saison.csv')
        csvSeason.to_sql('seasons', con=self.connection, if_exists='append', index=False)

    def addLeaguesDetailsSeason(self):
        csvleaguesDetails = pd.read_csv('data\league_details_saison.csv')
        csvleaguesDetails.drop(['URL', 'country_id'], axis=1, inplace=True)

        csvleaguesDetails.to_sql('leagues details season', con=self.connection, if_exists='append', index=False)

    def addClub(self):
        csvclubDetails = pd.read_csv('data\club_details.csv')
        csvclubDetails.replace('-', np.nan, inplace=True)
        csvclubDetails.to_sql('clubs', con=self.connection, if_exists='append', index=False)

    def addClubSeasons(self):
        csvclubDetails = pd.read_csv('data\club_season.csv')
        csvclubDetails.replace('-', np.nan, inplace=True)
        csvclubDetails.to_sql('clubsseason', con=self.connection, if_exists='append', index=False)

    def addClubsLeagues(self):
        csvClubsLeagues = pd.read_csv('data\club_league.csv')
        csvClubsLeagues.drop(['club_name', 'league_URL', 'club_URL'], axis=1, inplace=True)
        csvClubsLeagues.to_sql('club_leagues', con=self.connection, if_exists='append', index=False)

    def addClubsAwards(self):
        csvClubsAwards = pd.read_csv('data\club_awards.csv')
        csvClubsAwards.replace('-', np.nan, inplace=True)
        csvClubsAwards.to_sql('club_awards', con=self.connection, if_exists='append', index=False)

    def addPerson(self):
        csvPerson = pd.read_csv('data\person.csv')
        csvManeger = pd.read_csv('data\manager.csv')
        person_man = pd.concat([csvPerson, csvManeger], ignore_index=True)
        person_man.drop_duplicates(subset=['id'], keep='first', inplace=True)
        person_man.drop(['URL'], axis=1, inplace=True)
        person_man.to_sql('person', con=self.connection, if_exists='append', index=False)

    def addPerson_detail(self):
        csvPerson = pd.read_csv('data\player_details.csv')
        csvPerson.drop(['Age'], axis=1, inplace=True)

        csvPerson = csvPerson.dropna(subset=['person_id'])
        csvPerson.to_sql('person_detail', con=self.connection, if_exists='append', index=False)

    def addPlayer(self):
        csvPerson = pd.read_csv('finaldata\player_details_stats.csv')
        csvPerson['assists'] = csvPerson['assists'].fillna(0)
        csvPerson.dropna(subset=['person_id'], inplace=True)
        csvPerson.drop(['name'], axis=1, inplace=True)
        csvPerson.to_sql('player', con=self.connection, if_exists='append', index=False)

    def addPlayers_transfor(self):
        csvPerson = pd.read_csv('finaldata\player_trasfor_data.csv')
        csvPerson['date'] = pd.to_datetime(csvPerson['date'], format='%Y-%m-%d')
        # csvPerson['date'] = csvPerson['date'].dt.strftime('%Y-%m-%d')
        csvPerson.dropna(subset=['person_id'], inplace=True)
        csvPerson.to_sql('players_transfor', con=self.connection, if_exists='append', index=False)

    def addPerson_awards(self):
        csvPerson = pd.read_csv('finaldata\player_awards.csv')

        csvPerson.to_sql('player_awards', con=self.connection, if_exists='append', index=False)

    def addAll(self):
        self.addCountries()
        self.addLeagues()
        self.addSeasons()
        self.addPerson()
        self.addLeaguesDetailsSeason()
        self.addClub()
        self.addClubsLeagues()
        self.addClubSeasons()
        self.addClubsAwards()
        self.addPerson_detail()
        self.addPlayer()
        self.addPlayers_transfor()
        self.addPerson_awards()


class CreateTable:
    def __init__(self, database_name, username, password):
        engine = create_schema(username, password)
        with engine.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {database_name}"))
            conn.execute(text(f"CREATE DATABASE {database_name}"))
        engine = create_table(username, password, database_name)
        with engine.connect() as conn:
            conn.execute(text(f"USE {database_name}"))
            Base.metadata.create_all(bind=conn)


class DataCleaning:
    def __init__(self):
        self.path_output = './data'
        self.clean_player_details_stats()
        self.clean_player_details()
        self.clean_player_trasfor_data()

    def clean_player_details_stats(self):
        path = os.path.join(self.path_output, 'player_details_stats.csv')
        player_details = pd.read_csv(path)
        columns_to_convert = ['goals', 'own_goal', 'assists', 'substitutions_on', 'substitutions_off',
                              'yellow_cards', 'second_yellow_cards', 'red_cards', 'penalty_goals',
                              'goals_conceded', 'clean_sheets', 'minutes_per_goal', 'minutes_played', 'appearances']

        player_details[columns_to_convert] = player_details[columns_to_convert].fillna(0).astype(int)
        player_details['ppg'] = player_details['ppg'].str.replace(',', '.')
        player_details.rename(columns={'id': 'person_id'}, inplace=True)
        player_details.dropna(subset=['person_id'], inplace=True)
        player_details['assists'] = player_details['assists'].fillna(0)
        player_details.to_csv(path, index=False)

    def clean_player_details(self):

        path = os.path.join(self.path_output, 'player_details.csv')
        player_details = pd.read_csv(path)
        player_details['Date'].replace('k. A.', np.nan, inplace=True)
        player_details['Date'] = player_details['Date'].str.replace(' Happy Birthday', '')
        player_details['Date'] = pd.to_datetime(player_details['Date'], errors='coerce')

        player_details['Height'] = player_details['Height'].str.replace(',', '').str.replace('\xa0', '').str.replace(
            'm', '').str.replace(' ', '').str.replace('’', '')
        player_details['Height'] = player_details['Height'].replace('k. A.', np.nan)
        player_details['Height'] = player_details['Height'].replace('k.A.', np.nan)
        player_details['Height'] = player_details['Height'].replace('', np.nan)
        player_details['Height'] = player_details['Height'].astype(float)

        player_details['Joined'] = pd.to_datetime(player_details['Joined'], errors='coerce')
        player_details['Contract expires'] = pd.to_datetime(player_details['Contract expires'], errors='coerce')

        player_details['Foot'] = player_details['Foot'].replace('rechts', 'right')
        player_details['Foot'] = player_details['Foot'].replace('links', 'left')
        player_details['Foot'] = player_details['Foot'].replace('beidfüßig', 'both')
        player_details['Foot'] = player_details['Foot'].replace('k. A.', 'Unknown')
        player_details['Foot'] = player_details['Foot'].fillna('Unknown')

        player_details['position'] = player_details['position'].str.split(' - ').str[0]
        player_details['position'] = player_details['position'].replace('Torwart', 'Goalkeeper')
        player_details['position'] = player_details['position'].replace('Abwehr', 'Defender')
        player_details['position'] = player_details['position'].replace('Sturm', 'Attack')
        player_details['position'] = player_details['position'].replace('Mittelfeld', 'midfield')

        player_details.drop('Date of last contract extension', inplace=True, axis=1)

        player_details = player_details.dropna(subset=['id'])
        player_details['id'] = player_details['id'].astype(int)
        path_person = os.path.join(self.path_output, 'person.csv')
        person = pd.read_csv(path_person)
        merged_data = player_details.merge(person, on=["id"])
        merged_data.drop(['URL', 'name_y'], inplace=True, axis=1)

        merged_data.rename(
            columns={'name_x': 'name', 'Date': 'date', 'id': 'person_id', 'Player agent': 'player_agent',
                     'Contract expires': 'Contract_expires', 'Current club': 'current_club',
                     'Social Media': 'Social_Media'}, inplace=True)

        merged_data.to_csv(path, index=False)

    def clean_player_trasfor_data(self):
        path = os.path.join(self.path_output, 'player_trasfor_data.csv')
        player_details = pd.read_csv(path)
        player_details['date'] = pd.to_datetime(player_details['date'], errors='coerce')

        player_details['fee'] = player_details['fee'].replace('ablösefrei', 'free transfer')
        player_details['fee'] = player_details['fee'].replace('Leih-Ende', 'end of loan')
        player_details['fee'] = player_details['fee'].replace('Leihe', 'loan')
        player_details['fee'] = player_details['fee'].str.replace('Mio.', 'm')
        player_details['fee'] = player_details['fee'].str.replace('Tsd.', 'k')

        player_details['mv'] = player_details['mv'].str.replace('Mio.', 'm')
        player_details['mv'] = player_details['mv'].str.replace('Tsd.', 'k')

        player_details.dropna(subset=['person_id'], inplace=True)
        player_details['date'] = pd.to_datetime(player_details['date'], format='%m/%d/%Y')
        player_details['date'] = player_details['date'].dt.strftime('%Y-%m-%d')

        player_details.to_csv(path, index=False)


        path_se = os.path.join(self.path_output, 'saison.csv')
        se = pd.read_csv(path_se)
        list_of_sea = player_details['season'].unique().tolist()
        for s in list_of_sea:
            if s is not None:
                if s > 2040:
                    s -= 1000
                se.loc[len(se)] = [s]

        se.drop_duplicates(inplace=True)
        se.to_csv(path_se, index=False)


if __name__ == '__main__':
    clean = DataCleaning()
    creator = CreateTable('TransferMarket', 'fatemeh', '')
    add = AddToTable()
    add.addAll()
