from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import pypyodbc
from time import sleep
import time
from dateutil.relativedelta import relativedelta
import config

# Connect to SQL database
conn = pypyodbc.connect("Driver={SQL Server};"
                    f"Server={config.SERVER};"
                    f"Database={config.DATABASE};"
                    "Trusted_Connection=yes;",
                    autocommit=True)


mycursor = conn.cursor()

# initialize headers and url for scraping.
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.0 Safari/537.36'}
base_url = "http://www.harness.org.au/racing/results/?firstDate="
base1_url = "http://www.harness.org.au"

try:
    page1 = requests.get('http://www.harness.org.au/racing/results/?firstDate=', headers=headers)
except requests.exceptions.ConnectionError:
    print(f'connect error : http://www.harness.org.au/racing/results/?firstDate=')
    exit(0)

racing_session = requests.session()

format = "%d-%m-%y"
delta = timedelta(days=1)
enddate = datetime(2020, 10, 1)
# startdate = datetime(2021, 11, 21)
startdate = datetime.today()
today1 = datetime.today()
raceday1 = today1.strftime("%d %b %Y")
raceday33 = datetime.strptime(raceday1, "%d %b %Y")
raceday33 = raceday33.date()

# check whether scraping data is already existing.
def check_scrap_data_existing(date):
    mycursor.execute('SELECT COUNT(*) FROM horses2 WHERE DayCalender=?', [date])
    ret = mycursor.fetchone()
    return ret[0] > 100

# perform scraping horse data.
def start_horse_scraping(startdate, enddate):
    while startdate > enddate:
        enddate += timedelta(days=1)
        if check_scrap_data_existing(enddate):
            print(f'Data already exists on {enddate}')
            continue
        
        enddate1 = enddate.strftime("%d %m %Y")
        enddate5 = enddate.strftime("%d-%m-%Y")
        enddate2 = enddate.strftime("%Y-%m-%d")
        new_url = base_url + str(enddate5)

        # scrape data within range
        # https://www.harness.org.au/racing/results/?firstDate=02-10-2020
        soup12 = racing_session.get(new_url, headers=headers)

        soup1 = bs(soup12.content, "html.parser") 
        table1 = soup1.find('table', class_='meetingListFull')

        tr = table1.find_all('tr', {'class':['odd', 'even']})

        for tr1 in tr:
            tr2 = tr1.find('a').get_text()
            tr3 = tr1.find('a')['href']

            newurl = base1_url + tr3
            with requests.Session() as s:
                # scrape data for each venue
                # https://www.harness.org.au/racing/fields/race-fields/?mc=ML021020
                webpage_response = s.get(newurl, headers=headers)
                soup = bs(webpage_response.content, "html.parser")
                results = soup.find_all('div', {'class':'forPrint'})
                
                # iterates all resultin the browser 
                for race in results:
                    norace = race.find('p')
                    
                    if norace is not None:
                        continue

                    race_number = race.find('td', class_='raceNumber').get_text()
                    race_number = int(race_number)
                
                    norace = race.find('p')

                    race_name1 = race.find('td', class_='raceTitle').get_text()
                    
                    race_title1 = race.find('td', class_='raceInformation').get_text()
                    race_title1 = ' '.join(race_title1.split())
                    
                    race_distance1 = race.find('td', class_='distance').get_text()
                    race_distance1 = race_distance1.replace('M', '')
                    race_distance1 = float(race_distance1)

                    tableofrunners = race.find('table', class_='raceFieldTable resultTable')
                    tableofinfo = race.find('table', class_='raceMoreInfo')
                    prizemoney = race_title1.split()[0]
                    prizemoney = prizemoney.replace('$', '').replace(',', '')
                    prizemoney = prizemoney.split('.')[0]
                    prizemoney = int(prizemoney)
                    print(prizemoney)

                    racetime = race.find('td', class_='raceTime').get_text()
                    splitter = racetime.split(':')[0]
                    splitter = int(splitter)
                    pmoram = racetime[-2:]
                    print(splitter)
                    morningrace = 0
                    eveningrace = 0

                    if pmoram == 'am':
                        morningrace = 1
                        eveningrace = 0
                    elif pmoram == 'pm' and splitter == 12:
                        morningrace = 1
                        eveningrace = 0
                    elif 6 > splitter and pmoram == 'pm':
                        morningrace = 1
                        eveningrace = 0
                    else:
                        eveningrace = 1
                        morningrace = 0
    

                    data1 = {
                        'Trainer': [],
                        'RaceTitle': [],
                    }

                    for el in tableofrunners.find_all('td', class_='trainer'):
                        elem = el.get_text()

                        data1['Trainer'].append(elem)
                        data1['RaceTitle'].append(race_title1)

                    table2 = pd.DataFrame(data1, columns=['Trainer', 'RaceTitle'])
                    table2['counts'] = table2.groupby(['Trainer'])['RaceTitle'].transform('count')

                    trainerRunnerCount = {}
                    for index, row in table2.iterrows():
                        trainerRunnerCount[row['Trainer']] = row['counts']

                    if tableofinfo == 'Declared no race':
                        continue 

                    for row in tableofrunners.select("tr"):
                        data = {
                            'DayCalender': [],
                            'Venue': [],
                            'RaceNumber': [],
                            'RaceName': [],
                            'RaceTitle': [],
                            'RaceDistance': [],          
                            'Place': [],
                            'HorseName': [],
                            'HorseID': [],
                            'Age': [],
                            'Colour': [],
                            'Sire': [],
                            'Sex': [],
                            'Prizemoney': [],
                            'Handicap': [],
                            'Row': [],
                            'Trainer': [],
                            'Driver': [],
                            'Margin': [],
                            'MarginWinner': [],
                            'StartingOdds': [],
                            'StewardsComments': [],
                            'laststart': [],
                            'start2': [],
                            'start3': [],
                            'start4': [],
                            'spelllastfive': [],
                            'firstup': [],

                            'Placer': [],
                            'Winner': [],
                            'Firststarter': [],



                            'LifetimeRuns': [],
                            'RecentRuns': [],
                            'LifetimeWins': [],
                            'LifetimeSeconds': [],
                            'LifetimeThirds': [],
                            'LifetimePlacings': [],
                            'LifetimeWinPercent': [],
                            'LifetimePlacePercent': [],
                            'RecentPlacePercent': [],
                            'RecentWinPercent': [],
                            'RecentPlacings': [],
                            'RecentThirds': [],
                            'RecentSeconds': [],
                            'RecentWins': [],
                            'Class': [],
                            'Dayssincelast': [],
                            'Gatespeedstrikerate': [],
                            'broken': [],
                            'LeaderStrikeRate': [],
                            'RowLastStart': [],
                            'DistanceLastStart': [],
                            'TrainerLastStart': [],
                            'DriverLastStart': [],
                            'StewardsFull': [],
                            'BestWinningMile': [],
                            'samedistance': [],
                            'UpInDistance': [],
                            'DownInDistance': [],
                            'TimeLead': [],
                            'TimeGateSpeed': [],
                            'TimeBroken': [],
                            'PrizemoneyLastStart': [],
                            'Triallaststart': [],
                            'Standlaststart': [],
                            'racetime': [],
                            'morningrace': [],
                            'eveningrace': [],
                            'Timelastrace': [],
                            'morningracelaststart': [],
                            'eveningracelaststart': [],
                            'Prizemoneylastrace': [],
                            'tracknamelaststart': [],
                            'startingprice1': [],
                            'avestake': [],
                            'totalprize': [],
                            'sametrack': [],
                            'prizemoneysame': [],
                            'prizemoneyup': [],
                            'prizemoneydown': [],
                            'Tracklaststart': [],
                            'driverchange': [],
                            'trainerchange': [],
                            'trials': [],
                            'aveplace': [],
                            'countofdeath': [],
                            'Deathseat': [],
                            'Leading': [],
                            'countfront1': [],
                            'countfront2': [],
                            'countfront3': [],
                            'countfront4': [],
                            'countld1': [],
                            'countld2': [],
                            'countld3': [],
                            'countld4': [],
                            'frontrow': [],
                            'secondrow': []
                            
                            
                        }

                        # Skip "empty" rows
                        if row.find('td', class_='horse_number') == None:
                            continue

                        place = row.find('td', class_='horse_number').get_text()
                        place = place.replace('*', '').replace('r', '').replace('u', '').replace('f', '').replace('d', '')
                        try:
                            place = int(place, 0)
                        except:
                            place = 0

                        if place == 1:
                            winner = 1
                        else:
                            winner = 0

                        if place == 1 or place == 2 or place == 3:
                            placer = 1
                        else:
                            placer = 0

                        horsename = row.find('a', class_='horse_name_link')
                        horsename = horsename.text.replace('HorseName: ', '') if horsename else ''
                        horsename = horsename.replace(' NZ', '')
                        trainer = row.find('td', class_='trainer')
                        
                        trainer = trainer.text.replace('Trainer: ', '') if trainer else ''

                        driver = row.find('td', class_='driver-short')
                        driver = driver.text.replace('Driver: ', '') if driver else ''


                        horse_id = row.find('a').get('href')
                        horse_id = horse_id[-6:]
                        horse_id = int(horse_id)
                

                        horseweb = row.find('a').get('href')
                        horseurl = base1_url + horseweb
                        stewardscom = row.find('td', class_='stewards_comments')
                        stewardscom = stewardscom.get_text()
                        
                        # scrap data about each horse
                        #http://www.harness.org.au/racing/horse-search/?horseId=799431
                        with requests.Session() as s:
                            try:
                                webpage_response = s.get(horseurl, headers=headers)
                            except requests.exceptions.ConnectionError:
                                print(horseurl)
                                exit(0)
                                
                            soup = bs(webpage_response.content, "html.parser")
                            horseresult = soup.find('table', {"class": ["table horseHeader"]})
                            sleep(1)
                            horseresult2 = soup.select('table')[1]
                            horseresult3 = soup.select('table')[2]
                            horseresult4 = soup.select('table')[3]
                            if soup.select('table')[4] == None:
                                horseresult4 = horseresult4
                                pass
                                # continue

                            horseresult6 = soup.find('table', class_='table table-hover small horsePerformance')
                            Age1 = horseresult.find_all('td')[1].get_text().replace('\xa0', '')
                                
                            horseage1 = datetime.strptime(Age1, '%d %B %Y')
                            horsebday = datetime(2021, 9, 1)
                            age = relativedelta(horsebday, horseage1).years
    
                            Colour = horseresult.find_all('td')[3].get_text()
                            Colour1 = Colour.split()[0]

                    
                            Sex = horseresult.find_all('td')[5].get_text().replace('\xa0', '')
                        
                            Sire = horseresult.find_all('td')[7].get_text().replace('\xa0', '')
                            
                            Class = horseresult2.find_all('td')[-3].get_text()
                            Class = Class.replace('\xa0', '').replace('--', '')
                            Class = Class[2:5]
                            if Class == None or Class == '':
                                continue
                            
                            Class = int(Class)
                                

                            summary = horseresult3.find_all('td')[1].get_text()
                            if summary.split('-')[0] == None or summary.split('-')[0] == '\n':
                                continue
                            runs = summary.split('-')[0]
                            wins = summary.split('-')[1]
                            seconds = summary.split('-')[2]
                            thirds = summary.split('-')[3]
                        

                            runs = int(runs)
                            wins = int(wins)
                            seconds = int(seconds)
                            thirds = int(thirds)

                            summary = horseresult4.find_all('td')[1].get_text()
                            summary1 = summary.replace('\n', '')
                            summary1 = summary1.replace('0', '')
                            summary1 = summary1.replace('-', '')
                            if summary.split('-')[0] == '\n' or summary1.split('-')[0] == '':
                                runs1 = 0
                                wins1 = 0
                                seconds1 = 0
                                thirds1 = 0
                                placings1 = 0
                                winpercent1 = 0
                                placepercent1 = 0
                            else:
                                runs1 = summary.split('-')[0]
                                wins1 = summary.split('-')[1]
                                seconds1 = summary.split('-')[2]
                                thirds1 = summary.split('-')[3]
                                print(runs1, wins1, seconds1, thirds1)

                                runs1 = int(runs1)
                                wins1 = int(wins1)
                                seconds1 = int(seconds1)
                                thirds1 = int(thirds1)
                                placings1 = wins1 + seconds1 + thirds1
                                winpercent1 = wins1/runs1
                                placepercent1 = placings1/runs1
                                print(winpercent1, placepercent1)

                            if soup.select('table')[5] == None or soup.select('table')[5] == '':
                                continue
                                
                            

                            horseresult6 = soup.find('table', class_='table table-hover small horsePerformance')
                            #horseresult6body = horseresult6.body('tbody')
                            rows12 = horseresult6.find_all('td', class_='stewards')
                            if horseresult6.find('a') == None:
                                    continue
                            lastrace = horseresult6.find('a').get('href')
                            horseurl2 = base1_url + lastrace
                        
                            #horseresult6body = horseresult6.body('tbody')
                            racename1 = horseresult6.find('td', class_='raceName').get_text()
                            if "TRIAL" in racename1:
                                    
                                
                                TrialLastStart = 1
                                continue
                            else:
                                TrialLastStart = 0

                            daysbetween = horseresult6.find('td', class_='date').get_text().strip()
                        
                            print(daysbetween)
                            daysbetween3 = datetime.strptime(daysbetween, "%d %b %Y")
                            daysbetween3 = daysbetween3.date()

                            daysinbetweenruns = (raceday33 - daysbetween3).days
                            print(daysinbetweenruns)

                            count = 0
                            count1 = 0
                            count2 = 0
                            avestake = 0
                            totalprize = 0
                            runns = 0
                            howmany = 0
                            countld1 = 0 
                            countld2 = 0
                            countld3 = 0
                            countld4 = 0
                            countofld = 0
                            countofgs = 0
                            strikeRateBroken = 0
                            strikeRateGS = 0
                            strikeRateLD = 0
                            trials = 0
                            aveplace = 0
                            endstring = ''     
                            winning = 0
                            seconding = 0
                            thirding = 0
                            death = 'death'
                            Deathseat = 0
                            leader = 'led'
                            Leading = 0
                            death2 = '2'
                            countfront1 = 0
                            countfront2 = 0
                            countfront3 = 0
                            countfront4 = 0
                            frontrow = 0
                            secondrow = 0
                            lifetimeplace = 0
                            winpercent = 0
                            lifetimeplacepercent = 0
                            laststart = 0
                            start2 = 0
                            start3 = 0
                            start4 = 0
                            firststarter = 0
                            RowLastStart = 0
                            DistanceLastStart = 0
                            Trainerlaststart = 0
                            Driverlaststart = 0
                            BestWinningMile = 0
                            SameDistance = 0
                            UpinDistance = 0
                            DowninDistance = 0
                            stakewon = 0
                            startingprice1 = 0
                            tabler = 0
                            morningrace1 = 0
                            eveningrace1 = 0 
                            prizemoney4 = 0
                            tracklast = 0
                            sametrack = 0
                            prizemoneysame = 0
                            prizemoneydown = 0
                            prizemoneyup = 0
                            trackthis = 0
                            driverchange = 0
                            trainerchange = 0
                            countofdeath = 0
                            Deathseat = 0
                            Leading = 0
                            countfront1 = 0
                            countfront2 = 0
                            countfront3 = 0
                            countfront4 = 0
                            countld1 = 0
                            countld2 = 0
                            countld3 = 0
                            countld4 = 0
                            RowLastStart = 0
                            Standlaststart = 0
                            stetitle2 = 0

                            for tr in horseresult6.find_all('tr')[1:]:

                                racename = tr.find('td', class_='raceName').get_text()
                                date1 = tr.find('td', class_='date').find('a').get_text()
                                
                                date25 = datetime.strptime(date1, "%d %b %Y")
                                date2 = datetime.strftime(date25, "%d %m %Y")
                                
                                if "TRIAL" in racename:
                                    trials += 1
                                    continue

                                if time.strptime(date2, "%d %m %Y") == time.strptime(enddate1, "%d %m %Y"):
                                    refof = tr.find('a').get('href')
                                    if tr.find('td', class_='date').find_next('td', class_='date') is not None: #starts from 
                                        
                                        
                                        dateprior = tr.find('td', class_='date').find_next('td', class_='date').get_text().strip()
                                        try:
                                            start4 = tr.find('td', class_='place').find_next('td', class_='place').find_next('td', class_='place').find_next('td', class_='place').find_next('td', class_='place').get_text().strip()
                                            start4 = start4.replace('*', '').replace('r', '0').replace('u', '0').replace('f', '0').replace('d', '0').replace('s', '').replace('n', '').replace('b', '')
                                            firststarter = 0
                                        except:
                                            start4 = 0
                                        try:
                                            start3 = tr.find('td', class_='place').find_next('td', class_='place').find_next('td', class_='place').find_next('td', class_='place').get_text().strip()
                                            start3 = start3.replace('*', '').replace('r', '0').replace('u', '0').replace('f', '0').replace('d', '0').replace('s', '').replace('n', '').replace('b', '')
                                            firststarter = 0
                                        except:
                                            start3 = 0
                                        try:
                                            start2 = tr.find('td', class_='place').find_next('td', class_='place').find_next('td', class_='place').get_text().strip()
                                            start2 = start2.replace('*', '').replace('r', '0').replace('u', '0').replace('f', '0').replace('d', '0').replace('s', '').replace('n', '').replace('b', '')
                                            firststarter = 0
                                        except:
                                            start2 = 0
                                        
                                        racenamelast = tr.find('td', class_='raceName').find_next('td', class_='raceName').get_text()
                                        
                                        if "TRIAL" in racenamelast:
                                            TrialLastStart = 1
                                            trialresult = tr.find('td', class_='place').find_next('td', class_='place').get_text()
                                            laststart = 0 
                                            firststarter = 0
                                            startingprice1 = 0
                                            DistanceLastStart = 0
                                            Trainerlaststart = 0
                                            Driverlaststart = 0
                                            stetitle2 = 0
                                            SameDistance = 0
                                            UpinDistance = 0
                                            DowninDistance = 0
                                            stakewon = 0
                                            tabler = 0
                                            morningrace1 = 0
                                            eveningrace1 = 0 
                                            prizemoney4 = 0
                                            tracklast = 0
                                            sametrack = 0
                                            prizemoneysame = 0
                                            prizemoneydown = 0
                                            prizemoneyup = 0
                                            trackthis = 0
                                            driverchange = 0
                                            trainerchange = 0
                                            countofdeath = 0
                                            Deathseat = 0
                                            Leading = 0
                                            countfront1 = 0
                                            countfront2 = 0
                                            countfront3 = 0
                                            countfront4 = 0
                                            countld1 = 0
                                            countld2 = 0
                                            countld3 = 0
                                            countld4 = 0
                                            RowLastStart = 0
                                            Standlaststart = 0
                                            stetitle2 = 0

                                        elif racenamelast is None:
                                            stetitle2 = 0
                                            Standlaststart = 0
                                            RowLastStart = 0
                                            firststarter = 1
                                            TrialLastStart = 0
                                            RowLastStart = 0
                                            laststart = 0 
                                            firststarter = 0
                                            startingprice1 = 0
                                            DistanceLastStart = 0
                                            Trainerlaststart = 0
                                            Driverlaststart = 0
                                            stetitle2 = 0
                                            SameDistance = 0
                                            UpinDistance = 0
                                            DowninDistance = 0
                                            stakewon = 0
                                            tabler = 0
                                            morningrace1 = 0
                                            eveningrace1 = 0 
                                            prizemoney4 = 0
                                            tracklast = 0
                                            sametrack = 0
                                            prizemoneysame = 0
                                            prizemoneydown = 0
                                            prizemoneyup = 0
                                            trackthis = 0
                                            driverchange = 0
                                            trainerchange = 0
                                            countofdeath = 0
                                            Deathseat = 0
                                            Leading = 0
                                            countfront1 = 0
                                            countfront2 = 0
                                            countfront3 = 0
                                            countfront4 = 0
                                            countld1 = 0
                                            countld2 = 0
                                            countld3 = 0
                                            countld4 = 0
                                            
                                        else:
                                            RowLastStart = tr.find('td', class_='handicap').find_next('td', class_='handicap').get_text()

                                            listof = ['FT', 'm']
                                            if any(x in RowLastStart for x in listof):
                                                RowLastStart = RowLastStart.split()[0]
                                                RowLastStart = RowLastStart.replace('m', '').strip()
                                                RowLastStart = RowLastStart.replace('FT', '0').replace('10m', '20').replace('20m', '40').replace('30m', '60').replace('40m', '80').replace('50m', '100').replace('60m', '120')
                                                Standlaststart = 1
                                                print(RowLastStart)
                                            else:
                                                RowLastStart = RowLastStart.strip()
                                                RowLastStart = RowLastStart.replace('Fr1', '1').replace('Fr2', '2').replace('Fr3', '3').replace('Fr4', '4').replace('Fr5', '5').replace('Fr6', '6').replace('Fr7', '7').replace('Fr8', '8').replace('Sr1', '11').replace('Sr2', '12').replace('Sr3', '13').replace('Sr4', '14').replace('Sr5', '15').replace('Sr6', '16').replace('Fr9', '9').replace('Fr10', '10').replace('Sr7', '17')
                                                Standlaststart = 0
                                            laststart = tr.find('td', class_='place').find_next('td', class_='place').get_text().strip()
                                            laststart = laststart.replace('*', '').replace('r', '0').replace('u', '0').replace('f', '0').replace('d', '0').replace('s', '').replace('n', '').replace('b', '')
                                            
                                            if tr.find('td', class_='startPrice').find_next('td', class_='startPrice') == None:
                                                startingprice1 = 0
                                            else:
                                                startingprice1 = tr.find('td', class_='startPrice').find_next('td', class_='startPrice').get_text()
                            
                                                startingprice1 = startingprice1.replace('$', '').replace('fav', '').replace('&nbsp;&nbsp;', '')
                                            
                                                try:
                                                    startingprice1 = float(startingprice1)
                                                except:
                                                    startingprice1 = 0
                                            
                                            stakewon = tr.find('td', class_='stake').get_text()
                                            stakewon = stakewon.replace('$', '').replace(',', '')
                                            stakewon = int(stakewon)
                                            trackthis = tr.find('td', class_='track').get_text()
                                            tracklast = tr.find('td', class_='track').find_next('td', class_='track').get_text()
                                            stetitle = tr.find(class_='stewardsTooltip').find_next(class_='stewardsTooltip')
                                            stetitle2 = stetitle.get('title')

                                            Trainerthisstart = tr.find('td', class_='trainer').get_text()
                                            Trainerlaststart = tr.find('td', class_='trainer').find_next('td', class_='trainer').get_text()
                                            Driverthisstart = tr.find('td', class_='driver').get_text()
                                            Driverlaststart = tr.find('td', class_='driver').find_next('td', class_='driver').get_text()
                                            DistanceLastStart = tr.find('td', class_='distance').find_next('td', class_='distance').get_text()
                                            DistanceLastStart = DistanceLastStart.replace('M', '').replace('S', '')
                                            DistanceLastStart = float(DistanceLastStart)
                                            if death in stetitle2:
                                                Deathseat = 1
                                            else:
                                                Deathseat = 0

                                            if leader in stetitle2:
                                                Leading = 1
                                            else:
                                                Leading = 0
                                            
                                            if Trainerlaststart == Trainerthisstart:
                                                trainerchange = 0
                                            else:
                                                trainerchange = 1
                                            if Driverthisstart == Driverlaststart:
                                                driverchange = 0
                                            else:
                                                driverchange = 1

                                            if tracklast == trackthis:
                                                sametrack = 1
                                            else:
                                                sametrack = 0
                                            
                                            if DistanceLastStart == race_distance1:
                                                SameDistance = 1
                                                UpinDistance = 0
                                                DowninDistance = 0

                                            elif DistanceLastStart > race_distance1:
                                                SameDistance = 0
                                                UpinDistance = 0
                                                DowninDistance = 1

                                            elif DistanceLastStart < race_distance1:
                                                SameDistance = 0
                                                UpinDistance = 1
                                                DowninDistance = 0
                                            
                                            lastrace1 = tr.find('a').find_next('a').find_next('a').get('href')
                                            if 'mp4' in lastrace1:
                                                lastrace = tr.find('a').find_next('a').get('href')

                                            elif 'field' in lastrace1:
                                                lastrace = tr.find('a').find_next('a').find_next('a').get('href')

                                            else:
                                                continue
                                            horseurl2 = base1_url + lastrace
                                            with requests.Session() as s:
                                            
                                                try:
                                                    webpage_response = s.get(horseurl2, headers=headers)
                                                except requests.exceptions.ConnectionError:
                                                    print(f'connect error : {horseurl2}')
                                                    exit(0)
                                                
                                                soup2 = bs(webpage_response.content, "html.parser")
                                                sleep(1)
                                                
                                                for td in soup2.find_all('a', href = horseweb):
                                                    lastsibling = td.find_previous('div', class_='forPrint')
                                                    tabler = lastsibling.find('td', class_='raceTime').get_text()
                                                    prizemoney4 = lastsibling.find('td', class_='raceInformation').get_text()
                                                    prizemoney4 = prizemoney4.split()[0]
                                                    prizemoney4 = prizemoney4.replace('$', '').replace(',', '')
                                                    prizemoney4 = prizemoney4.split('.')[0]
                                                    prizemoney4 = int(prizemoney4)
                                                    splitter1 = tabler.split(':')[0]
                                                    splitter1 = int(splitter1)
                                                    pmoram = racetime[-2:]
                                                    print(splitter1)
                                                    morning = 0
                                                    if pmoram == 'am':
                                                        morningrace1 = 1
                                                        eveningrace1 = 0
                                                    elif pmoram == 'pm' and splitter1 == 12:
                                                        morningrace1 = 1
                                                        eveningrace1 = 0
                                                    elif 6 > splitter1 and pmoram == 'pm':
                                                        morningrace1 = 1
                                                        eveningrace1 = 0
                                                    else:
                                                        eveningrace1 = 1
                                                        morningrace1 = 0

                                                    if prizemoney == prizemoney4:
                                                        prizemoneysame = 1
                                                        prizemoneyup = 0
                                                        prizemoneydown = 0

                                                    elif prizemoney > prizemoney4:
                                                        prizemoneysame = 0
                                                        prizemoneyup = 1
                                                        prizemoneydown = 0

                                                    elif prizemoney < prizemoney4:
                                                        prizemoneysame = 0
                                                        prizemoneyup = 0
                                                        prizemoneydown = 1

                                        date35 = datetime.strptime(dateprior, "%d %b %Y")
                                        place56 = tr.find('td', class_='place').get_text().strip()
                                        place56 = place56.replace('*', '').replace('r', '0').replace('u', '0').replace('f', '0').replace('d', '0').replace('s', '').replace('n', '').replace('b', '')
                                        if place56 == 1:
                                            BestWinningMile = tr.find('td', class_='winnerMileRate').get_text()
                                            winning += 1
                                        elif place56 == 2:
                                            seconding += 1
                                        elif place56 == 3:
                                            thirding += 1
                                        else:
                                            BestWinningMile = 0
                                    

                                        daysbetween3 = date25.date()
                                        daysbetween4 = date35.date()
                                        daysinbetweenruns = (daysbetween3 - daysbetween4).days
                                        print(daysinbetweenruns)
                                    else:
                                        daysinbetweenruns = 0

                                    margin = tr.find('td', class_='beaten').get_text().strip()
                                    margin = margin.replace('SHFHD', '0.03')
                                    margin = margin.replace('HFHD', '0.05')
                                    margin = margin.replace('HD', '0.10')
                                    margin = margin.replace('HFNK', '0.15')
                                    margin = margin.replace('NK', '0.20')
                                    margin = margin.replace('m', '')
                                    margin = margin.replace('DH', '0.01')
                            
                                    if margin == '':
                                        margin = 0
                                    elif winner == 1:
                                        marginwinner = float(margin)
                                        margin = 0
                                    else:
                                        margin = float(margin)
                                        marginwinner = 0

                                if time.strptime(date2, "%d %m %Y") < time.strptime (enddate1, "%d %m %Y"):
                                    racename = tr.find('td', class_='raceName').get_text()
                                    
                                    print(racename)
                                    if not "TRIAL" in racename:
                                        countofld = 0
                                        countofgs = 0
                                        strikeRateBroken = 0
                                        strikeRateGS = 0
                                        strikeRateLD = 0
                                        countofdeath = 0
                                        
                                    stakewon1 = tr.find('td', class_='stake').get_text()
                                    stakewon1 = stakewon1.replace('$', '').replace(',', '')
                                    stakewon1 = int(stakewon1)
                                    
                                    if stakewon1 is not None:
                                        totalprize += stakewon1
                                        runns += 1
                                    avestake = totalprize/runns
                                    #add up places here
                                    print(runns)
                                    place3 = tr.find('td', class_='place').get_text()
                                    place3 = place3.replace('*', '').replace('r', '0').replace('u', '0').replace('f', '0').replace('d', '0').replace('s', '').replace('n', '').replace('b', '')
                                    place3 = int(place3)
                                    if place3 is not None:
                                        howmany += place3
                                    aveplace = howmany/runns
                                    if place3 == 1:
                                        BestWinningMile = tr.find('td', class_='winnerMileRate').get_text()
                                        winning += 1
                                    elif place3 == 2:
                                        seconding += 1
                                    elif place3 == 3:
                                        thirding += 1
                                    else:
                                        BestWinningMile = 0
                                    lifetimeplace = winning + seconding + thirding 
                                    lifetimeplacepercent = lifetimeplace/runns
                                    winpercent = winning/runns

                                    marginbeaten1 = tr.find('td', class_='beaten').get_text().replace('m', '').replace('DH', '0.01')

                                    if margin == None or margin == '\n':
                                        continue

                                    if place3 == None or place3 == 0:
                                        continue

                                    elif place3 == '1':
                                        marginwin1 = tr.find('td', class_='beaten').get_text().replace('m', '').replace('DH', '0.01')
                                        marginwin1 = marginwin1.replace('SHFHD', '0.03')
                                        marginwin1 = marginwin1.replace('HFHD', '0.05')
                                        marginwin1 = marginwin1.replace('HD', '0.10')
                                        marginwin1 = marginwin1.replace('HFNK', '0.15')
                                        marginwin1 = marginwin1.replace('NK', '0.20')
                                        marginwin1 = float(marginwin1.strip())
                                        marginbeaten = 0
                                        
                                    elif marginbeaten1 == None or marginbeaten1 == '\n':
                                        marginbeaten = 0
                                    else:
                                        marginbeaten = marginbeaten1
                                        marginbeaten = marginbeaten.replace('SHFHD', '0.03')
                                        marginbeaten = marginbeaten.replace('HFHD', '0.05')
                                        marginbeaten = marginbeaten.replace('HD', '0.10')
                                        marginbeaten = marginbeaten.replace('HFNK', '0.15')
                                        marginbeaten = marginbeaten.replace('NK', '0.20')
                                        marginbeaten = float(marginbeaten.strip())
                                        marginwin1 = 0
                                    
                                    rowsdata1 = tr.find('td', class_='stewards').get_text()

                                    if rowsdata1 is None:
                                        countofld = 0
                                        countofgs = 0
                                        strikeRateBroken = 0
                                        strikeRateGS = 0
                                        strikeRateLD = 0
                                        continue
                                    handicaprows = tr.find('td', class_='handicap').get_text().strip()
                                    frontrows = handicaprows.replace('Fr1', '1').replace('Fr2', '1').replace('Fr3', '2').replace('Fr4', '2').replace('Fr5', '3').replace('Fr6', '3').replace('Fr7', '4').replace('Fr8', '4').replace('Sr1', '5').replace('Sr2', '5').replace('Sr3', '5').replace('Sr4', '5').replace('Sr5', '5').replace('Sr6', '5').replace('Fr9', '5').replace('Fr10', '5').replace('Sr7', '5')

            
                                    countofld = ' L '
                                    if 'Fr' in handicaprows:
                                        frontrow += 1
                                    else:
                                        secondrow += 1

                                    if countofld in rowsdata1:
                                        count += 1 

                                    if frontrows == '1' and countofld in rowsdata1:
                                        countld1 += 1
                                    elif frontrows == '1':
                                        countfront1 += 1

                                    if frontrows == '2' and countofld in rowsdata1:
                                        countld2 += 1
                                    elif frontrows == '2':
                                        countfront2 += 1

                                    if frontrows == '3' and countofld in rowsdata1:
                                        countld3 += 1
                                    elif frontrows == '3':
                                        countfront3 += 1

                                    if frontrows == '4' and countofld in rowsdata1:
                                        countld4 += 1
                                    elif frontrows == '4':
                                        countfront4 += 1

                                    countofgs = 'GS'
                                    
                                    if countofgs in rowsdata1:
                                        count1 += 1 
                                    countofbroken = ' B '
                                    
                                    if countofbroken in rowsdata1:
                                        count2 += 1 
                                    if death2 in rowsdata1:
                                        countofdeath += 1
                                        
                                    strikeRateBroken = count2/runns
                                    strikeRateGS = count1/runns
                                    strikeRateLD = count/runns
                            if "SCRATCHED" in driver:
                                continue
                            horseinfo = row.find('td', class_='horse_name nowrap')
                        
                            horseinfo = horseinfo.find('i')
                        

                            if horseinfo != None:
                                continue

                            horse_number = row.find('td', class_='horse_number')
                            horse_number = horse_number.text.replace('Form: ', '') if horse_number else ''
                            horsename = row.find('a', class_='horse_name_link')
                            horsename = horsename.text.replace('HorseName: ', '') if horsename else ''
                            horsename = horsename.replace(' NZ', '')

                            handicap = row.find('td', class_='hcp').get_text()
                            handicap = handicap.replace('\xa0', '')
                            if handicap != '':
                                handicap = handicap.replace('FT', '1').replace('10m', '20').replace('20m', '40').replace('30m', '60').replace('40m', '80').replace('50m', '100').replace('60m', '120')
                                handicap = handicap.replace('m', '1')
                                handicap = float(handicap)
                            else:
                                handicap = 0 
                            barrier = row.find('td', class_='barrier').get_text()
                            barrier = barrier.replace('Fr1', '1').replace('Fr2', '2').replace('Fr3', '3').replace('Fr4', '4').replace('Fr5', '5').replace('Fr6', '6').replace('Fr7', '7').replace('Fr8', '8').replace('Sr1', '11').replace('Sr2', '12').replace('Sr3', '13').replace('Sr4', '14').replace('Sr5', '15').replace('Sr6', '16').replace('Fr9', '9').replace('Fr10', '10').replace('Sr7', '17')

                            if row.find('td', class_='starting_price nowrap') == None:
                                startingprice = 0
                            else:
                                startingprice = row.find('td', class_='starting_price nowrap').get_text()
                            
                                startingprice = startingprice.replace('Â', '').replace('\xa0', '')
                                startingprice = startingprice.replace('$', '').replace('fav', '').replace('&nbsp;&nbsp;', '')
                                try:
                                    startingprice = float(startingprice)
                                except:
                                    startingprice = 0
                                    
                            data['DayCalender'].append(enddate2)
                            data['Venue'].append(tr2)
                            data['RaceNumber'].append(race_number)
                            data['RaceName'].append(race_name1)
                            data['RaceTitle'].append(race_title1)
                            data['RaceDistance'].append(race_distance1)
                            data['Place'].append(place)
                            data['HorseName'].append(horsename)
                            data['HorseID'].append(horse_id)
                            data['Age'].append(age)
                            data['Colour'].append(Colour1)
                            data['Sex'].append(Sex)
                            data['Sire'].append(Sire)
                            data['Prizemoney'].append(prizemoney)
                            data['Handicap'].append(handicap)
                            data['Row'].append(barrier)
                            data['Trainer'].append(trainer)
                            data['Driver'].append(driver)
                            data['Margin'].append(margin)
                            data['MarginWinner'].append(marginwinner)
                            data['StartingOdds'].append(startingprice)
                            data['StewardsComments'].append(stewardscom)
                            data['laststart'].append(laststart)
                            data['start2'].append(start2)
                            data['start3'].append(start3)
                            data['start4'].append(start4)
                            data['Firststarter'].append(firststarter)
                            data['Placer'].append(placer)
                            data['Winner'].append(winner)

                            data['LifetimeRuns'].append(runns)
                            data['RecentRuns'].append(runs1)
                            data['LifetimeWins'].append(winning)
                            data['LifetimeSeconds'].append(seconding)
                            data['LifetimeThirds'].append(thirding)
                            data['LifetimePlacings'].append(lifetimeplace)
                            data['LifetimeWinPercent'].append(winpercent)
                            data['LifetimePlacePercent'].append(lifetimeplacepercent)
                            data['RecentPlacePercent'].append(placepercent1)
                            data['RecentWinPercent'].append(winpercent1)
                            data['RecentPlacings'].append(placings1)
                            data['RecentThirds'].append(thirds1)
                            data['RecentSeconds'].append(seconds1)
                            data['RecentWins'].append(wins1)
                            data['Class'].append(Class)
                            data['Dayssincelast'].append(daysinbetweenruns)
                            data['Gatespeedstrikerate'].append(strikeRateGS)
                            data['broken'].append(strikeRateBroken)
                            data['LeaderStrikeRate'].append(strikeRateLD)
                            
                            data['RowLastStart'].append(RowLastStart)
                            data['DistanceLastStart'].append(DistanceLastStart)
                            data['TrainerLastStart'].append(Trainerlaststart)
                            data['DriverLastStart'].append(Driverlaststart)
                            data['StewardsFull'].append(stetitle2)
                            data['BestWinningMile'].append(BestWinningMile)
                            data['samedistance'].append(SameDistance)
                            data['UpInDistance'].append(UpinDistance)
                            data['DownInDistance'].append(DowninDistance)
                            data['TimeLead'].append(count)
                            data['TimeGateSpeed'].append(count1)
                            data['TimeBroken'].append(count2)
                            data['PrizemoneyLastStart'].append(stakewon)
                            data['Triallaststart'].append(TrialLastStart)
                            data['Standlaststart'].append(Standlaststart)
                            data['racetime'].append(racetime)
                            data['morningrace'].append(morningrace)
                            data['eveningrace'].append(eveningrace)
                            data['Timelastrace'].append(tabler)
                            data['morningracelaststart'].append(morningrace1)
                            data['eveningracelaststart'].append(eveningrace1)
                            data['Prizemoneylastrace'].append(prizemoney4)
                            data['tracknamelaststart'].append(tracklast)
                            data['startingprice1'].append(startingprice1)
                            data['avestake'].append(avestake)
                            data['totalprize'].append(totalprize)
                            data['sametrack'].append(sametrack)
                            data['prizemoneysame'].append(prizemoneysame)
                            data['prizemoneyup'].append(prizemoneyup)
                            data['prizemoneydown'].append(prizemoneydown)
                            data['Tracklaststart'].append(trackthis)
                            data['driverchange'].append(driverchange)
                            data['trainerchange'].append(trainerchange)
                            data['trials'].append(trials)
                            data['aveplace'].append(aveplace)
                            data['countofdeath'].append(countofdeath)
                            data['Deathseat'].append(Deathseat)
                            data['Leading'].append(Leading)
                            data['countfront1'].append(countfront1)
                            data['countfront2'].append(countfront2)
                            data['countfront3'].append(countfront3)
                            data['countfront4'].append(countfront4)
                            data['countld1'].append(countld1)
                            data['countld2'].append(countld2)
                            data['countld3'].append(countld3)
                            data['countld4'].append(countld4)
                            data['frontrow'].append(frontrow)
                            data['secondrow'].append(secondrow)

                            table = pd.DataFrame(data, columns=['DayCalender','Venue','RaceNumber', 'RaceName', 'RaceTitle', 'RaceDistance', 'Place', 'HorseName', 'HorseID', 'Age', 'Colour', 'Sire', 'Sex', 'Prizemoney', 'Handicap', 'Row', 'Trainer', 'Driver', 'Margin', 'MarginWinner', 'StartingOdds', 'StewardsComments', 'laststart', 'start2', 'start3', 'start4', 'Firststarter', 'LifetimeRuns', 'RecentRuns', 'LifetimeWins', 'LifetimeSeconds', 'LifetimeThirds', 'LifetimePlacings', 'LifetimeWinPercent', 'LifetimePlacePercent', 'RecentPlacePercent', 'RecentWinPercent', 'RecentPlacings', 'RecentThirds', 'RecentSeconds', 'RecentWins', 'Class', 'Dayssincelast', 'Gatespeedstrikerate', 'broken', 'LeaderStrikeRate', 'Placer', 'Winner', 'Tracklaststart', 'RowLastStart', 'DistanceLastStart', 'TrainerLastStart', 'DriverLastStart', 'StewardsFull', 'BestWinningMile', 'samedistance', 'UpInDistance', 'DownInDistance', 'TimeLead', 'TimeGateSpeed', 'TimeBroken', 'PrizemoneyLastStart', 'Triallaststart', 'Standlaststart', 'racetime', 'morningrace', 'eveningrace', 'Timelastrace', 'morningracelaststart', 'eveningracelaststart', 'Prizemoneylastrace', 'tracknamelaststart', 'startingprice1', 'avestake', 'totalprize', 'sametrack', 'prizemoneysame', 'prizemoneyup', 'prizemoneydown', 'driverchange', 'trainerchange', 'trials', 'aveplace', 'countofdeath', 'Deathseat', 'Leading', 'countfront1', 'countfront2', 'countfront3', 'countfront4', 'countld1', 'countld2', 'countld3', 'countld4', 'frontrow', 'secondrow'])
                            
                            table['NumberTrainer'] = table['Trainer'].map(trainerRunnerCount)
                            
                            print(table)
                            for index,row in table.iterrows():
                                mycursor.execute('INSERT INTO horses2 (DayCalender, Venue, RaceNumber, RaceName, RaceTitle, RaceDistance, Place, HorseName, HorseID, Age, Colour, Sire, Sex, Prizemoney, Handicap, Row, Trainer, Driver, Margin, MarginWinner, StartingOdds, StewardsComments, laststart, start2, start3, start4, Firststarter, LifetimeRuns, RecentRuns, LifetimeWins, LifetimeSeconds, LifetimeThirds, LifetimePlacings, LifetimeWinPercent, LifetimePlacePercent, RecentPlacePercent, RecentWinPercent, RecentPlacings, RecentThirds, RecentSeconds, RecentWins, Class, Dayssincelast, Gatespeedstrikerate, broken, LeaderStrikeRate, Placer, Winner, RowLastStart, DistanceLastStart, TrainerLastStart, DriverLastStart, StewardsFull, BestWinningMile, samedistance, UpInDistance, DownInDistance, TimeLead, TimeGateSpeed, TimeBroken, PrizemoneyLastStart, Triallaststart, Standlaststart, racetime, morningrace, eveningrace, Timelastrace, morningracelaststart, eveningracelaststart, Prizemoneylastrace, tracknamelaststart, startingprice1, avestake, totalprize, sametrack, prizemoneysame, prizemoneyup, prizemoneydown, driverchange, trainerchange, Tracklaststart, trials, aveplace, countofdeath, Deathseat, Leading, countfront1, countfront2, countfront3, countfront4, countld1, countld2, countld3, countld4, frontrow, secondrow, NumberTrainer) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (row.DayCalender, row.Venue, row.RaceNumber, row.RaceName, row.RaceTitle, row.RaceDistance, row.Place, row.HorseName, row.HorseID, row.Age, row.Colour, row.Sire, row.Sex, row.Prizemoney, row.Handicap, row.Row, row.Trainer, row.Driver, row.Margin, row.MarginWinner, row.StartingOdds, row.StewardsComments, row.laststart, row.start2, row.start3, row.start4, row.Firststarter, row.LifetimeRuns, row.RecentRuns, row.LifetimeWins, row.LifetimeSeconds, row.LifetimeThirds, row.LifetimePlacings, row.LifetimeWinPercent, row.LifetimePlacePercent, row.RecentPlacePercent, row.RecentWinPercent, row.RecentPlacings, row.RecentThirds, row.RecentSeconds, row.RecentWins, row.Class, row.Dayssincelast, row.Gatespeedstrikerate, row.broken, row.LeaderStrikeRate, row.Placer, row.Winner, row.RowLastStart, row.DistanceLastStart, row.TrainerLastStart, row.DriverLastStart, row.StewardsFull, row.BestWinningMile, row.samedistance, row.UpInDistance, row.DownInDistance, row.TimeLead, row.TimeGateSpeed, row.TimeBroken, row.PrizemoneyLastStart, row.Triallaststart, row.Standlaststart, row.racetime, row.morningrace, row.eveningrace, row.Timelastrace, row.morningracelaststart, row.eveningracelaststart, row.Prizemoneylastrace, row.tracknamelaststart, row.startingprice1, row.avestake, row.totalprize, row.sametrack, row.prizemoneysame, row.prizemoneyup, row.prizemoneydown, row.driverchange, row.trainerchange, row.Tracklaststart, row.trials, row.aveplace, row.countofdeath, row.Deathseat, row.Leading, row.countfront1, row.countfront2, row.countfront3, row.countfront4, row.countld1, row.countld2, row.countld3, row.countld4, row.frontrow, row.secondrow, row.NumberTrainer))

                            conn.commit()
                            print(mycursor.rowcount, "records inserted")

# start scraping horse information.
start_horse_scraping(startdate, enddate)