import pandas as pd
import os
import re
from datetime import datetime
from datetime import timedelta

def read_conferences_file():
    file_path = 'Meta/conferences.csv'
    
    # Define the expected columns in the original file and their new names
    expected_columns = {
        'shorthand name': 'id',
        'full name': 'fullName',
        'division name': 'divisions',
        'flair': 'flair',  # Assuming flair is required and should be validated
        'championship game': 'hasConferenceChampGame',
        'conference games count': 'doConferenceGamesCount'
    }
    
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        
        # Check if the DataFrame has the expected columns
        if list(df.columns) != list(expected_columns.keys()):
            raise ValueError(f"Expected columns {list(expected_columns.keys())}, but got {list(df.columns)}")

        # Rename the columns
        df.rename(columns=expected_columns, inplace=True)

        # Convert 'divisions' column to a list
        df['divisions'] = df['divisions'].apply(lambda x: [x] if pd.notna(x) else [])

        # Group by the other columns and aggregate divisions
        df = df.groupby(['id', 'fullName', 'flair', 'hasConferenceChampGame', 'doConferenceGamesCount'], as_index=False).agg({
            'divisions': lambda x: [i for sublist in x for i in sublist]  # Flatten list of lists
        })

        # Validate data types
        type_validations = {
            'id': str,
            'fullName': str,
            'divisions': list,
            'flair': str,
            'hasConferenceChampGame': bool,
            'doConferenceGamesCount': bool
        }

        for column, expected_type in type_validations.items():
            if not df[column].apply(lambda x: isinstance(x, expected_type)).all():
                raise TypeError(f"Column '{column}' does not match expected type {expected_type.__name__}")

        # Validate specific content rules
        if df.isnull().values.any():
            raise ValueError("DataFrame contains NaN values, which are not allowed")

        if df['id'].duplicated().any():
            raise ValueError("Duplicate shorthand names found")

        if df['fullName'].duplicated().any():
            raise ValueError("Duplicate full names found")

        # If all checks pass, return the DataFrame
        return df
    
    except FileNotFoundError:
        print(f"Error: The file at path {file_path} was not found.")
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
    except pd.errors.ParserError:
        print("Error: The file contains parsing errors.")
    except Exception as e:
        print(f"Error: {e}")



def read_teams_file():
    file_path = 'Meta/teams.csv'
    
    # Define the expected columns in the original file and their expected types
    expected_columns = {
        'official name': 'id',
        'display name': 'displayName',
        'Whatifsports name': 'whatifsportsName',
        'flair': 'flair',
        'conference': 'conferenceID',
        'conference division': 'conferenceDivision',
        'year': 'year',
        'record': 'record',
        'initial ranking points': 'initialRankingPoints',
        'head coach': 'headCoachName',
        'punter': 'punterName',
        'kick returner': 'kickReturnerName',
        'punt returner': 'puntReturnerName'
    }

    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Check if the DataFrame has the expected columns
        if list(df.columns) != list(expected_columns.keys()):
            raise ValueError(f"Expected columns {list(expected_columns.keys())}, but got {list(df.columns)}")

        # Rename the columns
        df.rename(columns=expected_columns, inplace=True)

        # Fill NaN values in specific columns with empty strings or default values for consistency
        columns_to_fill_empty_string = [
            'flair', 'conferenceDivision', 'record', 'headCoachName',
            'punterName', 'kickReturnerName', 'puntReturnerName'
        ]
        df[columns_to_fill_empty_string] = df[columns_to_fill_empty_string].fillna('')

        # Fill NaN values in initialRankingPoints with -999
        df['initialRankingPoints'] = df['initialRankingPoints'].fillna(-999)

        # Validate data types
        type_validations = {
            'id': str,
            'displayName': str,
            'whatifsportsName': str,
            'flair': str,
            'conferenceID': str,
            'conferenceDivision': str,
            'year': int,
            'record': str,
            'initialRankingPoints': float,
            'headCoachName': str,
            'punterName': str,
            'kickReturnerName': str,
            'puntReturnerName': str
        }

        for column, expected_type in type_validations.items():
            if not df[column].apply(lambda x: isinstance(x, expected_type)).all():
                raise TypeError(f"Column '{column}' does not match expected type {expected_type.__name__}")

        # Validate specific content rules
        if df.isnull().values.any():
            raise ValueError("DataFrame contains NaN values, which are not allowed")

        # Check for duplicate team IDs
        duplicate_ids = df[df.duplicated(['id'], keep=False)]['id']
        if not duplicate_ids.empty:
            duplicate_teams = df[df['id'].isin(duplicate_ids)][['id', 'displayName']]
            raise ValueError(f"Duplicate team ids found: {duplicate_teams.to_dict(orient='records')}")

        # If all checks pass, return the DataFrame
        return df

    except FileNotFoundError:
        print(f"Error: The file at path {file_path} was not found.")
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
    except pd.errors.ParserError:
        print("Error: The file contains parsing errors.")
    except Exception as e:
        print(f"Error: {e}")



def parse_whatifsports_box_score(content):
    # create a dictionary to store the box score data
    game_data = {}

    # assert that content has a fully filled out box score, otherwise I need to add in those sections by hand
    if len(re.findall(r'^Rushing$', content, re.MULTILINE)) != 2:
        raise ValueError("Content does not contain 2 lines starting with 'Rushing'")
    if len(re.findall(r'^Receiving$', content, re.MULTILINE)) != 2:
        raise ValueError("Content does not contain 2 lines starting with 'Receiving'")
    if len(re.findall(r'^Passing$', content, re.MULTILINE)) != 2:
        raise ValueError("Content does not contain 2 lines starting with 'Passing'")
    if len(re.findall(r'^Defensive$', content, re.MULTILINE)) != 2:
        raise ValueError("Content does not contain 2 lines starting with 'Defensive'")
    if len(re.findall(r'^Field Goals$', content, re.MULTILINE)) != 2:
        raise ValueError("Content does not contain 2 lines starting with 'Field Goals'")

    # first line has {away team year} {away team name} at {home team year} {home team name}, store the team names
    # get first line of content
    first_line = content.split('\n')[0]
    # split the first line by ' at '
    teams = first_line.split(' at ')
    # store the away team year and name
    away_team = teams[0].split(' ')
    game_data['awayTeamYear'] = away_team[0]
    game_data['awayTeamName'] = ' '.join(away_team[1:])
    # store the home team year and name
    home_team = teams[1].split(' ')
    game_data['homeTeamYear'] = home_team[0]

    # second line contains headers like "1st 2nd 3rd 4th" to indicate quarters. If there's an OT1, OT2, OT, or something along those lines, it means the game went to overtime
    # get the line that starts with "Final -" and check if it contains "OT"
    final_line = re.search(r'^Final -.*', content, re.MULTILINE)
    if final_line and "OT" in final_line.group():
        game_data['wasOvertime'] = True
    else:
        game_data['wasOvertime'] = False
    
    # The 3rd line is the quarter-by-quarter scoring for the away team. The last number in the line is their final score.
    # get the line under the line that starts with "Final -" and get the last integer in the line
    away_team_score_line = re.search(r'^Final -.*[\r\n]+([^\r\n]+)[\r\n]+([^\r\n]+)', content, re.MULTILINE).group(1)
    game_data['awayTeamScore'] = int(away_team_score_line.split()[-1])

    # The 4th line is the quarter-by-quarter scoring for the home team. The last number in the line is their final score.
    # get the line under the line that starts with "Final -" and get the last integer in the line
    home_team_score_line = re.search(r'^Final -.*[\r\n]+([^\r\n]+)[\r\n]+([^\r\n]+)', content, re.MULTILINE).group(2)
    game_data['homeTeamScore'] = int(home_team_score_line.split()[-1])

    # Take the line that starts with "First Downs". The away team first downs is the following number, the home team first downs is the second number after that.
    # get the line that starts with "First Downs" and get the 2 numbers after it
    first_downs_line = re.search(r'^First Downs.*', content, re.MULTILINE)
    first_downs = first_downs_line.group().split()
    game_data['awayTeamTotalFirstDowns'] = int(first_downs[2])
    game_data['homeTeamTotalFirstDowns'] = int(first_downs[3])

    # The next line is " - Rushing" and this indicates the number of rushing first downs, again it goes away team then home team.
    # get the line that starts with "- Rushing" and get the 2 numbers after it
    rushing_first_downs_line = re.search(r'^- Rushing.*', content, re.MULTILINE)
    rushing_first_downs = rushing_first_downs_line.group().split()
    game_data['awayTeamRushingFirstDowns'] = int(rushing_first_downs[2])
    game_data['homeTeamRushingFirstDowns'] = int(rushing_first_downs[3])

    # After that is "- Passing" and it's the passing first downs, with the first number being away team and second being home team.
    # get the line that starts with "- Passing" and get the 2 numbers after it
    passing_first_downs_line = re.search(r'^- Passing.*', content, re.MULTILINE)
    passing_first_downs = passing_first_downs_line.group().split()
    game_data['awayTeamPassingFirstDowns'] = int(passing_first_downs[2])
    game_data['homeTeamPassingFirstDowns'] = int(passing_first_downs[3])

    # "- Penalty" follows the same logic.
    # get the line that starts with "- Penalty" and get the 2 numbers after it
    penalty_first_downs_line = re.search(r'^- Penalty.*', content, re.MULTILINE)
    penalty_first_downs = penalty_first_downs_line.group().split()
    game_data['awayTeamPenaltyFirstDowns'] = int(penalty_first_downs[2])
    game_data['homeTeamPenaltyFirstDowns'] = int(penalty_first_downs[3])

    # "3rd Down Eff" is formatted as {away 3rd down conversions}/{away 3rd down attempts} {home 3rd down conversions}/{home 3rd down attempts}
    # get the line that starts with "3rd Down Eff" and get the 4 numbers after it
    third_down_eff_line = re.search(r'^3rd Down Eff.*', content, re.MULTILINE)
    third_down_eff = third_down_eff_line.group().split()
    game_data['awayTeam3rdDownConversions'] = int(third_down_eff[3].split('/')[0])
    game_data['awayTeam3rdDownAttempts'] = int(third_down_eff[3].split('/')[1])
    game_data['homeTeam3rdDownConversions'] = int(third_down_eff[4].split('/')[0])
    game_data['homeTeam3rdDownAttempts'] = int(third_down_eff[4].split('/')[1])

    # "4th Down Eff" follows the same logic as 3rd Down Eff but for 4th downs.
    # get the line that starts with "4th Down Eff" and get the 4 numbers after it
    fourth_down_eff_line = re.search(r'^4th Down Eff.*', content, re.MULTILINE)
    fourth_down_eff = fourth_down_eff_line.group().split()
    game_data['awayTeam4thDownConversions'] = int(fourth_down_eff[3].split('/')[0])
    game_data['awayTeam4thDownAttempts'] = int(fourth_down_eff[3].split('/')[1])
    game_data['homeTeam4thDownConversions'] = int(fourth_down_eff[4].split('/')[0])
    game_data['homeTeam4thDownAttempts'] = int(fourth_down_eff[4].split('/')[1])

    # "Rushes-Yards" is formatted as {away carries}-{away rushing yards} {home carries}-{home rushing yards}
    # get the line that starts with "Rushes-Yards" and get the 4 numbers after it
    rushes_yards_line = re.search(r'^Rushes-Yards.*', content, re.MULTILINE)
    rushes_yards = rushes_yards_line.group().split()
    game_data['awayTeamCarries'] = int(rushes_yards[1].split('-')[0])
    game_data['awayTeamRushingYards'] = int(rushes_yards[1].split('-')[1])
    game_data['homeTeamCarries'] = int(rushes_yards[2].split('-')[0])
    game_data['homeTeamRushingYards'] = int(rushes_yards[2].split('-')[1])

    # "Comp-Att-Int" is formatted as {away completions}-{away pass attempts}-{away interceptions thrown} {home completions}-{home pass attempts}-{home interceptions thrown}
    # get the line that starts with "Comp-Att-Int" and get the 6 numbers after it
    comp_att_int_line = re.search(r'^Comp-Att-Int.*', content, re.MULTILINE)
    comp_att_int = comp_att_int_line.group().split()
    game_data['awayTeamCompletions'] = int(comp_att_int[1].split('-')[0])
    game_data['awayTeamPassAttempts'] = int(comp_att_int[1].split('-')[1])
    game_data['awayTeamInterceptionsThrown'] = int(comp_att_int[1].split('-')[2])
    game_data['homeTeamCompletions'] = int(comp_att_int[2].split('-')[0])
    game_data['homeTeamPassAttempts'] = int(comp_att_int[2].split('-')[1])
    game_data['homeTeamInterceptionsThrown'] = int(comp_att_int[2].split('-')[2])

    # "Passing Yards" is formatted as {away passing yards} {home passing yards}
    # get the line that starts with "Passing Yards" and get the 2 numbers after it
    passing_yards_line = re.search(r'^Passing Yards.*', content, re.MULTILINE)
    passing_yards = passing_yards_line.group().split()
    game_data['awayTeamPassingYards'] = int(passing_yards[2])
    game_data['homeTeamPassingYards'] = int(passing_yards[3])

    # "Sacks-Yards" is formatted as {away sacks allowed}-{away sacks allowed yardage} {home sacks allowed}-{home sacks allowed yardage}
    # get the line that starts with "Sacks-Yards" and get the 4 numbers after it
    sacks_yards_line = re.search(r'^Sacks-Yards.*', content, re.MULTILINE)
    sacks_yards = sacks_yards_line.group().split()
    game_data['awayTeamSacksAllowed'] = int(sacks_yards[1].split('-')[0])
    game_data['awayTeamSacksAllowedYards'] = int(sacks_yards[1].split('-')[1])
    game_data['homeTeamSacksAllowed'] = int(sacks_yards[2].split('-')[0])
    game_data['homeTeamSacksAllowedYards'] = int(sacks_yards[2].split('-')[1])

    # "Fumbles-Lost" is formatted as {away fumbles}-{away fumbles lost} {home fumbles}-{home fumbles lost}
    # get the line that starts with "Fumbles-Lost" and get the 4 numbers after it
    fumbles_lost_line = re.search(r'^Fumbles-Lost.*', content, re.MULTILINE)
    fumbles_lost = fumbles_lost_line.group().split()
    game_data['awayTeamFumbles'] = int(fumbles_lost[1].split('-')[0])
    game_data['awayTeamFumblesLost'] = int(fumbles_lost[1].split('-')[1])
    game_data['homeTeamFumbles'] = int(fumbles_lost[2].split('-')[0])
    game_data['homeTeamFumblesLost'] = int(fumbles_lost[2].split('-')[1])

    # "Punts-Avg" is formatted as {away punts}-{away punt average} {home punts}-{home punt average} and to get the away and home punt yardage, you can multiply the average by the number of punts, and round to the nearest integer.
    # get the line that starts with "Punts-Avg" and get the 4 numbers after it
    punts_avg_line = re.search(r'^Punts-Avg.*', content, re.MULTILINE)
    punts_avg = punts_avg_line.group().split()
    game_data['awayTeamPunts'] = int(punts_avg[1].split('-')[0])
    game_data['awayTeamPuntYards'] = round(float(punts_avg[1].split('-')[1]) * game_data['awayTeamPunts'])
    game_data['homeTeamPunts'] = int(punts_avg[2].split('-')[0])
    game_data['homeTeamPuntYards'] = round(float(punts_avg[2].split('-')[1]) * game_data['homeTeamPunts'])

    # "KR-Avg" is formatted as {away kick returns}-{away kick return yards} {home kick returns}-{home kick return yards} and like punts, we can multiply average by attempts to get the yardage.
    # get the line that starts with "KR-Avg" and get the 4 numbers after it
    kr_avg_line = re.search(r'^KR-Avg.*', content, re.MULTILINE)
    kr_avg = kr_avg_line.group().split()
    game_data['awayTeamKickReturns'] = int(kr_avg[1].split('-')[0])
    game_data['awayTeamKickReturnYards'] = round(float(kr_avg[1].split('-')[1]) * game_data['awayTeamKickReturns'])
    game_data['homeTeamKickReturns'] = int(kr_avg[2].split('-')[0])
    game_data['homeTeamKickReturnYards'] = round(float(kr_avg[2].split('-')[1]) * game_data['homeTeamKickReturns'])

    # "PR-Avg" follows the same logic as KR-Avg but for punt returns.
    # get the line that starts with "PR-Avg" and get the 4 numbers after it
    pr_avg_line = re.search(r'^PR-Avg.*', content, re.MULTILINE)
    pr_avg = pr_avg_line.group().split()
    game_data['awayTeamPuntReturns'] = int(pr_avg[1].split('-')[0])
    game_data['awayTeamPuntReturnYards'] = round(float(pr_avg[1].split('-')[1]) * game_data['awayTeamPuntReturns'])
    game_data['homeTeamPuntReturns'] = int(pr_avg[2].split('-')[0])
    game_data['homeTeamPuntReturnYards'] = round(float(pr_avg[2].split('-')[1]) * game_data['homeTeamPuntReturns'])

    # "Penalties-Yard" is formatted as {away penalties}-{away penalty yards} {home penalties}-{home penalty yards}
    # get the line that starts with "Penalties-Yard" and get the 4 numbers after it
    penalties_yard_line = re.search(r'^Penalties-Yard.*', content, re.MULTILINE)
    penalties_yard = penalties_yard_line.group().split()
    game_data['awayTeamPenalties'] = int(penalties_yard[1].split('-')[0])
    game_data['awayTeamPenaltyYards'] = int(penalties_yard[1].split('-')[1])
    game_data['homeTeamPenalties'] = int(penalties_yard[2].split('-')[0])
    game_data['homeTeamPenaltyYards'] = int(penalties_yard[2].split('-')[1])

    # "Time of Possession" is formatted as {away time of possession} {home time of possession}
    # get the line that starts with "Time of Possession" and get the 2 numbers after it and store them as datetime objects
    time_of_possession_line = re.search(r'^Time of Possession.*', content, re.MULTILINE)
    time_of_possession = time_of_possession_line.group().split()
    game_data['awayTimeOfPossession'] = datetime.strptime(time_of_possession[3], '%M:%S')
    minutes, seconds = map(int, time_of_possession[3].split(':'))
    game_data['awayTimeOfPossession'] = timedelta(minutes=minutes, seconds=seconds)
    game_data['homeTimeOfPossession'] = datetime.strptime(time_of_possession[4], '%M:%S')
    minutes, seconds = map(int, time_of_possession[4].split(':'))
    game_data['homeTimeOfPossession'] = timedelta(minutes=minutes, seconds=seconds)

    # The first subsection will be "Rushing" and the away team stats will come first. The next row is the " 	Att	Yds	20+	L	TD" and this is just the header telling you how to interpret the stats, we can ignore it. The next lines are formatted as follows:
    # {team year abbreviated} {player name} {rush attempts} {rush yards} {20 plus rush yard attempts} {longest rush} {rushing touchdowns}
    # and keep in mind that there might be multiple players, we keep reading these player rushing stats until we reach the next section "Rushing" which then will have the home team player rushing stats following it.
    # get the lines from the first line that starts with "Rushing" to the next line that starts with "Rushing"
    rushing_stats_lines = re.search(r'^Rushing(.*)Rushing', content, flags=re.DOTALL|re.MULTILINE).group(1)
    game_data.setdefault('playerAwayRushingStats', [])
    # iterate over each line in rushing_stats_lines except for the first 2 and last 2 lines
    for line in rushing_stats_lines.split('\n')[2:-2]:
        # split the line by spaces
        stats = line.split()
        # store the player rushing stats
        player_rushing_stats = {
            'playerName': ' '.join(stats[1:-5]),
            'carries': int(stats[-5]),
            'rushingYards': int(stats[-4]),
            '20PlusYardCarries': int(stats[-3]),
            'longestRush': int(stats[-2]),
            'rushingTouchdowns': int(stats[-1])
        }
        # store the player rushing stats in the game_data dictionary
        game_data.get('playerAwayRushingStats').append(player_rushing_stats)

    # Home team player rushing stats will follow the away team player rushing stats
    rushing_stats_lines = re.search(r'^Rushing.*Rushing(.*)^Receiving.*Receiving', content, flags=re.DOTALL|re.MULTILINE).group(1)
    game_data.setdefault('playerHomeRushingStats', [])
    # iterate over each line in rushing_stats_lines except for the first and last lines
    for line in rushing_stats_lines.split('\n')[2:-1]:
        # split the line by spaces
        stats = line.split()
        # store the player rushing stats
        player_rushing_stats = {
            'playerName': ' '.join(stats[1:-5]),
            'carries': int(stats[-5]),
            'rushingYards': int(stats[-4]),
            '20PlusYardCarries': int(stats[-3]),
            'longestRush': int(stats[-2]),
            'rushingTouchdowns': int(stats[-1])
        }
        # store the player rushing stats in the game_data dictionary
        game_data.get('playerHomeRushingStats').append(player_rushing_stats)
    
    # "Receiving" eventually comes up, with the away team first again. Player stats follow the format:
    # {team year abbreviated} {player name} {receptions} {receiving yards} {catches of 20 plus yards} {catches of 40 plus yards} {longest catch} {touchdown catches}
    # get the lines from the first line that starts with "Receiving" to the next line that starts with "Receiving"
    receiving_stats_lines = re.search(r'^Receiving(.*)Receiving', content, flags=re.DOTALL|re.MULTILINE).group(1)
    game_data.setdefault('playerAwayReceivingStats', [])
    # iterate over each line in receiving_stats_lines except for the first 2 lines and last line
    for line in receiving_stats_lines.split('\n')[2:-1]:
        # split the line by spaces
        stats = line.split()
        # store the player receiving stats
        player_receiving_stats = {
            'playerName': ' '.join(stats[1:-6]),
            'receptions': int(stats[-6]),
            'receivingYards': int(stats[-5]),
            '20PlusYardReceptions': int(stats[-4]),
            '40PlusYardReceptions': int(stats[-3]),
            'longestReception': int(stats[-2]),
            'receivingTouchdowns': int(stats[-1])
        }
        # store the player receiving stats in the game_data dictionary
        game_data.get('playerAwayReceivingStats').append(player_receiving_stats)
    
    # Home team player receiving stats will follow the away team player receiving stats
    receiving_stats_lines = re.search(r'^Receiving.*Receiving(.*)^Passing.*Passing', content, flags=re.DOTALL|re.MULTILINE).group(1)
    game_data.setdefault('playerHomeReceivingStats', [])
    # iterate over each line in receiving_stats_lines except for the first and last lines
    for line in receiving_stats_lines.split('\n')[2:-1]:
        # split the line by spaces
        stats = line.split()
        # store the player receiving stats
        player_receiving_stats = {
            'playerName': ' '.join(stats[1:-6]),
            'receptions': int(stats[-6]),
            'receivingYards': int(stats[-5]),
            '20PlusYardReceptions': int(stats[-4]),
            '40PlusYardReceptions': int(stats[-3]),
            'longestReception': int(stats[-2]),
            'receivingTouchdowns': int(stats[-1])
        }
        # store the player receiving stats in the game_data dictionary
        game_data.get('playerHomeReceivingStats').append(player_receiving_stats)
    
    # "Passing" comes up after, with the following format:
    # {team year abbreviated} {player name} {pass completions} {pass attempts} {pass yards} {pass touchdowns} {interceptions thrown}
    # get the lines from the first line that starts with "Passing" to the next line that starts with "Passing"
    passing_stats_lines = re.search(r'^Passing$(.*)Passing', content, flags=re.DOTALL|re.MULTILINE).group(1)
    game_data.setdefault('playerAwayPassingStats', [])
    # iterate over each line in passing_stats_lines except for the first 2 lines and last line
    for line in passing_stats_lines.split('\n')[2:-1]:
        # split the line by spaces
        stats = line.split()
        # store the player passing stats
        player_passing_stats = {
            'playerName': ' '.join(stats[1:-5]),
            'passCompletions': int(stats[-5]),
            'passAttempts': int(stats[-4]),
            'passingYards': int(stats[-3]),
            'passingTouchdowns': int(stats[-2]),
            'interceptionsThrown': int(stats[-1])
        }
        # store the player passing stats in the game_data dictionary
        game_data.get('playerAwayPassingStats').append(player_passing_stats)
    
    # Home team player passing stats will follow the away team player passing stats
    passing_stats_lines = re.search(r'^Passing.*Passing(.*)^Defensive.*Defensive', content, flags=re.DOTALL|re.MULTILINE).group(1)
    game_data.setdefault('playerHomePassingStats', [])
    # iterate over each line in passing_stats_lines except for the first and last lines
    for line in passing_stats_lines.split('\n')[2:-1]:
        # split the line by spaces
        stats = line.split()
        # store the player passing stats
        player_passing_stats = {
            'playerName': ' '.join(stats[1:-5]),
            'passCompletions': int(stats[-5]),
            'passAttempts': int(stats[-4]),
            'passingYards': int(stats[-3]),
            'passingTouchdowns': int(stats[-2]),
            'interceptionsThrown': int(stats[-1])
        }
        # store the player passing stats in the game_data dictionary
        game_data.get('playerHomePassingStats').append(player_passing_stats)
    
    # "Defensive" format:
    # {team year abbreviated} {player name} {sacks} {interceptions}
    # get the lines from the first line that starts with "Defensive" to the next line that starts with "Defensive"
    defensive_stats_lines = re.search(r'^Defensive$(.*)Defensive', content, flags=re.DOTALL|re.MULTILINE).group(1)
    game_data.setdefault('playerAwayDefensiveStats', [])
    # iterate over each line in defensive_stats_lines except for the first 2 lines and last line
    for line in defensive_stats_lines.split('\n')[2:-1]:
        # split the line by spaces
        stats = line.split()
        # store the player defensive stats
        player_defensive_stats = {
            'playerName': ' '.join(stats[1:-2]),
            'sacks': int(stats[-2]),
            'interceptions': int(stats[-1])
        }
        # store the player defensive stats in the game_data dictionary
        game_data.get('playerAwayDefensiveStats').append(player_defensive_stats)
    
    # Home team player defensive stats will follow the away team player defensive stats
    defensive_stats_lines = re.search(r'^Defensive.*Defensive(.*)^Field Goals.*Field Goals', content, flags=re.DOTALL|re.MULTILINE).group(1)
    game_data.setdefault('playerHomeDefensiveStats', [])
    # iterate over each line in defensive_stats_lines except for the first and last lines
    for line in defensive_stats_lines.split('\n')[2:-1]:
        # split the line by spaces
        stats = line.split()
        # store the player defensive stats
        player_defensive_stats = {
            'playerName': ' '.join(stats[1:-2]),
            'sacks': int(stats[-2]),
            'interceptions': int(stats[-1])
        }
        # store the player defensive stats in the game_data dictionary
        game_data.get('playerHomeDefensiveStats').append(player_defensive_stats)
    
    # "Field Goals" will come up next, with the following format:
    # {team year abbreviated} {player name} {field goals made} {field goals missed}
    # The {field goals made} and {field goals missed} sections will be comma separated values of field goals made and missed, respectively. If no field goals were made and/or missed, a "-" will be there.
    # get the lines from the first line that starts with "Field Goals" to the next line that starts with "Field Goals"
    field_goals_lines = re.search(r'^Field Goals$(.*)Field Goals', content, flags=re.DOTALL|re.MULTILINE).group(1)
    game_data.setdefault('playerAwayKickingStats', [])
    # iterate over each line in field_goals_lines except for the first 2 lines and last line
    for line in field_goals_lines.split('\n')[2:-1]:
        # split the line by spaces
        stats = line.split()
        # store the player kicking stats
        player_kicking_stats = {
            'playerName': ' '.join(stats[1:-2]),
            'fieldGoalsMade': [] if stats[-2] == '-' else [int(x) for x in stats[-2].split(',')],
            'fieldGoalsMissed': [] if stats[-1] == '-' else [int(x) for x in stats[-1].split(',')]
        }
        # store the player kicking stats in the game_data dictionary
        game_data.get('playerAwayKickingStats').append(player_kicking_stats)
    
    # Home team player kicking stats will follow the away team player kicking stats
    field_goals_lines = re.search(r'^Field Goals.*Field Goals(.*)^Player of the Game$', content, flags=re.DOTALL|re.MULTILINE).group(1)
    game_data.setdefault('playerHomeKickingStats', [])
    # iterate over each line in field_goals_lines except for the first and last lines
    for line in field_goals_lines.split('\n')[2:-1]:
        # split the line by spaces
        stats = line.split()
        # store the player kicking stats
        player_kicking_stats = {
            'playerName': ' '.join(stats[1:-2]),
            'fieldGoalsMade': [] if stats[-2] == '-' else [int(x) for x in stats[-2].split(',')],
            'fieldGoalsMissed': [] if stats[-1] == '-' else [int(x) for x in stats[-1].split(',')]
        }
        # store the player kicking stats in the game_data dictionary
        game_data.get('playerHomeKickingStats').append(player_kicking_stats)
    
    # Finally is the "Player of the Game" section, which will have the following format:
    # {team year abbreviated} {player name} ({team year} {team})
    # get the lines from the first line that starts with "Player of the Game" to the end of the content
    player_of_the_game_lines = re.search(r'^Player of the Game$(.*)$', content, flags=re.DOTALL|re.MULTILINE).group(1)
    # get the last line of player_of_the_game_lines
    player_of_the_game_line = player_of_the_game_lines.split('\n')[-1]
    # player of the game is what comes after '{integer} and before '('
    player_of_the_game = ' '.join(player_of_the_game_line.split('(')[0].split()[1:])
    game_data['playerOfTheGame'] = player_of_the_game
    # player of the game team is what comes after '(' and before ')' without the year
    player_of_the_game_team = ' '.join(player_of_the_game_line.split('(')[1].split(')')[0].split()[1:])
    game_data['playerOfTheGameTeamName'] = player_of_the_game_team

    return game_data



def read_game_files(TeamInfo):
    # Define columns for each dataframe
    games_columns = [
        'gameID', 'awayTeamName', 'homeTeamName', 'isNeutralSiteGame', 
        'gameSignificance', 'gameDate', 'weekPlayed', 'winningTeamName', 
        'losingTeamName', 'awayTeamScore', 'homeTeamScore', 'winningTeamScore', 'losingTeamScore', 'wasOvertime', 
        'awayTeamTotalFirstDowns', 'homeTeamTotalFirstDowns', 
        'awayTeamRushingFirstDowns', 'homeTeamRushingFirstDowns', 
        'awayTeamPassingFirstDowns', 'homeTeamPassingFirstDowns', 
        'awayTeamPenaltyFirstDowns', 'homeTeamPenaltyFirstDowns', 
        'awayTeam3rdDownConversions', 'homeTeam3rdDownConversions', 
        'awayTeam3rdDownAttempts', 'homeTeam3rdDownAttempts', 
        'awayTeam4thDownConversions', 'homeTeam4thDownConversions', 
        'awayTeam4thDownAttempts', 'homeTeam4thDownAttempts', 
        'awayTeamCarries', 'homeTeamCarries', 'awayTeamRushingYards', 
        'homeTeamRushingYards', 'awayTeamCompletions', 'homeTeamCompletions', 
        'awayTeamPassAttempts', 'homeTeamPassAttempts', 
        'awayTeamInterceptionsThrown', 'homeTeamInterceptionsThrown', 
        'awayTeamPassingYards', 'homeTeamPassingYards', 'awayTeamSacksAllowed', 
        'homeTeamSacksAllowed', 'awayTeamSacksAllowedYards', 
        'homeTeamSacksAllowedYards', 'awayTeamFumbles', 'homeTeamFumbles', 
        'awayTeamFumblesLost', 'homeTeamFumblesLost', 'awayTeamPunts', 
        'homeTeamPunts', 'awayTeamPuntYards', 'homeTeamPuntYards', 
        'awayTeamKickReturns', 'homeTeamKickReturns', 
        'awayTeamKickReturnYards', 'homeTeamKickReturnYards', 
        'awayTeamPuntReturns', 'homeTeamPuntReturns', 
        'awayTeamPuntReturnYards', 'homeTeamPuntReturnYards', 
        'awayTeamPenalties', 'homeTeamPenalties', 'awayTeamPenaltyYards', 
        'homeTeamPenaltyYards', 'awayTimeOfPossession', 'homeTimeOfPossession', 
        'awayTeamKickReturnTouchdowns', 'homeTeamKickReturnTouchdowns', 
        'awayTeamPuntReturnTouchdowns', 'homeTeamPuntReturnTouchdowns', 
        'playerOfTheGame', 'playerOfTheGameTeamName', 'gameDescription'
    ]

    player_rushing_stats_columns = [
        'gameID', 'playerName', 'teamName', 'carries', 'rushingYards', 
        '20PlusYardCarries', 'longestRush', 'rushingTouchdowns'
    ]

    player_receiving_stats_columns = [
        'gameID', 'playerName', 'teamName', 'receptions', 'receivingYards', 
        '20PlusYardReceptions', '40PlusYardReceptions', 'longestReception', 
        'receivingTouchdowns'
    ]

    player_passing_stats_columns = [
        'gameID', 'playerName', 'teamName', 'passCompletions', 'passAttempts', 
        'passingYards', 'passingTouchdowns', 'interceptionsThrown'
    ]

    player_defensive_stats_columns = [
        'gameID', 'playerName', 'teamName', 'sacks', 'interceptions'
    ]

    player_kicking_stats_columns = [
        'gameID', 'playerName', 'teamName', 'fieldGoalsMade', 'fieldGoalsMissed'
    ]

    player_returning_stats_columns = [
        'gameID', 'playerName', 'teamName',
        'kickReturns', 'kickReturnYards', 'kickReturnTouchdowns',
        'puntReturns', 'puntReturnYards', 'puntReturnTouchdowns'
    ]

    player_of_the_game_stats_columns = [
        'gameID', 'playerName', 'teamName', 'playerOfTheGameAwards'
    ]

    # Create empty dataframes
    games_df = pd.DataFrame(columns=games_columns)
    player_rushing_stats_df = pd.DataFrame(columns=player_rushing_stats_columns)
    player_receiving_stats_df = pd.DataFrame(columns=player_receiving_stats_columns)
    player_passing_stats_df = pd.DataFrame(columns=player_passing_stats_columns)
    player_defensive_stats_df = pd.DataFrame(columns=player_defensive_stats_columns)
    player_kicking_stats_df = pd.DataFrame(columns=player_kicking_stats_columns)
    player_returning_stats_df = pd.DataFrame(columns=player_returning_stats_columns)
    player_of_the_game_stats_df = pd.DataFrame(columns=player_of_the_game_stats_columns)

    # Define accepted values for validation
    accepted_significance = ['non-conference', 'conference', 'conference championship', 'postseason']
    
    # Iterate through the directories and files
    for week_dir in os.listdir('Scores'):
        week_path = os.path.join('Scores', week_dir)
        if os.path.isdir(week_path):
            for game_file in os.listdir(week_path):
                game_file_path = os.path.join(week_path, game_file)
                if game_file_path.endswith('.txt'):
                    with open(game_file_path, 'r') as file:
                        content = file.read()
                        
                        # Extract relevant data from the content
                        matchup_pattern = re.compile(r'^Matchup:\s*(.*)$', re.MULTILINE)
                        significance_pattern = re.compile(r'^Significance:\s*(.*)$', re.MULTILINE)
                        bowl_pattern = re.compile(r'^Bowl Name:(.*)$', re.MULTILINE)
                        date_pattern = re.compile(r'^Date:\s*(\d{2}/\d{2}/\d{4})$', re.MULTILINE)
                        week_pattern = re.compile(r'^Week:\s*(\d+|bowl)$', re.MULTILINE)

                        matchup_match = matchup_pattern.search(content)
                        significance_match = significance_pattern.search(content)
                        bowl_match = bowl_pattern.search(content)
                        date_match = date_pattern.search(content)
                        week_match = week_pattern.search(content)

                        # print("Matchup match:", matchup_match)
                        # print("Significance match:", significance_match)
                        # print("Bowl match:", bowl_match)
                        # print("Date match:", date_match)
                        # print("Week match:", week_match)

                        if not (matchup_match and significance_match and date_match and week_match and bowl_match):
                            raise ValueError(f"Missing required information in file {game_file_path}. matchup: {matchup_match}, significance: {significance_match}, date: {date_match}, week: {week_match}, bowl: {bowl_match}")

                        matchup = matchup_match.group(1).strip()
                        significance = significance_match.group(1).strip()
                        bowl_name = bowl_match.group(1).strip()
                        date = date_match.group(1).strip()
                        week = week_match.group(1).strip()

                        # Validate matchup
                        away_team, home_team = None, None
                        if '@' in matchup:
                            away_team, home_team = re.split(r'\s+@\s+', matchup)
                            isNeutralSiteGame = False
                        elif 'vs' in matchup:
                            away_team, home_team = re.split(r'\s+vs\s+', matchup)
                            isNeutralSiteGame = True
                        if not away_team or not home_team:
                            raise ValueError(f"Invalid matchup format {matchup} in file {game_file_path}")
                        if away_team not in TeamInfo['id'].values or home_team not in TeamInfo['id'].values:
                            raise ValueError(f"Invalid team name in matchup {matchup} in file {game_file_path}")

                        # Validate significance
                        if significance not in accepted_significance:
                            raise ValueError(f"Invalid significance value {significance} in file {game_file_path}")

                        # Validate bowl name
                        if significance != 'postseason' and bowl_name:
                            raise ValueError(f"Bowl Name {bowl_name} should be empty for non-postseason games in file {game_file_path}")

                        # Validate date
                        try:
                            game_date = datetime.strptime(date, '%m/%d/%Y')
                        except ValueError:
                            raise ValueError(f"Invalid date format {date} in file {game_file_path}")

                        # Validate week
                        if not (week.isdigit() and 1 <= int(week) <= 17):
                            raise ValueError(f"Invalid week value {week} in file {game_file_path}")
                        week_played = int(week) if week.isdigit() else 16

                        # get what comes after "# Box Score" and before "# Game Description" without leading/trailing whitespace
                        box_score = re.search(r'# Box Score(.*)# Game Description', content, flags=re.DOTALL).group(1).strip()
                        # parse box score
                        box_score_data = parse_whatifsports_box_score(box_score)

                        # get what comes after "# Game Description" without leading/trailing whitespace
                        game_description = re.search(r'# Game Description(.*)', content, flags=re.DOTALL).group(1).strip()


                        # Placeholder: Generate a unique game ID and determine other values
                        game_id = f"{away_team}_{home_team}_{game_date.strftime('%Y%m%d')}"
                        if box_score_data['awayTeamScore'] > box_score_data['homeTeamScore']:
                            winningTeamName = away_team
                            losingTeamName = home_team
                            winningTeamScore = box_score_data['awayTeamScore']
                            losingTeamScore = box_score_data['homeTeamScore']
                        elif box_score_data['awayTeamScore'] < box_score_data['homeTeamScore']:
                            winningTeamName = home_team
                            losingTeamName = away_team
                            winningTeamScore = box_score_data['homeTeamScore']
                            losingTeamScore = box_score_data['awayTeamScore']
                        else:
                            raise ValueError(f"Invalid box score data {box_score_data} in file {game_file_path}, away score {box_score_data['awayTeamScore']} and home score {box_score_data['homeTeamScore']} are equal.")
                        
                        game_data = {
                            'gameID': game_id,
                            'awayTeamName': away_team,
                            'homeTeamName': home_team,
                            'isNeutralSiteGame': isNeutralSiteGame,
                            'gameSignificance': significance,
                            'gameDate': game_date,
                            'weekPlayed': week_played,
                            'winningTeamName': winningTeamName,
                            'losingTeamName': losingTeamName,
                            'awayTeamScore': box_score_data['awayTeamScore'],
                            'homeTeamScore': box_score_data['homeTeamScore'],
                            'winningTeamScore': winningTeamScore,
                            'losingTeamScore': losingTeamScore,
                            'wasOvertime': box_score_data['wasOvertime'],
                            'awayTeamTotalFirstDowns': box_score_data['awayTeamTotalFirstDowns'],
                            'homeTeamTotalFirstDowns': box_score_data['homeTeamTotalFirstDowns'],
                            'awayTeamRushingFirstDowns': box_score_data['awayTeamRushingFirstDowns'],
                            'homeTeamRushingFirstDowns': box_score_data['homeTeamRushingFirstDowns'],
                            'awayTeamPassingFirstDowns': box_score_data['awayTeamPassingFirstDowns'],
                            'homeTeamPassingFirstDowns': box_score_data['homeTeamPassingFirstDowns'],
                            'awayTeamPenaltyFirstDowns': box_score_data['awayTeamPenaltyFirstDowns'],
                            'homeTeamPenaltyFirstDowns': box_score_data['homeTeamPenaltyFirstDowns'],
                            'awayTeam3rdDownConversions': box_score_data['awayTeam3rdDownConversions'],
                            'homeTeam3rdDownConversions': box_score_data['homeTeam3rdDownConversions'],
                            'awayTeam3rdDownAttempts': box_score_data['awayTeam3rdDownAttempts'],
                            'homeTeam3rdDownAttempts': box_score_data['homeTeam3rdDownAttempts'],
                            'awayTeam4thDownConversions': box_score_data['awayTeam4thDownConversions'],
                            'homeTeam4thDownConversions': box_score_data['homeTeam4thDownConversions'],
                            'awayTeam4thDownAttempts': box_score_data['awayTeam4thDownAttempts'],
                            'homeTeam4thDownAttempts': box_score_data['homeTeam4thDownAttempts'],
                            'awayTeamCarries': box_score_data['awayTeamCarries'],
                            'homeTeamCarries': box_score_data['homeTeamCarries'],
                            'awayTeamRushingYards': box_score_data['awayTeamRushingYards'],
                            'homeTeamRushingYards': box_score_data['homeTeamRushingYards'],
                            'awayTeamCompletions': box_score_data['awayTeamCompletions'],
                            'homeTeamCompletions': box_score_data['homeTeamCompletions'],
                            'awayTeamPassAttempts': box_score_data['awayTeamPassAttempts'],
                            'homeTeamPassAttempts': box_score_data['homeTeamPassAttempts'],
                            'awayTeamInterceptionsThrown': box_score_data['awayTeamInterceptionsThrown'],
                            'homeTeamInterceptionsThrown': box_score_data['homeTeamInterceptionsThrown'],
                            'awayTeamPassingYards': box_score_data['awayTeamPassingYards'],
                            'homeTeamPassingYards': box_score_data['homeTeamPassingYards'],
                            'awayTeamSacksAllowed': box_score_data['awayTeamSacksAllowed'],
                            'homeTeamSacksAllowed': box_score_data['homeTeamSacksAllowed'],
                            'awayTeamSacksAllowedYards': box_score_data['awayTeamSacksAllowedYards'],
                            'homeTeamSacksAllowedYards': box_score_data['homeTeamSacksAllowedYards'],
                            'awayTeamFumbles': box_score_data['awayTeamFumbles'],
                            'homeTeamFumbles': box_score_data['homeTeamFumbles'],
                            'awayTeamFumblesLost': box_score_data['awayTeamFumblesLost'],
                            'homeTeamFumblesLost': box_score_data['homeTeamFumblesLost'],
                            'awayTeamPunts': box_score_data['awayTeamPunts'],
                            'homeTeamPunts': box_score_data['homeTeamPunts'],
                            'awayTeamPuntYards': box_score_data['awayTeamPuntYards'],
                            'homeTeamPuntYards': box_score_data['homeTeamPuntYards'],
                            'awayTeamKickReturns': box_score_data['awayTeamKickReturns'],
                            'homeTeamKickReturns': box_score_data['homeTeamKickReturns'],
                            'awayTeamKickReturnYards': box_score_data['awayTeamKickReturnYards'],
                            'homeTeamKickReturnYards': box_score_data['homeTeamKickReturnYards'],
                            'awayTeamPuntReturns': box_score_data['awayTeamPuntReturns'],
                            'homeTeamPuntReturns': box_score_data['homeTeamPuntReturns'],
                            'awayTeamPuntReturnYards': box_score_data['awayTeamPuntReturnYards'],
                            'homeTeamPuntReturnYards': box_score_data['homeTeamPuntReturnYards'],
                            'awayTeamPenalties': box_score_data['awayTeamPenalties'],
                            'homeTeamPenalties': box_score_data['homeTeamPenalties'],
                            'awayTeamPenaltyYards': box_score_data['awayTeamPenaltyYards'],
                            'homeTeamPenaltyYards': box_score_data['homeTeamPenaltyYards'],
                            'awayTimeOfPossession': box_score_data['awayTimeOfPossession'],
                            'homeTimeOfPossession': box_score_data['homeTimeOfPossession'],
                            'awayTeamKickReturnTouchdowns': 0,  # Placeholder
                            'homeTeamKickReturnTouchdowns': 0,  # Placeholder
                            'awayTeamPuntReturnTouchdowns': 0,  # Placeholder
                            'homeTeamPuntReturnTouchdowns': 0,  # Placeholder
                            'playerOfTheGame': box_score_data['playerOfTheGame'],
                            'playerOfTheGameTeamName': box_score_data['playerOfTheGameTeamName'],
                            'gameDescription': game_description
                        }
                        
                        # Append data to the DataFrame
                        games_df = games_df.append(game_data, ignore_index=True)

                        # get player rushing data by going through box_score_data['playerAwayRushingStats'] and box_score_data['playerHomeRushingStats']
                        for player_rushing_stats in box_score_data['playerAwayRushingStats']:
                            player_rushing_stats['gameID'] = game_id
                            player_rushing_stats['teamName'] = away_team
                            player_rushing_stats_df = player_rushing_stats_df.append(player_rushing_stats, ignore_index=True)
                        for player_rushing_stats in box_score_data['playerHomeRushingStats']:
                            player_rushing_stats['gameID'] = game_id
                            player_rushing_stats['teamName'] = home_team
                            player_rushing_stats_df = player_rushing_stats_df.append(player_rushing_stats, ignore_index=True)
                        # remove player from player_rushing_stats_df if player plays for an FCS team (conference is FCS in TeamInfo)
                        player_rushing_stats_df = player_rushing_stats_df[~player_rushing_stats_df['teamName'].isin(TeamInfo[TeamInfo['conferenceID'] == 'FCS']['id'])]

                        # get player receiving data by going through box_score_data['playerAwayReceivingStats'] and box_score_data['playerHomeReceivingStats']
                        for player_receiving_stats in box_score_data['playerAwayReceivingStats']:
                            player_receiving_stats['gameID'] = game_id
                            player_receiving_stats['teamName'] = away_team
                            player_receiving_stats_df = player_receiving_stats_df.append(player_receiving_stats, ignore_index=True)
                        for player_receiving_stats in box_score_data['playerHomeReceivingStats']:
                            player_receiving_stats['gameID'] = game_id
                            player_receiving_stats['teamName'] = home_team
                            player_receiving_stats_df = player_receiving_stats_df.append(player_receiving_stats, ignore_index=True)
                        # remove player from player_receiving_stats_df if player plays for an FCS team (conference is FCS in TeamInfo)
                        player_receiving_stats_df = player_receiving_stats_df[~player_receiving_stats_df['teamName'].isin(TeamInfo[TeamInfo['conferenceID'] == 'FCS']['id'])]
                        
                        # get player passing data by going through box_score_data['playerAwayPassingStats'] and box_score_data['playerHomePassingStats']
                        for player_passing_stats in box_score_data['playerAwayPassingStats']:
                            player_passing_stats['gameID'] = game_id
                            player_passing_stats['teamName'] = away_team
                            player_passing_stats_df = player_passing_stats_df.append(player_passing_stats, ignore_index=True)
                        for player_passing_stats in box_score_data['playerHomePassingStats']:
                            player_passing_stats['gameID'] = game_id
                            player_passing_stats['teamName'] = home_team
                            player_passing_stats_df = player_passing_stats_df.append(player_passing_stats, ignore_index=True)
                        # remove player from player_passing_stats_df if player plays for an FCS team (conference is FCS in TeamInfo)
                        player_passing_stats_df = player_passing_stats_df[~player_passing_stats_df['teamName'].isin(TeamInfo[TeamInfo['conferenceID'] == 'FCS']['id'])]
                        
                        # get player defensive data by going through box_score_data['playerAwayDefensiveStats'] and box_score_data['playerHomeDefensiveStats']
                        for player_defensive_stats in box_score_data['playerAwayDefensiveStats']:
                            player_defensive_stats['gameID'] = game_id
                            player_defensive_stats['teamName'] = away_team
                            player_defensive_stats_df = player_defensive_stats_df.append(player_defensive_stats, ignore_index=True)
                        for player_defensive_stats in box_score_data['playerHomeDefensiveStats']:
                            player_defensive_stats['gameID'] = game_id
                            player_defensive_stats['teamName'] = home_team
                            player_defensive_stats_df = player_defensive_stats_df.append(player_defensive_stats, ignore_index=True)
                        # remove player from player_defensive_stats_df if player plays for an FCS team (conference is FCS in TeamInfo)
                        player_defensive_stats_df = player_defensive_stats_df[~player_defensive_stats_df['teamName'].isin(TeamInfo[TeamInfo['conferenceID'] == 'FCS']['id'])]
                        
                        # get player kicking data by going through box_score_data['playerAwayKickingStats'] and box_score_data['playerHomeKickingStats']
                        for player_kicking_stats in box_score_data['playerAwayKickingStats']:
                            player_kicking_stats['gameID'] = game_id
                            player_kicking_stats['teamName'] = away_team
                            player_kicking_stats_df = player_kicking_stats_df.append(player_kicking_stats, ignore_index=True)
                        for player_kicking_stats in box_score_data['playerHomeKickingStats']:
                            player_kicking_stats['gameID'] = game_id
                            player_kicking_stats['teamName'] = home_team
                            player_kicking_stats_df = player_kicking_stats_df.append(player_kicking_stats, ignore_index=True)
                        # remove player from player_kicking_stats_df if player plays for an FCS team (conference is FCS in TeamInfo)
                        player_kicking_stats_df = player_kicking_stats_df[~player_kicking_stats_df['teamName'].isin(TeamInfo[TeamInfo['conferenceID'] == 'FCS']['id'])]
                        
                        # get player returning data by going through box_score_data['playerAwayKickReturns'] and box_score_data['playerHomeKickReturns']
                        # get player returning data by going through box_score_data['playerAwayPuntReturns'] and box_score_data['playerHomePuntReturns']
                        # get away team kick returner and punt returner names from TeamInfo
                        away_team_kick_returner = TeamInfo[TeamInfo['id'] == away_team]['kickReturnerName'].values[0]
                        away_team_punt_returner = TeamInfo[TeamInfo['id'] == away_team]['puntReturnerName'].values[0]
                        if away_team_kick_returner == away_team_punt_returner:
                            # if the kick returner and punt returner are the same, only add one row
                            player_returning_stats = {
                                'gameID': game_id,
                                'playerName': away_team_kick_returner,
                                'teamName': away_team,
                                'kickReturns': box_score_data['awayTeamKickReturns'],
                                'kickReturnYards': box_score_data['awayTeamKickReturnYards'],
                                'kickReturnTouchdowns': 0,
                                'puntReturns': box_score_data['awayTeamPuntReturns'],
                                'puntReturnYards': box_score_data['awayTeamPuntReturnYards'],
                                'puntReturnTouchdowns': 0
                            }
                            player_returning_stats_df = player_returning_stats_df.append(player_returning_stats, ignore_index=True)
                        else:
                            # if the kick returner and punt returner are different, add two rows
                            player_returning_stats = {
                                'gameID': game_id,
                                'playerName': away_team_kick_returner,
                                'teamName': away_team,
                                'kickReturns': box_score_data['awayTeamKickReturns'],
                                'kickReturnYards': box_score_data['awayTeamKickReturnYards'],
                                'kickReturnTouchdowns': 0,
                                'puntReturns': 0,
                                'puntReturnYards': 0,
                                'puntReturnTouchdowns': 0
                            }
                            player_returning_stats_df = player_returning_stats_df.append(player_returning_stats, ignore_index=True)
                            player_returning_stats = {
                                'gameID': game_id,
                                'playerName': away_team_punt_returner,
                                'teamName': away_team,
                                'kickReturns': 0,
                                'kickReturnYards': 0,
                                'kickReturnTouchdowns': 0,
                                'puntReturns': box_score_data['awayTeamPuntReturns'],
                                'puntReturnYards': box_score_data['awayTeamPuntReturnYards'],
                                'puntReturnTouchdowns': 0
                            }
                            player_returning_stats_df = player_returning_stats_df.append(player_returning_stats, ignore_index=True)
                        
                        # get home team kick returner and punt returner names from TeamInfo
                        home_team_kick_returner = TeamInfo[TeamInfo['id'] == home_team]['kickReturnerName'].values[0]
                        home_team_punt_returner = TeamInfo[TeamInfo['id'] == home_team]['puntReturnerName'].values[0]
                        if home_team_kick_returner == home_team_punt_returner:
                            # if the kick returner and punt returner are the same, only add one row
                            player_returning_stats = {
                                'gameID': game_id,
                                'playerName': home_team_kick_returner,
                                'teamName': home_team,
                                'kickReturns': box_score_data['homeTeamKickReturns'],
                                'kickReturnYards': box_score_data['homeTeamKickReturnYards'],
                                'kickReturnTouchdowns': 0,
                                'puntReturns': box_score_data['homeTeamPuntReturns'],
                                'puntReturnYards': box_score_data['homeTeamPuntReturnYards'],
                                'puntReturnTouchdowns': 0
                            }
                            player_returning_stats_df = player_returning_stats_df.append(player_returning_stats, ignore_index=True)
                        else:
                            # if the kick returner and punt returner are different, add two rows
                            player_returning_stats = {
                                'gameID': game_id,
                                'playerName': home_team_kick_returner,
                                'teamName': home_team,
                                'kickReturns': box_score_data['homeTeamKickReturns'],
                                'kickReturnYards': box_score_data['homeTeamKickReturnYards'],
                                'kickReturnTouchdowns': 0,
                                'puntReturns': 0,
                                'puntReturnYards': 0,
                                'puntReturnTouchdowns': 0
                            }
                            player_returning_stats_df = player_returning_stats_df.append(player_returning_stats, ignore_index=True)
                            player_returning_stats = {
                                'gameID': game_id,
                                'playerName': home_team_punt_returner,
                                'teamName': home_team,
                                'kickReturns': 0,
                                'kickReturnYards': 0,
                                'kickReturnTouchdowns': 0,
                                'puntReturns': box_score_data['homeTeamPuntReturns'],
                                'puntReturnYards': box_score_data['homeTeamPuntReturnYards'],
                                'puntReturnTouchdowns': 0
                            }
                            player_returning_stats_df = player_returning_stats_df.append(player_returning_stats, ignore_index=True)
                        
                        # remove player from player_returning_stats_df if player plays for an FCS team (conference is FCS in TeamInfo)
                        player_returning_stats_df = player_returning_stats_df[~player_returning_stats_df['teamName'].isin(TeamInfo[TeamInfo['conferenceID'] == 'FCS']['id'])]

                        # get player of the game data by going through box_score_data['playerOfTheGame']
                        # check that box_score_data['playerOfTheGameTeamName'] is in TeamInfo['whatifsportsName']
                        if box_score_data['playerOfTheGameTeamName'] not in TeamInfo['whatifsportsName'].values:
                            raise ValueError(f"Invalid player of the game team name {box_score_data['playerOfTheGameTeamName']} in file {game_file_path}")
                        # player_of_the_game_team_name is the id of the team in TeamInfo
                        player_of_the_game_team_name = TeamInfo[TeamInfo['whatifsportsName'] == box_score_data['playerOfTheGameTeamName']]['id'].values[0]
                        
                        player_of_the_game_stats = {
                            'gameID': game_id,
                            'playerName': box_score_data['playerOfTheGame'],
                            'teamName': player_of_the_game_team_name,
                            'playerOfTheGameAwards': 1
                        }
                        player_of_the_game_stats_df = player_of_the_game_stats_df.append(player_of_the_game_stats, ignore_index=True)
                        # remove player from player_of_the_game_stats_df if player plays for an FCS team (conference is FCS in TeamInfo)
                        player_of_the_game_stats_df = player_of_the_game_stats_df[~player_of_the_game_stats_df['teamName'].isin(TeamInfo[TeamInfo['conferenceID'] == 'FCS']['id'])]

    # validate in games_df that winning team score > losing team score for every row
    if not games_df[(games_df['winningTeamScore'] <= games_df['losingTeamScore'])].empty:
        raise ValueError("Winning team score must be greater than losing team score for every row in games_df")

    return (
        games_df, 
        player_rushing_stats_df, 
        player_receiving_stats_df, 
        player_passing_stats_df, 
        player_defensive_stats_df, 
        player_kicking_stats_df, 
        player_returning_stats_df, 
        player_of_the_game_stats_df
    )

def update_ranks_to_have_tied_ranks(rankings_output_df):
    # Initialize variables to keep track of rank assignments
    current_rank = 1
    previous_points = None

    # Update ranks based on ranking_points
    new_ranks = []
    for index, row in rankings_output_df.iterrows():
        if row['ranking_points'] == previous_points:
            new_ranks.append(current_rank)
        else:
            new_ranks.append(row['rank'])
            current_rank = row['rank']
            previous_points = row['ranking_points']

    # Update the 'rank' column in the original DataFrame
    rankings_output_df['rank'] = new_ranks

    return rankings_output_df

def create_rankings_df(TeamInfo, Games):
    # Define columns for the rankings dataframe
    rankings_columns = ['teamName', 'week1', 'week2', 'week3', 'week4', 'week5',
                        'week6', 'week7', 'week8', 'week9', 'week10', 'week11', 'week12',
                        'week13', 'week14', 'week15', 'week16', 'week17', 'final']
    rankings_df = pd.DataFrame(columns=rankings_columns)

    # add the team names to the rankings dataframe if their conference isn't FCS
    for index, row in TeamInfo.iterrows():
        if row['conferenceID'] != 'FCS':
            rankings_df = rankings_df.append({'teamName': row['id']}, ignore_index=True)

    # for week1 ranking, rank teams by their initialRankingPoints column in TeamInfo. Team with the highest initialRankingPoints is ranked 1
    rankings_df['week1'] = rankings_df['teamName'].apply(lambda x: TeamInfo[TeamInfo['id'] == x]['initialRankingPoints'].values[0])
    rankings_df['week1'] = rankings_df['week1'].rank(ascending=False)
    # convert week1 column to int
    rankings_df['week1'] = rankings_df['week1'].astype(int)

    # Create a copy of the Games dataframe
    Games_copy = Games.copy()
    # Create the additional columns with empty values in the copy
    Games_copy['empty1'] = ''
    Games_copy['empty2'] = ''
    Games_copy['empty3'] = ''
    Games_copy['empty4'] = ''

    # sort Games_copy by week
    Games_copy = Games_copy.sort_values(by='weekPlayed')

    # output games from Games dataframe into Output/Temp/Season-Scores-For-Ranking.csv
    # in the form of {game id},{week},{date}},,,{winning team},{winning team points},,{losing team},{losing team points},
    output_columns = [
        'gameID', 'weekPlayed', 'gameDate', 'empty1', 'empty2', 'winningTeamName', 'winningTeamScore', 'empty3',
        'losingTeamName', 'losingTeamScore', 'empty4'
    ]
    Games_copy.to_csv('Output/Temp/Season-Scores-For-Ranking.csv', columns=output_columns, index=False, header=False)

    for week in range(2, 19):
        # run the rankings program
        os.system(f'./Output/Temp/ranker_program Output/Temp/Season-Teams-For-Ranking.csv Output/Temp/Season-Scores-For-Ranking.csv {week-1} > Output/Temp/Ranking-Output.csv')
        # read the rankings output file into a dataframe
        weeks_ranking_df = pd.read_csv('Output/Temp/Ranking-Output.csv')
        if week >= 2 and week <= 5:
            # if team has about the same ranking points as the team above them, they should be ranked the same
            weeks_ranking_df = update_ranks_to_have_tied_ranks(weeks_ranking_df)
        # rename 'name' to 'teamName'
        weeks_ranking_df = weeks_ranking_df.rename(columns={'name': 'teamName'})
        
        # ranking logic
        week_column = f'week{week}' if week < 18 else 'final'
        rankings_df = pd.merge(rankings_df, weeks_ranking_df, on='teamName', how='outer')
        if week == 2:
            rankings_df[week_column] = rankings_df['week1'] * 0.8 + rankings_df['rank'] * 0.2
            rankings_df[week_column] = rankings_df[week_column].rank(ascending=True)
        elif week == 3:
            rankings_df[week_column] = rankings_df['week1'] * 0.6 + rankings_df['rank'] * 0.4
            rankings_df[week_column] = rankings_df[week_column].rank(ascending=True)
        elif week == 4:
            rankings_df[week_column] = rankings_df['week1'] * 0.4 + rankings_df['rank'] * 0.6
            rankings_df[week_column] = rankings_df[week_column].rank(ascending=True)
        elif week == 5:
            rankings_df[week_column] = rankings_df['week1'] * 0.2 + rankings_df['rank'] * 0.8
            rankings_df[week_column] = rankings_df[week_column].rank(ascending=True)
        else:
            rankings_df[week_column] = rankings_df['rank']
        rankings_df = rankings_df.drop(columns=['rank', 'ranking_points'])

        # convert week column to int
        rankings_df[week_column] = rankings_df[week_column].astype(int)
        # print(week)
        # print(weeks_ranking_df)
        # print(week)
        # print(rankings_df)

    return rankings_df


# main
if __name__ == '__main__':
    Conferences = read_conferences_file()
    TeamInfo = read_teams_file()
    Games, PlayerGameRushingStats, PlayerGameReceivingStats, PlayerGamePassingStats, PlayerGameDefensiveStats, PlayerGameKickingStats, PlayerGameReturningStats, PlayerGamePlayerOfTheGameStats = read_game_files(TeamInfo)
    Rankings = create_rankings_df(TeamInfo, Games)
