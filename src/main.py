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

def compile_season_stats(week, TeamInfo, Games, PlayerGameRushingStats, PlayerGameReceivingStats, PlayerGamePassingStats, PlayerGameDefensiveStats, PlayerGameKickingStats, PlayerGameReturningStats, PlayerGamePlayerOfTheGameStats):
    TeamSeasonStats = pd.DataFrame(columns=['id', 'numGamesPlayed', 'numWins', 'numLosses', 'numConferenceWins',
                                            'numConferenceLosses', 'numTotalFirstDowns', 'numRushingFirstDowns', 
                                            'numPassingFirstDowns', 'numPenaltyFirstDowns', 'num3rdDownConversions', 
                                            'num3rdDownAttempts', 'num4thDownConversions', 'num4thDownAttempts', 'numCarries', 
                                            'numRushingYards', 'numCompletions', 'numPassAttempts', 
                                            'numInterceptionsThrown', 'numPassingYards', 
                                            'numSacksAllowed', 'numSacksAllowedYards', 'numFumbles', 'numFumblesLost', 
                                            'numPunts', 'numPuntYards', 'numKickReturns', 'numKickReturnYards', 
                                            'numKickReturnTouchdowns', 'numPuntReturns', 'numPuntReturnYards', 
                                            'numPuntReturnTouchdowns', 'numPenalties', 'numPenaltyYards', 'totalTimeOfPossession', 
                                            'numDefenseTotalFirstDownsAllowed', 'numDefenseRushingFirstDownsAllowed', 
                                            'numDefensePassingFirstDownsAllowed', 'numDefensePenaltyFirstDownsAllowed', 
                                            'numDefense3rdDownConversionsAllowed', 'numDefense3rdDownAttemptsAllowed', 
                                            'numDefense4thDownConversionsAllowed', 'numDefense4thDownAttemptsAllowed', 
                                            'numDefenseCarriesAllowed', 'numDefenseRushingYardsAllowed', 
                                            'numDefenseCompletionsAllowed', 
                                            'numDefensePassAttemptsAllowed',
                                            'numDefenseInterceptions', 'numDefensePassingYardsAllowed', 'numDefenseSacks', 
                                            'numDefenseSackYards', 'numDefenseFumblesForced', 'numDefenseFumblesRecovered', 
                                            'numPuntsByOpponents', 'numPuntYardsByOpponents', 'numKickReturnsByOpponents', 
                                            'numKickReturnYardsAllowed', 'numKickReturnTouchdownsAllowed', 
                                            'numPuntReturnsByOpponents', 'numPuntReturnYardsAllowed', 'numPuntReturnTouchdownsAllowed'])
    
    PlayerSeasonRushingStats = pd.DataFrame(columns=['playerName', 'teamName', 'carries', 'rushingYards', '20PlusYardCarries', 
                                                     'longestRush', 'rushingTouchdowns'])
    
    PlayerSeasonReceivingStats = pd.DataFrame(columns=['playerName', 'teamName', 'receptions', 'receivingYards', '20PlusYardReceptions', 
                                                       '40PlusYardReceptions', 'longestReception', 'receivingTouchdowns'])

    PlayerSeasonPassingStats = pd.DataFrame(columns=['playerName', 'teamName', 'passCompletions', 'passAttempts', 'passingYards', 
                                                     'passingTouchdowns', 'interceptionsThrown'])
    
    PlayerSeasonDefensiveStats = pd.DataFrame(columns=['playerName', 'teamName', 'sacks', 'interceptions'])

    PlayerSeasonKickingStats = pd.DataFrame(columns=['playerName', 'teamName', 'fieldGoalsMade', 'fieldGoalsMissed', 'fieldGoals0To29YardsMade', 'fieldGoals0To29YardsMissed',
                                                     'fieldGoals30To39YardsMade', 'fieldGoals30To39YardsMissed', 'fieldGoals40To49YardsMade',
                                                     'fieldGoals40To49YardsMissed', 'fieldGoals50PlusYardsMade', 'fieldGoals50PlusYardsMissed'])
    
    PlayerSeasonReturningStats = pd.DataFrame(columns=['playerName', 'teamName', 'kickReturns', 'kickReturnYards', 
                                                       'kickReturnTouchdowns', 'puntReturns', 'puntReturnYards', 'puntReturnTouchdowns'])
    
    PlayerSeasonPlayerOfTheGameStats = pd.DataFrame(columns=['playerName', 'teamName', 'playerOfTheGameAwards'])

    # isolate Games but only up to and including the week
    Games = Games[Games['weekPlayed'] <= week]
    
    # add the team names to the TeamSeasonStats dataframe if their conference isn't FCS
    TeamSeasonStats['id'] = TeamInfo[TeamInfo['conferenceID'] != 'FCS']['id']

    # numGamesPlayed is the number of games the team has played
    TeamSeasonStats['numGamesPlayed'] = TeamSeasonStats['id'].apply(lambda x: len(Games[(Games['awayTeamName'] == x) | (Games['homeTeamName'] == x)]))

    # numWins is the number of games the team has won
    TeamSeasonStats['numWins'] = TeamSeasonStats['id'].apply(lambda x: len(Games[(Games['winningTeamName'] == x)]))

    # numLosses is the number of games the team has lost
    TeamSeasonStats['numLosses'] = TeamSeasonStats['id'].apply(lambda x: len(Games[(Games['losingTeamName'] == x)]))

    # check numWins + numLosses == numGamesPlayed
    if not TeamSeasonStats[(TeamSeasonStats['numWins'] + TeamSeasonStats['numLosses'] != TeamSeasonStats['numGamesPlayed'])].empty:
        raise ValueError("numWins + numLosses must equal numGamesPlayed for every row in TeamSeasonStats")
    
    # numConferenceWins is the number of conference games the team has won
    TeamSeasonStats['numConferenceWins'] = TeamSeasonStats['id'].apply(lambda x: len(Games[(Games['winningTeamName'] == x) & (Games['gameSignificance'] == 'conference')]))

    # numConferenceLosses is the number of conference games the team has lost
    TeamSeasonStats['numConferenceLosses'] = TeamSeasonStats['id'].apply(lambda x: len(Games[(Games['losingTeamName'] == x) & (Games['gameSignificance'] == 'conference')]))

    # numTotalFirstDowns is the total number of first downs the team has gained
    # sum all the home first downs when the team is the home team
    TeamSeasonStats['numTotalFirstDowns'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamTotalFirstDowns'].sum())
    # sum all the away first downs when the team is the away team
    TeamSeasonStats['numTotalFirstDowns'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamTotalFirstDowns'].sum())

    # numRushingFirstDowns is the total number of rushing first downs the team has gained
    # sum all the home rushing first downs when the team is the home team
    TeamSeasonStats['numRushingFirstDowns'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamRushingFirstDowns'].sum())
    # sum all the away rushing first downs when the team is the away team
    TeamSeasonStats['numRushingFirstDowns'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamRushingFirstDowns'].sum())

    # numPassingFirstDowns is the total number of passing first downs the team has gained
    # sum all the home passing first downs when the team is the home team
    TeamSeasonStats['numPassingFirstDowns'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamPassingFirstDowns'].sum())
    # sum all the away passing first downs when the team is the away team
    TeamSeasonStats['numPassingFirstDowns'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamPassingFirstDowns'].sum())

    # numPenaltyFirstDowns is the total number of penalty first downs the team has gained
    # sum all the home penalty first downs when the team is the home team
    TeamSeasonStats['numPenaltyFirstDowns'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamPenaltyFirstDowns'].sum())
    # sum all the away penalty first downs when the team is the away team
    TeamSeasonStats['numPenaltyFirstDowns'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamPenaltyFirstDowns'].sum())

    # num3rdDownConversions is the total number of 3rd down conversions the team has made
    # sum all the home 3rd down conversions when the team is the home team
    TeamSeasonStats['num3rdDownConversions'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeam3rdDownConversions'].sum())
    # sum all the away 3rd down conversions when the team is the away team
    TeamSeasonStats['num3rdDownConversions'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeam3rdDownConversions'].sum())

    # num3rdDownAttempts is the total number of 3rd down attempts the team has made
    # sum all the home 3rd down attempts when the team is the home team
    TeamSeasonStats['num3rdDownAttempts'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeam3rdDownAttempts'].sum())
    # sum all the away 3rd down attempts when the team is the away team
    TeamSeasonStats['num3rdDownAttempts'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeam3rdDownAttempts'].sum())

    # num4thDownConversions is the total number of 4th down conversions the team has made
    # sum all the home 4th down conversions when the team is the home team
    TeamSeasonStats['num4thDownConversions'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeam4thDownConversions'].sum())
    # sum all the away 4th down conversions when the team is the away team
    TeamSeasonStats['num4thDownConversions'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeam4thDownConversions'].sum())

    # num4thDownAttempts is the total number of 4th down attempts the team has made
    # sum all the home 4th down attempts when the team is the home team
    TeamSeasonStats['num4thDownAttempts'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeam4thDownAttempts'].sum())
    # sum all the away 4th down attempts when the team is the away team
    TeamSeasonStats['num4thDownAttempts'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeam4thDownAttempts'].sum())

    # numCarries is the total number of carries the team has made
    # sum all the home carries when the team is the home team
    TeamSeasonStats['numCarries'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamCarries'].sum())
    # sum all the away carries when the team is the away team
    TeamSeasonStats['numCarries'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamCarries'].sum())

    # numRushingYards is the total number of rushing yards the team has gained
    # sum all the home rushing yards when the team is the home team
    TeamSeasonStats['numRushingYards'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamRushingYards'].sum())
    # sum all the away rushing yards when the team is the away team
    TeamSeasonStats['numRushingYards'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamRushingYards'].sum())

    # numCompletions is the total number of completions the team has made
    # sum all the home completions when the team is the home team
    TeamSeasonStats['numCompletions'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamCompletions'].sum())
    # sum all the away completions when the team is the away team
    TeamSeasonStats['numCompletions'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamCompletions'].sum())

    # numPassAttempts is the total number of pass attempts the team has made
    # sum all the home pass attempts when the team is the home team
    TeamSeasonStats['numPassAttempts'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamPassAttempts'].sum())
    # sum all the away pass attempts when the team is the away team
    TeamSeasonStats['numPassAttempts'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamPassAttempts'].sum())

    # numInterceptionsThrown is the total number of interceptions the team has thrown
    # sum all the home interceptions thrown when the team is the home team
    TeamSeasonStats['numInterceptionsThrown'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamInterceptionsThrown'].sum())
    # sum all the away interceptions thrown when the team is the away team
    TeamSeasonStats['numInterceptionsThrown'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamInterceptionsThrown'].sum())

    # numPassingYards is the total number of passing yards the team has gained
    # sum all the home passing yards when the team is the home team
    TeamSeasonStats['numPassingYards'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamPassingYards'].sum())
    # sum all the away passing yards when the team is the away team
    TeamSeasonStats['numPassingYards'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamPassingYards'].sum())

    # numSacksAllowed is the total number of sacks the team has allowed
    # sum all the home sacks allowed when the team is the home team
    TeamSeasonStats['numSacksAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamSacksAllowed'].sum())
    # sum all the away sacks allowed when the team is the away team
    TeamSeasonStats['numSacksAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamSacksAllowed'].sum())

    # numSacksAllowedYards is the total number of yards lost due to sacks the team has allowed
    # sum all the home sack yards allowed when the team is the home team
    TeamSeasonStats['numSacksAllowedYards'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamSacksAllowedYards'].sum())
    # sum all the away sack yards allowed when the team is the away team
    TeamSeasonStats['numSacksAllowedYards'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamSacksAllowedYards'].sum())

    # numFumbles is the total number of fumbles the team has made
    # sum all the home fumbles when the team is the home team
    TeamSeasonStats['numFumbles'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamFumbles'].sum())
    # sum all the away fumbles when the team is the away team
    TeamSeasonStats['numFumbles'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamFumbles'].sum())

    # numFumblesLost is the total number of fumbles the team has lost
    # sum all the home fumbles lost when the team is the home team
    TeamSeasonStats['numFumblesLost'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamFumblesLost'].sum())
    # sum all the away fumbles lost when the team is the away team
    TeamSeasonStats['numFumblesLost'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamFumblesLost'].sum())

    # numPunts is the total number of punts the team has made
    # sum all the home punts when the team is the home team
    TeamSeasonStats['numPunts'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamPunts'].sum())
    # sum all the away punts when the team is the away team
    TeamSeasonStats['numPunts'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamPunts'].sum())

    # numPuntYards is the total number of punt yards the team has made
    # sum all the home punt yards when the team is the home team
    TeamSeasonStats['numPuntYards'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamPuntYards'].sum())
    # sum all the away punt yards when the team is the away team
    TeamSeasonStats['numPuntYards'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamPuntYards'].sum())

    # numKickReturns is the total number of kick returns the team has made
    # sum all the home kick returns when the team is the home team
    TeamSeasonStats['numKickReturns'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamKickReturns'].sum())
    # sum all the away kick returns when the team is the away team
    TeamSeasonStats['numKickReturns'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamKickReturns'].sum())

    # numKickReturnYards is the total number of kick return yards the team has made
    # sum all the home kick return yards when the team is the home team
    TeamSeasonStats['numKickReturnYards'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamKickReturnYards'].sum())
    # sum all the away kick return yards when the team is the away team
    TeamSeasonStats['numKickReturnYards'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamKickReturnYards'].sum())

    # numKickReturnTouchdowns is the total number of kick return touchdowns the team has made
    # sum all the home kick return touchdowns when the team is the home team
    TeamSeasonStats['numKickReturnTouchdowns'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamKickReturnTouchdowns'].sum())
    # sum all the away kick return touchdowns when the team is the away team
    TeamSeasonStats['numKickReturnTouchdowns'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamKickReturnTouchdowns'].sum())

    # numPuntReturns is the total number of punt returns the team has made
    # sum all the home punt returns when the team is the home team
    TeamSeasonStats['numPuntReturns'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamPuntReturns'].sum())
    # sum all the away punt returns when the team is the away team
    TeamSeasonStats['numPuntReturns'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamPuntReturns'].sum())

    # numPuntReturnYards is the total number of punt return yards the team has made
    # sum all the home punt return yards when the team is the home team
    TeamSeasonStats['numPuntReturnYards'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamPuntReturnYards'].sum())
    # sum all the away punt return yards when the team is the away team
    TeamSeasonStats['numPuntReturnYards'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamPuntReturnYards'].sum())

    # numPuntReturnTouchdowns is the total number of punt return touchdowns the team has made
    # sum all the home punt return touchdowns when the team is the home team
    TeamSeasonStats['numPuntReturnTouchdowns'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamPuntReturnTouchdowns'].sum())
    # sum all the away punt return touchdowns when the team is the away team
    TeamSeasonStats['numPuntReturnTouchdowns'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamPuntReturnTouchdowns'].sum())

    # numPenalties is the total number of penalties the team has made
    # sum all the home penalties when the team is the home team
    TeamSeasonStats['numPenalties'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamPenalties'].sum())
    # sum all the away penalties when the team is the away team
    TeamSeasonStats['numPenalties'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamPenalties'].sum())

    # numPenaltyYards is the total number of penalty yards the team has made
    # sum all the home penalty yards when the team is the home team
    TeamSeasonStats['numPenaltyYards'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTeamPenaltyYards'].sum())
    # sum all the away penalty yards when the team is the away team
    TeamSeasonStats['numPenaltyYards'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTeamPenaltyYards'].sum())

    # totalTimeOfPossession is the total time of possession the team has had
    # sum all the home time of possession when the team is the home team
    TeamSeasonStats['totalTimeOfPossession'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['homeTimeOfPossession'].sum())
    # sum all the away time of possession when the team is the away team
    TeamSeasonStats['totalTimeOfPossession'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['awayTimeOfPossession'].sum())

    # numDefenseTotalFirstDownsAllowed is the total number of first downs the team has allowed
    # sum all the away total first downs allowed when the team is the home team
    TeamSeasonStats['numDefenseTotalFirstDownsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamTotalFirstDowns'].sum())
    # sum all the home total first downs allowed when the team is the away team
    TeamSeasonStats['numDefenseTotalFirstDownsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamTotalFirstDowns'].sum())

    # numDefenseRushingFirstDownsAllowed is the total number of rushing first downs the team has allowed
    # sum all the away rushing first downs allowed when the team is the home team
    TeamSeasonStats['numDefenseRushingFirstDownsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamRushingFirstDowns'].sum())
    # sum all the home rushing first downs allowed when the team is the away team
    TeamSeasonStats['numDefenseRushingFirstDownsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamRushingFirstDowns'].sum())

    # numDefensePassingFirstDownsAllowed is the total number of passing first downs the team has allowed
    # sum all the away passing first downs allowed when the team is the home team
    TeamSeasonStats['numDefensePassingFirstDownsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamPassingFirstDowns'].sum())
    # sum all the home passing first downs allowed when the team is the away team
    TeamSeasonStats['numDefensePassingFirstDownsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamPassingFirstDowns'].sum())

    # numDefensePenaltyFirstDownsAllowed is the total number of penalty first downs the team has allowed
    # sum all the away penalty first downs allowed when the team is the home team
    TeamSeasonStats['numDefensePenaltyFirstDownsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamPenaltyFirstDowns'].sum())
    # sum all the home penalty first downs allowed when the team is the away team
    TeamSeasonStats['numDefensePenaltyFirstDownsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamPenaltyFirstDowns'].sum())

    # numDefense3rdDownConversionsAllowed is the total number of 3rd down conversions the team has allowed
    # sum all the away 3rd down conversions allowed when the team is the home team
    TeamSeasonStats['numDefense3rdDownConversionsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeam3rdDownConversions'].sum())
    # sum all the home 3rd down conversions allowed when the team is the away team
    TeamSeasonStats['numDefense3rdDownConversionsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeam3rdDownConversions'].sum())

    # numDefense3rdDownAttemptsAllowed is the total number of 3rd down attempts the team has allowed
    # sum all the away 3rd down attempts allowed when the team is the home team
    TeamSeasonStats['numDefense3rdDownAttemptsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeam3rdDownAttempts'].sum())
    # sum all the home 3rd down attempts allowed when the team is the away team
    TeamSeasonStats['numDefense3rdDownAttemptsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeam3rdDownAttempts'].sum())

    # numDefense4thDownConversionsAllowed is the total number of 4th down conversions the team has allowed
    # sum all the away 4th down conversions allowed when the team is the home team
    TeamSeasonStats['numDefense4thDownConversionsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeam4thDownConversions'].sum())
    # sum all the home 4th down conversions allowed when the team is the away team
    TeamSeasonStats['numDefense4thDownConversionsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeam4thDownConversions'].sum())

    # numDefense4thDownAttemptsAllowed is the total number of 4th down attempts the team has allowed
    # sum all the away 4th down attempts allowed when the team is the home team
    TeamSeasonStats['numDefense4thDownAttemptsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeam4thDownAttempts'].sum())
    # sum all the home 4th down attempts allowed when the team is the away team
    TeamSeasonStats['numDefense4thDownAttemptsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeam4thDownAttempts'].sum())

    # numDefenseCarriesAllowed is the total number of carries the team has allowed
    # sum all the away carries allowed when the team is the home team
    TeamSeasonStats['numDefenseCarriesAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamCarries'].sum())
    # sum all the home carries allowed when the team is the away team
    TeamSeasonStats['numDefenseCarriesAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamCarries'].sum())

    # numDefenseRushingYardsAllowed is the total number of rushing yards the team has allowed
    # sum all the away rushing yards allowed when the team is the home team
    TeamSeasonStats['numDefenseRushingYardsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamRushingYards'].sum())
    # sum all the home rushing yards allowed when the team is the away team
    TeamSeasonStats['numDefenseRushingYardsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamRushingYards'].sum())

    # numDefenseCompletionsAllowed is the total number of completions the team has allowed
    # sum all the away completions allowed when the team is the home team
    TeamSeasonStats['numDefenseCompletionsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamCompletions'].sum())
    # sum all the home completions allowed when the team is the away team
    TeamSeasonStats['numDefenseCompletionsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamCompletions'].sum())

    # numDefensePassAttemptsAllowed is the total number of pass attempts the team has allowed
    # sum all the away pass attempts allowed when the team is the home team
    TeamSeasonStats['numDefensePassAttemptsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamPassAttempts'].sum())
    # sum all the home pass attempts allowed when the team is the away team
    TeamSeasonStats['numDefensePassAttemptsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamPassAttempts'].sum())

    # numDefenseInterceptions is the total number of interceptions the team has made
    # sum all the away interceptions when the team is the home team
    TeamSeasonStats['numDefenseInterceptions'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamInterceptionsThrown'].sum())
    # sum all the home interceptions when the team is the away team
    TeamSeasonStats['numDefenseInterceptions'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamInterceptionsThrown'].sum())

    # numDefensePassingYardsAllowed is the total number of passing yards the team has allowed
    # sum all the away passing yards allowed when the team is the home team
    TeamSeasonStats['numDefensePassingYardsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamPassingYards'].sum())
    # sum all the home passing yards allowed when the team is the away team
    TeamSeasonStats['numDefensePassingYardsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamPassingYards'].sum())

    # numDefenseSacks is the total number of sacks the team has made
    # sum all the away sacks when the team is the home team
    TeamSeasonStats['numDefenseSacks'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamSacksAllowed'].sum())
    # sum all the home sacks when the team is the away team
    TeamSeasonStats['numDefenseSacks'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamSacksAllowed'].sum())

    # numDefenseSackYards is the total number of sack yards the team has made
    # sum all the away sack yards when the team is the home team
    TeamSeasonStats['numDefenseSackYards'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamSacksAllowedYards'].sum())
    # sum all the home sack yards when the team is the away team
    TeamSeasonStats['numDefenseSackYards'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamSacksAllowedYards'].sum())

    # numDefenseFumblesForced is the total number of fumbles the team has forced
    # sum all the away fumbles forced when the team is the home team
    TeamSeasonStats['numDefenseFumblesForced'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamFumbles'].sum())
    # sum all the home fumbles forced when the team is the away team
    TeamSeasonStats['numDefenseFumblesForced'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamFumbles'].sum())

    # numDefenseFumblesRecovered is the total number of fumbles the team has recovered
    # sum all the away fumbles recovered when the team is the home team
    TeamSeasonStats['numDefenseFumblesRecovered'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamFumblesLost'].sum())
    # sum all the home fumbles recovered when the team is the away team
    TeamSeasonStats['numDefenseFumblesRecovered'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamFumblesLost'].sum())

    # numPuntsByOpponents is the total number of punts the team's opponents have made
    # sum all the away punts when the team is the home team
    TeamSeasonStats['numPuntsByOpponents'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamPunts'].sum())
    # sum all the home punts when the team is the away team
    TeamSeasonStats['numPuntsByOpponents'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamPunts'].sum())

    # numPuntYardsByOpponents is the total number of punt yards the team's opponents have made
    # sum all the away punt yards when the team is the home team
    TeamSeasonStats['numPuntYardsByOpponents'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamPuntYards'].sum())
    # sum all the home punt yards when the team is the away team
    TeamSeasonStats['numPuntYardsByOpponents'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamPuntYards'].sum())

    # numKickReturnsByOpponents is the total number of kick returns the team's opponents have made
    # sum all the away kick returns when the team is the home team
    TeamSeasonStats['numKickReturnsByOpponents'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamKickReturns'].sum())
    # sum all the home kick returns when the team is the away team
    TeamSeasonStats['numKickReturnsByOpponents'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamKickReturns'].sum())

    # numKickReturnYardsAllowed is the total number of kick return yards the team's opponents have made
    # sum all the away kick return yards when the team is the home team
    TeamSeasonStats['numKickReturnYardsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamKickReturnYards'].sum())
    # sum all the home kick return yards when the team is the away team
    TeamSeasonStats['numKickReturnYardsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamKickReturnYards'].sum())

    # numKickReturnTouchdownsAllowed is the total number of kick return touchdowns the team's opponents have made
    # sum all the away kick return touchdowns when the team is the home team
    TeamSeasonStats['numKickReturnTouchdownsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamKickReturnTouchdowns'].sum())
    # sum all the home kick return touchdowns when the team is the away team
    TeamSeasonStats['numKickReturnTouchdownsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamKickReturnTouchdowns'].sum())

    # numPuntReturnsByOpponents is the total number of punt returns the team's opponents have made
    # sum all the away punt returns when the team is the home team
    TeamSeasonStats['numPuntReturnsByOpponents'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamPuntReturns'].sum())
    # sum all the home punt returns when the team is the away team
    TeamSeasonStats['numPuntReturnsByOpponents'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamPuntReturns'].sum())

    # numPuntReturnYardsAllowed is the total number of punt return yards the team's opponents have made
    # sum all the away punt return yards when the team is the home team
    TeamSeasonStats['numPuntReturnYardsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamPuntReturnYards'].sum())
    # sum all the home punt return yards when the team is the away team
    TeamSeasonStats['numPuntReturnYardsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamPuntReturnYards'].sum())

    # numPuntReturnTouchdownsAllowed is the total number of punt return touchdowns the team's opponents have made
    # sum all the away punt return touchdowns when the team is the home team
    TeamSeasonStats['numPuntReturnTouchdownsAllowed'] = TeamSeasonStats['id'].apply(lambda x: Games[Games['homeTeamName'] == x]['awayTeamPuntReturnTouchdowns'].sum())
    # sum all the home punt return touchdowns when the team is the away team
    TeamSeasonStats['numPuntReturnTouchdownsAllowed'] += TeamSeasonStats['id'].apply(lambda x: Games[Games['awayTeamName'] == x]['homeTeamPuntReturnTouchdowns'].sum())

    # add the unique player names and team names combinations from PlayerGameRushingStats to the playerSeasonRushingStats dataframe
    PlayerSeasonRushingStats = PlayerGameRushingStats[['playerName', 'teamName']].drop_duplicates()

    # carries is the total number of carries the player has made
    # sum all the carries the player/team combo has made
    PlayerSeasonRushingStats['carries'] = PlayerSeasonRushingStats.apply(lambda x: PlayerGameRushingStats[(PlayerGameRushingStats['playerName'] == x['playerName']) & (PlayerGameRushingStats['teamName'] == x['teamName'])]['carries'].sum(), axis=1)

    # rushingYards is the total number of rushing yards the player has gained
    # sum all the rushing yards the player/team combo has gained
    PlayerSeasonRushingStats['rushingYards'] = PlayerSeasonRushingStats.apply(lambda x: PlayerGameRushingStats[(PlayerGameRushingStats['playerName'] == x['playerName']) & (PlayerGameRushingStats['teamName'] == x['teamName'])]['rushingYards'].sum(), axis=1)

    # 20PlusYardCarries is the total number of carries the player has made that have gained 20 or more yards
    # sum all the 20+ yard carries the player/team combo has made
    PlayerSeasonRushingStats['20PlusYardCarries'] = PlayerSeasonRushingStats.apply(lambda x: PlayerGameRushingStats[(PlayerGameRushingStats['playerName'] == x['playerName']) & (PlayerGameRushingStats['teamName'] == x['teamName'])]['20PlusYardCarries'].sum(), axis=1)

    # longestRush is the longest rush the player has made
    # find the longest rush the player/team combo has made
    PlayerSeasonRushingStats['longestRush'] = PlayerSeasonRushingStats.apply(lambda x: PlayerGameRushingStats[(PlayerGameRushingStats['playerName'] == x['playerName']) & (PlayerGameRushingStats['teamName'] == x['teamName'])]['longestRush'].max(), axis=1)

    # rushingTouchdowns is the total number of rushing touchdowns the player has made
    # sum all the rushing touchdowns the player/team combo has made
    PlayerSeasonRushingStats['rushingTouchdowns'] = PlayerSeasonRushingStats.apply(lambda x: PlayerGameRushingStats[(PlayerGameRushingStats['playerName'] == x['playerName']) & (PlayerGameRushingStats['teamName'] == x['teamName'])]['rushingTouchdowns'].sum(), axis=1)

    # add the unique player names and team names combinations from PlayerGameReceivingStats to the playerSeasonReceivingStats dataframe
    PlayerSeasonReceivingStats = PlayerGameReceivingStats[['playerName', 'teamName']].drop_duplicates()

    # receptions is the total number of receptions the player has made
    # sum all the receptions the player/team combo has made
    PlayerSeasonReceivingStats['receptions'] = PlayerSeasonReceivingStats.apply(lambda x: PlayerGameReceivingStats[(PlayerGameReceivingStats['playerName'] == x['playerName']) & (PlayerGameReceivingStats['teamName'] == x['teamName'])]['receptions'].sum(), axis=1)

    # receivingYards is the total number of receiving yards the player has gained
    # sum all the receiving yards the player/team combo has gained
    PlayerSeasonReceivingStats['receivingYards'] = PlayerSeasonReceivingStats.apply(lambda x: PlayerGameReceivingStats[(PlayerGameReceivingStats['playerName'] == x['playerName']) & (PlayerGameReceivingStats['teamName'] == x['teamName'])]['receivingYards'].sum(), axis=1)

    # 20PlusYardReceptions is the total number of receptions the player has made that have gained 20 or more yards
    # sum all the 20+ yard receptions the player/team combo has made
    PlayerSeasonReceivingStats['20PlusYardReceptions'] = PlayerSeasonReceivingStats.apply(lambda x: PlayerGameReceivingStats[(PlayerGameReceivingStats['playerName'] == x['playerName']) & (PlayerGameReceivingStats['teamName'] == x['teamName'])]['20PlusYardReceptions'].sum(), axis=1)

    # 40PlusYardReceptions is the total number of receptions the player has made that have gained 40 or more yards
    # sum all the 40+ yard receptions the player/team combo has made
    PlayerSeasonReceivingStats['40PlusYardReceptions'] = PlayerSeasonReceivingStats.apply(lambda x: PlayerGameReceivingStats[(PlayerGameReceivingStats['playerName'] == x['playerName']) & (PlayerGameReceivingStats['teamName'] == x['teamName'])]['40PlusYardReceptions'].sum(), axis=1)

    # longestReception is the longest reception the player has made
    # find the longest reception the player/team combo has made
    PlayerSeasonReceivingStats['longestReception'] = PlayerSeasonReceivingStats.apply(lambda x: PlayerGameReceivingStats[(PlayerGameReceivingStats['playerName'] == x['playerName']) & (PlayerGameReceivingStats['teamName'] == x['teamName'])]['longestReception'].max(), axis=1)

    # receivingTouchdowns is the total number of receiving touchdowns the player has made
    # sum all the receiving touchdowns the player/team combo has made
    PlayerSeasonReceivingStats['receivingTouchdowns'] = PlayerSeasonReceivingStats.apply(lambda x: PlayerGameReceivingStats[(PlayerGameReceivingStats['playerName'] == x['playerName']) & (PlayerGameReceivingStats['teamName'] == x['teamName'])]['receivingTouchdowns'].sum(), axis=1)

    # add the unique player names and team names combinations from PlayerGamePassingStats to the playerSeasonPassingStats dataframe
    PlayerSeasonPassingStats = PlayerGamePassingStats[['playerName', 'teamName']].drop_duplicates()

    # passCompletions is the total number of completions the player has made
    # sum all the completions the player/team combo has made
    PlayerSeasonPassingStats['passCompletions'] = PlayerSeasonPassingStats.apply(lambda x: PlayerGamePassingStats[(PlayerGamePassingStats['playerName'] == x['playerName']) & (PlayerGamePassingStats['teamName'] == x['teamName'])]['passCompletions'].sum(), axis=1)

    # passAttempts is the total number of pass attempts the player has made
    # sum all the pass attempts the player/team combo has made
    PlayerSeasonPassingStats['passAttempts'] = PlayerSeasonPassingStats.apply(lambda x: PlayerGamePassingStats[(PlayerGamePassingStats['playerName'] == x['playerName']) & (PlayerGamePassingStats['teamName'] == x['teamName'])]['passAttempts'].sum(), axis=1)

    # passingYards is the total number of passing yards the player has gained
    # sum all the passing yards the player/team combo has gained
    PlayerSeasonPassingStats['passingYards'] = PlayerSeasonPassingStats.apply(lambda x: PlayerGamePassingStats[(PlayerGamePassingStats['playerName'] == x['playerName']) & (PlayerGamePassingStats['teamName'] == x['teamName'])]['passingYards'].sum(), axis=1)

    # passingTouchdowns is the total number of passing touchdowns the player has made
    # sum all the passing touchdowns the player/team combo has made
    PlayerSeasonPassingStats['passingTouchdowns'] = PlayerSeasonPassingStats.apply(lambda x: PlayerGamePassingStats[(PlayerGamePassingStats['playerName'] == x['playerName']) & (PlayerGamePassingStats['teamName'] == x['teamName'])]['passingTouchdowns'].sum(), axis=1)

    # interceptionsThrown is the total number of interceptions the player has thrown
    # sum all the interceptions the player/team combo has thrown
    PlayerSeasonPassingStats['interceptionsThrown'] = PlayerSeasonPassingStats.apply(lambda x: PlayerGamePassingStats[(PlayerGamePassingStats['playerName'] == x['playerName']) & (PlayerGamePassingStats['teamName'] == x['teamName'])]['interceptionsThrown'].sum(), axis=1)

    # add the unique player names and team names combinations from PlayerGameDefensiveStats to the playerSeasonDefensiveStats dataframe
    PlayerSeasonDefensiveStats = PlayerGameDefensiveStats[['playerName', 'teamName']].drop_duplicates()

    # sacks is the total number of sacks the player has made
    # sum all the sacks the player/team combo has made
    PlayerSeasonDefensiveStats['sacks'] = PlayerSeasonDefensiveStats.apply(lambda x: PlayerGameDefensiveStats[(PlayerGameDefensiveStats['playerName'] == x['playerName']) & (PlayerGameDefensiveStats['teamName'] == x['teamName'])]['sacks'].sum(), axis=1)

    # interceptions is the total number of interceptions the player has made
    # sum all the interceptions the player/team combo has made
    PlayerSeasonDefensiveStats['interceptions'] = PlayerSeasonDefensiveStats.apply(lambda x: PlayerGameDefensiveStats[(PlayerGameDefensiveStats['playerName'] == x['playerName']) & (PlayerGameDefensiveStats['teamName'] == x['teamName'])]['interceptions'].sum(), axis=1)

    # add the unique player names and team names combinations from PlayerGameKickingStats to the playerSeasonKickingStats dataframe
    PlayerSeasonKickingStats = PlayerGameKickingStats[['playerName', 'teamName']].drop_duplicates()

    # fieldGoalsMade is the total number of field goals the player has made
    # sum all the field goals the player/team combo has made, which is the number of elements in the fieldGoalsMade list
    PlayerSeasonKickingStats['fieldGoalsMade'] = PlayerSeasonKickingStats.apply(lambda x: PlayerGameKickingStats[(PlayerGameKickingStats['playerName'] == x['playerName']) & (PlayerGameKickingStats['teamName'] == x['teamName'])]['fieldGoalsMade'].apply(lambda x: len(x)).sum(), axis=1)

    # fieldGoalsMissed is the total number of field goals the player has missed
    # sum all the field goals the player/team combo has missed, which is the number of elements in the fieldGoalsMissed list
    PlayerSeasonKickingStats['fieldGoalsMissed'] = PlayerSeasonKickingStats.apply(lambda x: PlayerGameKickingStats[(PlayerGameKickingStats['playerName'] == x['playerName']) & (PlayerGameKickingStats['teamName'] == x['teamName'])]['fieldGoalsMissed'].apply(lambda x: len(x)).sum(), axis=1)

    # fieldGoals0To29YardsMade is the total number of field goals the player has made from 0 to 29 yards
    # sum all the field goals the player/team combo has made from 0 to 29 yards, which is the number of elements in the fieldGoalsMade list that are between 0 and 29
    PlayerSeasonKickingStats['fieldGoals0To29YardsMade'] = PlayerSeasonKickingStats.apply(lambda x: PlayerGameKickingStats[(PlayerGameKickingStats['playerName'] == x['playerName']) & (PlayerGameKickingStats['teamName'] == x['teamName'])]['fieldGoalsMade'].apply(lambda x: len([y for y in x if y <= 29])).sum(), axis=1)

    # fieldGoals0To29YardsMissed is the total number of field goals the player has missed from 0 to 29 yards
    # sum all the field goals the player/team combo has missed from 0 to 29 yards, which is the number of elements in the fieldGoalsMissed list that are between 0 and 29
    PlayerSeasonKickingStats['fieldGoals0To29YardsMissed'] = PlayerSeasonKickingStats.apply(lambda x: PlayerGameKickingStats[(PlayerGameKickingStats['playerName'] == x['playerName']) & (PlayerGameKickingStats['teamName'] == x['teamName'])]['fieldGoalsMissed'].apply(lambda x: len([y for y in x if y <= 29])).sum(), axis=1)

    # fieldGoals30To39YardsMade is the total number of field goals the player has made from 30 to 39 yards
    # sum all the field goals the player/team combo has made from 30 to 39 yards, which is the number of elements in the fieldGoalsMade list that are between 30 and 39
    PlayerSeasonKickingStats['fieldGoals30To39YardsMade'] = PlayerSeasonKickingStats.apply(lambda x: PlayerGameKickingStats[(PlayerGameKickingStats['playerName'] == x['playerName']) & (PlayerGameKickingStats['teamName'] == x['teamName'])]['fieldGoalsMade'].apply(lambda x: len([y for y in x if y >= 30 and y <= 39])).sum(), axis=1)

    # fieldGoals30To39YardsMissed is the total number of field goals the player has missed from 30 to 39 yards
    # sum all the field goals the player/team combo has missed from 30 to 39 yards, which is the number of elements in the fieldGoalsMissed list that are between 30 and 39
    PlayerSeasonKickingStats['fieldGoals30To39YardsMissed'] = PlayerSeasonKickingStats.apply(lambda x: PlayerGameKickingStats[(PlayerGameKickingStats['playerName'] == x['playerName']) & (PlayerGameKickingStats['teamName'] == x['teamName'])]['fieldGoalsMissed'].apply(lambda x: len([y for y in x if y >= 30 and y <= 39])).sum(), axis=1)

    # fieldGoals40To49YardsMade is the total number of field goals the player has made from 40 to 49 yards
    # sum all the field goals the player/team combo has made from 40 to 49 yards, which is the number of elements in the fieldGoalsMade list that are between 40 and 49
    PlayerSeasonKickingStats['fieldGoals40To49YardsMade'] = PlayerSeasonKickingStats.apply(lambda x: PlayerGameKickingStats[(PlayerGameKickingStats['playerName'] == x['playerName']) & (PlayerGameKickingStats['teamName'] == x['teamName'])]['fieldGoalsMade'].apply(lambda x: len([y for y in x if y >= 40 and y <= 49])).sum(), axis=1)

    # fieldGoals40To49YardsMissed is the total number of field goals the player has missed from 40 to 49 yards
    # sum all the field goals the player/team combo has missed from 40 to 49 yards, which is the number of elements in the fieldGoalsMissed list that are between 40 and 49
    PlayerSeasonKickingStats['fieldGoals40To49YardsMissed'] = PlayerSeasonKickingStats.apply(lambda x: PlayerGameKickingStats[(PlayerGameKickingStats['playerName'] == x['playerName']) & (PlayerGameKickingStats['teamName'] == x['teamName'])]['fieldGoalsMissed'].apply(lambda x: len([y for y in x if y >= 40 and y <= 49])).sum(), axis=1)

    # fieldGoals50PlusYardsMade is the total number of field goals the player has made from 50+ yards
    # sum all the field goals the player/team combo has made from 50+ yards, which is the number of elements in the fieldGoalsMade list that are 50 or more
    PlayerSeasonKickingStats['fieldGoals50PlusYardsMade'] = PlayerSeasonKickingStats.apply(lambda x: PlayerGameKickingStats[(PlayerGameKickingStats['playerName'] == x['playerName']) & (PlayerGameKickingStats['teamName'] == x['teamName'])]['fieldGoalsMade'].apply(lambda x: len([y for y in x if y >= 50])).sum(), axis=1)

    # fieldGoals50PlusYardsMissed is the total number of field goals the player has missed from 50+ yards
    # sum all the field goals the player/team combo has missed from 50+ yards, which is the number of elements in the fieldGoalsMissed list that are 50 or more
    PlayerSeasonKickingStats['fieldGoals50PlusYardsMissed'] = PlayerSeasonKickingStats.apply(lambda x: PlayerGameKickingStats[(PlayerGameKickingStats['playerName'] == x['playerName']) & (PlayerGameKickingStats['teamName'] == x['teamName'])]['fieldGoalsMissed'].apply(lambda x: len([y for y in x if y >= 50])).sum(), axis=1)

    # add the unique player names and team names combinations from PlayerGameReturningStats to the playerSeasonReturningStats dataframe
    PlayerSeasonReturningStats = PlayerGameReturningStats[['playerName', 'teamName']].drop_duplicates()

    # kickReturns is the total number of kick returns the player has made
    # sum all the kick returns the player/team combo has made
    PlayerSeasonReturningStats['kickReturns'] = PlayerSeasonReturningStats.apply(lambda x: PlayerGameReturningStats[(PlayerGameReturningStats['playerName'] == x['playerName']) & (PlayerGameReturningStats['teamName'] == x['teamName'])]['kickReturns'].sum(), axis=1)

    # kickReturnYards is the total number of kick return yards the player has gained
    # sum all the kick return yards the player/team combo has gained
    PlayerSeasonReturningStats['kickReturnYards'] = PlayerSeasonReturningStats.apply(lambda x: PlayerGameReturningStats[(PlayerGameReturningStats['playerName'] == x['playerName']) & (PlayerGameReturningStats['teamName'] == x['teamName'])]['kickReturnYards'].sum(), axis=1)

    # kickReturnTouchdowns is the total number of kick return touchdowns the player has made
    # sum all the kick return touchdowns the player/team combo has made
    PlayerSeasonReturningStats['kickReturnTouchdowns'] = PlayerSeasonReturningStats.apply(lambda x: PlayerGameReturningStats[(PlayerGameReturningStats['playerName'] == x['playerName']) & (PlayerGameReturningStats['teamName'] == x['teamName'])]['kickReturnTouchdowns'].sum(), axis=1)

    # puntReturns is the total number of punt returns the player has made
    # sum all the punt returns the player/team combo has made
    PlayerSeasonReturningStats['puntReturns'] = PlayerSeasonReturningStats.apply(lambda x: PlayerGameReturningStats[(PlayerGameReturningStats['playerName'] == x['playerName']) & (PlayerGameReturningStats['teamName'] == x['teamName'])]['puntReturns'].sum(), axis=1)

    # puntReturnYards is the total number of punt return yards the player has gained
    # sum all the punt return yards the player/team combo has gained
    PlayerSeasonReturningStats['puntReturnYards'] = PlayerSeasonReturningStats.apply(lambda x: PlayerGameReturningStats[(PlayerGameReturningStats['playerName'] == x['playerName']) & (PlayerGameReturningStats['teamName'] == x['teamName'])]['puntReturnYards'].sum(), axis=1)

    # puntReturnTouchdowns is the total number of punt return touchdowns the player has made
    # sum all the punt return touchdowns the player/team combo has made
    PlayerSeasonReturningStats['puntReturnTouchdowns'] = PlayerSeasonReturningStats.apply(lambda x: PlayerGameReturningStats[(PlayerGameReturningStats['playerName'] == x['playerName']) & (PlayerGameReturningStats['teamName'] == x['teamName'])]['puntReturnTouchdowns'].sum(), axis=1)

    # add the unique player names and team names combinations from PlayerGamePlayerOfTheGameStats to the playerSeasonPlayerOfTheGameStats dataframe
    PlayerSeasonPlayerOfTheGameStats = PlayerGamePlayerOfTheGameStats[['playerName', 'teamName']].drop_duplicates()

    # playerOfTheGameAwards is the total number of player of the game awards the player has won
    # sum all the player of the game awards the player/team combo has won
    PlayerSeasonPlayerOfTheGameStats['playerOfTheGameAwards'] = PlayerSeasonPlayerOfTheGameStats.apply(lambda x: PlayerGamePlayerOfTheGameStats[(PlayerGamePlayerOfTheGameStats['playerName'] == x['playerName']) & (PlayerGamePlayerOfTheGameStats['teamName'] == x['teamName'])]['playerOfTheGameAwards'].sum(), axis=1)

    return (
        TeamSeasonStats, 
        PlayerSeasonRushingStats, 
        PlayerSeasonReceivingStats, 
        PlayerSeasonPassingStats, 
        PlayerSeasonDefensiveStats, 
        PlayerSeasonKickingStats, 
        PlayerSeasonReturningStats, 
        PlayerSeasonPlayerOfTheGameStats
    )


# main
if __name__ == '__main__':
    Conferences = read_conferences_file()
    TeamInfo = read_teams_file()
    Games, PlayerGameRushingStats, PlayerGameReceivingStats, PlayerGamePassingStats, PlayerGameDefensiveStats, PlayerGameKickingStats, PlayerGameReturningStats, PlayerGamePlayerOfTheGameStats = read_game_files(TeamInfo)
    Rankings = create_rankings_df(TeamInfo, Games)
    TeamSeasonStats, PlayerSeasonRushingStats, PlayerSeasonReceivingStats, PlayerSeasonPassingStats, PlayerSeasonDefensiveStats, PlayerSeasonKickingStats, PlayerSeasonReturningStats, PlayerSeasonPlayerOfTheGameStats = compile_season_stats(1, TeamInfo, Games, PlayerGameRushingStats, PlayerGameReceivingStats, PlayerGamePassingStats, PlayerGameDefensiveStats, PlayerGameKickingStats, PlayerGameReturningStats, PlayerGamePlayerOfTheGameStats)
