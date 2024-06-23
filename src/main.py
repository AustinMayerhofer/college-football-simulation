import pandas as pd
import os
import re
from datetime import datetime

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



def read_game_files():
    # Define columns for each dataframe
    games_columns = [
        'gameID', 'awayTeamName', 'homeTeamName', 'isNeutralSiteGame', 
        'gameSignificance', 'gameDate', 'weekPlayed', 'winningTeamName', 
        'losingTeamName', 'awayTeamScore', 'homeTeamScore', 'wasOvertime', 
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
                        elif 'vs' in matchup:
                            away_team, home_team = re.split(r'\s+vs\s+', matchup)
                        if not away_team or not home_team:
                            raise ValueError(f"Invalid matchup format {matchup} in file {game_file_path}")

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
                        if not (week.isdigit() and 1 <= int(week) <= 16) and week != 'bowl':
                            raise ValueError(f"Invalid week value {week} in file {game_file_path}")
                        week_played = int(week) if week.isdigit() else 16

                        # Placeholder: Generate a unique game ID and determine other values
                        game_id = f"{away_team}_{home_team}_{game_date.strftime('%Y%m%d')}"
                        game_data = {
                            'gameID': game_id,
                            'awayTeamName': away_team,
                            'homeTeamName': home_team,
                            'isNeutralSiteGame': False,  # Assume False for now
                            'gameSignificance': significance,
                            'gameDate': game_date,
                            'weekPlayed': week_played,
                            'winningTeamName': '',  # Determine from the box score
                            'losingTeamName': '',  # Determine from the box score
                            'awayTeamScore': 0,  # Extract from the box score
                            'homeTeamScore': 0,  # Extract from the box score
                            'wasOvertime': False,  # Determine from the box score
                            'awayTeamTotalFirstDowns': 0,  # Extract from the box score
                            'homeTeamTotalFirstDowns': 0,  # Extract from the box score
                            'awayTeamRushingFirstDowns': 0,  # Extract from the box score
                            'homeTeamRushingFirstDowns': 0,  # Extract from the box score
                            'awayTeamPassingFirstDowns': 0,  # Extract from the box score
                            'homeTeamPassingFirstDowns': 0,  # Extract from the box score
                            'awayTeamPenaltyFirstDowns': 0,  # Extract from the box score
                            'homeTeamPenaltyFirstDowns': 0,  # Extract from the box score
                            'awayTeam3rdDownConversions': 0,  # Extract from the box score
                            'homeTeam3rdDownConversions': 0,  # Extract from the box score
                            'awayTeam3rdDownAttempts': 0,  # Extract from the box score
                            'homeTeam3rdDownAttempts': 0,  # Extract from the box score
                            'awayTeam4thDownConversions': 0,  # Extract from the box score
                            'homeTeam4thDownConversions': 0,  # Extract from the box score
                            'awayTeam4thDownAttempts': 0,  # Extract from the box score
                            'homeTeam4thDownAttempts': 0,  # Extract from the box score
                            'awayTeamCarries': 0,  # Extract from the box score
                            'homeTeamCarries': 0,  # Extract from the box score
                            'awayTeamRushingYards': 0,  # Extract from the box score
                            'homeTeamRushingYards': 0,  # Extract from the box score
                            'awayTeamCompletions': 0,  # Extract from the box score
                            'homeTeamCompletions': 0,  # Extract from the box score
                            'awayTeamPassAttempts': 0,  # Extract from the box score
                            'homeTeamPassAttempts': 0,  # Extract from the box score
                            'awayTeamInterceptionsThrown': 0,  # Extract from the box score
                            'homeTeamInterceptionsThrown': 0,  # Extract from the box score
                            'awayTeamPassingYards': 0,  # Extract from the box score
                            'homeTeamPassingYards': 0,  # Extract from the box score
                            'awayTeamSacksAllowed': 0,  # Extract from the box score
                            'homeTeamSacksAllowed': 0,  # Extract from the box score
                            'awayTeamSacksAllowedYards': 0,  # Extract from the box score
                            'homeTeamSacksAllowedYards': 0,  # Extract from the box score
                            'awayTeamFumbles': 0,  # Extract from the box score
                            'homeTeamFumbles': 0,  # Extract from the box score
                            'awayTeamFumblesLost': 0,  # Extract from the box score
                            'homeTeamFumblesLost': 0,  # Extract from the box score
                            'awayTeamPunts': 0,  # Extract from the box score
                            'homeTeamPunts': 0,  # Extract from the box score
                            'awayTeamPuntYards': 0,  # Extract from the box score
                            'homeTeamPuntYards': 0,  # Extract from the box score
                            'awayTeamKickReturns': 0,  # Extract from the box score
                            'homeTeamKickReturns': 0,  # Extract from the box score
                            'awayTeamKickReturnYards': 0,  # Extract from the box score
                            'homeTeamKickReturnYards': 0,  # Extract from the box score
                            'awayTeamPuntReturns': 0,  # Extract from the box score
                            'homeTeamPuntReturns': 0,  # Extract from the box score
                            'awayTeamPuntReturnYards': 0,  # Extract from the box score
                            'homeTeamPuntReturnYards': 0,  # Extract from the box score
                            'awayTeamPenalties': 0,  # Extract from the box score
                            'homeTeamPenalties': 0,  # Extract from the box score
                            'awayTeamPenaltyYards': 0,  # Extract from the box score
                            'homeTeamPenaltyYards': 0,  # Extract from the box score
                            'awayTimeOfPossession': '00:00',  # Extract from the box score
                            'homeTimeOfPossession': '00:00',  # Extract from the box score
                            'awayTeamKickReturnTouchdowns': 0,  # Extract from the box score
                            'homeTeamKickReturnTouchdowns': 0,  # Extract from the box score
                            'awayTeamPuntReturnTouchdowns': 0,  # Extract from the box score
                            'homeTeamPuntReturnTouchdowns': 0,  # Extract from the box score
                            'playerOfTheGame': '',  # Extract from the box score
                            'playerOfTheGameTeamName': '',  # Extract from the box score
                            'gameDescription': ''  # Extract from the game description
                        }

                        # Append data to the DataFrame
                        games_df = games_df.append(game_data, ignore_index=True)

    return (
        games_df, 
        player_rushing_stats_df, 
        player_receiving_stats_df, 
        player_passing_stats_df, 
        player_defensive_stats_df, 
        player_kicking_stats_df, 
        player_of_the_game_stats_df
    )
