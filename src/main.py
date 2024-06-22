import pandas as pd
import os

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
