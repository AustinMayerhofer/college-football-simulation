import pandas as pd
import os

def read_conferences_file(filepath='Meta/conferences.csv'):
    """
    Reads the conferences.csv file into a dataframe and returns it.

    Parameters:
        filepath (str or Path): The path to the conferences.csv file.

    Returns:
        pd.DataFrame: The dataframe containing the conferences data, or an empty dataframe if an error occurs.
    """
    filepath = str(filepath)  # Ensure the filepath is a string

    # Check if the file exists
    if not os.path.exists(filepath):
        print(f"Error: The file '{filepath}' does not exist.")
        return pd.DataFrame()

    # Check if the file is a CSV file
    if not filepath.endswith('.csv'):
        print(f"Error: The file '{filepath}' is not a CSV file.")
        return pd.DataFrame()

    try:
        # Attempt to read the CSV file into a dataframe
        conferences_df = pd.read_csv(filepath)

        # Perform basic type checking on the dataframe
        expected_columns = ["shorthand name", "full name", "division name", "flair", "championship game", "conferences games count"]
        if not all(column in conferences_df.columns for column in expected_columns):
            print("Error: The conferences file does not contain the expected columns.")
            return pd.DataFrame()

        # Simplified type checks: Ensure that the dataframe is not empty
        if conferences_df.empty:
            print("Error: The dataframe is empty.")
            return pd.DataFrame()

        return conferences_df
    except Exception as e:
        print(f"An error occurred while reading the conferences file: {e}")
        return pd.DataFrame()  # Return an empty dataframe in case of error



def read_teams_file(filepath='Meta/teams.csv'):
    """
    Reads the teams.csv file into a dataframe and returns it.

    Parameters:
        filepath (str or Path): The path to the teams.csv file.

    Returns:
        pd.DataFrame: The dataframe containing the teams data, or an empty dataframe if an error occurs.
    """
    filepath = str(filepath)  # Ensure the filepath is a string

    # Check if the file exists
    if not os.path.exists(filepath):
        print(f"Error: The file '{filepath}' does not exist.")
        return pd.DataFrame()

    # Check if the file is a CSV file
    if not filepath.endswith('.csv'):
        print(f"Error: The file '{filepath}' is not a CSV file.")
        return pd.DataFrame()

    try:
        # Attempt to read the CSV file into a dataframe
        teams_df = pd.read_csv(filepath)

        # Perform basic type checking on the dataframe
        expected_columns = ["official name", "display name", "Whatifsports name", "flair", 
                            "conference", "conference division", "year", "record", 
                            "initial ranking points", "head coach", "punter", 
                            "kick returner", "punt returner"]
        if not all(column in teams_df.columns for column in expected_columns):
            print("Error: The teams file does not contain the expected columns.")
            return pd.DataFrame()

        # Simplified type checks: Ensure that the dataframe is not empty
        if teams_df.empty:
            print("Error: The dataframe is empty.")
            return pd.DataFrame()

        return teams_df
    except Exception as e:
        print(f"An error occurred while reading the teams file: {e}")
        return pd.DataFrame()  # Return an empty dataframe in case of error
