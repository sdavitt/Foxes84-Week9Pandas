# build minimum viable product/proof of concept for a user recently played spotify data pipeline
# so we're going to building a python object oriented program that allows us to gather and organize a spotify user's recently played music
    # we need permission from that user to access their recently played info so this will be set up for a single user
    # but, could easily be used for multiple users/take in user info as input if we wanted to build that in the future
# Goal of the system:
# Extract 50 most recently played songs for a user
# Reorganize the data we think is relevant into a pandas dataframe
# Transform the popularity metric to our own categorization
# Load the resulting dataframe into a SQL database
# We can create a SQL table to keep track of all music a user listens to - updating with 50 recently played songs every time this script is run

# This kind of process is known as a data pipeline following the ETL order of operations
# ETL:
# Extract - extract the data from a 3rd party source (in this case spotify's api)
# Transform - transform the data into our customized structure
# Load - load the data into our storage system for future use

# a data pipeline could follow a ELT system - extract the data, load the extracted data directly into our storage, and then transform later

# Before we get to work, we need to install the tools we're going to use
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import get_key
from sqlalchemy.types import Text, DateTime

# we're going to be working in an object-oriented approach here - the goal is to design a class that can run our entire ETL process in one function call
class Spotify_ETL_Pipeline:
    def __init__(self):
        self.client_id = get_key('.env', 'CLIENT_ID')
        self.client_secret = get_key('.env', 'CLIENT_SECRET')
        self.apiauth = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=self.client_id, 
            client_secret=self.client_secret, 
            redirect_uri='http://localhost:3000/callback', # functions by default on port 3000 on localhost - for specifically working with user data (requests a login/permission)
            scope='user-library-read user-read-recently-played user-read-currently-playing'))
    # functions to:
    # initially get the data from the spotify api
    def get_data(self):
        """
        Make the API call to the spotifyAPI gaining authorization and accessing the user's recently played
        Return data of the user's 50 recently played songs
        """
        return self.apiauth.current_user_recently_played(limit=50)
        


    def get_currently_playing(self):
        return self.apiauth.current_user_playing_track()

    # take that data and extract only what we need
    def extract(self):
        """
        The extract function continues/oversees our entire extraction process
        API call made -> data we want isolated and placed into a pandas dataframe
        Returns the pandas dataframe
        """
        print('... Extracting ...')
        data = self.get_data()
        # currently, our data has a ton of random information we don't need
        # I want just the following columns
            # Song - string
        song_name = [x['track']['name'] for x in data['items']]
            # Artists - string - comma separated artist names
        artists = [', '.join([x['name'] for x in song['track']['artists']]) for song in data['items']]
            # Album - string
        song_album = [x['track']['album']['name'] for x in data['items']]
            # Album Cover URL - string
        album_cover = [x['track']['album']['images'][0]['url'] for x in data['items']]
            # Played time - timestamp
        song_played = [x['played_at'] for x in data['items']]
            # Popularity - integer
        song_pop = [x['track']['popularity'] for x in data['items']]
            # Preview_url - string
        song_preview = [x['track']['preview_url'] for x in data['items']]

        # reorganize into a dictionary to translate into a dataframe
        data = {
            'Track': song_name,
            'Artist': artists,
            'Album': song_album,
            'Album Cover': album_cover,
            'Preview': song_preview,
            'Played At': song_played,
            'Popularity': song_pop
        }
        songs_df = pd.DataFrame(data)
        print('... Extraction Complete ...')
        return songs_df

    # transform that data to our preferred format
    # in this case take spotify's integer popularity index and turn it into popularity categories
    # UDF to apply to transform popularity column
    def popularity_helper(self, popularity):
        if popularity < 25:
            return 'Undiscovered'
        elif popularity < 50:
            return 'Low'
        elif popularity < 75:
            return 'High'
        else:
            return 'Overplayed'

    def transform(self):
        """
        Calls and executes the extraction process - then accesses the dataframe and applies our UDF to transform the popularity column
        Returns the transformed dataframe
        """
        df = self.extract()
        print('... Transforming ...')

        # do we have any data at all? has this user even listened to anything?
        if df.empty:
            print('This user has never listened to music. Bizarre.')
            raise Exception('No recently played songs.')

        # did something go wrong in the extraction? do we have null values where we shouldn't?
        # aka is there a single null value in this dataframe (there shouldn't be)
        if df.isnull().values.any():
            raise Exception('Null values found - check extraction process/api call')
        
        # check for improper data - by that I mean corruptions/repeated primary keys - look and make sure there isnt a repeated value where one shouldn't exist
        if not df['Played At'].is_unique:
            raise Exception('Error during transformation - duplicated timestamps present')

        # remove duplicates - I don't need duplicates in my database
        df.drop_duplicates(subset=['Track', 'Artist'], inplace=True)
        df.reset_index(inplace=True, drop=True)

        # at this point I should have no nulls, no errors, no duplicates
        # ready for our popularity transformation
        df['Popularity'] = df['Popularity'].apply(self.popularity_helper)
        print('... Transformation Complete ...')
        return df

    # load that data into our database
    def load(self):
        songs = self.transform()
        if songs.empty:
            raise Exception('Issue with transform - dataframe empty')
        print('... Data ready, proceed to load into database ...')
        # dburl
        dburl = get_key('.env', 'DB_URL')
        # use the to_sql method to upload our dataframe into our SQL database provided the proper parameters
        songs.to_sql('sam_recently_played', 
            index=False, 
            con=dburl,
            schema='public',
            chunksize=500,
            if_exists='replace',
            dtype={
                'Track': Text,
                'Artist': Text,
                'Album': Text,
                'Album Cover': Text,
                'Preview': Text,
                'Played At': DateTime,
                'Popularity': Text
            })

        print('... Data Loading Complete - Your database is ready!')

# test instance of the class
test = Spotify_ETL_Pipeline()
test.load()

