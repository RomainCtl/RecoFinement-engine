from src.content import Track
from datetime import datetime
import pandas as pd


pd.set_option("max_rows", 20)

st_time = datetime.utcnow()
print("============ Start at %s ============" %
      (st_time.strftime("%H:%M:%S")))

given_user_id = 1012808

track_df = Track.get()
trackWithGenres_df = Track.get_with_genres(track_df)
ratings_df = Track.get_meta(["user_id", "track_id", "rating"])


print("============= SQL request & PreProcessing Duration : %s =============" %
      (datetime.utcnow()-st_time))

# =========== STEP 1: learning user profile
user_input = ratings_df[ratings_df["user_id"] == given_user_id]

# Ressetting the index to avoid future issues
user_input = user_input.reset_index(drop=True)

# Filtering out the tracks from the input
users_tracks = trackWithGenres_df[trackWithGenres_df['track_id'].isin(
    user_input['track_id'].tolist())]

# Resetting the index to avoid future issues
users_tracks = users_tracks.reset_index(drop=True)

# Dropping unnecessary issues due to save memory and to avoid issues
user_genre_table = users_tracks.drop(['track_id', 'title', 'genres'], 1)

# we're going to turn each genre into weights. We can do this by using the input's reviews and multiplying them into the input's genre table and then summing up the resulting table by column.

# Dot produt to get weights
user_profile = user_genre_table.transpose().dot(user_input['rating'])

# Now, we have the weights for every of the user's preferences.
# TODO maybe later, use user interest (genre he explicitly like) (by maybe multiply by ?)

# =========== STEP 2: Create recommendations according to this profile

# Now let's get the genres of every movie in our original dataframe
genre_table = trackWithGenres_df.set_index(trackWithGenres_df['track_id'])

# And drop the unnecessary information
genre_table = genre_table.drop(['track_id', 'title', 'genres'], 1)


# With the input's profile and the complete list of movies and their genres in hand, we're going to take the weighted average of every movie based on the input profile and recommend the top twenty movies that most satisfy it.
# Multiply the genres by the weights and then take the weighted average
recommendationTable_df = (
    (genre_table*user_profile).sum(axis=1))/(user_profile.sum())

# Sort our recommendations in descending order
recommendationTable_df = recommendationTable_df.sort_values(ascending=False)

# The final recommendation table
# Adding the .keys() displays all the values not just the headers.
result = track_df.loc[track_df['track_id'].isin(
    recommendationTable_df.head(50).keys())]

print(result)

print("============= Duration : %s =============" %
      (datetime.utcnow()-st_time))

# Duration: average of 2-3 sec to get track recommendation for a specific user (but is for track...) (something 5sec)
# On an Linux OS, 8GB ram, Intel I5 - 4 core (with grapical interface)
