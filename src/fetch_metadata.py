# fetch_metadata.py

import atproto
import pandas as pd
import datetime
import json

# Load credentials
with open('config/credentials.json', 'r') as file:
    credentials = json.load(file)

USERNAME = credentials["username"]
PASSWORD = credentials["password"]

# Define keywords and date range
KEYWORDS = [
    '#election2024', '#uselections', '#election', '#elections', '#uspolitics', '#uspol',
    '#congress', '#usdemocracy', '#swingstates', '#bidenharris', '#bidensuccess', '#biden',
    '#republicanparty', '#gop', '#proudblue', '#democrat', '#bluecrew', '#teamblue',
    '#voteblue', '#liberal', '#trumpcheats', '#trump', '#trumpslittlejohnson', '#trumpvirus',
    '#convictedfelon', '#republican', '#maga', '#conservative', '#makeamericagreatagain',
    '#trumptrain', '#americafirst', '#presidenttrump', '#keepamericagreat',
    '2024 elections', 'us election', 'us elections', 'us congress', 'usdemocracy',
    'electoral college', 'election day', 'democrat', 'democrats', 'democratic party',
    'biden-harris', 'president biden', 'vice president harris', 'kamala harris',
    'joe biden', 'republican', 'republicans', 'trump-appointed', 'trump', 'donald trump',
    'maga', 'i voted', 'swing state', 'swing states'
]

START_DATE = datetime.date(2023, 12, 1)
END_DATE = datetime.date(2023, 12, 31)

# Initialize the ATProto client and authenticate
client = atproto.Client()
client.login(USERNAME, PASSWORD)

# List to store all metadata
data = []

# Loop through each keyword
for keyword in KEYWORDS:
    current_date = START_DATE

    # Loop day by day
    while current_date <= END_DATE:
        next_day = current_date + datetime.timedelta(days=1)
        since = current_date.strftime("%Y-%m-%dT00:00:00Z")
        until = next_day.strftime("%Y-%m-%dT00:00:00Z")

        cursor = None
        while True:
            try:
                fetched = client.app.bsky.feed.search_posts(params={
                    'q': keyword,
                    'cursor': cursor,
                    'since': since,
                    'until': until
                })

                if not fetched or not hasattr(fetched, 'posts'):
                    break

                for post in fetched.posts:
                    if hasattr(post.record, 'langs') and 'en' in post.record.langs:
                        author_handle = post.author.handle if hasattr(post.author, 'handle') else ""
                        profile_data = {}

                        # Fetch additional profile data
                        try:
                            if author_handle:
                                profile = client.app.bsky.actor.get_profile({'actor': author_handle})
                                profile_data = {
                                    "description": profile.description if hasattr(profile, 'description') else "",
                                    "followers_count": profile.followers_count if hasattr(profile, 'followers_count') else 0,
                                    "following_count": profile.follows_count if hasattr(profile, 'follows_count') else 0,
                                    "posts_count": profile.posts_count if hasattr(profile, 'posts_count') else 0
                                }
                        except Exception as e:
                            print(f"Error fetching profile for {author_handle}: {e}")

                        # Append data to the list
                        data.append({
                            "keyword": keyword,
                            "author": author_handle,
                            "avatar": post.author.avatar if hasattr(post.author, 'avatar') else "",
                            "display_name": post.author.display_name if hasattr(post.author, 'display_name') else "",
                            "description": profile_data.get("description", ""),
                            "followers_count": profile_data.get("followers_count", 0),
                            "following_count": profile_data.get("following_count", 0),
                            "posts_count": profile_data.get("posts_count", 0),
                            "created_at": post.record.created_at if hasattr(post.record, 'created_at') else "",
                            "text": post.record.text if hasattr(post.record, 'text') else "",
                            "uri": post.uri if hasattr(post, 'uri') else "",
                            "like_count": post.like_count if hasattr(post, 'like_count') else 0,
                            "quote_count": post.quote_count if hasattr(post, 'quote_count') else 0,
                            "reply_count": post.reply_count if hasattr(post, 'reply_count') else 0,
                            "repost_count": post.repost_count if hasattr(post, 'repost_count') else 0
                        })

                if not fetched.cursor:
                    break

                cursor = fetched.cursor

            except Exception as e:
                if "NoneType" in str(e):
                    break
                print(f"Error fetching data for {keyword} in {since} to {until}: {e}")
                break

        current_date = next_day

# Save to CSV
output_file = "../data/output/us_election_11_23.csv"
df = pd.DataFrame(data)
df.to_csv(output_file, index=False)
print(f"Data saved to {output_file}")
