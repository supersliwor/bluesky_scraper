# fetch_metadata.py

import atproto
import pandas as pd
import datetime
import json

# Load credentials
with open('config/credentials.json', 'r') as file:
    credentials = json.load(file)

username = credentials["username"]
password = credentials["password"]

# Define keywords and date range
keywords = [
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

startDate = datetime.date(2024, 1, 1)
endDate = datetime.date(2024, 1, 31)

# Initialize the ATProto client and authenticate
client = atproto.Client()
client.login(username, password)

# List to store all metadata
data = []

# Loop through each keyword
for keyword in keywords:
    currentDate = startDate

    # Loop day by day
    while currentDate <= endDate:
        nextDay = currentDate + datetime.timedelta(days=1)
        since = currentDate.strftime("%Y-%m-%dT00:00:00Z")
        until = nextDay.strftime("%Y-%m-%dT00:00:00Z")

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
                        authorHandle = post.author.handle if hasattr(post.author, 'handle') else ""
                        profileData = {}

                        # Fetch additional profile data
                        try:
                            if authorHandle:
                                profile = client.app.bsky.actor.get_profile({'actor': authorHandle})
                                profileData = {
                                    "description": profile.description if hasattr(profile, 'description') else "",
                                    "followersCount": profile.followers_count if hasattr(profile, 'followers_count') else 0,
                                    "followingCount": profile.follows_count if hasattr(profile, 'follows_count') else 0,
                                    "postsCount": profile.posts_count if hasattr(profile, 'posts_count') else 0
                                }
                        except Exception as e:
                            print(f"Error fetching profile for {authorHandle}: {e}")

                        # Append data to the list
                        data.append({
                            "keyword": keyword,
                            "author": authorHandle,
                            "avatar": post.author.avatar if hasattr(post.author, 'avatar') else "",
                            "displayName": post.author.display_name if hasattr(post.author, 'display_name') else "",
                            "description": profileData.get("description", ""),
                            "followersCount": profileData.get("followersCount", 0),
                            "followingCount": profileData.get("followingCount", 0),
                            "postsCount": profileData.get("postsCount", 0),
                            "createdAt": post.record.created_at if hasattr(post.record, 'created_at') else "",
                            "text": post.record.text if hasattr(post.record, 'text') else "",
                            "uri": post.uri if hasattr(post, 'uri') else "",
                            "likeCount": post.like_count if hasattr(post, 'like_count') else 0,
                            "quoteCount": post.quote_count if hasattr(post, 'quote_count') else 0,
                            "replyCount": post.reply_count if hasattr(post, 'reply_count') else 0,
                            "repostCount": post.repost_count if hasattr(post, 'repost_count') else 0
                        })

                if not fetched.cursor:
                    break

                cursor = fetched.cursor

            except Exception as e:
                if "NoneType" in str(e):
                    break
                print(f"Error fetching data for {keyword} in {since} to {until}: {e}")
                break

        currentDate = nextDay

# Save to CSV
output_file = "../data/output/us_election_11_23.csv"
df = pd.DataFrame(data)
df.to_csv(output_file, index=False)
print(f"Data saved to {output_file}")
