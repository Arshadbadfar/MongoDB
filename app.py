from flask import Flask, render_template
from pymongo import MongoClient

app = Flask(__name__)

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/logs')
db = client.get_database()
collection = db['log_entries']

@app.route('/')
def index():
    # Define MongoDB aggregation pipelines
    top_data = {}
    for week in range(1, 7):  # Assuming weeks range from 1 to 52
        top_songs_pipeline = [
            {"$match": {"week": week}},
            {"$group": {"_id": "$song", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        top_artists_pipeline = [
            {"$match": {"week": week}},
            {"$group": {"_id": "$artist", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]

        # Execute aggregation queries
        top_songs = list(collection.aggregate(top_songs_pipeline))
        top_artists = list(collection.aggregate(top_artists_pipeline))

        # Add top songs and artists for the current week to top_data dictionary
        top_data[week] = {"top_songs": top_songs, "top_artists": top_artists}

    return render_template('index.html', top_data=top_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)