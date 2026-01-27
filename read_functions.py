import pymongo
from datetime import datetime


# Connect to mongodb
myclient = pymongo.MongoClient("mongodb://localhost:27017/", username='root', password='password')

mydb = myclient["social"]  # select the database
mycol = mydb["posts"]  # select the collection


def find_recent_posts_by_user(user_id):
    """
    Retrieves the two most recent posts made by a user, given a user_id.
    
    Args:
        user_id: The user ID to search for
        
    Returns:
        List of the 2 most recent posts by the user
    """
    # Convert user_id to string since that's how we stored it
    user_id_str = str(user_id)
    
    # Find posts by user, sort by timestamp descending, limit to 2
    posts = mycol.find(
        {"user_id": user_id_str},
        {"_id": 1, "user_id": 1, "text": 1, "timestamp": 1}
    ).sort("timestamp", -1).limit(2)
    
    return list(posts)


def get_comments_for_post(post_id):
    """
    Fetches all comments for a given post.
    
    Args:
        post_id: The post ID to get comments for
        
    Returns:
        List of comments for the post (or empty list if no comments)
    """
    # Convert post_id to string
    post_id_str = str(post_id)
    
    # Find the post and get its comments
    post = mycol.find_one(
        {"_id": post_id_str},
        {"comments": 1}
    )
    
    if post and "comments" in post:
        return post["comments"]
    else:
        return []


def get_posts_with_min_likes(min_likes):
    """
    Finds posts with at least min_likes, but only returns the top three posts 
    that have the most comments.
    
    Args:
        min_likes: Minimum number of likes required
        
    Returns:
        List of top 3 posts with at least min_likes, sorted by comment count (descending)
    """
    # Use aggregation pipeline to:
    # 1. Add fields for likes_count (handles both post types)
    # 2. Match posts with at least min_likes
    # 3. Add field for comment count
    # 4. Sort by comment count descending
    # 5. Limit to top 3
    
    pipeline = [
        {
            "$addFields": {
                # Calculate likes_count based on post type
                "likes_count": {
                    "$cond": {
                        "if": {"$isArray": "$user_likes"},
                        "then": {"$size": "$user_likes"},  # Initial posts: array size
                        "else": {"$ifNull": ["$like_count", 0]}  # New posts: numeric count
                    }
                }
            }
        },
        {
            "$match": {
                "likes_count": {"$gte": min_likes}
            }
        },
        {
            "$addFields": {
                "comment_count": {
                    "$cond": {
                        "if": {"$isArray": "$comments"},
                        "then": {"$size": "$comments"},
                        "else": 0
                    }
                }
            }
        },
        {
            "$sort": {"comment_count": -1}
        },
        {
            "$limit": 3
        },
        {
            "$project": {
                "_id": 1,
                "user_id": 1,
                "text": 1,
                "timestamp": 1,
                "user_likes": 1,
                "like_count": 1,
                "comment_count": 1
            }
        }
    ]
    
    results = mycol.aggregate(pipeline)
    return list(results)


if __name__ == "__main__":
    user_id = 999334
    post_id = 58141
    min_likes = 49
    
    print("Most recent 2 posts by user:", find_recent_posts_by_user(user_id), "\n")
    print("Comments for post:", get_comments_for_post(post_id), "\n")
    print("Top 3 posts with at least X likes sorted by comments:\n")
    for post in get_posts_with_min_likes(min_likes):
        print(post, "\n")