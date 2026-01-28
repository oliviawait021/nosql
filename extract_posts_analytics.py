import pymongo
import pandas as pd
from datetime import datetime
import os
import shutil
import pyarrow


# Connect to mongodb
myclient = pymongo.MongoClient("mongodb://localhost:27017/", username='root', password='password')

mydb = myclient["social"]  # select the database
posts_col = mydb["posts"]  # select the collection


def extract_posts_analytics():
    """
    Extract posts from MongoDB and create a normalized analytics dataframe.
    
    Handles two types of posts:
    - Initial posts: with user_likes array
    - New posts: with numeric like_count
    
    Returns:
        pandas DataFrame with columns: post_id, author_id, timestamp, text,
                                       like_count, comment_count, last_activity_timestamp
    """
    
    print("Extracting posts from MongoDB...")
    
    # Fetch all posts
    posts = list(posts_col.find({}))
    
    print(f"Retrieved {len(posts)} posts")
    print()
    
    # Build list of records for dataframe
    records = []
    
    for post in posts:
        # Basic fields
        post_id = post.get('_id')
        author_id = post.get('user_id')
        timestamp = post.get('timestamp')
        text = post.get('text')
        
        # Calculate like_count based on post type
        if 'user_likes' in post:
            # Initial posts: count users in user_likes array
            like_count = len(post['user_likes']) if post['user_likes'] else 0
        elif 'like_count' in post:
            # New posts: use existing like_count
            like_count = post['like_count']
        else:
            like_count = 0
        
        # Calculate comment_count
        comments = post.get('comments', [])
        comment_count = len(comments) if comments else 0
        
        # Calculate last_activity_timestamp
        # Start with post timestamp
        post_timestamp = timestamp
        last_activity = post_timestamp
        
        # Check if any comments have a more recent timestamp
        if comments:
            for comment in comments:
                comment_timestamp = comment.get('timestamp')
                if comment_timestamp and comment_timestamp > last_activity:
                    last_activity = comment_timestamp
        
        # Create record
        record = {
            'post_id': post_id,
            'author_id': author_id,
            'timestamp': timestamp,
            'text': text,
            'like_count': like_count,
            'comment_count': comment_count,
            'last_activity_timestamp': last_activity
        }
        
        records.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Sort by post_id for consistency
    df = df.sort_values('post_id').reset_index(drop=True)
    
    return df


def write_partitioned_parquet(df, output_dir='out/posts_analytics'):
    """
    Write DataFrame to partitioned Parquet files organized by post_date.
    
    Directory structure: out/posts_analytics/post_date=YYYY-MM-DD/posts.parquet
    
    This function is idempotent - re-running will overwrite existing partitions,
    not create duplicates.
    
    Args:
        df: DataFrame with posts data
        output_dir: Base output directory
    """
    
    print("Writing partitioned Parquet files...")
    print()
    
    # Create post_date column from timestamp
    # Extract date from ISO format timestamp (YYYY-MM-DDTHH:MM:SSZ)
    df['post_date'] = df['timestamp'].str[:10]  # Extract YYYY-MM-DD
    
    # Get unique dates
    unique_dates = df['post_date'].unique()
    print(f"Found {len(unique_dates)} unique post dates")
    print()
    
    # Track statistics
    partitions_written = 0
    total_records = 0
    
    for post_date in sorted(unique_dates):
        # Filter data for this date
        partition_df = df[df['post_date'] == post_date].copy()
        
        # Drop the post_date column (it's in the path, not needed in the data)
        partition_df = partition_df.drop('post_date', axis=1)
        
        # Create partition directory
        partition_dir = os.path.join(output_dir, f'post_date={post_date}')
        
        # Remove existing partition directory if it exists (for idempotency)
        if os.path.exists(partition_dir):
            shutil.rmtree(partition_dir)
        
        # Create directory
        os.makedirs(partition_dir, exist_ok=True)
        
        # Write Parquet file
        output_file = os.path.join(partition_dir, 'posts.parquet')
        partition_df.to_parquet(output_file, engine='pyarrow', index=False)
        
        partitions_written += 1
        total_records += len(partition_df)
        
        print(f"  ✓ Written partition: post_date={post_date} ({len(partition_df)} records)")
    
    print()
    print(f"Total partitions written: {partitions_written}")
    print(f"Total records written: {total_records}")
    print()
    
    return partitions_written, total_records


def verify_output(output_dir='out/posts_analytics'):
    """
    Verify the partitioned output by reading back and checking for duplicates.
    
    Args:
        output_dir: Base output directory
    """
    
    print("Verifying output...")
    print()
    
    if not os.path.exists(output_dir):
        print("  ✗ Output directory does not exist")
        return
    
    # List all partitions
    partitions = sorted([d for d in os.listdir(output_dir) if d.startswith('post_date=')])
    
    print(f"Found {len(partitions)} partitions:")
    for partition in partitions[:5]:  # Show first 5
        partition_path = os.path.join(output_dir, partition, 'posts.parquet')
        if os.path.exists(partition_path):
            df = pd.read_parquet(partition_path)
            print(f"  - {partition}: {len(df)} records")
    
    if len(partitions) > 5:
        print(f"  ... and {len(partitions) - 5} more")
    
    print()
    
    # Check for duplicates across all partitions
    all_post_ids = []
    total_records = 0
    
    for partition in partitions:
        partition_path = os.path.join(output_dir, partition, 'posts.parquet')
        if os.path.exists(partition_path):
            df = pd.read_parquet(partition_path)
            all_post_ids.extend(df['post_id'].tolist())
            total_records += len(df)
    
    unique_post_ids = len(set(all_post_ids))
    
    print(f"Total records across all partitions: {total_records}")
    print(f"Unique post IDs: {unique_post_ids}")
    
    if total_records == unique_post_ids:
        print("✓ No duplicates found - output is clean!")
    else:
        print(f"⚠ Warning: Found {total_records - unique_post_ids} duplicate records")
    
    print()


if __name__ == "__main__":
    print("=" * 70)
    print("Posts Analytics Extraction & Export")
    print("=" * 70)
    print()
    
    # Extract data
    df = extract_posts_analytics()
    
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print()
    
    # Show summary statistics
    print("Summary Statistics:")
    print("-" * 70)
    print(f"Total posts: {len(df)}")
    print(f"Average likes per post: {df['like_count'].mean():.2f}")
    print(f"Average comments per post: {df['comment_count'].mean():.2f}")
    print(f"Posts with comments: {(df['comment_count'] > 0).sum()}")
    print(f"Posts without comments: {(df['comment_count'] == 0).sum()}")
    print()
    
    # Show sample rows
    print("Sample rows (first 5):")
    print("-" * 70)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 50)
    print(df.head(5))
    print()
    
    # Write partitioned Parquet files
    print("=" * 70)
    partitions_written, total_records = write_partitioned_parquet(df)
    
    # Verify output
    print("=" * 70)
    verify_output()
    
    print("=" * 70)
    print("✓ Extraction and export complete!")
    print()
    print("Output structure:")
    print("  out/posts_analytics/")
    print("    post_date=YYYY-MM-DD/")
    print("      posts.parquet")
    print()
    print("Note: Re-running this script will overwrite existing partitions")
    print("      (idempotent operation - no duplicates will be created)")
    print("=" * 70)