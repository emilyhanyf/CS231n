import os
import random
import csv
import yt_dlp
import ffmpeg
import time
from datetime import timedelta
import tempfile
import json

DATA_DIR = os.path.abspath("data")  # Get absolute path
FRAME_DIR = os.path.join(DATA_DIR, "frames")
META_FILE = os.path.join(DATA_DIR, "balanced_train_segments.csv")  # Your AudioSet CSV file
OUTPUT_CSV = os.path.join(DATA_DIR, "frame_metadata.csv")

os.makedirs(FRAME_DIR, exist_ok=True)

def get_random_frame(ytid, start_sec, duration, index, max_retries=3):
    url = f"https://www.youtube.com/watch?v={ytid}"
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Calculate the middle point of the segment
            middle_point = start_sec + (duration / 2)
            # We want 10 seconds centered around the middle point
            download_start = max(0, middle_point - 5)
            download_end = middle_point + 5
            
            # Temporary output file
            temp_dir = tempfile.gettempdir()
            temp_output = os.path.join(temp_dir, f"{ytid}_{int(time.time())}.mp4")
            
            # Configure yt-dlp options for downloading ONLY the segment we need
            ydl_opts = {
                'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                'quiet': True,
                'no_warnings': True,
                'outtmpl': temp_output,
                'ignoreerrors': True,
                'noplaylist': True,
                # These options should work with recent yt-dlp versions to download segments
                'download_ranges': 'chapters',  # Required for the next option to work
                'extractor_args': {
                    'youtube': {
                        'player_skip': ['webpage', 'js'],  # For efficiency
                        'player_client': ['web'],  # Use web client
                    }
                },
                # Define a virtual chapter for our segment
                'force_keyframes_at_cuts': True,
                'external_downloader': 'ffmpeg',  # Use ffmpeg as the external downloader
                'external_downloader_args': {
                    'ffmpeg': ['-ss', str(download_start), '-t', '1']  # Download 10 seconds
                }
            }
            
            # First get video info
            with yt_dlp.YoutubeDL({'quiet': True, 'no_warnings': True}) as ydl_info:
                try:
                    info = ydl_info.extract_info(url, download=False)
                    if info is None:
                        print(f"Could not get info for {url} - video may be private or removed")
                        return None
                        
                    video_duration = info.get('duration', 0)
                    if video_duration == 0:
                        print(f"Could not get duration for {url}")
                        return None
                    
                    # Ensure our timestamp is within the video duration
                    if download_start >= video_duration:
                        print(f"Start time {download_start} is beyond video duration {video_duration}")
                        return None
                    
                    # Format timestamp as MM:SS
                    timestamp_str = str(timedelta(seconds=int(middle_point)))
                    
                    # Now download just the segment using ffmpeg through yt-dlp
                    print(f"Downloading 10-second segment from {url} at {download_start}s...")
                    
                    # Use a direct ffmpeg command through subprocess for more control
                    import subprocess
                    ffmpeg_command = [
                        'ffmpeg',
                        '-hide_banner',
                        '-loglevel', 'error',
                        '-ss', str(download_start),
                        '-i', f"$(yt-dlp -f 'best[ext=mp4]/best' --get-url {url})",
                        '-t', '10',
                        '-c:v', 'copy',
                        '-c:a', 'copy',
                        temp_output
                    ]
                    
                    # Create a bash script to execute the command (handles the subshell expansion)
                    script_path = os.path.join(temp_dir, f"download_{ytid}.sh")
                    with open(script_path, 'w') as f:
                        f.write("#!/bin/bash\n")
                        f.write(" ".join(ffmpeg_command))
                    
                    os.chmod(script_path, 0o755)  # Make executable
                    
                    # Run the script
                    try:
                        subprocess.run(['/bin/bash', script_path], check=True, 
                                      stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                    except subprocess.CalledProcessError as e:
                        print(f"Error downloading segment: {e.stderr.decode() if e.stderr else str(e)}")
                        return None
                    
                    # Clean up script
                    os.remove(script_path)
                    
                    # Check if download was successful
                    if not os.path.exists(temp_output) or os.path.getsize(temp_output) < 1000:  # Tiny files are likely errors
                        print(f"Downloaded segment too small or missing: {temp_output}")
                        
                        # Alternative direct approach using yt-dlp
                        print("Trying alternative download method...")
                        with yt_dlp.YoutubeDL({
                            'format': 'best[ext=mp4]/best',
                            'quiet': True,
                            'no_warnings': True,
                            'outtmpl': temp_output,
                            # Let yt-dlp handle it internally
                            'postprocessor_args': {
                                'ffmpeg': ['-ss', str(download_start), '-t', '10']
                            }
                        }) as ydl:
                            ydl.download([url])
                    
                    # Extract frame from the 10-second clip
                    frame_path = os.path.join(FRAME_DIR, f"{index}.jpg")
                    
                    if os.path.exists(temp_output) and os.path.getsize(temp_output) > 1000:
                        try:
                            # Get the duration of our clip
                            probe = ffmpeg.probe(temp_output)
                            clip_duration = float(probe['format']['duration'])
                            
                            # Extract frame from the middle of our clip
                            middle_time = min(5, clip_duration / 2)  # 5 seconds or half of shorter clips
                            
                            # Extract the frame
                            stream = ffmpeg.input(temp_output, ss=middle_time)
                            stream = ffmpeg.output(stream, frame_path, vframes=1, loglevel="error")
                            ffmpeg.run(stream, overwrite_output=True, capture_stdout=True, capture_stderr=True)
                            
                            # Clean up the temporary files
                            if os.path.exists(temp_output):
                                os.remove(temp_output)
                            
                            if os.path.exists(frame_path) and os.path.getsize(frame_path) > 0:
                                print(f"Successfully extracted frame from {url}")
                                return {
                                    "youtube_url": url,
                                    "timestamp": timestamp_str,
                                    "frame_path": os.path.abspath(frame_path)
                                }
                            else:
                                print(f"Failed to save frame for {url}")
                                return None
                                
                        except ffmpeg.Error as e:
                            print(f"FFmpeg error for {url}: {e.stderr.decode() if e.stderr else str(e)}")
                            if os.path.exists(temp_output):
                                os.remove(temp_output)
                            return None
                        except Exception as e:
                            print(f"Error processing video {url}: {str(e)}")
                            if os.path.exists(temp_output):
                                os.remove(temp_output)
                            return None
                    else:
                        print(f"Failed to download segment for {url}")
                        return None

                except Exception as e:
                    print(f"Error extracting info for {url}: {str(e)}")
                    return None

        except Exception as e:
            retry_count += 1
            if retry_count < max_retries:
                print(f"Error on {url}, retrying ({retry_count}/{max_retries})...")
                time.sleep(2 * retry_count)  # Exponential backoff
                continue
            print(f"Failed on {url} after {max_retries} retries: {str(e)}")
            return None

def sample_metadata(n=100):
    header = None
    data_lines = []
    with open(META_FILE, 'r') as f:
        for line in f:
            if line.startswith('#'):
                if 'YTID' in line and 'positive_labels' in line:
                    header = [h.strip() for h in line.lstrip('# ').split(',')]
                continue
            data_lines.append(line.strip())

    if header is None:
        raise RuntimeError("Could not find header line in CSV")

    reader = csv.DictReader(data_lines, fieldnames=header)
    entries = [row for row in reader if '/m/09x0r' in row['positive_labels']]
    return random.sample(entries, n)

def collect_dataset(n=100):
    results = []
    entries = sample_metadata(n)
    
    # Create CSV file with headers
    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['youtube_url', 'timestamp', 'frame_path'])
        writer.writeheader()
        
        successful = 0
        for i, entry in enumerate(entries):
            print(f"[{i+1}/{n}] Processing {entry['YTID']}")
            result = get_random_frame(
                entry['YTID'], 
                float(entry['start_seconds']), 
                float(entry['end_seconds']) - float(entry['start_seconds']), 
                i
            )
            if result:
                results.append(result)
                writer.writerow(result)
                f.flush()  # Ensure data is written immediately
                successful += 1
                print(f"Successfully processed {entry['YTID']} ({successful}/{i+1} successful)")
            else:
                print(f"Failed to process {entry['YTID']} ({successful}/{i+1} successful)")

if __name__ == "__main__":
    collect_dataset(n=10)