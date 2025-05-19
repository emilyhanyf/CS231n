import os
import json
import base64
import requests
from pathlib import Path
import time
from openai import OpenAI
import csv

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Directories
DATA_DIR = os.path.abspath("data")
FRAME_DIR = os.path.join(DATA_DIR, "frames")
OPENAI_DIR = os.path.abspath("openai_data")  # At same level as data folder
os.makedirs(OPENAI_DIR, exist_ok=True)

def encode_image(image_path):
    """Encode image to base64."""
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def generate_caption(image_path):
    """Generate caption for an image using GPT-4 Vision."""
    try:
        # Encode the image
        base64_image = encode_image(image_path)
        
        # Call GPT-4 Vision API with the current model (updated from deprecated gpt-4-vision-preview)
        response = client.chat.completions.create(
            model="gpt-4o",  # Updated to the current model that supports vision
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Describe this image in detail, focusing on the animal(s) present and their actions. Keep the description concise but informative."
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=150
        )
        
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error generating caption for {image_path}: {str(e)}")
        return None

def process_images():
    """Process all images in the frames directory."""
    # Read the original frame metadata
    frame_metadata = os.path.join(DATA_DIR, "frame_metadata.csv")
    if not os.path.exists(frame_metadata):
        print("Error: frame_metadata.csv not found")
        return

    # Create new CSV for OpenAI results
    results_file = os.path.join(OPENAI_DIR, "openai_captions.csv")
    with open(results_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['youtube_url', 'timestamp', 'frame_path', 'openai_caption'])
        
        # Read original metadata
        with open(frame_metadata, 'r') as meta_f:
            reader = csv.DictReader(meta_f)
            for i, row in enumerate(reader):
                print(f"Processing image {i+1}: {row['frame_path']}")
                
                # Generate caption
                caption = generate_caption(row['frame_path'])
                if not caption:
                    print(f"Skipping {row['frame_path']} due to caption generation failure")
                    continue
                
                # Save results
                writer.writerow([
                    row['youtube_url'],
                    row['timestamp'],
                    row['frame_path'],
                    caption
                ])
                print(f"Successfully processed {row['frame_path']}")
                
                # Add a small delay to avoid rate limits
                time.sleep(1)

if __name__ == "__main__":
    # Check for API key
    if not os.getenv('OPENAI_API_KEY'):
        print("Please set your OPENAI_API_KEY environment variable")
        exit(1)
    
    print("Starting caption generation...")
    process_images()
    print("Processing complete!")