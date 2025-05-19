import os
import csv
import time
import torch
from audiocraft.models import AudioGen
from audiocraft.data.audio import audio_write

# Create audio directory if it doesn't exist
AUDIO_DIR = "openai_data/audio"
os.makedirs(AUDIO_DIR, exist_ok=True)

def generate_audio(text, output_path):
    """Generate audio from text using Audiocraft's AudioGen model."""
    try:
        # Initialize the model
        model = AudioGen.get_pretrained('facebook/audiogen-medium')
        model.set_generation_params(duration=5)  # Generate 5 seconds of audio
        
        # Generate audio
        wav = model.generate([text], progress=True)
        
        # Save the audio file
        audio_write(output_path, wav[0].cpu(), model.sample_rate, strategy="loudness")
        return True
    except Exception as e:
        print(f"Error generating audio: {e}")
        return False

def process_captions():
    """Process captions from CSV and generate audio files."""
    captions_file = "openai_data/openai_captions.csv"
    output_file = "openai_data/audio_metadata.csv"
    
    if not os.path.exists(captions_file):
        print(f"Captions file not found: {captions_file}")
        return
    
    with open(captions_file, 'r') as f_in, open(output_file, 'w', newline='') as f_out:
        reader = csv.DictReader(f_in)
        # Get the fieldnames from the input file and add 'audio_path'
        fieldnames = reader.fieldnames + ['audio_path']
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        
        for i, row in enumerate(reader):
            caption = row['openai_caption']
            audio_path = os.path.join(AUDIO_DIR, f"{i}.wav")
            
            print(f"Processing caption {i}: {caption[:100]}...")
            
            if generate_audio(caption, audio_path):
                row['audio_path'] = audio_path
                writer.writerow(row)
                print(f"Generated audio for caption {i}")
            else:
                print(f"Failed to generate audio for caption {i}")
            
            # Add a small delay to avoid rate limits
            time.sleep(1)

if __name__ == "__main__":
    process_captions()