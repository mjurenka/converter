import os
from pathlib import Path
import subprocess
import traceback

def convert_video(local_file: Path, encoder, bitrate="10M") -> Path:
    output_file = Path(f"{local_file.stem}.converted.mp4")
    command = [
        'ffmpeg',
        '-y',
        '-i', str(local_file.name),
        '-c:v', encoder,
        '-preset', 'fast',
        '-crf', '18',
        '-maxrate', '50M',
        '-bufsize', '25M',
        '-vf', 'scale=4096x2048',
        '-pix_fmt', 'yuv420p',
        '-c:a', 'aac',
        '-b:a', '160k',
        '-b:v', bitrate,
        '-movflags', 'faststart',
        str(output_file),
    ]
    subprocess.check_call(command)
    return output_file