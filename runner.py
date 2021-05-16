from converter import Converter, ENCODER

def start():
    config = {
        "remote_server": "mediaserver",
        "username": "pi",
        "remote_ingest_folder": "/mnt/disk-4tb/vr/convert-ingest",
        "remote_output_folder": "/mnt/disk-4tb/vr/convert-output",
        "encoder": ENCODER.NVIDIA.value
    }
    c = Converter(**config)
    c.execute()

if __name__ == "__main__":
    start()