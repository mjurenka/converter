from converter import Converter, ENCODER

def start():
    config = {
        "remote_server": "mediaserver",
        "username": "martin",
        "remote_ingest_folder": "/volume2/vr-video/convert-ingest",
        "remote_output_folder": "/volume2/vr-video/convert-output",
        "encoder": ENCODER.NVIDIA.value
    }
    c = Converter(**config)
    c.execute()

if __name__ == "__main__":
    start()