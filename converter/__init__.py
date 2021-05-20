from pathlib import PurePosixPath, Path
import subprocess
import sys
import hashlib
import traceback
from enum import Enum
import os
import time

from .utils import make_logger
from .media_converter import convert_video

class ENCODER(Enum):
    CPU = "libx265"
    NVIDIA = "hevc_nvenc"

class Converter:

    def __init__(self, remote_server=None, username=None, remote_ingest_folder=None, remote_output_folder=None, encoder=None):
        self.logger = make_logger(self.__class__.__name__)
        if remote_server is None:
            raise RuntimeError("Missing remote_server")
        if username is None:
            raise RuntimeError("Missing username")
        if remote_ingest_folder is None:
            raise RuntimeError("Missing remote_ingest_folder")
        if remote_output_folder is None:
            raise RuntimeError("Missing remote_output_folder")
        if encoder is None:
            raise RuntimeError("Missing encoder")
        self.remote_server = remote_server
        self.username = username
        self.remote_ingest_folder = PurePosixPath(remote_ingest_folder)
        self.remote_output_folder = PurePosixPath(remote_output_folder)
        self.encoder = encoder
        self.allowed_size_increase = 1.3 # skips uploading of file that conversion caused it to balloon to 1.3 times the size or larger

    def execute(self):
        while True:
            try:
                self.start_flow()
            except:
                raise

    def start_flow(self):
        # flow:
        # list all files
        all_files = self.list_files(self.remote_ingest_folder)
        if len(all_files) == 0:
            raise RuntimeError("No files to process")

        # take first file
        selected_file = all_files[0]
        print(selected_file)
        for attempt in range(3):
            # download it
            local_file = self.download_file(selected_file)
            remote_checksum = self.get_remote_checksum(selected_file)
            local_checksum = self.get_local_checksum(local_file)
            # check checksum against server
            if remote_checksum == local_checksum:
                break
            else:
                self.logger.warning(f"Checksums don't match, file: {selected_file}, attempt: {attempt+1}")
                continue
        else:
            raise RuntimeError(f"Checksums never matched, file: {selected_file}")

        # process
        for attempt in range(3):
            try:
                local_converted_file = convert_video(local_file, encoder=self.encoder)
                break
            except:
                self.logger.error(f"Convert failed for file: {local_file}, reason: {traceback.format_exc()}")
                continue
        else:
            raise RuntimeError(f"Convert failed for file: {local_file}")

        # compare 2 files
        local_file_size = local_file.stat().st_size
        local_converted_file_size = local_converted_file.stat().st_size

        should_upload_converted_file = True
        if local_converted_file_size > local_file_size:
            size_increase = local_converted_file_size / local_file_size
            # skip uploading the file if we have increase compared to original size
            if size_increase > self.allowed_size_increase:
                should_upload_converted_file = False

        if should_upload_converted_file:
            for attempt in range(3):
                # upload file
                remote_converted_file = self.upload_file(local_converted_file, self.remote_output_folder)
                local_checksum = self.get_local_checksum(local_converted_file)
                remote_checksum = self.get_remote_checksum(remote_converted_file)
                # check checksum
                if local_checksum == remote_checksum:
                    break
                else:
                    self.logger.warning(f"Checksums after upload don't match, file: {local_converted_file}, attempt: {attempt+1}")
            else:
                raise RuntimeError(f"Checksums after upload never matched, file: {selected_file}")
        else:
            # we should just move the file to the output folder, but add "processed" to the name
            self.copy_and_rename_file(selected_file, self.remote_output_folder)

        # delete local files
        os.remove(local_file)
        os.remove(local_converted_file)
        # delete remote file
        self.delete_remote_file(selected_file)

    def list_files(self, remote_folder: PurePosixPath):
        cmd = [
            "ssh", self.remote_server, "ls", "-1", str(remote_folder)
        ]
        p = subprocess.check_output(cmd).decode(sys.stdout.encoding)
        all_rows = p.split("\n")
        all_files = filter(lambda x: len(x), all_rows)
        return list(map(lambda x: remote_folder / x, all_files))

    def copy_and_rename_file(self, remote_file: PurePosixPath, remote_folder: PurePosixPath) -> Path:
        new_file_name = f"{remote_file.stem}.processed{remote_file.suffix}"
        new_file_path = remote_folder / new_file_name
        cmd = [
            "ssh", self.remote_server, "cp", f'"{remote_file}"', f'"{new_file_path}"'
        ]
        p = subprocess.check_call(cmd)

    def download_file(self, remote_file: PurePosixPath) -> Path:
        local_file = Path(".") / remote_file.name
        cmd = [
            "scp", "-T", f'{self.remote_server}:"{remote_file}"', f'{local_file}'
        ]
        p = subprocess.check_call(cmd)
        return local_file

    def upload_file(self, local_file: Path, remote_folder: PurePosixPath) -> Path:
        remote_file = remote_folder / local_file.name
        cmd = [
            "scp", "-T", f'{local_file}', f'{self.remote_server}:"{remote_file}"'
        ]
        p = subprocess.check_call(cmd)
        return remote_file

    def get_remote_checksum(self, remote_file: PurePosixPath) -> str:
        cmd = [
            "ssh", self.remote_server, "md5sum", f'"{remote_file}"'
        ]
        p = subprocess.check_output(cmd).decode(sys.stdout.encoding)
        checksum = p.split(' ')[0]
        return checksum

    def get_local_checksum(self, local_file: Path) -> str:
        with local_file.open("rb") as f:
            file_hash = hashlib.md5()
            while chunk := f.read(8192):
                file_hash.update(chunk)
            
            return file_hash.hexdigest()

    def delete_remote_file(self, remote_file: PurePosixPath):
        cmd = [
            "ssh", self.remote_server, "rm", "-f", f'"{remote_file}"'
        ]
        subprocess.check_call(cmd)

