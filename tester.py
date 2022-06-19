from converter import Converter, ENCODER
from converter.utils import make_logger
logger = make_logger("test")

def start():
	for i in range(1000):
		logger.info(f"test {i}")


if __name__ == "__main__":
    start()