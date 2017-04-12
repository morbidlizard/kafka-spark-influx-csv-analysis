from dispatcher import Dispatcher
from config import Config

if __name__ == "__main__":
    try:
        config = Config("test/data/config.json")
        dispatcher = Dispatcher(config)
        dispatcher.run_pipeline()
    except BaseException as ex:
        print("Caught exception: ", ex)
        exit(1)
