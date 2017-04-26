import logging
from config_parsing.config import Config
from dispatcher.dispatcher import Dispatcher

if __name__ == "__main__":
    try:
        config = Config("config.json")
        dispatcher = Dispatcher(config)
        dispatcher.run_pipeline()
    except BaseException as ex:
        logging.exception(ex)
        exit(1)
