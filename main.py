import sys
import logging
from config_parsing.config import Config
from dispatcher.dispatcher import Dispatcher

if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            logging.critical("Invalid amount of arguments\nUsage: main.py config.json")
            exit(1)

        config = Config(sys.argv[1].strip())
        dispatcher = Dispatcher(config)
        dispatcher.run_pipeline()
        dispatcher.stop_pipeline()
    except KeyboardInterrupt:
        logging.warning("You terminated execution.")
        exit(2)
    except BaseException as ex:
        logging.exception(ex)
        exit(1)
