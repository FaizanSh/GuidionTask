import logging
from utils.config_reader import Config_Reader
from pre_processing import Pre_Processing
from processing import Processing
from data_quality_checks import Data_Quality_Checks
import argparse


class Init_Class:
    def __init__(self, output, yml):
        config = Config_Reader(yml)
        logcheck = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO
        }
        if output != 'console':
            logging.basicConfig(filename=output, filemode='w', level=logcheck[config.get_logging()], format='%(asctime)s - %(filename)s - %(message)s')
        else:
            logging.basicConfig(format='%(asctime)s - %(filename)s - %(message)s', level=logcheck[config.get_logging()])

        # pre processing data | dropping duplicate | extracting required values
        preprocess = Pre_Processing(config)
        preprocess.main()

        # deviding tables in fact and dimention
        process = Processing(config, preprocess)
        process.main()

        # data qulity checks
        dq = Data_Quality_Checks(config, preprocess)
        dq.main()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', dest='output', help='[optional] Allowed values console and filepath', default='console')
    parser.add_argument('--config', dest='yml', help='[optional] Allowed yaml config file path', default='config.yml')
    arguments = parser.parse_args()
    main = Init_Class(arguments.output, arguments.yml)
