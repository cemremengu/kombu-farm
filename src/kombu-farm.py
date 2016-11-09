import argparse
from types import ModuleType
from harvester import Harvester

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Farm')
    parser.add_argument(
        '-s', '--source',
        help='Harvest source (AMQP host such as amqp://guest:guest@localhost:5672)',
        required=True)
    parser.add_argument(
        '-q', '--queue',
        help='Queue name to harvest from',
        required=True)
    parser.add_argument(
        '-a', '--add',
        help='Harvester instance (file)',
        required=True,
        type=argparse.FileType('rb'))
    return vars(parser.parse_args())

if __name__ == '__main__':

    OPTS = parse_args()

    COMPILED = compile(OPTS['add'].read(), OPTS['add'].name, 'exec')

    try:
        exec(COMPILED, ModuleType("my_harvester").__dict__)
        vars()['Harvester'].__subclasses__()[0](OPTS['source'], OPTS['queue']).run()
    except KeyboardInterrupt:
        print("bye bye")
    except Exception as ex:
        print("Error occured when bootstrapping the harvester", ex)
