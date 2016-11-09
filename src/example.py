from harvester import Harvester

class ExampleHarvester(Harvester):
    """
    An example harvester instance
    """
    def gather(self, body):
        print('gather', body)

    def process(self, data):
        print('process')

    def dump(self, result):
        print('dump')
