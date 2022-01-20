import argparse
import sys
import traceback

from os.path import exists
from qcg.pilotjob.executor.slurmres import test_environment

class SlurmEnvResources:

    def __init__(self, args=None):
        """
        Parse slurm resources based on environment settings.

        Args:
            args(str[]) - arguments, if None the command line arguments are parsed
        """
        self.__args = self.__get_argument_parser().parse_args(args)

        self.env_file = self.__args.envfile


    def parse(self):
        test_environment(self.env_file.read())


    def __get_argument_parser(self):
        """
        Create argument parser.
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("envfile",
                            help="path to the file with environment settings",
                            type=argparse.FileType('r'), default=None)

        return parser


if __name__ == "__main__":
    try:
        SlurmEnvResources().parse()
    except Exception as e:
        sys.stderr.write('error: %s\n' % (str(e)))
        traceback.print_exc()
        exit(1)
