import sys
from dailyMain import main


if __name__ == '__main__':
    """
    Script expects at least 2 cmd line arguments.
    1. Schema in Redshift
    2. fullpath of output csv file
    3. optional. timeshift. # in hours to shift timestamp in logs
    """
    main(sys.argv[1:])
