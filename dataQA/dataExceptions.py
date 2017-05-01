import csv


class DataException(Exception):
    """
    This exception is raised when either the table doesn't exists or requisite columns do not
    """
    schema = ''

    def __init__(self, message):
        self.msg = message

    def __str__(self):
        return "Data Exception! Schema {}".format(self.schema) + '\n' + self.msg


def write_out(message, fp):
    """
    write out error message for csv instead of normal pandas dataframe
    """
    with open(fp, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"')
        writer.writerow([message])
