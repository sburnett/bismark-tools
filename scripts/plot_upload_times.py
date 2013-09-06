import datetime
import glob
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import os.path
import sys

def convert_time(s):
    return datetime.datetime.fromtimestamp(int(s))

def main():
    for filename in glob.glob('/data/users/sburnett/upload-times-csv/*.csv'):
        plt.clf()
        basename = os.path.basename(filename)
        name, _ = os.path.splitext(basename)
        experiment, node = name.split('_')
        print node, experiment,
        sys.stdout.flush()

        converters = dict(created=convert_time, received=convert_time)
        data = mlab.csv2rec(
                filename,
                names=['created', 'received'],
                converterd=converters)
        if len(data) < 30:
            print 'skipped'
            continue

        plt.plot(data['created'], data['received'], '.')
        plt.title('%s on %s' % (experiment, node))
        plt.xlabel('Creation timestamp')
        plt.ylabel('Received timestamp')
        plt.tight_layout()
        plt.savefig('/data/users/sburnett/upload-times-csv/plots/%s.png' % name)
        print 'plotted'

if __name__ == '__main__':
    main()
