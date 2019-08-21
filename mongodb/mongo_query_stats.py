########################
# MongoDB Query Stats
# Created by dinesh
#
# Usage: Utilize this script to collect information on Query stats
#
# Output: Script will output in CSV to a file named <log_name>_query_stats.csv by default. The '-o' option will allow you to change the name.
#
#######################

import argparse
import csv
import re
import collections


def parse_queries(input):
    # Final dictionary returned
    collections = {}

    with open(input) as infile:
        for line in infile:
            try:
                action = line.split()[2]
                # Only action item is currently COMMAND. Need to expand to other mongo actions
                if action == 'COMMAND':
                    execution_time = line.split()[-1]
                    pattern = re.compile("\A\d+ms\Z")
                    collection = ' '.join(str(x) for x in line.split()[4:])

                    if pattern.match(execution_time) is None:
                      pass
                    else:
                      time_in_ms = int(execution_time.strip('ms'))
                      try:
                        if collection in collections[time_in_ms]:
                            pass
                        else:
                            collections[time_in_ms].append(collection)
                      except KeyError:
                        collections[time_in_ms] = [collection]
            except IndexError:
                pass

        return collections

def outputResults(output,output_file):
    # CSV output is currently default
    with open(output_file,'w') as csvfile:
        csv_out = csv.writer(csvfile)
        csv_out.writerow(['Execution time in MS', 'Collections Accessed'])
        for k, v in output.iteritems():
            csv_out.writerow([k, (",").join(output[k])])

    print "- Total {} query written".format(len(output))

def main():
    parser = argparse.ArgumentParser(description='Parse query statistics from MongoDB 2.x logs')
    group = parser.add_argument_group()
    group.add_argument('-o','--out',metavar='FILE',help='File to output to [default is <log_name>_query_stats.csv]')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-l','--log',metavar='LOG',help='Log to Parse')

    args = parser.parse_args()

    if args.log:
        try:
            print "- Parsing query information from log file {}...".format(args.log)
            output = parse_queries(args.log)
            if not output:
                print "- No query information was found..."
            else:
                if not args.out:
                    output_file_name = args.log + "_query_stats.csv"
                else:
                    output_file_name = args.out
                sorted_output = collections.OrderedDict(sorted(output.items(), reverse=True))
                outputResults(sorted_output,output_file_name)
                print "- Output written to {}...".format(output_file_name)
        except IOError:
            print "- Error parsing file {}...".format(args.log)

if __name__ == "__main__":
    main()
