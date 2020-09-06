import os
import sys
import csv
import glob
from multiprocessing.dummy import Pool


def clean_csv(csvfile):

    print(f"[-] Cleaning csvfile {csvfile}...")
    with open(csvfile, 'r') as fin:
        data = fin.read().splitlines(True)
        fin.close()
        if "Compliance Notice:" in data[0]:
            # Strip the compliance notice off the top of the file if it exists.
            with open(csvfile, 'w') as fout:
                fout.writelines(data[1:])
            fout.close()

    print(f"[-] Done!")


def get_targs(csvfile):

    results = set()

    with open(csvfile, 'r') as csvf:
        csv_reader = csv.DictReader(csvf, quotechar='"')
        for row in csv_reader:
            hosts = row['Location on Site'].split(";")
            for host in hosts:
                host = host.replace('/*', '')
                host = host.replace('/ mobile', '')
                host = host + "\n"
                results.add(host)

    return {"csvfile": csvfile, "results": results}


def main():

    indir = sys.argv[1]
    outfile = sys.argv[2]

    p = Pool()
    os.chdir(indir)
    files = glob.glob("*.csv")
    p.imap_unordered(clean_csv, files)

    total = 0
    with open('outfile', 'w') as outf:
        for _ in p.imap_unordered(get_targs, files):
            print(f"[+] Got {len(_.get('results'))} hosts for {_.get('csvfile')}")
            for host in _.get('results'):
                outf.write(host)
                total += len(_.get('results'))
        print(f"[+] Done! Wrote a total of {total} hosts to {outfile}!")


if __name__ == "__main__":

    main()
