import glob, os

source_dir = '/data/hdfs/test/data'
target_dir = '/data/hdfs/test/merge'
os.chdir(source_dir)
files = glob.glob('*.csv')
sorted_files = sorted(files, key=lambda x: x.split('.')[0])
split = 50
i = 0
j = 1
merge_file = None
file_content = []
for file in sorted_files:
    i = i + 1
    merge_file_name = "%s/WTM_%s.csv" % (target_dir, j)
    with open(file, 'r') as f:
        print "Merge %s into %s" % (file, merge_file_name)
        file_content.extend(f.readlines())
    f.closed
    if i % split == 0 or i == len(sorted_files):
        with open(merge_file_name, "w") as merge_file:
            print "Saving %s" % merge_file_name
            for item in file_content:
                merge_file.write("%s" % item)
        merge_file.closed
        file_content = []
        j = j + 1
