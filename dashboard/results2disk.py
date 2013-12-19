try:
    import simplejson as json
except ImportError:
    import json
from urllib2 import urlopen, HTTPError
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import sys, os, gzip
from utils import mkdirp
from auxiliary import replace_nan_inf

def get_simple_measures_definition(measure):
    return {
        "kind":                     "exponential",
        "low":                      1,
        "high":                     30000,
        "n_buckets":                50,
        "extended_statistics_ok":   True,
        "description":              "Histogram aggregation for simple measure: %s" % measure[15:]
    }

def load_json_from_file(path, compressed, fallback_value = None):
    if os.path.isfile(path):
        if compressed:
            with gzip.open(path, 'r') as f:
                return json.load(f)
        else:
            with open(path, 'r') as f:
                return json.load(f)
    return fallback_value

def save_json_to_file(path, value, compress, pretty_print):
    print_indent = None
    if pretty_print:
        print_indent = 2
    if compress:
        with gzip.open(path, 'w') as f:
            json.dump(value, f, indent = print_indent)
    else:
        with open(path, 'w') as f:
            json.dump(value, f, indent = print_indent)

# Size of internal histograms.json cache
HGRAMS_JSON_CACHE_SIZE = 15
FALLBACK_REVISION = "http://hg.mozilla.org/mozilla-central/rev/518f5bff0ae4"

class ChannelVersionManager:
    """ Manages data stored for a specific channel / version """
    def __init__(self, root_folder, channel, version, compress, pretty_print, cached = False):
        self.data_folder = os.path.join(root_folder, channel, version)
        mkdirp(self.data_folder)
        self.compress = compress
        self.pretty_print = pretty_print
        self.max_filter_id = None
        self.cached = cached
        if cached:
            self.cache = {}

        # Load filter-tree
        self.filter_tree = self.json_from_file(
            "filter-tree.json",
            {'_id': 0, 'name': 'reason'}
        )

        # Load histogram definitions
        self.histograms = self.json_from_file("histograms.json", {})

        # Load histogram revision meta-data
        self.revisions = self.json_from_file("revisions.json", {})

        # Histograms.json cache
        self.histograms_json_cache = [(None, None)] * HGRAMS_JSON_CACHE_SIZE
        self.histograms_json_cache_next = 0

    def json_from_file(self, filename, fallback_value):
        """ Load json from file, return fallback_value if no file exists """
        if self.cached:
            data = self.cache.get(filename, None)
            if data is None:
                path = os.path.join(self.data_folder, filename)
                data = load_json_from_file(path, self.compress, fallback_value)
                self.cache[filename] = data
            return data
        path = os.path.join(self.data_folder, filename)
        return load_json_from_file(path, self.compress, fallback_value)

    def json_to_file(self, filename, value):
        """ Write JSON to file """
        if self.cached:
            self.cache[filename] = value
        else:
            path = os.path.join(self.data_folder, filename)
            save_json_to_file(path, value, self.compress, self.pretty_print)

    def get_next_filter_id(self):
        """ Get the next filter id available """
        if self.max_filter_id is None:
            def find_max(tree):
                maxid = int(tree['_id'])
                for subtree in tree.itervalues():
                    if type(subtree) is dict:
                        m = find_max(subtree)
                        if maxid < m:
                            maxid = m
                return maxid
            self.max_filter_id = find_max(self.filter_tree)
        self.max_filter_id += 1
        return self.max_filter_id

    def get_filter_id(self, filterPath):
        """ Get/create filter id for filterPath """
        filter_names = ['reason', 'appName', 'OS', 'osVersion', 'arch']
        parent = self.filter_tree
        for i in xrange(0, len(filterPath)):
            entry = filterPath[i]
            child = parent.get(entry, None)
            if child is None:
                child = {
                    '_id':      self.get_next_filter_id(),
                }
                if i < len(filter_names) - 1:
                    child['name'] = filter_names[i + 1]
                parent[entry] = child
            parent = child
        return parent['_id']

    def flush(self):
        """ Output cache values """
        # Output all files
        if self.cached:
            for filename, value in self.cache.iteritems():
                path = os.path.join(self.data_folder, filename)
                save_json_to_file(path, value, self.compress, self.pretty_print)

        # Output filter tree
        self.json_to_file('filter-tree.json', self.filter_tree)

        # Output histogram definitions
        self.json_to_file('histograms.json', self.histograms)

        # Output histogram revisions meta-data
        self.json_to_file('revisions.json', self.revisions)

    def fetch_histogram_definition(self, measure, revision):
        """ Fetch histogram definition from histogram.json server """
        if revision == 'simple-measures-hack':
            assert measure.startswith("SIMPLE_MEASURES_"), "simple measures should be prefixed!"
            return get_simple_measures_definition(measure)
        histograms = None
        for rev, defs in self.histograms_json_cache:
            if rev == revision and rev != None:
                histograms = defs
                break
        if histograms is None:
            url = "http://localhost:9898/histograms?revision=%s" % revision
            try:
                request = urlopen(url)
            except HTTPError:
                url = "http://localhost:9898/histograms?revision=%s" % FALLBACK_REVISION
                try:
                    request = urlopen(url)
                except:
                    print >> sys.stderr, "Failed to fetch revision: %s" % revision
                    raise
            histograms = json.load(request)
            # Cache results
            i = self.histograms_json_cache_next
            self.histograms_json_cache[i] = (revision, histograms)
            self.histograms_json_cache_next = (i + 1) % HGRAMS_JSON_CACHE_SIZE
        if histograms.has_key(measure):
            return histograms[measure]
        if measure.startswith("SIMPLE_MEASURES_"):
            return get_simple_measures_definition(measure)
        if measure.startswith("STARTUP_") and  histograms.has_key(measure[8:]):
            return histograms[measure[8:]]
        print >> sys.stderr, "Measure is not defined: %s" % measure
        return {}

    def merge_in_blob(self, measure, byDateType, blob):
        """
            Merge in a map on the form
            {filterPath: HistogramAggregator.dump()}
            to file <measure>-<byDateType> where <byDateType> is with
            "by-build-date" or "by-submission-date"
        """
        # Find most recent: revision, buildId and data-array-length
        metadata = self.revisions.get(measure, {})
        revision = metadata.get('revision', None)
        buildId  = metadata.get('buildId', '')
        length   = metadata.get('length', 0)

        # Look for a newer buildId
        update_definition = False
        purge_data = False
        for dump in blob.itervalues():
            if buildId < dump.get('buildId', ''):
                #print "update-def: '%s' < '%s' for %s" % (buildId, dump.get('buildId', ''), measure)
                # Okay histogram definition needs to be updated
                update_definition = True
                # if we have a data-array length mismatch, then we purge existing data
                if length != len(dump['values']):
                    purge_data = True
                revision    = dump['revision']
                buildId     = dump['buildId']
                length      = len(dump['values'])

        # A newer buildId was found
        if update_definition:
            try:
                definition = self.fetch_histogram_definition(measure, revision)
            except:
                print >> sys.stderr, "Failed to fetch definition: %s" % revision
                return
            self.histograms[measure] = definition
            self.revisions[measure] = {
                'revision':     revision,
                'buildId':      buildId,
                'length':       length
            }

        # Delete files if lengths mismatched...
        if purge_data:
            byBuildDateFileName         = measure + "-by-build-date.json"
            bySubmissionDateFileName    = measure + "-by-submission-date.json"
            if os.path.isfile(bySubmissionDateFileName):
                os.remove(bySubmissionDateFileName)
            if os.path.isfile(byBuildDateFileName):
                os.remove(byBuildDateFileName)
            self.json_to_file(byBuildDateFileName, {})
            self.json_to_file(bySubmissionDateFileName, {})
            oldlen = metadata.get('length', None)
            if oldlen != None:
                print >> sys.stderr, "Purging data for %s from length %i to %i" % (measure, oldlen, length)

        # Filename to merge blob into
        filename = measure + "-" + byDateType + ".json"

        # Get existing data
        dataset = self.json_from_file(filename, {})

        # Merge in loaded values
        for filterPath, dump in blob.iteritems():
            # Split filter path and get the date
            filterPath = filterPath.split('/')
            date = filterPath[0]
            filterPath = filterPath[1:]

            # Skip if there is a length mismatch
            if length != len(dump['values']):
                continue

            # Get data for a date
            data = dataset.setdefault(date, [])

            # Get filter id
            fid = self.get_filter_id(filterPath)

            new_values = dump['values']

            # Get existing values, if any
            values = None
            for array in data:
                if array[-1] == fid:
                    values = array

            # Merge in values, if we don't have any and
            if values != None:
                for i in xrange(0, len(new_values) - 6):
                    values[i] += new_values[i]
                # Entries [-6:-1] may have -1 indicating missing entry
                for i in xrange(len(new_values) - 6, len(new_values) - 1):
                    # Missing entries are indicated with -1,
                    # we shouldn't add these up
                    if values[i] == -1 and new_values[i] == -1:
                        continue
                    values[i] += new_values[i]
                # Last entry (count) cannot be negative
                values[-2] += new_values[-1]

                # Replace nan and friends
                replace_nan_inf(values)
            else:
                replace_nan_inf(new_values)
                data.append(new_values + [fid])

        # Store dataset
        self.json_to_file(filename, dataset)

def results2disk(result_file, output_folder, compress, pretty_print):
    cache = {}
    with open(result_file, 'r') as f:
        for line in f:
            filePath, blob = line.split('\t')
            (channel, majorVersion, measure, byDateType) = filePath.split('/')
            blob = json.loads(blob)
            manager = cache.get((channel, majorVersion), None)
            if manager is None:
                manager = ChannelVersionManager(output_folder,
                                                channel, majorVersion,
                                                compress, pretty_print, False)
                cache[(channel, majorVersion)] = manager
            manager.merge_in_blob(measure, byDateType, blob)

    # Update versions.json and flush managers
    version_file = os.path.join(output_folder, 'versions.json')
    versions = load_json_from_file(version_file, compress, [])
    for channelVersion, manager in cache.iteritems():
        manager.flush()
        version = "/".join(channelVersion)
        if version not in versions:
            versions.append(version)
    save_json_to_file(version_file, versions, compress, pretty_print)

def main():
    p = ArgumentParser(
        description = 'Merge analysis results to disk',
        formatter_class = ArgumentDefaultsHelpFormatter
    )
    p.add_argument(
        "-f", "--input-file",
        help = "Input file to merge to data folder",
        required = True
    )
    p.add_argument(
        "-o", "--output-folder",
        help = "Output folder to merge data into",
        required = True
    )
    p.add_argument(
        "-z", "--gzip",
        help = "gzip compressed output",
        action = 'store_true'
    )
    p.add_argument(
        "-p", "--pretty-print",
        help = "Pretty print generated JSON",
        action = 'store_true'
    )

    cfg = p.parse_args()
    results2disk(cfg.input_file, cfg.output_folder, cfg.gzip, cfg.pretty_print)


if __name__ == "__main__":
    sys.exit(main())