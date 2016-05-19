#!/usr/bin/env python
# encoding: utf-8
"""
GenBagit.py

Created by John Plocher on 2016-03-01.  Last edit 2016-04-27

Generate an exportable BagIt data snapshot of an account's contents.

Ref: https://tools.ietf.org/html/draft-kunze-bagit-12

The BagIt specification is organized around the notion of a “bag”.
A bag is a named file system directory that minimally contains:

* a “data” directory that includes the payload, or data files that
  comprise the digital content being preserved.  Files can also be
  placed in subdirectories, but empty directories are not supported
* at least one manifest file that itemizes the filenames present in
  the “data” directory, as well as their checksums.  The particular
  checksum algorithm is included as part of the manifest filename.
  For instance a manifest file with MD5 checksums is named
  “manifest-md5.txt” 
* a “bagit.txt” file that identifies the directory
  as a bag, the version of the BagIt specification that it adheres
  to, and the character encoding used for tag files

Future work: 
* optimize number of threads used for swift download
* speed up filesystem traversal for MD5/Quick check validation 
* put the activity and audit log info in tagmanifest-md5.txt 
* capture all program output (stdout and stderr) and place it into a set
  of logs/output-date.txt files for archive creation provenance auditing
* extract per-object storage metadata from Swift and store it in a tag file

History:

4/17: cloudberry puts segment objects for auto-generated SLOs in the
same container as the original.  A simplistic extraction results in a
full duplication of content - when downloading the SLO manifest object,
swift automatically re-inflates the resulting large file with all the
segment content, and then swift obediently downloads each of the segments
independently.  Swift's design to avoid this problem is for the user to
manually put segments in a different container from the manifest object.

Plan:
    Use the -lfgV flags to just download a full manifest and stop
    Manually prune out the ...../objectname!CB_... segment names from
        the manifest
    Rerun this program using the -m flag to not redownload the manifest
        (this causes it to skip the (manually removed) segments from the
        rest of the pipeline)

3/5: Swift's historical folder abstraction uses objects to represent folder
hierarchies within a container:
   CONTAINER/object1
   CONTAINER/folder  -or
   CONTAINER/folder/
   CONTAINER/folder/object2

Upon extraction using the default swift API, these placeholders get downloaded
and instantiated as files in the filesystem instead of directories; worse, the
swift download API claims download success for the shadowed files even though
they are not actually downloaded!

$ swift download CONTAINER
  -rwx------ size CONTAINER/object1
  -rwx------ size CONTAINER/folder
  -rwx------ size CONTAINER/folder/object2 #  Silent FAIL

To prevent this, zero length objects are marked as FOLDER in the manifest and
are explicitly NOT downloaded.  This violates the BagIt 0.97 specification
(see section 2.1.3...)
TODO: consult contextual metadata to determine whether this is indeed a folder
or a valid zero length object/file.

"""

import os
import subprocess
import sys
import getopt
import pprint
import io
import json
import requests
from swiftclient import client as sclient
from swiftclient import service as sservice
from keystoneclient.v2_0 import client as ksclient
import ConfigParser
import time
import csv
import StringIO
import hashlib
import re
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()


help_message = '''
Generate an exportable BagIt data snapshot of an OpenStack Swift account's contents,
optionally validate it and upload it elsewhere.  With EVault LTS2, optionally extract
the account's activity logs and the admin audit trail.

-h, --help          This message
-n, --noaction      Test access permissions, but do not access content
-v, --verbose       Display underlying commands and data structures for debugging
-q, --quick         Skip the MD5 manifest file validation and simply check for file existance
-V, --novalidate    Skip the MD5 manifest file validation stage completely
-m, --nomanifest    Don't download/create a new manifest.md5 file - use the existing one
-l, --nologs        Skip the Activity and Audit log downloads 
-f, --nofiles       Skip downloading content from the cloud
-g, --nogoogle      Skip re-uploading content to Google Storage
-c <conf>, --config=<conf>
-b <conf>, --bamf=<conf>
    conf    = a config file with  the customer's (or LTS2 Billing system) account info
    
    Contents of customer config file:
    
        [Customer]
        tenant=MyTenantName
        password=MyPassword
        username=MyUsername
        botofile = path to optional boto config file for accessing G*Storage
        googleprojectid = 39551554193
        downloads = comma, delimited, list-of, containers,      to-download
        uploads =   comma, delimited, list-of, container-names, to-use-with-G*Storage
    
    Contents of Billing config file (not useful for non-LTS2 use cases)
        [BAMF]
        tenant=7393-4354
        password=AdminPassword
        username=AdminName

Examples:
    $ python genBagit.py -c ../myconfig -l -n       # validate access and auth settings on conf file
    $ python genBagit.py -c ../myconfig -l -g       # create manifest, download and validate content,  
                                                    # do not download LTS2 logs or upload to G*Storage
    $ python genBagit.py -c ../myconfig -l -m -g    # as above, but don't (re)create manifest
'''

FOLDER = '-FOLDER-'
FILENAME_BAGIT    = 'bagit.txt'
FILENAME_MANIFEST = 'manifest-md5.txt'
FILENAME_ACTIVITY = 'LTS2-ActivityLog.csv'
FILENAME_AUDIT    = 'LTS2-AuditLog.json'
FILENAME_UPLOAD   = 'LTS2-gsutil.log'
#auth_url          = 'https://auth.lts2.evault.com/v2.0'

pp = pprint.PrettyPrinter(indent=4)

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

"""
    Count the number of lines in a text file
    Input:
        text file name
    Output:
        an integer representing the number of lines in the file

"""
def filelines(fname):
    with open(fname, 'rb') as f:
        for i, l in enumerate(f):
            pass
        return i + 1

def genBagitFile():
    with open(FILENAME_BAGIT, 'ab') as bagitfile:
        bagitfile.write('BagIt-Version: 0.97\n');
        bagitfile.write('Tag-File-Character-Encoding: UTF-8\n');
    print '# created Bagit ID file'
    
    
def genManifest(swift, container_name):
    # create a manifest
    with open(FILENAME_MANIFEST, 'ab') as manifestfile:
        linecount = 0
        if verbose :
            print '>>> swift.get_container({})'.format(container_name)
        oheaders, objects = swift.get_container(container_name, full_listing = True)
        
        regexp = re.compile(r'!CB_[^/]+$')
        for obj in objects:
            o = obj['name'].encode('utf-8')
            if (regexp.search(o) is None):
                linecount = linecount + 1
                path = 'data/{}/{}'.format(container_name, o)
                # print "... ", path
                hval = obj['hash']
                if obj['bytes'] is 0:   # don't trip on horizon fake folders that show up as files
                    hval = FOLDER
                manifestfile.write('{}\t{}\n'.format(hval, path))
        print '# {}: {} items'.format(container_name, linecount)

def genFileList(swift, container_name):
    # create a manifest
    print '# Files in {}'.format(container_name)
    oheaders, objects = swift.get_container(container_name)
    for obj in objects:
        print '{}\t{}\t{}'.format(obj['name'].encode('utf-8'), obj['bytes'], obj['hash'])


def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')



def do_download(container, objects, auth_url, tenant_name, username, password):
    options = { 
        #'yes_all':True,
        'skip_identical':True, 
        'out_directory':'data/{}'.format(container),
        'auth_version': '2',
        'os_username': username,
        'os_password': password,
        'os_tenant_name': tenant_name,
        'os_auth_url': auth_url            
    }
    with sservice.SwiftService(options=options) as ss:
        dl_iterator = ss.download(container=container, objects=objects, options=options)
        for result in dl_iterator:
            #pp.pprint(result)
            if result['success']:
                print("OK:         %s" % result['object'])
            elif 'reason' not in result['response_dict']:
                print "Error: {}".format(result['error'])
            elif (result['response_dict']['reason'] == 'Not Modified') or (result['response_dict']['reason'] == 'OK'):
                print("Unmodified: %s" % result['object'])
            else:    
                print("Failed:     %s" % result['object'])
                print("            {}: {}".format(result['response_dict']['status'],result['response_dict']['reason']))

    
def download(container, auth_url, tenant_name, username, password):
    """
    :param container:   name of container
    :return:            none

    Download all the objects in a container to the data directory
    using the manifest as a source for the objects to download.
    """

    destination_directory = 'data/{}'.format(container)
    if not os.path.exists(destination_directory):
        if verbose :
            print '% mkdir -p {}'.format(destination_directory)
        os.makedirs(destination_directory)
    
    objects_to_dl = []
    count = 0
    with io.open(FILENAME_MANIFEST, 'r', encoding='utf8') as manifest:
        regexp = re.compile(r'(data/{}/)(.+)'.format(container))
        for line in unicode_csv_reader(manifest, dialect="excel-tab"):
            md5 = line[0]
            filename = line[1]
            m = regexp.match(filename) 
            if m: # found item in the desired container...
                o = m.group(2)
                # print '# adding object to list: {}'.format(o)
                objects_to_dl.append(o.encode('utf-8'))
                count = count + 1
            # report progress after each batch
            if len(objects_to_dl) > 1000:
                print '# Downloading {:,d} - {:,d} from {}'.format(count - len(objects_to_dl), count, container)
                do_download(container, objects_to_dl, auth_url, tenant_name, username, password)
                objects_to_dl = []
    if len(objects_to_dl) > 0:
        print '# Downloading {:,d} - {:,d} from {}'.format(count - len(objects_to_dl), count, container)
        do_download(container, objects_to_dl, auth_url, tenant_name, username, password)

def genActivityLog(accountid, bamf_swift):
    seen = {}
    fieldnames = [
        'Account',
        'Container',
        'Time',
        'Remote IP',
        'Requester',
        'Request ID',
        'Operation',
        'Object',
        'Hash Algorithm',
        'Hash',
        'Request-URI',
        'HTTP Status',
        'Bytes Sent',
        'Byte Received',
        'Total Time'
    ]
    print '# Gathering activity logs: '

    with open(FILENAME_ACTIVITY, 'wb') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fieldnames)
        oheaders, objects = bamf_swift.get_container(accountid, prefix='activities')
        for obj in sorted(objects, key=lambda object: object['name']):
            if verbose:
                print '>>> GET <BAMF> <{}>/{}'.format(accountid, obj['name'].encode('utf-8'))
            oh, o = bamf_swift.get_object(accountid, obj['name'].encode('utf-8'))
            csv.excel.skipinitialspace=True
            reader = csv.reader(StringIO.StringIO(o), csv.excel)
            for i in reader:
                if (len(i) > 5):
                    if i[5] not in seen:
                        csvwriter.writerow(i)
                        seen[i[5]] = True

def genAuditLog(accountid, bamf_swift):
    print '# Gathering audit logs: '
    with open(FILENAME_AUDIT, 'wb') as auditfile:
        oheaders, objects = bamf_swift.get_container(accountid, prefix='audittrail')
        for obj in sorted(objects, key=lambda object: object['name'].encode('utf-8')):
            if verbose :
                print '>>> GET <BAMF> <{}>/{}'.format(accountid, obj['name'].encode('utf-8'))
            auditfile.write('\n# Audit log: ' +  obj['name'].encode('utf-8') + '\n')
            oh, o = bamf_swift.get_object(accountid, obj['name'].encode('utf-8'))
            j = json.loads(o)
            auditfile.write( json.dumps(j, sort_keys=True, indent=4, separators=(',', ': ')))

def getSwiftFor(username, password, tenant, auth_url):
    auth_url_tokens   = auth_url + '/tokens'
    headers = {'Content-type': 'application/json', 'User-Agent': 'python-keystoneclient'}
    raw_auth_data = {'auth':
                         {'passwordCredentials':
                              {'username': username,
                               'password': password
                               },
                          'tenantName': tenant
                          }
                     }
    auth_data = json.dumps(raw_auth_data)
    r = requests.post(auth_url_tokens, auth_data, headers=headers)
    data = r.json()

    if verbose:
        print ">>> Auth:"
        pp.pprint(raw_auth_data)
        print ">>> Response"
        pp.pprint(data)

    tenant_name = data['access']['token']['tenant']['name']
    if verbose:
        print ">>> Tenant: ", tenant_name

    keystone = ksclient.Client(username=username,
                               password=password,
                               tenant_name=tenant_name,
                               auth_url=auth_url)

    if verbose:
        pp.pprint(keystone.tenants.list())

    swift_endpoint = keystone.service_catalog.url_for(service_type='object-store',
                                                      endpoint_type='publicURL')
    swift_path_loc = swift_endpoint.find('/v1/')
    swift_host = swift_endpoint[0:swift_path_loc]
    swift_path =swift_endpoint[swift_path_loc:]

    swift = sclient.Connection(auth_version='2',
                           user=username,
                           key=password,
                           tenant_name=tenant_name,
                           authurl=auth_url)
    return swift, swift_host, swift_path


"""
    Extract the contents of an OpenStack Swift LTS2 account into a BagIt directory structure

    Input:
        config file that contains
            Account/Tenant ID
            Admin username
            Admin Password

        config file that contains
            BAMF account/tenant ID
            BAMF username
            BAMF user password

    Output:
        A BagIt directory structure under a directory named with the Account/Tenant ID

"""

def main(argv=None):
    global verbose

    verbose        = False
    noLogDownload  = False
    noFileDownload = False
    noManifest     = False
    noMD5          = False
    noValidate     = False
    noGoogleUpload = False

    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], 'hnvVmlfgqc:b:',
                                       ['help', 'noaction', 'verbose', 'novalidate', 'nomanifest',
                                        'nologs', 'nofiles', 'nogoogle','quick', 'config', 'bamf'])
        except getopt.error, msg:
            raise Usage(msg)

        # option processing
        for option, value in opts:
            if option == '-v':
                verbose = True
            if option == '-n':
                noManifest = True
                noFileDownload = True
                noLogDownload = True
                noGoogleUpload = True
                noValidate = True
            if option in ('-m', '--nomanifest'):
                noManifest = True
            if option in ('-q', '--quick'):
                noMD5 = True
            if option in ('-f', '--nofiles'):
                noFileDownload = True
            if option in ('-l', '--nologs'):
                noLogDownload = True
            if option in ('-g', '--nogoogle'):
                noGoogleUpload = True
            if option in ('-V', '--novalidate'):
                noValidate = True
            if option in ('-c', '--config'):
                configfilename = value
            if option in ('-b', '--bamf'):
                bamfconfigfilename = value
            if option in ('-h', '--help'):
                raise Usage(help_message)

        if (configfilename   is None):  raise ValueError('Need customer config file')
        if (not os.path.isfile(configfilename)):  raise ValueError('Customer config file not found: ' + configfilename)

        if (noLogDownload is False):
            if (bamfconfigfilename   is None):  raise ValueError('Need bamf config file')
            if (not os.path.isfile(bamfconfigfilename)):  raise ValueError('BAMF config file not found: ' + bamfconfigfilename)

    except ValueError as err:
        print >> sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.args)
        print >> sys.stderr, '\t for help use --help'
        return 2

    except Usage, err:
        print >> sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
        print >> sys.stderr, '\t for help use --help'
        return 1
        

    if os.path.isfile(FILENAME_MANIFEST):
        if not noManifest:
            os.remove(FILENAME_MANIFEST)

    try:
        config = ConfigParser.ConfigParser()
        config.read(configfilename)
        tenant    = config.get('Customer', 'tenant')
        username  = config.get('Customer', 'username')
        password  = config.get('Customer', 'password')
        auth_url  = config.get('Customer', 'auth_url')
        botofile  = config.get('Customer', 'botofile')
        projectid = config.get('Customer', 'googleprojectid')
        uploadlist = []
        downloadlist = []
        useDownloadlist = False
        useUploadlist = False
        try:
            downloadlist = [e.strip() for e in config.get('Customer', 'downloads').split(',')]
            useDownloadlist = True
            try:
                uploadlist   = [e.strip() for e in config.get('Customer', 'uploads').split(',')]
                useUploadlist = True
            except ConfigParser.NoOptionError:
                pass
        except ConfigParser.NoOptionError:
            pass
        if (tenant   is None):  raise ValueError('Need customer tenant ID')
        if (username is None):  raise ValueError('Need customer username')
        if (password is None):  raise ValueError('Need customer password')
        if (auth_url is None):  raise ValueError('Need Swift system auth endpoint URL')

        if (noLogDownload is False):
            bamfconfig = ConfigParser.ConfigParser()
            bamfconfig.read(bamfconfigfilename)
            bamf_tenant   = bamfconfig.get('BAMF', 'tenant')
            bamf_username = bamfconfig.get('BAMF', 'username')
            bamf_password = bamfconfig.get('BAMF', 'password')

            if (bamf_tenant   is None):  raise ValueError('Need bamf tenant ID')
            if (bamf_username is None):  raise ValueError('Need bamf username')
            if (bamf_password is None):  raise ValueError('Need bamf password')

        if (len(downloadlist) != len(uploadlist)): raise ValueError('Download and upload lists need to be same length if used')
        if useDownloadlist != useUploadlist: raise ValueError('Download and upload lists need to be same')


    except ValueError as err:
        print >> sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.args)
        print >> sys.stderr, '\t for help use --help'
        return 2


    if (noLogDownload is False):
        (bamf_swift, bamf_swift_host,bamf_swift_path) =getSwiftFor(bamf_username, bamf_password, bamf_tenant, auth_url)
    (swift, swift_host, swift_path) =getSwiftFor(username, password, tenant, auth_url)

    a = swift.head_account()
    print '# =========================================='
    print '#  Account information'
    print '#    {} ({})'.format(username, tenant)
    print '#  as of ', a['date']
    print '#'
    print '#  {:8,d} Containers'               .format(int(a['x-account-container-count']))
    print '# =========================================='

    containerlist = []
    cheaders, containers = swift.get_account()
    for container in containers:
        #c_info = swift.head_container(container['name'])
        tag=' '
        if (useDownloadlist and downloadlist):
            if container['name'] in downloadlist:
                containerlist.append(container['name'])
                tag = '*'
        else:
            tag = '*'
            containerlist.append(container['name'])
        #print '#  {:8,d} Objects {:16,d} Bytes {} {}'.format(int(c_info['x-container-object-count']),
        #                                                    int(c_info['x-container-bytes-used']),
        #                                                    tag, container['name'])
        print '# {} {}'.format(tag, container['name'])

    print '# =========================================='
    print '#  {:8,d} Objects, {:16,d} Bytes'   .format(int(a['x-account-object-count']),
                                                       int(a['x-account-bytes-used']))
    print '#'


    if noManifest:
        print '# NOT generating manifest (-m, --nomanifest specified)'
    else:
        for container in containerlist:
            print '# Adding {} to manifest'.format(container)
            genManifest(swift, container)
    if noFileDownload:
        print '# NOT downloading account content (-f, --nofiles specified)'
    else:
        for container in containerlist:
            print '# Container: {}'.format(container)
            download(container, auth_url, tenant, username, password)

    if noValidate:
        print '# NOT validating downloaded MD5 against manifest (-V, --novalidate specified)'
    else:
        if noMD5:
            print '# Quick Validating downloaded objects against manifest contents'
        else:
            print '# Validating downloaded objects against manifest contents with correct MD5 checksums'
            
        manlength = filelines(FILENAME_MANIFEST)

        with io.open(FILENAME_MANIFEST, 'r', encoding='utf8') as manifest:
            counter = 0
            errors = 0
            for line in unicode_csv_reader(manifest, dialect="excel-tab"):
                counter = counter + 1
                md5 = line[0]
                filename = line[1]
                print '# {:12,d}/{:12,d}...\r'.format(counter, manlength),
                if os.path.isdir(filename):
                    pass
                elif not os.path.isfile(filename):
                    errors = errors + 1
                    print '\nERROR: file in Swift manifest not found in data directory: \"{}\"'.format(filename)
                elif md5 == FOLDER:
                    pass
                else:
                    if noMD5:
                        pass
                    with open(filename, 'rb') as f:
                        d = hashlib.md5(f.read()).hexdigest()
                        if (d != md5):
                            errors = errors + 1
                            print "\nERROR: calculated md5 != stored MD5"
                            print "    {}\t{}\t manifest".format(md5, filename)
                            print "    {}\t{}\t calculated".format(d, filename)
                        else:
                            # print "OK: {}\t{}".format(md5, filename)
                            pass
            print '# {:28s} {} errors'.format('Completed validating content', errors)

    if noLogDownload:
        print '# NOT downloading activity and audit logs (-l, --nologs specified)'
    else:
        print '#'
        print '# Pausing 30 seconds to allow BAMF activity records to be recorded...'
        time.sleep(10);
        print '# Pausing 20 seconds to allow BAMF activity records to be recorded...'
        time.sleep(10);
        print '# Pausing 10 seconds to allow BAMF activity records to be recorded...'
        time.sleep(10);
        print '# Gathering Activity and Audit logs...'

        ver_loc = swift_path.find('/v1/')
        accountid = swift_path[ver_loc + 4:]

        genActivityLog(accountid, bamf_swift)
        genAuditLog(accountid, bamf_swift)
        
    genBagitFile()

    if noGoogleUpload:
        print '# NOT uploading to G*Storage -  (-g, --nogoogle specified)'
    elif not botofile:
        print '# NOT uploading to G*Storage - no boto file found in customer config file'
    else:
        idx = 0
        print "# Creating G*Storage containers..."
        for container in downloadlist:
            cmd='BOTO_CONFIG={} gsutil mb -p \"{}\" -c DRA \"gs://{}\"'.format(botofile, projectid, uploadlist[idx])
            if verbose:
                print '>>> % {}'.format(cmd)
            subprocess.call(cmd, shell=True)

            print "# Uploading customer data ..."
            cmd='BOTO_CONFIG={} gsutil -m cp -L {} -r \"data/{}/*\" \"gs://{}\"'.format(botofile, FILENAME_UPLOAD,
                                                                                        container, uploadlist[idx])
            if verbose:
                print '>>> % {}'.format(cmd)
            subprocess.call(cmd, shell=True)
            idx = idx + 1
        if (noLogDownload is False):
            print "# Creating LTS2 Metadata container..."
            metabucket="metadata-from-lts2-{}".format(projectid)
            cmd='BOTO_CONFIG={} gsutil mb -p \"{}\" -c DRA \"gs://{}\"'.format(botofile, projectid, metabucket)
            if verbose:
                print '>>> % {}'.format(cmd)
            subprocess.call(cmd, shell=True)
            files = [FILENAME_ACTIVITY, FILENAME_AUDIT, FILENAME_UPLOAD, FILENAME_BAGIT]
            if noManifest is False:
                files.append(FILENAME_MANIFEST)
            print "# Uploading LTS2 Metadata ..."

            for meta in files:
                cmd='BOTO_CONFIG={} gsutil -m  cp \"{}\" \"gs://{}\"'.format(botofile, meta, metabucket)
                if verbose:
                    print '>>> % {}'.format(cmd)
                subprocess.call(cmd, shell=True)
    print '# ================= DONE ==================='


if __name__ == '__main__':
    sys.exit(main())
