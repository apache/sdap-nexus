# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Name:        filelist.py
# Purpose:     File listing class/functions.
#
# Author:      Brian Wilson
#
# Created:     Mon Apr 10 11:01:06 2006
#-----------------------------------------------------------------------------
#
USAGE = """
filelist.py [--help] [--bottomUp] [--directory] [--delete]
            [--fetchDir <outputDir>] [--fetchWitSubDirs]
            [--list] [--matchUrl] --quiet] [--regex '.*\.[cC]']
            [--size] [--topOnly] [--url]
            [--wildcard '*.txt.*'] [--xml]  <topPaths ...>

Recursively traverse and print (with full paths or URL's) all files
under the topPath(s) that match ANY of one or more regular expressions
and/or wildcard glob) strings.  By default, it simply prints the matches,
but one can also get their sizes, fetch them, or delete them.

The topPaths can be a mixture of local and remote (ftp or http)
paths, in which case a list of URL's is returned.  If xml mode is
turned on, then the output is an XML list.

If no regex or wildcard patterns are specified, then ALL files
are returned.  If files are fetched, then the URL's are
REWRITTEN to point to the local copies.

"""
# See the bottom of the file for exact switches and example of use.

import sys, os, re, string, getopt, types, getpass
import urllib.request, urllib.parse, urllib.error, urllib.request, urllib.error, urllib.parse, urllib.parse, time, shutil, socket, stat
from fnmatch import fnmatchcase
from ftplib import FTP
#import dataenc

def matchAnyThenConstrain(root, name, haveRegs, regs, haveWilds, wildCards,
                          constraintFunction):
    """Return True if the file name matches any of the compiled regular
    expressions or any of the wildcard (glob) specs, and (if present) the
    constraintFunction returns True.  The regex can be a pair of match &
    substitution patterns.  The 'name' of the file might be altered by a
    regex substitution and/or the constraintFunction.
    """
    if not haveRegs and not haveWilds:
        if constraintFunction is not None:
            return constraintFunction(root, name)
        else:
            return (True, name)
    else:
        match = False
        if haveRegs:
            for reg in regs:
                pattern, subst = reg
                if pattern.search(name):
                    match = True
                    if subst:
                        name = pattern.sub(subst, name)
                    break
        if haveWilds and not match:
            for wild in wildCards:
                if fnmatchcase(name, wild):
                    match = True
                    break
        if match and constraintFunction is not None:
            match, name = constraintFunction(root, name)
        return (match, name)


# Users call this function
def filelist(urlPaths, regSpecs=[], wildCards=[], needCredentials=False, userCredentials=None,
             matchFunction=matchAnyThenConstrain, constraintFunction=None,
             matchUrl=False, walkDirectories=True,
             urlMode=True, xmlMode=True, quietMode=False, verboseMode=False, getFileInfo=False,
             fetchDir=None, fetchIfNewer=False, fetchWithSubDirs=False,
             directoryMode=False, listMode=False, deleteMode=False, topDown=True,
             stream=sys.stdout):
    """Recursively traverse and print (with full paths or URL's) all files
    under the topPath(s) that match one or more regular expressions and/or
    wildcard (glob) strings, and an optional constraint (T/F) function to
    further winnow the candidate matches.  (The matchFunction can also be
    entirely replaced with custom logic.)

    By default, it simply generates the matches, but one can also fetch them,
    get their sizes, or delete them (if they are local files).
    Handles local directory paths and ftp/http URL's.

    Returns three file lists: matched, actually fetched, & destination names.
    """
    try:
        matchedFiles = []       # source files that match criteria
        fetchedFiles = []       # files that were actually fetched this run
        destinationFiles = []   # destination (local) file names (rewritten URL)

        topPaths = []
        for url in urlPaths:
            if url == '' or url == None: continue
            remote, protocol, netloc, path = remoteUrl(url)
            if not remote: url = os.path.abspath(url)
            if url[-1] == '/': url = url[:-1]
            topPaths.append(url)

        if needCredentials and userCredentials is None:
            userCredentials = promptForCredentials(topPaths)

        if fetchDir:
            workDir = os.path.join(fetchDir, '.tmp')
            # fetch into tmp directory & then rename so fetching is atomic
            try: os.mkdir(workDir)
            except: pass
            if not os.path.exists(workDir):
                die("filelist: Cannot write to fetch directory %s" % fetchDir)

        if isinstance(topPaths, bytes): topPaths = [topPaths]
        regSpecs = [s for s in regSpecs if s != '' and s != None]
        wildCards = [s for s in wildCards if s != '' and s != None]

        haveRegs = False; regs = []; haveWilds = False; haveMatchFunction = False
        if len(regSpecs) > 0:
            haveRegs = True
            regs = []
            for reg in regSpecs:
                (pattern, subst) = parse_re_with_subst(reg)
                regs.append( (re.compile(pattern), subst) )
        if len(wildCards) > 0:
            haveWilds = True

        prefix = ''
        extra = ''
        suffix = ''
        if deleteMode:
            suffix += ' deleted.'
            if '.' in topPaths:
                die("filelist: Recursively deleting from the dot (.) path is not safe.  Shame.")

        if directoryMode: listMode = False
        if listMode: getFileInfo = True
        if quietMode: stream = None
        sumSizes = 0
        if xmlMode:
            matchedFiles.append('<files>')
            fetchedFiles.append('<files>')
            _output('<files>', destinationFiles, stream)
            prefix += '  <file>'
            suffix += '</file>'

        for top in topPaths:
            if verboseMode: warn('filelist: searching', top)
            topMatchCount = 0; topFetchCount = 0

            for root, dirs, files, infos in walk(top, userCredentials, walkDirectories, topDown):
                if verboseMode: warn('filelist: found files in', root)
                remote, protocol, netloc, path = remoteUrl(root)
                if directoryMode:
                    contents = dirs
                else:
                    contents = files

                for i in range(len(contents)):
                    line = ''
                    file = contents[i]
                    try:
                        info = infos[i]
                    except:
                        info = None
                    if matchUrl:
                        name = os.path.join(root, file)
                    else:
                        name = file

                    match, newname = matchFunction(root, name, haveRegs, regs,
                                                   haveWilds, wildCards, constraintFunction)
                    if match:
                        line = ''
                        topMatchCount += 1
                        fn = os.path.join(root, file)

                        if getFileInfo or (fetchIfNewer and not remote):
                            if remote:
                                if info and getFileInfo:
                                    if listMode: line = info.line
                                    extra = ' ' + str(info.size) + ' ' + str(info.modTime)
                                    sumSizes += info.size
                            else:
                                st = os.stat(fn)
                                line = ' '.join( map(str, \
                                        (st.st_mode, st.st_uid, st.st_gid, st.st_size, st.st_mtime, fn)))
                                info = FileInfo(line, st.st_size, st.st_mtime, st.st_uid, st.st_gid, st.st_mode)
                                if getFileInfo:
                                    extra = ' ' + str(info.size) + ' ' + str(info.modTime)
                                    sumSizes += info.size

                        if not remote and urlMode: fn = makeFileUrl(fn)
                        matchedFiles.append(prefix + fn + extra + suffix)

                        if matchUrl:
                            newfn = newname
                        else:
                            newfn = os.path.join(root, newname)
                        newr, newp, newloc, newpath = remoteUrl(newfn)
                        newfile = os.path.split(newpath)[1]

                        if fetchDir:
                            if fetchDir == '.': fetchDir = os.getcwd()
                            if fetchWithSubDirs:
                                destDir = os.path.join(fetchDir, newpath[1:])
                            else:
                                destDir = fetchDir
                                destFile = os.path.join(destDir, newfile)
                                tmpFile = os.path.join(workDir, newfile)

                            if shouldFetch(remote, destFile, fetchIfNewer, info):
                                if not quietMode:
                                    warn('filelist: Fetching ', fn)
                                    warn('filelist: Writing  ', destFile)
                                try:
                                    os.makedirs(destDir)
                                except:
                                    # kludge, makedirs throws exception if any part of path exists
                                    pass
                                if remote:
                                    urllib.request.urlretrieve(fn, tmpFile)
                                else:
                                    shutil.copyfile(fn, tmpFile)
                                os.rename(tmpFile, destFile)   # atomic rename of file into destDir

                                topFetchCount += 1
                                fetchedFiles.append(prefix + fn + suffix)
                                if getFileInfo: line = line + ' ' + destFile

                                # now rewrite URL to point to local copy of file
                                fn = destFile
                                if not remote and urlMode: fn = makeFileUrl(fn)

                        if not listMode:
                            line = prefix + fn + extra + suffix
                        _output(line, destinationFiles, stream)
                        if deleteMode:
                            if remote:
                                die('filelist: Cannot delete remote files (yet)')
                            else:
                                os.unlink(fn)

            if verboseMode and fetchDir:
                warn('filelist: Matched %d files from %s' % (topMatchCount, top))
                warn('filelist: Fetched %d files from %s' % (topFetchCount, top))
        if fetchDir:
            for f in os.listdir(workDir): os.remove(os.path.join(workDir, f))
            os.rmdir(workDir)

        if xmlMode:
            matchedFiles.append('</files>')
            fetchedFiles.append('</files>')
            _output('<files>', destinationFiles, stream)

        if getFileInfo:
            if xmlMode:
                line = '<totalSize>%s</totalSize>' % sumSizes
            else:
                line = '#filelist: total size %s' % sumSizes
            matchedFiles.append(line)
            _output(line, destinationFiles, stream)

    except KeyboardInterrupt:
        if fetchDir:
            for f in os.listdir(workDir): os.remove(os.path.join(workDir, f))
            os.rmdir(workDir)
        die('filelist: Keyboard Interrupt')

    return (matchedFiles, fetchedFiles, destinationFiles)


def shouldFetch(remote, destFile, fetchIfNewer, srcFileInfo):
    if remote:
        if os.path.exists(destFile):
            doFetch = False
        else:
            doFetch = True
    else:
        if os.path.exists(destFile):
            if fetchIfNewer:
                destModTime = os.path.getmtime(destFile)
                if destModTime < srcFileInfo.modTime:
                    doFetch = True
                else:
                    doFetch = False
            else:
                doFetch = False
        else:
            doFetch = True
    return doFetch

def _output(line, lines, stream=None):
    """Internal function: Add line to output lines and optionally print to stream."""
    lines.append(line)
    if stream: print(line, file=stream)

class FileInfo:
    """Holder class for those file info. elements that are consistent among local
    files (output of stat), ftp directories, http, etc.  Minimum useful fields are
    modification time and size.  Line contains usual string output of ls -l.
    """
    def __init__(self, line, size, modTime, userId=None, groupId=None, protectMode=None):
        self.line=line; self.size=size; self.modTime=modTime
        self.userId=userId; self.groupId=groupId; self.protectMode=protectMode

class UserCredential(object):
    """Container for user credential info. like username, password, certificate, etc.
    """
    def __init__(self, username=None, password=None, validInterval=None, certificate=None):
        self.username = username
        self.password = password
        self.validInterval = validInterval     # tuple of Ints (days, hours, minutes)
        if password is not None and validInterval is None:
            die('UserCredential: If password is present, validInterval is also required.')
        self.certificate = certificate

    def getPassword(self):
        pw = self._password
        if pw:
            pw, daynumber, timestamp = dataenc.pass_dec(pw)
            if dataenc.unexpired(daynumber, timestamp, self.validInterval):
                return pw
            else:
                return None
        else:
            return None
    def setPassword(self, pw):
        if pw and pw != '':
            self._password = dataenc.pass_enc(pw, daynumber=True, timestamp=True)
        else:
            self._password = pw
    password = property(getPassword, setPassword)

class UserCredentials:
    """Contains dictionary of (url, credential) pairs and optionally an httpProxy.
    """
    def __init__(self, httpProxy=None, credentials={}):
        self.httpProxy = httpProxy
        self.credentials = credentials
    def add(self, url, credential):
        self.credentials[url] = credential; return self
    def forUrl(self, url):
        for key in self.credentials:
            if url.startswith(key):
                return self.credentials[key]
        return None

def promptForCredentials(urls, httpProxy=None):
    if httpProxy == None:
        httpProxy = input('Enter HTTP proxy [none]: ')
        if httpProxy == '': httpProxy = None
    credentials = UserCredentials(httpProxy)
    localUserName = getpass.getuser()
    for url in urls:
        remote, protocol, netloc, path = remoteUrl(url)
        if remote:
            username, password, validInterval = promptForCredential(url, localUserName)
            credential = UserCredential(username, password, validInterval)
            credentials.add(url, credential)
    return credentials

def promptForCredential(url, localUserName):
    remote, protocol, netloc, path = remoteUrl(url)
    if protocol == 'ftp':
        defaultUserName = 'anonymous'
    else:
        defaultUserName = localUserName
    username = input('Need credentials for URL %s\nUsername [%s]: ' \
                         % (url, defaultUserName))
    if username == '': username = defaultUserName
    password = ''
    while password == '':
        password = getpass.getpass()
    validInterval = [0, 1, 0]
    if password != '':
        response = input('Enter valid time period for credential [(days, hours, minutes) = 0 1 0]: ')
        if response != '':
            validInterval = response.split()
    return (username, password, validInterval)

class DirectoryWalker:
    """Recursively walk directories using the protocol specified in a URL.
    Sublclasses handle ftp, http, sftp, local file system, etc.
    """
    def __init__(self, userCredentials=None, retries=3, sleepTime=5):
        self.userCredentials = userCredentials
        self.retries = retries
        self.sleepTime = sleepTime

    def walk(self, top, walkDirectories=True):
        """Recursively walk directories on a remote site to retrieve file lists.
        """
        remote, protocol, netloc, path = remoteUrl(top)
        status, dir_listing = self.retrieveDirList(top)
        if status:
            if len(dir_listing) == 0:
                yield (top, [], [], [])
            else:
                (dirs, files, infos) = self.parseDirList(dir_listing, path)
                yield (top, dirs, files, infos)

                if walkDirectories:
                    for dir in dirs:
                        # Depth-first recursion
                        for root, dirs, files, infos in self.walk(top + '/' + dir, walkDirectories):
                            yield (root, dirs, files, infos)
        else:
            warn('DirectoryWalker: error, unable to retrieve directory listing at', top)
            yield (top, [], [], [])

    def retrieveDirList(self, url):
        """Retrieve directory listing as a list of text lines.  Returns (status, dirList)."""
        pass
    def parseDirList(self, dirList, path=None):
        """Parse directory listing (text) and return three lists (dirs, files, fileInfos)."""
        pass

class FtpDirectoryWalker(DirectoryWalker):
    """Recursively walk directories on an ftp site."""
    def __init__(self, userCredentials=None, retries=3, sleepTime=5):
        DirectoryWalker.__init__(self, userCredentials, retries, sleepTime)

    def retrieveDirList(self, url):
        """Retrieve a directory listing via ftp with retries.
        """
        remote, protocol, netloc, path = remoteUrl(url)
        credential = None
        if self.userCredentials:
            credential = self.userCredentials.forUrl(url)
        dir = ''; dir_list = []
        ftp = FTP()
        for i in range(self.retries):
            try:
                ftp.connect(netloc)
                if credential is None or \
                   credential.username == 'anonymous' or \
                   credential.username == '':
                    ftp.login()
                else:
                    ftp.login(credential.username, credential.password)
                ftp.cwd(path)
                ftp.retrlines('LIST', dir_list.append)
                ftp.quit()
                dir = '\n'.join(dir_list)
                return (True, dir)
            except:
                pass
            time.sleep(self.sleepTime)
            warn('FtpDirectoryWalker: connect retry to ', netloc, path)
        return (False, dir)

    def parseDirList(self, dir, path=None):
        """Parse long directory listing returned by ftp or (ls -l).
        Separate entries into directories and files.
        """
        dirs = []; files = []; infos = []
        for entry in dir.split('\n'):
            fields = entry.split()
            if len(fields) < 7: continue
            fn = fields[-1]
            if fn == '.' or fn == '..': continue
            if re.match('^d', fields[0])and fields[0][7] == 'r':
                dirs.append(fn)
            else:
                files.append(fn)
                info = FileInfo(entry, int(fields[4]), '-'.join(fields[5:8]), \
                                fields[2], fields[3], fields[0])
                infos.append(info)
        return (dirs, files, infos)

class DirListingParser(object):
    """Base class for directory listing parsers."""
    def __init__(self, regex):
        self.regex = regex
        self.compiledRegex = re.compile(self.regex)
        
    def parse(self, dir, listingHtml):
        """Return (dirs, files, infos)."""
        dirs = []; files = []; infos = []
        raise NotImplementedError("Override this method in sub class.")
    
class ApacheDirListingParser(DirListingParser):
    """Parser class for apache."""
    def parse(self, dir, listingHtml):
        dirs = []; files = []; infos = []
        items = self.compiledRegex.findall(listingHtml)
        for item, itemName in items:
            if itemName.strip() == 'Parent Directory': continue
            if isinstance(item, str):
                name = item
            else:
                name, dateTime, size = item[:]

            if name.endswith('/'):
                type = 'd'
                dirs.append(name[:-1])
            else:
                type = '-'
                files.append(name)
            #not doing file info
            '''
            size = size.lower()
            if size.endswith('k'):
                size = int(size[:-1]) * 1024
            elif size.endswith('m'):
                size = int(size[:-1]) * 1024 * 1024
            else:
                size = -1
            line = '%s---------  1 ? ? %15d %s %s' % (type, size, dateTime, name)
            info = FileInfo(line, size, dateTime)
            '''
            infos.append(None)
        return (dirs, files, infos)
    
class CDAACDirListingParser(DirListingParser):
    """Parser class for CDAAC data server."""
    def parse(self, dir, listingHtml):
        dirs = []; files = []; infos = []
        items = self.compiledRegex.findall(listingHtml)
        for item, itemName in items:
            if itemName.strip() == 'Parent Directory': continue
            if isinstance(item, str):
                name = item
            else:
                name, dateTime, size = item[:]
            if name.endswith('/'):
                type = 'd'
                dirs.append(name)
            else:
                type = '-'
                files.append(name)
            #not doing file info
            '''
            size = size.lower()
            if size.endswith('k'):
                size = int(size[:-1]) * 1024
            elif size.endswith('m'):
                size = int(size[:-1]) * 1024 * 1024
            else:
                size = -1
            line = '%s---------  1 ? ? %15d %s %s' % (type, size, dateTime, name)
            info = FileInfo(line, size, dateTime)
            '''
            infos.append(None)
        return (dirs, files, infos)

class HttpDirectoryWalker(DirectoryWalker):
    """Recursively walk directories on an http (web) site to retrieve file lists.
    Handles many styles of HTML directory listings, but still very FRAGILE.
    """
    
    #list of directory listing parser plugins
    DIR_LIST_REGEX_PLUGINS = [
        #apache 2.0.55 directory listing
        ApacheDirListingParser(r'(?i)alt="\[.*?\]">\s*<A HREF="(?P<name>.*?)">(.*?)</A>'),
        #CDAAC (COSMIC Data)
        CDAACDirListingParser(r'(?i)<LI><A HREF="(?P<name>.*?)">(.*?)</A>'),
        ]
    
    def __init__(self, userCredentials=None, retries=3, sleepTime=5):
        DirectoryWalker.__init__(self, userCredentials, retries, sleepTime)
        if self.userCredentials:
            if self.userCredentials.httpProxy:
                os.environ['http_proxy'] = self.userCredentials.httpProxy
                # global kludge, default proxyHandler looks up proxy there
            passwordMgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
            for url, cred in self.userCredentials.credentials.items():
                passwordMgr.add_password(None, url, cred.username, cred.password)
            authHandler = urllib.request.HTTPBasicAuthHandler(passwordMgr)
            opener = urllib.request.build_opener(authHandler)
        else:
#            opener = urllib2.build_opener()
            opener = None
#        opener.add_headers = [('User-agent', 'Mozilla/5.0')]
        self.opener = opener

    def retrieveDirList(self, url):
        """Retrieve an HTML directory listing via http with retries.
        """
###        url = os.path.join(url, 'contents.html')     ### hack for DAP servers at GES-DISC
        dir_listing = ''
        proxies = {}
        for i in range(self.retries):
            try:
                if self.opener:
                    response = self.opener.open(url)
                else:
                    response = urllib.request.urlopen(url)
            except IOError as e:
                if hasattr(e, 'reason'):
                    warn('HttpDirectoryWalker: Error, failed to reach server because: %s' % e.reason)
                elif hasattr(e, 'code'):
                    warn('HttpDirectoryWalker: Server could not fulfill request, error code %s' % e.code)
            else:
                dir_listing = response.read()
                return (True, dir_listing)
            time.sleep(self.sleepTime)
            warn('HttpDirectoryWalker: retrying ', url)
        return (False, dir_listing)

    reDirPath = re.compile(r'(?i)<H1>.*?Index of\s*?(\S+?)\s*?</H1>')

    def parseDirList(self, dir, path):
        """Parse fragile HTML directory listings returned by various HTTP servers,
        including Apache and OpenDAP.  Separate entries into directories and files.
        """
        dirs = []; files = []; infos = []
        if path:
            match = HttpDirectoryWalker.reDirPath.search(dir)
            if not match:
                die('HttpDirectoryWalker: Cannot find directory name %s in HTML listing:\n%s' % (path, dir))
            dirName = match.group(1)
            if dirName not in path:
                warn('HttpDirectoryWalker: Directory name %s in HTML listing does not agree with path %s:\n%s' % (dirName, path, dir))

        # Try to find directory lines that contain file info
        reDirListWithStat = re.compile( \
            r'(?i)<A HREF=[\'"]*?(?P<name>[^\?].*?' + dirName + r'.*?)[\'"]*?>.*?</A>\s*(?P<dateTime>\S+ \S+)\s+?(?P<size>\S+)\s*?$')
        items = reDirListWithStat.findall(dir)
        # If not, then try to find simple directory lines
        if len(items) == 0:
            reDirList = re.compile( \
                r'(?i)<A HREF=[\'"]*?(?P<name>[^\?].*?' + dirName + r'.*?)[\'"]*?>.*?</A>')
            items = reDirList.findall(dir)
            
        if len(items) != 0:
            dateTime = '? ?'; size = ''
            for item in items:
                if isinstance(item, str):
                    name = item
                else:
                    name, dateTime, size = item[:]
                if dirName not in name: continue
    
                if name.endswith('/'):
                    type = 'd'
                    dirs.append(name)
                else:
                    type = '-'
                    files.append(name)
                size = size.lower()
                if size.endswith('k'):
                    size = int(size[:-1]) * 1024
                elif size.endswith('m'):
                    size = int(size[:-1]) * 1024 * 1024
                else:
                    size = -1
                line = '%s---------  1 ? ? %15d %s %s' % (type, size, dateTime, name)
                info = FileInfo(line, size, dateTime)
                infos.append(info)
                print(line)
        
        #try plugins
        else:
            for plugin in self.DIR_LIST_REGEX_PLUGINS:
                pluginResults = plugin.parse(dirName, dir)
                if len(pluginResults[0]) != 0 or len(pluginResults[1]) != 0 or \
                    len(pluginResults[2]) != 0: return pluginResults
                
        return (dirs, files, infos)


def walk(top, userCredentials=None, walkDirectories=True, topDown=True):
    """Recursively walk directories to retrieve file lists.
    Returns the topPath, contained subdirectories and files, and
    optionally FileInfo objects (if info is included in protocol results).
    Handles local directory paths and ftp/http protocols (URL's).
    """
    remote, protocol, netloc, path = remoteUrl(top)
    if remote:
        if protocol == 'ftp':
            ftpWalker = FtpDirectoryWalker(userCredentials)
            for root, dirs, files, infos in ftpWalker.walk(top, walkDirectories):
                yield (root, dirs, files, infos)
        elif protocol == 'http':
#            import pdb; pdb.set_trace()
            httpWalker = HttpDirectoryWalker(userCredentials)
            for root, dirs, files,infos in httpWalker.walk(top, walkDirectories):
                yield (root, dirs, files, infos)
        elif protocol == 'sftp':
            sftpWalker = SftpDirectoryWalker(userCredentials)
            for root, dirs, files,infos in sftpWalker.walk(top, walkDirectories):
                yield (root, dirs, files, infos)
        else:
            die('filelist: Cannot handle protocol ', protocol)
    else:
        if walkDirectories:
            for root, dirs, files in os.walk(top, topDown):
                yield (root, dirs, files, [])
        else:
            files = os.listdir(top)
            yield (top, [], files, [])

def remoteUrl(url):
    """Returns True if the URL is remote; also returns protocol,
    net location (host:port), and path."""
    protocol, netloc, path, params, query, fragment = urllib.parse.urlparse(url)
    if protocol == '':
        return (False, protocol, netloc, path)
    else:
        return (True, protocol, netloc, path)


# utils
RE_WITH_SUBST_PATTERN = re.compile(r'^s/(.+)/(.+)/$')
def parse_re_with_subst(str):
    match = RE_WITH_SUBST_PATTERN.match(str)
    if match:
        return (match.group(1), match.group(2))
    else:
        return (str, None)

def hostName():
    return socket.gethostbyaddr(socket.gethostname())[0]

FILE_URL_PREFIX = 'file://' + hostName()
def makeFileUrl(file):
    return FILE_URL_PREFIX + file

def warn(*str): sys.stderr.write(' '.join(str) + '\n')
def die(str, status=1): warn(str); sys.exit(status)

def main():
    """Main function for outside scripts to call."""

    from sys import argv

    if len(argv) < 2: die(USAGE)
    try:
        opts, argv = getopt.getopt(argv[1:], 'hbcdf:ilqr:stuvw:x',
                         ['help', 'bottomUp', 'credentials', 'delete', 'directory',
                          'fetchDir=', 'fetchIfNewer', 'fetchWithSubDirs', 'info',
                          'list', 'quiet', 'regex=', 'size', 'topOnly',
                          'url', 'verbose', 'wildcard=', 'xml'])
    except getopt.GetoptError as xxx_todo_changeme:
        (msg, bad_opt) = xxx_todo_changeme.args
        die("%s error: Bad option: %s, %s" % (argv[0], bad_opt, msg))

    regSpecs = []; wildCards = []; matchUrl=False; walkDirectories = True
    needCredentials = False; userCredentials = None
    urlMode=False; xmlMode=False; quietMode=False; verboseMode=False; getFileInfo=False
    fetchDir = None; fetchIfNewer=False; fetchWithSubDirs=False
    directoryMode = False; deleteMode = False; topDown = True; listMode = False

    for opt, val in opts:
        if opt   in ('-h', '--help'):       die(USAGE)
        elif opt in ('-b', '--bottomUp'):   topDown = False
        elif opt in ('-c', '--credentials'):   needCredentials = True
        elif opt in ('-d', '--directory'):  directoryMode=True
        elif opt in ('--delete'):           deleteMode=True
        elif opt in ('-f', '--fetchDir'):   fetchDir = val
                                            # retrieve remote files to this dir
        elif opt in ('--fetchIfNewer'):     fetchIfNewer=True
                                            # only fetch if src file is newer than existing dest file
        elif opt in ('--fetchWithSubDirs'): fetchWithSubDirs=True
                                            # mirror subdirectories when fetching
        elif opt in ('-i', '--info'):       getFileInfo=True
        elif opt in ('-l', '--list'):       listMode=True
        elif opt in ('-m', '--matchUrl'):   matchUrl=True
                                            # regexs match entire URL/path, not just file name
        elif opt in ('-q', '--quiet'):      quietMode=True
                                            # don't print files during walk
        elif opt in ('-r', '--regex'):      regSpecs.append(val)
        elif opt in ('-s', '--size'):       sizeMode=True
        elif opt in ('-t', '--topOnly'):    walkDirectories=False
        elif opt in ('-u', '--url'):        urlMode=True
                                            # return URL's (file:, ftp:, http:, etc.)
        elif opt in ('-v', '--verbose'):    verboseMode=True
        elif opt in ('-w', '--wildcard'):   wildCards.append(val)
        elif opt in ('-x', '--xml'):        xmlMode=True   # return list in XML format
        else: die(USAGE)

#    import pdb; pdb.set_trace()

    matchedFiles, fetchedFiles, destinationFiles = \
            filelist(argv, regSpecs, wildCards, needCredentials, userCredentials,
                     matchAnyThenConstrain, None, matchUrl, walkDirectories,
                     urlMode, xmlMode, quietMode, verboseMode, getFileInfo,
                     fetchDir, fetchIfNewer, fetchWithSubDirs,
                     directoryMode, listMode, deleteMode, topDown)

    if quietMode:
        if listMode == 'match':
            print(matchedFiles)
        elif listMode == 'fetch':
            print(fetchedFiles)
        elif listMode == 'destination':
            print(destinationFiles)
        else:
            pass


if __name__ ==  '__main__': main()
