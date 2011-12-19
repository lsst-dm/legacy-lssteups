#!/usr/bin/env python
# -*- python -*-
#
# various specializations for LSST (during DC2)
#
import sys, os, os.path, re, atexit, shutil
import eups.distrib.server as eupsServer
import eups.distrib        as eupsDistrib
import eups.lock

defaultPackageBase = "http://dev.lsstcorp.org/pkgs/prod"

class DistribServer(eupsServer.ConfigurableDistribServer):
    """a class that encapsulates the communication with a package server.

    This class allows the mechanisms (e.g. the URLs) used to retrieve 
    information from a server to be specialized to that server. 

    This implementation captures the behavior of the LSST distribution server
    during DC3 (and beyond)
    """

    validConfigKeys = eupsServer.ConfigurableDistribServer.validConfigKeys + \
      [ "EXTERNAL_TABLE_URL", "EXTERNAL_TABLE_FLAVOR_URL", "EXTERNAL_DIST_URL" ]

    def _initConfig_(self):
        eupsServer.ConfigurableDistribServer._initConfig_(self)
        if not self.config.has_key('MANIFEST_URL'):
            self.config['MANIFEST_URL'] = \
                "%(base)s/manifests/%(product)s-%(version)s.manifest";
        if not self.config.has_key('MANIFEST_FLAVOR_URL'):
            self.config['MANIFEST_FLAVOR_URL'] = \
                "%(base)s/manifests/%(flavor)s/%(product)s-%(version)s.manifest";

        if not self.config.has_key('TABLE_URL'):
            self.config['TABLE_URL'] = \
                "%(base)s/%(product)s/%(version)s/%(product)s.table";
        if not self.config.has_key('EXTERNAL_TABLE_URL'):
            self.config['EXTERNAL_TABLE_URL'] = \
                "%(base)s/external/%(product)s/%(version)s/%(product)s.table";
        if not self.config.has_key('TABLE_FLAVOR_URL'):
            self.config['TABLE_FLAVOR_URL'] = \
                "%(base)s/%(product)s/%(version)s/%(flavor)s/%(product)s.table";
        if not self.config.has_key('EXTERNAL_TABLE_FLAVOR_URL'):
            self.config['EXTERNAL_TABLE_FLAVOR_URL'] = \
                "%(base)s/external/%(product)s/%(version)s/%(flavor)s/%(product)s.table";

        if not self.config.has_key('LIST_URL'):
            self.config['LIST_URL'] = "%(base)s/%(tag)s.list";
        if not self.config.has_key('LIST_FLAVOR_URL'):
            self.config['LIST_FLAVOR_URL'] = "%(base)s/%(flavor)s/%(tag)s.list";

        if not self.config.has_key('DIST_URL'):
            self.config['DIST_URL'] = "%(base)s/%(path)s";
        if not self.config.has_key('EXTERNAL_DIST_URL'):
            self.config['EXTERNAL_DIST_URL'] = "%(base)s/external/%(path)s";
        if not self.config.has_key('TARBALL_FLAVOR_URL'):
            self.config['TARBALL_FLAVOR_URL'] = "%(base)s/%(product)s/%(version)s/%(flavor)s/%(path)s";

        if not self.config.has_key('FILE_URL'):
            self.config['FILE_URL'] = \
                "%(base)s/%(product)s/%(version)s/%(path)s";
        if not self.config.has_key('PRODUCT_FILE_URL'):
            self.config['PRODUCT_FILE_URL'] = \
                "%(base)s/%(product)s/%(version)s/%(path)s";
        if not self.config.has_key('PRODUCT_FILE_FLAVOR_URL'):
            self.config['PRODUCT_FILE_FLAVOR_URL'] = \
                "%(base)s/%(product)s/%(version)s/%(flavor)s/%(path)s";

        if not self.config.has_key('MANIFEST_DIR_URL'):
            self.config['MANIFEST_DIR_URL'] = "%(base)s/manifests";
        if not self.config.has_key('MANIFEST_FILE_RE'):
            self.config['MANIFEST_FILE_RE'] = \
                r"^(?P<product>[^\-\s]+)(-(?P<version>\S+))?" + \
                r"(@(?P<flavor>[^\-\s]+))?.manifest$"

        if not self.config.has_key('DISTRIB_CLASS'):
            self.setConfigProperty('DISTRIB_CLASS',
                                   'pacman: lssteups.DistribPacman')

    def getFileForProduct(self, path, product, version, flavor, 
                          ftype=None, filename=None, noaction=False):
        """return a copy of a file with a given path on the server associated
        with a given product.

        @param path        the path on the remote server to the desired file
        @param product     the desired product name
        @param version     the desired version of the product
        @param flavor      the flavor of the target platform
        @param ftype       a type of file to assume; if not provided, the 
                              extension will be used to determine the type
        @param filename    the recommended name of the file to write to; the
                             actual name may be different (if, say, a local 
                             copy is already cached).  If None, a name will
                             be generated.
        @param noaction    if True, simulate the retrieval
        """

        # determine the extension to determine the type of file we are 
        # retrieving; this may affect the ultimate URL
        if ftype is None:
            ftype = os.path.splitext(path)[1]
            if ftype.startswith("."):  ftype = ftype[1:]
        ftype = ftype.upper()

        # determine if we looking for an external product
        prefix = "external/"
        if path.startswith(prefix) and not ftype.startswith("EXTERNAL_"):
            ftype = "EXTERNAL_" + ftype
            # path = path[len(prefix):]

        return eupsServer.ConfigurableDistribServer.getFileForProduct(self,
                    path, product, version, flavor, ftype, filename, noaction)

    def getTableFile(self, product, version, flavor, filename=None, 
                     noaction=False):
        """return the name of a local file containing a copy of the EUPS table
        file for a desired product retrieved from the server.

        This method encapsulates the mechanism for retrieving a table file 
        from the server; sub-classes may over-ride this to specialize for a 
        particular type of server.  

        @param product     the desired product name
        @param version     the desired version of the product
        @param flavor      the flavor of the target platform
        @param filename    the recommended name of the file to write to; the
                             actual name may be different (if, say, a local 
                             copy is already cached).  If None, a name will
                             be generated.
        @param noaction    if True, simulate the retrieval
        """
        try:
            # search for a version specialized for the exact version
            return eupsServer.ConfigurableDistribServer.getTableFile(product,
                                          version, flavor, filename, noaction)
        except eupsServer.RemoteFileNotFound, ex:
            # try a generic one for the release (before +/- in version)
            release = re.sub(r'[+\-].+$', '', version);
            return eupsServer.ConfigurableDistribServer.getTableFile(product,
                                          release, flavor, filename, noaction)


class BuildDistrib(eupsDistrib.DefaultDistrib):
    """This class captures the mechanism used by LSST-NCSA to distribute 
    packages that build products from source.  
    """

    NAME = "lsstbuild"

    def __init__(self, Eups, distServ, flavor, tag="current", options=None,
                 verbosity=0, log=sys.stderr):
        eupsDistrib.Distrib.__init__(self, Eups, distServ, flavor, tag, options,
                                     verbosity, log)

        self.setupfile = self.getOption('setupsFile', "eupssetups.sh")
        self.nobuild = self.options.get("nobuild", False)
        self.noclean = self.options.get("noclean", False)

        # this will be used to determine if the manifestToFile option was 
        # not used in conjunction with -j 
        self._outmanfile = [ self.options.get("manifestToFile", None), None ]

    # @staticmethod   # requires python 2.4
    def parseDistID(distID):
        """Return a valid package location if and only we recognize the 
        given distribution identifier

        This implementation always returns None
        """
        prefix = BuildDistrib.NAME + ":"
        distID = distID.strip()
        if distID.startswith(prefix):
            return distID[len(prefix):]

        return None

    parseDistID = staticmethod(parseDistID)  # should work as of python 2.2

    def installPackage(self, location, product, version, productRoot, 
                       installDir=None, setups=None, buildDir=None):
        """install a package, (typically) building from source.  The setups
        will be used to set the environment used to build the package.
        """
        if not buildDir:
            buildDir = self.getOption('buildDir', 'EupsBuildDir')
        if self.verbose > 0:
            print >> self.log, "Building in", buildDir

        # set the installation directory
        if installDir is None:
            installDir = os.path.join(product, version)

        installRoot = eupsDistrib.findInstallableRoot(self.Eups)
        if not installRoot:
            raise RuntimeError("Unable to find a stack I can write to among $EUPS_PATH")
        installDir = os.path.join(installRoot, self.Eups.flavor, installDir)

        if not os.path.isdir(buildDir):
            try:
                os.makedirs(buildDir)
            except:
                raise RuntimeError("%s: unable to create build directory" % buildDir)

        # fetch the package from the server;  by default, the URL will be 
        # of the form pkgroot/location.  With this convention, the location
        # will include the product, version, and flavor components explicitly.
        if not self.nobuild:
            distFile = os.path.basename(location)
            self.distServer.getFileForProduct(location, product, version, 
                                              self.Eups.flavor, ftype="DIST",
                                              filename=os.path.join(buildDir, 
                                                                    distFile))

            # catch the setup commands to a file in the build directory
            # make sure every setup line is includes the -j option
            setupre = re.compile(r"\bsetup\b")
            setupfile = os.path.join(buildDir, self.setupfile)
            if os.path.exists(setupfile):
                os.unlink(setupfile)
            if setups and len(setups) > 0:
                fd = open(setupfile, 'w')
                try:
                    for setup in setups:
                        setup = setupre.sub("setup -j", setup)
                        print >> fd, setup
                finally:
                    fd.close()

            lockReleased = False
            try:
              # need to release an exclusive lock to allow the script 
              # declare it's product, if it wishes.  (I;m not happy about 
              # this
              pid = os.getpid()
              if os.environ.get('LOCK_PID', '') == str(pid):
                  lockReleased = self._releaseLock(productRoot)

              try:
                eupsServer.system("cd %s && lssteupsbuild.sh -p %s -D -b %s -r %s %s %s %s %s" % 
                                  (buildDir, os.environ["EUPS_PATH"], buildDir, self.distServer.base, 
                                   distFile, installDir, product, version), 
                                  self.Eups.noaction, self.verbose, self.log) 
              except OSError, e:
                raise RuntimeError("Failed to build and install " + location)

            finally:
              if lockReleased:
                  self._reestablishLock(productRoot)

            if os.path.exists(installDir):
                self.setGroupPerms(installDir)

        if not self.noclean:
            try:
                eupsServer.system("cd %s && lssteupscleanup.sh -b %s" %
                                  (os.path.dirname(buildDir), buildDir),
                                  self.Eups.noaction, self.verbose, self.log)
            except OSError, e:
                raise RuntimeError("Failed to clean up build dir, " + buildDir)

    def _releaseLock(self, productRoot):
        import pwd
        who = pwd.getpwuid(os.geteuid())[0]
        pid = os.getpid()
        lockfile =  "exclusive-%s.%d" % (who, pid)
        lockdir = os.path.join(eups.lock.getLockPath(productRoot), 
                               eups.lock._lockDir)
        if not os.path.exists(os.path.join(lockdir, lockfile)):
            return False
        if self.verbose > 1:
            print >> self.log, "Released lock to run scons"
        eups.lock.giveLocks([(lockdir, lockfile)], self.verbose)
        return True

    def _reestablishLock(self, productRoot):
        eups.lock.takeLocks("eups distrib", productRoot, "exclusive", 
                            verbose=self.verbose)

    def getDistIdForPackage(self, product, version, flavor=None):
        """return the distribution ID that for a package distribution created
        by this Distrib class (via createPackage())
        @param product        the name of the product to create the package 
                                distribution for
        @param version        the name of the product version
        @param flavor         the flavor of the target platform; this may 
                                be ignored by the implentation.  None means
                                that a non-flavor-specific ID is preferred, 
                                if supported.
        """
        return "lsstbuild:" + self._getDistLocation(product, version)

    def packageCreated(self, serverDir, product, version, flavor=None):
        """return True if a distribution package for a given product has 
        apparently been deployed into the given server directory.  
        @param serverDir      a local directory representing the root of the 
                                  package distribution tree
        @param product        the name of the product to create the package 
                                distribution for
        @param version        the name of the product version
        @param flavor         the flavor of the target platform; this may 
                                be ignored by the implentation.  None means
                                that the status of a non-flavor-specific package
                                is of interest, if supported.
        """
        return os.path.exists(os.path.join(serverDir, 
                                           self._getDistLocation(product, 
                                                                 version)))

    _buildExtRe = re.compile(r'[\+\-](.*)$')

    def _getBuildExt(self, version):
        mat = self._buildExtRe.search(version)
        if mat:
            return mat.group(0)
        return None

    def _getDistLocation(self, product, version,
                         flavor='generic', prodinfo=None):
        verdir = self._buildExtRe.sub('', version)
        tarfile = "%s-%s.tar.gz" % (product, verdir)
        distdir = os.path.join(product, verdir)

        if not prodinfo and not self.noeups:
            try :
                pinfo = self.Eups.getProduct(product, version)
            except eups.ProductNotFound:
                print >> self.log, "Note: Product not found, so assuming", \
                                   "it's non-external"

        if prodinfo:
            path = prodinfo.dir
            p = path.find(os.path.join(product,version))
            if p > 0:
                db = path[:p]
                if db.endswith("/external/"):
                    distdir = os.path.join("external", distdir)
                
            db = prodinfo.stackRoot()

            buildfile = product+".bld"
            if os.path.exists(os.path.join(prodinfo.dir,"ups",buildfile)):
                tarfile = buildfile

        return os.path.join(distdir, tarfile)
        
    def createPackage(self, serverDir, product, version, flavor=None, 
                      overwrite=False):
        """Write a package distribution into server directory tree and 
        return the distribution ID.  If a package is made up of several files,
        all of them (except for the manifest) should be deployed by this 
        function.  This includes the table file if it is not incorporated
        another file.  

        Note that the package artifacts will not be created if the 
        manifestToFile option is set.  This option instructs lssteups to 
        produce only the manifest file.  

        @param serverDir      a local directory representing the root of the 
                                  package distribution tree
        @param product        the name of the product to create the package 
                                distribution for
        @param version        the name of the product version
        @param flavor         the flavor of the target platform; this may 
                                be ignored by the implentation.  None means
                                that a non-flavor-specific package is preferred, 
                                if supported.
        @param overwrite      if True, this package will overwrite any 
                                previously existing distribution files even if Eups.force is false
        """
        if self._outmanfile[0] is not None:
            if self._outmanfile[1] is False and \
               os.path.exists(self._outmanfile[0]):
                print >> self.log, "Warning: it appears that the", \
                    "manifestToFile option was used without -j."
                print >> self.log, "         I will not overwrite %s." % \
                    self._outmanfile[0]
                self._outmanfile[1] = True
            return

        installdir = None
        instProd = None
        try:
            instProd = self.Eups.getProduct(product, version)
            installdir = instProd.dir
        except:
            pass

        distId = self._getDistLocation(product, version, prodinfo=instProd)
        distIdFile = os.path.join(serverDir, distId)
        distDir = os.path.dirname(distIdFile)

        # make the product directory
        if not os.path.exists(distDir):
            os.makedirs(distDir)

        # copy the table file over, if available
        tfile = os.path.join(installdir, "ups", product+".table")
        if os.path.exists(tfile):
            shutil.copyfile(tfile, os.path.join(distDir,os.path.basename(tfile)))

        # copy over the src tar file, if available
        tardir = self.options.get("srctardir")
        if not tardir:
            tardir = os.path.join(installdir, "ups")
        basever = self._buildExtRe.sub('', version)
        tfile = os.path.join(tardir, "%s-%s.tar.gz" % (product, basever) )
        if os.path.exists(tfile):
            shutil.copyfile(tfile, os.path.join(distDir, os.path.basename(tfile)))
        else:
            if self.verbose > 0:
                print >> self.log, "Note: Can't find package source", \
                    "code for", product, version, "(%s)" % tfile

        # copy a build file over if it exists
        tfile = os.path.join(installdir, "ups", os.path.basename(distIdFile))
        if distIdFile.endswith(".bld") and os.path.exists(tfile):
            shutil.copy(tfile, distIdFile)

        return self.getDistIdForPackage(product, version, flavor)

    def updateDependencies(self, productList, flavor=None, mapping=None):
        """fill in information in the list of product dependencies based
        on what is known from the system and assumptions about server
        conventions.

        This implementation will modify the product dependencies' attributes
        to match the conventions of the LSST distribution server.  This
        implementation calls getDistIdForPackage(), and from that value it
        will set distId, the table file, and the install directory.

        @param productList     list of products (output from createDependencies)
        @param flavor          the flavor of the target platform; this may 
                                 be ignored by the implentation
        @param mapping         (ignored by this implementation)
        """

        # find the delete list
        if self.options.has_key("ignoredepfile") and \
           os.path.exists(self.options["ignoredepfile"]):
            # this list of products will be removed from the list
            ignore = self._loadIgnoreDepFile(self.options["ignoredepfile"])
            for prod in ignore:
                found = map(lambda p: p[0],
                            filter(lambda d: d[1].product == prod,
                                   enumerate(productList)))
                found.reverse()
                for i in found:
                    productList.pop(i)
        
        for dep in productList:
            flav = flavor
            if not flav:
                flav = dep.flavor
            distId = self.getDistIdForPackage(dep.product, dep.version, flavor)
            
            pair = distId.split(':', 1)
            if len(pair) < 2: pair.insert(0, 'tarball')
            distType, path = pair
            pdir = os.path.dirname(path)

            vers = self._buildExtRe.sub('', dep.version)
            release = dep.version

            dep.distId = distId
            dep.tablefile = os.path.join(pdir, "%s.table" % dep.product)
            dep.instDir = os.path.join(os.path.dirname(pdir), release)

    def _loadIgnoreDepFile(self, filepath):
        out = []

        fd = open(filepath)
        try:
            for line in fd:
                out += line.strip().split()
        finally:
            fd.close()
        return out
                
    def getManifestPath(self, serverDir, product, version,
                        flavor=None, depData=None):
        """return the path where the manifest for a particular product will
        be deployed on the server.  In this implementation, manifests will
        be stored provisionally into the product directory.  (A separate
        release step will "release" them into the manifests directory.)

        @param serverDir      the local directory representing the root of 
                                 the package distribution tree.  In this 
                                 implementation, the returned path will 
                                 start with this directory.
        @param product        the name of the product that the manifest is 
                                for
        @param version        the name of the product version
        @param flavor         the flavor of the target platform for the 
                                manifest.  In this implementation, a value
                                of None will default to "generic".  
        @param dep            an optional Dependency object corresponding 
                                to the product itself.  If provided, the 
                                implementation may use other attributes to 
                                determine the place to put the manifest.  
                                Default is None.  This implementation uses
                                this information when available to place the
                                manifest in the product directory.
        """
        if not depData or not depData.distId:
            print >> self.log, "Warning: without access to a distId, " \
                  "we must write manifest to a non-LSST-specific area."
            return eupsDistrib.DefaultDistrib.getManifestPath(self, serverDir,
                                                              product, version,
                                                              flavor)

        manpre = self.options.get("manifestPrefix", "b");

        buildExt = self._getBuildExt(version)
        if not buildExt:
            # has no trailing "+N"
            mfile = "%s0.manifest" % manpre
        elif buildExt[0] == '+':
            # has trailing "+N"
            mfile = "%s%s.manifest" % (manpre, buildExt[1:])
        else:
            # has trailing "-.*"; treat it as a pre-release
            mfile = "pre%s.manifest" % buildExt[1:]

        path = depData.distId.split(':', 1)[-1]
        pdir = os.path.dirname(path)
        return os.path.join(serverDir, pdir, mfile)


    def writeManifest(self, serverDir, productDeps, product, version,
                      flavor=None, force=False):
        """write out a manifest file for a given product with the given
        dependencies.  By default, getManifestPath() determines where 
        the manifest will be written (see that function's documentation 
        for more info).  If the manifestToFile is option is set, it's value
        is taken as the output filename to write to.  Use of this option
        implies the use of -j (do not create dependency products); care will 
        be taken to only write out the top product's manifest.  

        @param serverDir      a local directory representing the root of the 
                                  package distribution tree
        @param productDeps    the list of product dependencies.  Each item in
                                  the list is a Dependency instance
        @param product        the name of the product to create the package 
                                distribution for
        @param version        the name of the product version
        @param flavor         the flavor of the target platform; this may 
                                be ignored by the implentation
        """
        self.initServerTree(serverDir)

        if self._outmanfile[1] is not None and \
           os.path.exists(self._outmanfile[0]):
            if not self._outmanfile[1]:
                print >> self.log, "Warning: it appears that the", \
                    "manifestToFile option was used without -j."
                print >> self.log, "         I will not overwrite", \
                    self._outmanfile[0]
                self._outmanfile[1] = True
            return

        selectmine = lambda d: d.product == product and d.version == version
        mydep = filter(selectmine, productDeps)
        mydep = (len(mydep) > 0 and mydep[0]) or None

        if self._outmanfile[0]:
            out = self._outmanfile[0]
            self._outmanfile[1] = False   # changed None => False
        else:
            out = self.getManifestPath(serverDir, product, version, 
                                       flavor, mydep)

        # create the manifest
        man = eupsServer.Manifest(product, version, self.Eups, 
                                  verbosity=self.verbose-1, log=self.log)
        for dep in productDeps:
            man.addDepInst(dep)

        # write out manifest
        man.write(out, flavor=flavor, noOptional=False)
        self.setGroupPerms(out)
        if self.verbose and self._outmanfile[0]:
            print >> self.log, "Wrote manifest to %s." % self._outmanfile[0]

    def initServerTree(self, serverDir):
        """initialize the given directory to serve as a package distribution
        tree.
        @param serverDir    the directory to initialize
        """
        eupsDistrib.Distrib.initServerTree(self, serverDir)

        for dir in "manifests external".split():
            dir = os.path.join(serverDir, dir)
            if not os.path.exists(dir):
                os.makedirs(dir)

                # set group owner ship and permissions, if desired
                self.setGroupPerms(dir)

