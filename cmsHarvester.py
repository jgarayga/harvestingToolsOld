#!/usr/bin/env python

###########################################################################
## File       : cmsHarvest.py
## Author     : Jeroen Hegeman
##              jeroen.hegeman@cern.ch
## Last change: 20090902
##
## Purpose    : Main program to run all kinds of harvesting.
###########################################################################

"""Main program to run all kinds of harvesting.

Basically there are four kinds of harvesting in CMS:
- (offline) DQM: Run for real data and for full MC samples.
- ProdVal      : Run for preproduction MC samples to validate production.
                 Maps to HARVESTING:productionValidation.
- RelVal       : Run for release validation samples. Makes heavy use of
                 MC truth information.
- AlCa         : Run for validation of alignment and calibration.
"""

###########################################################################

__version__ = "1.0"
__author__ = "Jeroen Hegeman (jeroen.hegeman@cern.ch)"

###########################################################################

###########################################################################
## TODO list
## - Should we really allow the expert to run anything? Or should we
##   ban from creating single-step jobs for spread-out samples?
## - Put in good default value for CASTOR area.
## - Put in warning in case default CASTOR area is used.
## - Should the raising of Usage quit with exit status zero? Or one?
## - Instrument all points where exceptions are raised with some logging
##   output.
## - Move all informative output of the input parameters etc. to one
##   central place.
## - Figure out how to fill this process.ConfigurationMetadata piece
##   of the Python config.
## - Is this options.evt_type used anywhere?
## - Add implementation of email address of user.
## - Implement CRAB server use?
## - Does write_crab_config need the job_info input?
## - Two-step harvesting.
## - Add process.dqmSaver.workflow to harvesting config.
## - We need a (better) naming convention for the harvesting output files.
## - Combine all these dbs_resolve_xxx into a single call to DBS(?).
## - ??? How large can run numbers become ???
###########################################################################

import os
import sys
import commands
import re
import logging
import optparse

# Debugging stuff.
import pdb
try:
    import debug_hook
except ImportError:
    pass

###########################################################################
## Helper class: Usage exception.
###########################################################################
class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

    # End of Usage.

###########################################################################
## Helper class: Error exception.
###########################################################################
class Error(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

#############################################################################
#### Helper class: Job information.
#############################################################################
##class JobInfo(object):
##    """Little helper class clustering information on a harvesting job.

##    """

##    pass

##    # End of class JobInfo.

###########################################################################
## CMSHarvester class.
###########################################################################

class CMSHarvester(object):
    """Class to perform CMS harvesting.

    More documentation `obviously' to follow.

    """

    ##########

    def __init__(self, cmd_line_opts=None):
        "Initialize class and process command line options."

        self.version = __version__

        # These are the harvesting types allowed:
        #   - DQM
        #   - ProdVal
        #   - RelVal
        #   - AlCa
        # TODO TODO TODO
        # Check the naming of these with Luca to conform to CMS-wide
        # naming of harvesting schemes.
        self.harvesting_types = ["DQM", "ProdVal", "RelVal", "AlCa"]
        # TODO TODO TODO end

        # These are the two possible harvesting modes:
        #   - Single-step: harvesting takes place on-site in a single
        #   step. For each samples only a single ROOT file containing
        #   the harvesting results is returned.
        #   - Two-step: harvesting takes place in two steps. The first
        #   step returns a series of EDM summaries for each
        #   sample. The second step then merges these summaries
        #   locally and does the real harvesting. This second step
        #   produces the ROOT file containing the harvesting results.
        self.harvesting_modes = ["single-step", "two-step"]

        # This contains information specific to each of the harvesting
        # types. Used to create the harvesting configuration.
        harvesting_info = {}
        # TODO TODO TODO
        # Check all these values, especially the ones related to data-vs-MC.
        harvesting_info["DQM"] = {}
        harvesting_info["DQM"]["step_string"] = "dqmHarvesting"

        harvesting_info["ProdVal"] = {}
        harvesting_info["ProdVal"]["step_string"] = "validationprodHarvesting"
        harvesting_info["ProdVal"]["beamspot"] = None
        harvesting_info["ProdVal"]["eventcontent"] = None
        harvesting_info["ProdVal"]["harvesting"] = "AtRunEnd"

        harvesting_info["RelVal"] = {}
        harvesting_info["RelVal"]["step_string"] = "unknown"

        harvesting_info["AlCa"] = {}
        harvesting_info["AlCa"]["step_string"] = "unknown"
        # TODO TODO TODO end
        self.harvesting_info = harvesting_info

        ###

        # These are default `unused' values that will be filled in
        # depending on the command line options.

        # The type of harvesting we're doing: DQM, ProdVal, RelVal, or
        # Alca.
        self.harvesting_type = None
        # The harvesting mode, popularly known as single-step
        # vs. two-step. The thing to remember at this point is that
        # single-step is only possible for samples located completely
        # at a single site (i.e. SE).
        self.harvesting_mode = None
        # The input method: are we reading a dataset name (or regexp)
        # directly from the command line or are we reading a file
        # containing a list of dataset specifications.
        self.input_method = {}
        self.input_method["use"] = None
        self.input_method["ignore"] = None
##        # Similar for the datasets to ignore.
##        self.input_method_ignore = None
        # The name of whatever input we're using.
        self.input_name = {}
        self.input_name["use"] = None
        self.input_name["ignore"] = None
##        # Similar for the datasets to ignore.
##        self.input_name_ignore = None
        # If this is true, we're running in `force mode'. In this case
        # the sanity checks are performed but failure will not halt
        # everything.
        self.force_running = None
        # The base path of the output dir in CASTOR.
        self.castor_base_dir = None

        # Hmmm, hard-coded prefix of the CERN CASTOR area. This is the
        # only supported CASTOR area.
        self.castor_prefix = "castor/cern.ch"

        # This will become the list of datasets to consider
        self.dataset_names = []
        # and this will become the list of datasets to skip.
        self.dataset_names_ignore = []

        # Store command line options for later use.
        if cmd_line_opts is None:
            cmd_line_opts = sys.argv[1:]
        self.cmd_line_opts = cmd_line_opts

        # Set up the logger.
        log_handler = logging.StreamHandler()
        # This is the default log formatter, the debug option switches
        # on some more information.
        log_formatter = logging.Formatter("%(message)s")
        log_handler.setFormatter(log_formatter)
        logger = logging.getLogger()
        logger.name = "main"
        logger.addHandler(log_handler)
        self.logger = logger
        # The default output mode is quite verbose.
        self.set_output_level("VERBOSE")

        #logger.debug("Initialized successfully")

        # End of __init__.

    ##########

    def cleanup(self):
        "Clean up after ourselves."

        # NOTE: This is the safe replacement of __del__.

        #self.logger.debug("All done -> shutting down")
        logging.shutdown()

        # End of cleanup.

    ##########

    def ident_string(self):
        """Spit out an identification string for cmsHarvester.py.

        """

        ident_str = "`Created by cmsHarvester.py " \
                    "version %s': cmsHarvester.py %s" % \
                    (__version__,
                     reduce(lambda x, y: x+' '+y, sys.argv[1:]))

        return ident_str

    ##########

    def set_output_level(self, output_level):
        """Adjust the level of output generated.

        Choices are:
          - normal  : default level of output
          - quiet   : less output than the default
          - verbose : some additional information
          - debug   : lots more information, may be overwhelming

        NOTE: The debug option is a bit special in the sense that it
              also modifies the output format.

        """

        # NOTE: These levels are hooked up to the ones used in the
        #       logging module.
        output_levels = {
            "NORMAL"  : logging.WARNING,
            "QUIET"   : logging.FATAL,
            "VERBOSE" : logging.INFO,
            "DEBUG"   : logging.DEBUG
            }

        output_level = output_level.upper()

        try:
            # Update the logger.
            self.log_level = output_levels[output_level]
            self.logger.setLevel(self.log_level)
        except KeyError:
            # Show a complaint
            self.logger.fatal("Unknown output level `%s'" % ouput_level)
            # and re-raise an exception.
            raise Exception

        # End of set_output_level.

    ##########

    def option_handler_debug(self, option, opt_str, value, parser):
        """Switch to debug mode.

        This both increases the amount of output generated, as well as
        changes the format used (more detailed information is given).

        """

        # Switch to a more informative log formatter for debugging.
        log_formatter_debug = logging.Formatter("[%(levelname)s] " \
                                                # NOTE: funcName was
                                                # only implemented
                                                # starting with python
                                                # 2.5.
                                                #"%(funcName)s() " \
                                                #"@%(filename)s:%(lineno)d " \
                                                "%(message)s")
        # Hmmm, not very nice. This assumes there's only a single
        # handler associated with the current logger.
        log_handler = self.logger.handlers[0]
        log_handler.setFormatter(log_formatter_debug)
        self.set_output_level("DEBUG")

        # End of option_handler_debug.

    ##########

    def option_handler_quiet(self, option, opt_str, value, parser):
        "Switch to quiet mode: less verbose."

        self.set_output_level("QUIET")

        # End of option_handler_quiet.

    ##########

    def option_handler_force(self, option, opt_str, value, parser):
        """Switch on `force mode' in which case we don't brake for nobody.

        In so-called `force mode' all sanity checks are performed but
        we don't halt on failure. Of course this requires some care
        from the user.

        """

        self.logger.debug("Switching on `force mode'.")
        self.force_running = True

        # End of option_handler_force.

    ##########

    def option_handler_harvesting_type(self, option, opt_str, value, parser):
        """Set the harvesting type to be used.

        This checks that no harvesting type is already set, and sets
        the harvesting type to be used to the one specified. If a
        harvesting type is already set an exception is thrown. The
        same happens when an unknown type is specified.

        """

        # Check for (in)valid harvesting types.
        # NOTE: The matching is done in a bit of a complicated
        # way. This allows the specification of the type to be
        # case-insensitive while still ending up with the properly
        # `cased' version afterwards.
        harvesting_types_lowered = [i.lower() for i in self.harvesting_types]
        try:
            type_index = harvesting_types_lowered.index(value)
            # If this works, we now have the index to the `properly
            # cased' version of the harvesting type.
        except ValueError:
            self.logger.fatal("Unknown harvesting type `%s'" % \
                              value)
            self.logger.fatal("  possible types are: %s" %
                              ", ".join(self.harvesting_types))
            raise Usage("Unknown harvesting type `%s'" % \
                        value)

        # Check if multiple (by definition conflicting) harvesting
        # types are being specified.
        if not self.harvesting_type is None:
            msg = "Only one harvesting type should be specified"
            self.logger.fatal(msg)
            raise Usage(msg)
        self.harvesting_type = self.harvesting_types[type_index]

        self.logger.info("Harvesting type to be used: `%s' (%s)" % \
                         (self.harvesting_type,
                          "HARVESTING:%s" % \
                          self.harvesting_info[self.harvesting_type] \
                          ["step_string"]))

        # End of option_handler_harvesting_type.

    ##########

    def option_handler_harvesting_mode(self, option, opt_str, value, parser):
        """Set the harvesting mode to be used.

        Single-step harvesting can be used for samples that are
        located completely at a single site (= SE). Otherwise use
        two-step mode.

        """

        # Check for valid mode.
        harvesting_mode = value.lower()
        if not harvesting_mode in self.harvesting_modes:
            msg = "Unknown harvesting mode `%s'" % harvesting_mode
            self.logger.fatal(msg)
            self.logger.fatal("  possible modes are: %s" % \
                              ", ".join(self.harvesting_modes))
            raise Usage(msg)

        # Check if we've been given only a single mode, otherwise
        # complain.
        if not self.harvesting_mode is None:
            msg = "Only one harvesting mode should be specified"
            self.logger.fatal(msg)
            raise Usage(msg)
        self.harvesting_mode = harvesting_mode

        self.logger.info("Harvesting mode to be used: `%s'" % \
                         self.harvesting_mode)

        # End of option_handler_harvesting_mode.

    ##########

    def option_handler_input_spec(self, option, opt_str, value, parser):
        """TODO TODO TODO
        Document this...

        """

        # Figure out if we were called for the `use these datasets' or
        # the `ignore these datasets' case.
        if opt_str.lower().find("ignore") > -1:
            spec_type = "ignore"
        else:
            spec_type = "use"

        if not self.input_method[spec_type] is None:
            msg = "Please only specify one input method " \
                  "(for the `%s' case)" % spec_type
            self.logger.fatal(msg)
            raise Usage(msg)

        input_method = opt_str.replace("-","").replace("ignore", "")
        self.input_method[spec_type] = input_method
        self.input_name[spec_type] = value

        self.logger.info("Input method for the `%s' case: %s" % \
                         (spec_type, input_method))

        # End of option_handler_input_spec.

    ##########

    # OBSOLETE OBSOLETE OBSOLETE

##    def option_handler_dataset_name(self, option, opt_str, value, parser):
##        """Specify the name(s) of the dataset(s) to be processed.

##        It is checked to make sure that no dataset name or listfile
##        names are given yet. If all is well (i.e. we still have a
##        clean slate) the dataset name is stored for later use,
##        otherwise a Usage exception is raised.

##        """

##        if not self.input_method is None:
##            if self.input_method == "dataset":
##                raise Usage("Please only feed me one dataset specification")
##            elif self.input_method == "listfile":
##                raise Usage("Cannot specify both dataset and input list file")
##            else:
##                assert False, "Unknown input method `%s'" % self.input_method
##        self.input_method = "dataset"
##        self.input_name = value
##        self.logger.info("Input method used: %s" % self.input_method)

##        # End of option_handler_dataset_name.

##    ##########

##    def option_handler_listfile_name(self, option, opt_str, value, parser):
##        """Specify the input list file containing datasets to be processed.

##        It is checked to make sure that no dataset name or listfile
##        names are given yet. If all is well (i.e. we still have a
##        clean slate) the listfile name is stored for later use,
##        otherwise a Usage exception is raised.

##        """

##        if not self.input_method is None:
##            if self.input_method == "listfile":
##                raise Usage("Please only feed me one list file")
##            elif self.input_method == "dataset":
##                raise Usage("Cannot specify both dataset and input list file")
##            else:
##                assert False, "Unknown input method `%s'" % self.input_method
##        self.input_method = "listfile"
##        self.input_name = value
##        self.logger.info("Input method used: %s" % self.input_method)

##        # End of option_handler_listfile_name.

    # OBSOLETE OBSOLETE OBSOLETE end

    ##########

    def option_handler_castor_dir(self, option, opt_str, value, parser):
        """Specify where on CASTOR the output should go.

        At the moment only output to CERN CASTOR is
        supported. Eventually the harvested results should go into the
        central place for DQM on CASTOR anyway.

        """

        # Check format of specified CASTOR area.
        castor_dir = value
        castor_dir = castor_dir.lstrip(os.path.sep)
        castor_prefix = self.castor_prefix
        if not castor_dir.startswith(castor_prefix):
            self.logger.warning("CASTOR area does not start with " \
                                "`%s' --> prepending" % \
                                castor_prefix)
            castor_dir = os.path.join(castor_prefix, castor_dir)

        castor_dir = os.path.join(os.path.sep, castor_dir)
        self.castor_base_dir = os.path.normpath(castor_dir)
        self.logger.info("Using CASTOR (base) area `%s'" % \
                         self.castor_base_dir)

        # End of option_handler_castor_dir.

    ##########

    def create_castor_path_name(self, dataset_name):
        """Build the output path to be used on CASTOR.

        This consists of the CASTOR area base path specified by the
        user and a piece depending on the data type (data vs. MC) and
        the harvesting type.

        # NOTE: It's not possible to create different kinds of
        # harvesting jobs in a single call to this tool. However, in
        # principle it could be possible to create both data and MC
        # jobs in a single go.

        """

        datatype = self.datasets_information[dataset_name]["datatype"]
        harvesting_type = self.harvesting_type

        castor_base_path = self.castor_base_dir
        sub_dir_piece = os.path.join(datatype, harvesting_type.lower())

        castor_path = os.path.join(castor_base_path, sub_dir_piece)
        castor_path = os.path.normpath(castor_path)

        # End of create_castor_path_name.
        return castor_path

    ##########

    def create_and_check_castor_dirs(self):
        """Make sure all required CASTOR output dirs exist.

        This checks the CASTOR base dir specified by the user as well
        as all the subdirs required by the current set of jobs.

        """

        # Call the real checker method for the base dir.
        self.create_and_check_castor_dir(self.castor_base_dir)

        # Now call the checker for all (unique) subdirs.
        castor_dirs = [i["castor_path"] \
                       for i in self.datasets_information.values()]
        castor_dirs_unique = list(set(castor_dirs))
        for castor_dir in castor_dirs_unique:
            self.create_and_check_castor_dir(castor_dir)

        # End of create_and_check_castor_dirs.

    ##########

    def create_and_check_castor_dir(self, castor_dir):
        """Check existence of the give CASTOR dir, if necessary create
        it.

        Some special care has to be taken with several things like
        setting the correct permissions such that CRAB can store the
        output results. Of course this means that things like
        /castor/cern.ch/ and user/j/ have to be recognised and treated
        properly.

        NOTE: Only CERN CASTOR area (/castor/cern.ch/) supported for
        the moment.

        """

        ###

        # Local helper function to fully split a path into pieces.
        def split_completely(path):
            (parent_path, name) = os.path.split(path)
            if name == "":
                return (parent_path, )
            else:
                return split_completely(parent_path) + (name, )

        ###

        # Local helper function to check rfio (i.e. CASTOR)
        # directories.
        def extract_permissions(rfstat_output):
            """Parse the output from rfstat and return the
            5-digit permissions string."""

            permissions_line = [i for i in output.split("\n") \
                                if i.lower().find("protection") > -1]
            regexp = re.compile(".*\(([0123456789]{5})\).*")
            match = regexp.search(rfstat_output)
            if not match or len(match.groups()) != 1:
                msg = "Could not extract permissions " \
                      "from output: %s" % rfstat_output
                self.logger.fatal(msg)
                raise Error(msg)
            permissions = match.group(1)

            # End of extract_permissions.
            return permissions

        ###

        # These are the pieces of CASTOR directories that we do not
        # want to touch when modifying permissions.

        # NOTE: This is all a bit involved, basically driven by the
        # fact that one wants to treat the `j' directory of
        # `/castor/cern.ch/user/j/jhegeman/' specially.
        castor_paths_dont_touch = {
            0: ["/", "castor", "cern.ch", "cms", "store", "dqm", "user"],
            -1: ["user"]
            }

        self.logger.info("Checking CASTOR path `%s'" % castor_dir)

        ###

        # First we take the full CASTOR path apart.
        castor_path_pieces = split_completely(castor_dir)

        # Now slowly rebuild the CASTOR path and see if a) all
        # permissions are set correctly and b) the final destination
        # exists.
        path = ""
        check_sizes = castor_paths_dont_touch.keys()
        check_sizes.sort()
        len_castor_path_pieces = len(castor_path_pieces)
        for piece_index in xrange (len_castor_path_pieces):
            skip_this_path_piece = False
            piece = castor_path_pieces[piece_index]
##            self.logger.debug("Checking CASTOR path piece `%s'" % \
##                              piece)
            for check_size in check_sizes:
                # Do we need to do anything with this?
                if (piece_index + check_size) > -1:
##                    self.logger.debug("Checking `%s' against `%s'" % \
##                                      (castor_path_pieces[piece_index + check_size],
##                                       castor_paths_dont_touch[check_size]))
                    if castor_path_pieces[piece_index + check_size] in castor_paths_dont_touch[check_size]:
##                        self.logger.debug("  skipping")
                        skip_this_path_piece = True
##                    else:
##                        # Piece not in the list, fine.
##                        self.logger.debug("  accepting")
            # Add piece to the path we're building.
##            self.logger.debug("!!! Skip path piece `%s'? %s" % \
##                              (piece, str(skip_this_path_piece)))
##            self.logger.debug("Adding piece to path...")
            path = os.path.join(path, piece)
##            self.logger.debug("Path is now `%s'" % \
##                              path)

            # Now, unless we're supposed to skip this piece of the
            # path, let's make sure it exists and set the permissions
            # correctly for use by CRAB. This means that:
            # - the final output directory should (at least) have
            #   permissions 775
            # - all directories above that should (at least) have
            #   permissions 755.

            if not skip_this_path_piece:

                # Ok, first thing: let's make sure this directory
                # exists.
                # NOTE: The nice complication is of course that the
                # usual os.path.isdir() etc. methods don't work for an
                # rfio filesystem. So we call rfstat and interpret an
                # error as meaning that the path does not exist.
                cmd = "rfstat %s" % path
                (status, output) = commands.getstatusoutput(cmd)
                if status != 0:
                    # Path does not exist, let's try and create it.
                    self.logger.debug("Creating path `%s'" % path)
                    cmd = "rfmkdir %s" % path
                    (status, output) = commands.getstatusoutput(cmd)
                    if status != 0:
                        msg = "Could not create directory `%s'" % path
                        self.logger.fatal(msg)
                        raise Error(msg)
                    cmd = "rfstat %s" % path
                    (status, output) = commands.getstatusoutput(cmd)
                # Now check that it looks like a directory. If I'm not
                # mistaken one can deduce this from the fact that the
                # (octal) permissions string starts with `40' (instead
                # of `100').
                permissions = extract_permissions(output)
                if not permissions.startswith("40"):
                    msg = "Path `%s' is not a directory(?)" % path
                    self.logger.fatal(msg)
                    raise Error(msg)

                # Figure out the current permissions for this
                # (partial) path.
                self.logger.debug("Checking permissions for path `%s'" % path)
                cmd = "rfstat %s" % path
                (status, output) = commands.getstatusoutput(cmd)
                if status != 0:
                    msg = "Could not obtain permissions for directory `%s'" % \
                          path
                    self.logger.fatal(msg)
                    raise Error(msg)
                # Take the last three digits of the permissions.
                permissions = extract_permissions(output)[-3:]

                # Now if necessary fix permissions.
                # NOTE: Be careful never to `downgrade' permissions.
                if piece_index == (len_castor_path_pieces - 1):
                    # This means we're looking at the final
                    # destination directory.
                    permissions_target = "775"
                else:
                    # `Only' an intermediate directory.
                    permissions_target = "755"

                # Compare permissions.
                permissions_new = []
                for (i, j) in zip(permissions, permissions_target):
                    permissions_new.append(str(max(int(i), int(j))))
                permissions_new = "".join(permissions_new)
                self.logger.debug("  current permissions: %s" % \
                                  permissions)
                self.logger.debug("  target permissions : %s" % \
                                  permissions_target)
                if permissions_new != permissions:
                    # We have to modify the permissions.
                    self.logger.debug("Changing permissions of `%s' " \
                                      "to %s (were %s)" % \
                                      (path, permissions_new, permissions))
                    cmd = "rfchmod %s %s" % (permissions_new, path)
                    (status, output) = commands.getstatusoutput(cmd)
                    if status != 0:
                        msg = "Could not change permissions for path `%s' " \
                              "to %s" % (path, permissions_new)
                        self.logger.fatal(msg)
                        raise Error(msg)

                self.logger.debug("  Permissions ok (%s)" % permissions_new)

        # End of create_and_check_castor_dir.

    ##########

    def parse_cmd_line_options(self):

        # Set up the command line options.
        parser = optparse.OptionParser(version="%s %s" % \
                                       ("%prog", self.version))
        self.option_parser = parser

        # The debug switch.
        parser.add_option("-d", "--debug",
                          help="Switch on debug mode",
                          action="callback",
                          callback=self.option_handler_debug)

        # The quiet switch.
        parser.add_option("-q", "--quiet",
                          help="Be less verbose",
                          action="callback",
                          callback=self.option_handler_quiet)

        # The force switch. If this switch is used sanity checks are
        # performed but failures do not lead to aborts. Use with care.
        parser.add_option("", "--force",
                          help="Force mode. Do not abort on sanity check "
                          "failures",
                          action="callback",
                          callback=self.option_handler_force)

        # Choose between the different kinds of harvesting.
        harvesting_types_tmp = ["%s = HARVESTING:%s" % \
                                (i, self.harvesting_info[i]["step_string"]) \
                                for i in self.harvesting_types]
        parser.add_option("-t", "--type",
                          help="Harvesting type: %s (%s)" % \
                          (", ".join(self.harvesting_types),
                           ", ".join(harvesting_types_tmp)),
                          action="callback",
                          callback=self.option_handler_harvesting_type,
                          type="string",
                          #nargs=1,
                          #dest="self.harvesting_type",
                          metavar="HARVESTING_TYPE")

        # Choose between single-step and two-step mode.
        parser.add_option("-m", "--mode",
                          help="Harvesting mode: %s" % \
                          ", ".join(self.harvesting_modes),
                          action="callback",
                          callback=self.option_handler_harvesting_mode,
                          type="string",
                          metavar="HARVESTING_MODE")

        # Option to specify a dataset name.
        parser.add_option("", "--dataset",
                          help="Name(s) of dataset(s) to process",
                          action="callback",
                          #callback=self.option_handler_dataset_name,
                          callback=self.option_handler_input_spec,
                          type="string",
                          #dest="self.input_name",
                          metavar="DATASET")

        # Option to specify a file containing a list of dataset names.
        parser.add_option("", "--listfile",
                          help="File containing list of dataset names " \
                          "to process",
                          action="callback",
                          #callback=self.option_handler_listfile_name,
                          callback=self.option_handler_input_spec,
                          type="string",
                          #dest="self.input_name",
                          metavar="LISTFILE")

        # Specify the place in CASTOR where the output should go.
        # NOTE: Only output to CASTOR is supported for the moment,
        # since the central DQM results place is on CASTOR anyway.
        parser.add_option("", "--castordir",
                          help="Place on CASTOR to store results",
                          action="callback",
                          callback=self.option_handler_castor_dir,
                          type="string",
                          metavar="CASTORDIR")

        # If nothing was specified: tell the user how to do things the
        # next time and exit.
        # NOTE: We just use the OptParse standard way of doing this by
        #       acting as if a '--help' was specified.
        if len(self.cmd_line_opts) < 1:
            self.cmd_line_opts = ["--help"]

        # Everything is set up, now parse what we were given.
        parser.set_defaults()
        (self.options, self.args) = parser.parse_args(self.cmd_line_opts)

        # End of parse_cmd_line_options.

    ##########

    def check_input_status(self):
        """Check completeness of input information.

        """

        # We need a harvesting method to be specified
        if self.harvesting_type is None:
            raise Usage("Please specify a harvesting type")
        # as well as a harvesting mode.
        if self.harvesting_mode is None:
            raise Usage("Please specify a harvesting mode")

        # We need an input method so we can find the dataset name(s).
        if self.input_method is None:
            raise Usage("Please specify an input dataset name or " \
                        "a list file name")
        # DEBUG DEBUG DEBUG
        # If we get here, we should also have an input name.
        assert not self.input_name is None
        # DEBUG DEBUG DEBUG end

        # We need to know where to put the stuff (okay, the results)
        # on CASTOR.
        if self.castor_base_dir is None:
            self.logger.fatal("Please specify a CASTOR area " \
                              "to store the results")
            raise Usage("Please specify a CASTOR area " \
                        "to store the results")

        # End of check_input_status.

    ##########

    def check_cmssw(self):
        """Check if CMSSW is setup.

        """

        # Try to access the CMSSW_VERSION environment variable. If
        # it's something useful we consider CMSSW to be set up
        # properly. Otherwise we raise an error.
        cmssw_version = os.getenv("CMSSW_VERSION")
        if cmssw_version is None:
            self.logger.fatal("It seems like CMSSW is not setup...")
            self.logger.fatal("($CMSSW_VERSION is empty)")
            raise Error("ERROR: CMSSW needs to be setup first!")

        self.cmssw_version = cmssw_version
        self.logger.info("Found CMSSW version %s properly set up" % \
                          self.cmssw_version)

        # End of check_cmsssw.
        return True

    ##########

    def check_dbs(self):
        """Check if DBS is setup.

        """

        # Try to access the DBSCMD_HOME environment variable. If this
        # looks useful we consider DBS to be set up
        # properly. Otherwise we raise an error.
        dbs_home = os.getenv("DBSCMD_HOME")
        if dbs_home is None:
            self.logger.fatal("It seems like DBS is not setup...")
            self.logger.fatal("  $DBSCMD_HOME is empty")
            raise Error("ERROR: DBS needs to be setup first!")

##        # Now we try to do a very simple DBS search. If that works
##        # instead of giving us the `Unsupported API call' crap, we
##        # should be good to go.
##        # NOTE: Not ideal, I know, but it reduces the amount of
##        #       complaints I get...
##        cmd = "dbs search --query=\"find dataset where dataset = impossible\""
##        (status, output) = commands.getstatusoutput(cmd)
##        pdb.set_trace()
##        if status != 0 or \
##           output.lower().find("unsupported api call") > -1:
##            self.logger.fatal("It seems like DBS is not setup...")
##            self.logger.fatal("  %s returns crap:" % cmd)
##            for line in output.split("\n"):
##                self.logger.fatal("  %s" % line)
##            raise Error("ERROR: DBS needs to be setup first!")

        self.logger.debug("Found DBS properly set up")

        # End of check_dbs.
        return True

    ##########

    def setup_dbs(self):
        """Setup the Python side of DBS.

        For more information see the DBS Python API documentation:
        https://twiki.cern.ch/twiki/bin/view/CMS/DBSApiDocumentation

        """

        # These we need to communicate with DBS
        # global DBSAPI
        from DBSAPI.dbsApi import DbsApi
        import DBSAPI.dbsException
        import DBSAPI.dbsApiException
        # and these we need to parse the DBS output.
        global xml
        global SAXParseException
        import xml.sax
        from xml.sax import SAXParseException

        try:
            args={}
            args["url"]= "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/" \
                         "servlet/DBSServlet"
            api = DbsApi(args)
            self.dbs_api = api

        except DBSAPI.dbsApiException.DbsApiException, ex:
            self.logger.fatal("Caught DBS API exception %s: %s "  % \
                              (ex.getClassName(), ex.getErrorMessage()))
            if ex.getErrorCode() not in (None, ""):
                logger.debug("DBS exception error code: ", ex.getErrorCode())
            raise

        # End of setup_dbs.

    ##########

    def dbs_resolve_dataset_name(self, dataset_name):
        """Use DBS to resolve a wildcarded dataset name.

        """

        # DEBUG DEBUG DEBUG
        # If we get here DBS should have been set up already.
        assert not self.dbs_api is None
        # DEBUG DEBUG DEBUG end

        api = self.dbs_api
        dbs_query = "find dataset where dataset like %s " \
                    "and dataset.status = VALID" % \
                    dataset_name
        try:
            api_result = api.executeQuery(dbs_query)
        except DbsApiException:
            raise Error("ERROR: Could not execute DBS query")

        try:
            datasets = []
            class Handler(xml.sax.handler.ContentHandler):
                def startElement(self, name, attrs):
                    if name == "result":
                        datasets.append(str(attrs["PATH"]))
            xml.sax.parseString(api_result, Handler())
        except SAXParseException:
            raise Error("ERROR: Could not parse DBS server output")

        # End of dbs_resolve_dataset_name.
        return datasets

    ##########

    def dbs_resolve_cmssw_version(self, dataset_name):
        """Ask DBS for the CMSSW version used to create this dataset.

        """

        # DEBUG DEBUG DEBUG
        # If we get here DBS should have been set up already.
        assert not self.dbs_api is None
        # DEBUG DEBUG DEBUG end

        api = self.dbs_api
        dbs_query = "find algo.version where dataset = %s " \
                    "and dataset.status = VALID" % \
                    dataset_name
        try:
            api_result = api.executeQuery(dbs_query)
        except DbsApiException:
            raise Error("ERROR: Could not execute DBS query")

        try:
            cmssw_version = []
            class Handler(xml.sax.handler.ContentHandler):
                def startElement(self, name, attrs):
                    if name == "result":
                        cmssw_version.append(str(attrs["APPVERSION_VERSION"]))
            xml.sax.parseString(api_result, Handler())
        except SAXParseException:
            raise Error("ERROR: Could not parse DBS server output")

        # DEBUG DEBUG DEBUG
        assert len(cmssw_version) == 1
        # DEBUG DEBUG DEBUG end

        cmssw_version = cmssw_version[0]

        # End of dbs_resolve_cmssw_version.
        return cmssw_version

    ##########

    def dbs_resolve_runs(self, dataset_name):
        """Ask DBS for the list of runs in a given dataset.

        # NOTE: This does not (yet?) skip/remove empty runs. There is
        # a bug in the DBS entry run.numevents (i.e. it always returns
        # zero) which should be fixed in the `next DBS release'.
        # See also:
        #   https://savannah.cern.ch/bugs/?53452
        #   https://savannah.cern.ch/bugs/?53711

        """

        # TODO TODO TODO
        # We should remove empty runs as soon as the above mentioned
        # bug is fixed.
        # TODO TODO TODO end

        # DEBUG DEBUG DEBUG
        # If we get here DBS should have been set up already.
        assert not self.dbs_api is None
        # DEBUG DEBUG DEBUG end

        api = self.dbs_api
        dbs_query = "find run where dataset = %s " \
                    "and dataset.status = VALID" % \
                    dataset_name
        try:
            api_result = api.executeQuery(dbs_query)
        except DbsApiException:
            raise Error("ERROR: Could not execute DBS query")

        try:
            runs = []
            class Handler(xml.sax.handler.ContentHandler):
                def startElement(self, name, attrs):
                    if name == "result":
                        runs.append(int(attrs["RUNS_RUNNUMBER"]))
            xml.sax.parseString(api_result, Handler())
        except SAXParseException:
            raise Error("ERROR: Could not parse DBS server output")

        # End of dbs_resolve_runs.
        return runs

    ##########

    def dbs_resolve_globaltag(self, dataset_name):
        """Ask DBS for the globaltag corresponding to a given dataset.

        """

        # DEBUG DEBUG DEBUG
        # If we get here DBS should have been set up already.
        assert not self.dbs_api is None
        # DEBUG DEBUG DEBUG end

        api = self.dbs_api
        dbs_query = "find dataset.tag where dataset = %s " \
                    "and dataset.status = VALID" % \
                    dataset_name
        try:
            api_result = api.executeQuery(dbs_query)
        except DbsApiException:
            raise Error("ERROR: Could not execute DBS query")

        try:
            globaltag = []
            class Handler(xml.sax.handler.ContentHandler):
                def startElement(self, name, attrs):
                    if name == "result":
                        globaltag.append(str(attrs["PROCESSEDDATASET_GLOBALTAG"]))
            xml.sax.parseString(api_result, Handler())
        except SAXParseException:
            raise Error("ERROR: Could not parse DBS server output")

        # DEBUG DEBUG DEBUG
        assert len(globaltag) == 1
        # DEBUG DEBUG DEBUG end

        globaltag = globaltag[0]

        # End of dbs_resolve_globaltag.
        return globaltag

    ##########

    def dbs_resolve_datatype(self, dataset_name):
        """Ask DBS for the the data type (data or mc) of a given
        dataset.

        """

        # DEBUG DEBUG DEBUG
        # If we get here DBS should have been set up already.
        assert not self.dbs_api is None
        # DEBUG DEBUG DEBUG end

        api = self.dbs_api
        dbs_query = "find datatype.type where dataset = %s " \
                    "and dataset.status = VALID" % \
                    dataset_name
        try:
            api_result = api.executeQuery(dbs_query)
        except DbsApiException:
            raise Error("ERROR: Could not execute DBS query")

        try:
            datatype = []
            class Handler(xml.sax.handler.ContentHandler):
                def startElement(self, name, attrs):
                    if name == "result":
                        datatype.append(str(attrs["PRIMARYDSTYPE_TYPE"]))
            xml.sax.parseString(api_result, Handler())
        except SAXParseException:
            raise Error("ERROR: Could not parse DBS server output")

        # DEBUG DEBUG DEBUG
        assert len(datatype) == 1
        # DEBUG DEBUG DEBUG end

        datatype = datatype[0]

        # End of dbs_resolve_datatype.
        return datatype

    ##########

    def dbs_resolve_dataset_number_of_sites(self, dataset_name):
        """Ask DBS across how many sites this dataset has been spread
        out.

        This is especially useful to check that we do not submit a job
        supposed to run on a complete sample that is not contained at
        a single site.

        """

        # DEBUG DEBUG DEBUG
        # If we get here DBS should have been set up already.
        assert not self.dbs_api is None
        # DEBUG DEBUG DEBUG end

        api = self.dbs_api
        dbs_query = "find count(site) where dataset = %s " \
                    "and dataset.status = VALID" % \
                    dataset_name
        try:
            api_result = api.executeQuery(dbs_query)
        except DbsApiException:
            raise Error("ERROR: Could not execute DBS query")

        try:
            num_sites = []
            class Handler(xml.sax.handler.ContentHandler):
                def startElement(self, name, attrs):
                    if name == "result":
                        num_sites.append(str(attrs["COUNT_STORAGEELEMENT"]))
            xml.sax.parseString(api_result, Handler())
        except SAXParseException:
            raise Error("ERROR: Could not parse DBS server output")

        # DEBUG DEBUG DEBUG
        assert len(num_sites) == 1
        # DEBUG DEBUG DEBUG end

        num_sites = int(num_sites[0])

        # End of dbs_resolve_dataset_number_of_sites.
        return num_sites

    ##########

    def build_dataset_list(self, input_method, input_name):
        """Build a list of all datasets to be processed.

        """

        dataset_names = []

        if self.input_method == "dataset":
            # Input comes from a dataset name directly on the command
            # line. But, this can also contain wildcards so we need
            # DBS to translate it conclusively into a list of explicit
            # dataset names.
            dataset_names = self.dbs_resolve_dataset_name(self.input_name)
        elif self.input_method == "listfile":
            # In this case a file containing a list of dataset names
            # is specified. Still, each line may contain wildcards so
            # this step also needs help from DBS.
            # NOTE: Lines starting with a `#' are ignored.
            self.logger.info("Reading input from list file `%s'" % \
                             self.input_name)
            try:
                listfile = open(self.input_name, "r")
                for dataset in listfile:
                    if dataset.strip()[0] != "#":
                        dataset_names.extend(self. \
                                             dbs_resolve_dataset_name(dataset))
                listfile.close()
            except IOError:
                raise Error("ERROR: Could not open input list file `%s'" % \
                            self.input_name)
        else:
            # DEBUG DEBUG DEBUG
            # We should never get here.
            assert False, "Unknown input method `%s'" % self.input_method
            # DEBUG DEBUG DEBUG end

        # Remove duplicates from the dataset list.
        # NOTE: There should not be any duplicates in any list coming
        # from DBS, but maybe the user provided a list file with less
        # care.
        dataset_names = list(set(dataset_names))

        # Store for later use.
        dataset_names.sort()
        self.dataset_names = dataset_names

        self.logger.info("Found %d datasets:" % len(self.dataset_names))
        for dataset in self.dataset_names:
            self.logger.info("  `%s'" % dataset)

        # End of build_dataset_list.
        return dataset_names

    ##########

##    def build_dataset_use_list(self):
##        """

##        """

##        self.dataset_names = self.build_dataset_list(self.input_method,
##                                                     self.input_name)

##        # End of build_dataset_list.

##    ##########

##    def build_dataset_ignore_list(self):
##        """

##        """

##        self.dataset_names_ignore = self.build_dataset_list(self.input_method_ignore,
##                                                            self.input_name_ignore)

##        # End of build_dataset_ignore_list.

    ##########

    # TODO TODO TODO

    def process_dataset_ignore_list(self):
        """Update the list of datasets taking into account the ones to
        ignore.

        Both lists have been generated before from DBS and both are
        assumed to be unique.

        NOTE: The advantage of creating the ignore list from DBS (in
        case a regexp is given) and matching that instead of directly
        matching the ignore criterion against the list of datasets (to
        consider) built from DBS is that in the former case we're sure
        that all regexps are treated exactly as DBS would have done
        without the cmsHarvester.

        """

        # Simple approach: just loop and search.
        dataset_names_filtered = []
        for dataset_name in self.dataset_names:
            if not dataset_name in self.dataset_names_ignore:
                dataset_names_filtered.append(dataset_name)

        self.dataset_names = dataset_names_filtered

    # End of process_dataset_ignore_list.

    # TODO TODO TODO end

    ##########

    def check_dataset_list(self):
        """Check list of dataset names for impossible ones.

        Two kinds of checks are done:
        - Checks for things that do not make sense. These lead to
          errors and skipped datasets.
        - Sanity checks. For these warnings are issued but the user is
          considered to be the authoritative expert.

        Checks performed:
        - The CMSSW version encoded in the dataset name should match
          self.cmssw_version. This is critical.
        # TODO TODO TODO
        - There should be some events in the dataset. This is
          acritical.
        # TODO TODO TODO end
        - It is not possible to run single-step harvesting jobs on
          samples that are not fully contained at a single site.

        """

        dataset_names_after_checks = list(self.dataset_names)

        for dataset_name in self.dataset_names:

            # Check CMSSW version.
            version_from_dataset = self.dbs_resolve_cmssw_version(dataset_name)
            if version_from_dataset != self.cmssw_version:
                msg = "CMSSW version mismatch for dataset `%s'" \
                      "(%s vs. %s)" % \
                      (dataset_name,
                       self.cmssw_version, version_from_dataset)
                if self.force_running:
                    # Expert mode: just warn, then continue.
                    self.logger.warning("%s" \
                                        "--> `force mode' active: " \
                                        "run anyway" % msg)
                else:
                    dataset_names_after_checks.remove(dataset_name)
                    self.logger.fatal("%s " \
                                      "--> skipping" % msg)

            ###

            # TODO TODO TODO
            # Require that the dataset is non-empty.
            # TODO TODO TODO end

            ###

            # TODO TODO TODO
            # If we're running single-step harvesting: only allow
            # samples located on a single site.
            if self.harvesting_mode == "single-step":
                num_sites = self.datasets_information[dataset_name] \
                            ["num_sites"]
                if num_sites > 1:
                    # Cannot do this with a single-step job.
                    msg = "Cannot run single-step harvesting on samples " \
                          "spread across multiple sites"
                    if self.force_running:
                        # Expert mode: just warn, then continue.
                        self.logger.warning("%s" \
                                            "--> `force mode' active: " \
                                            "run anyway" % msg)
                    else:
                        dataset_names_after_checks.remove(dataset_name)
                        self.logger.fatal("%s " \
                                          "--> skipping" % msg)
            # TODO TODO TODO end

        # Now store the modified version of the dataset list.
        self.dataset_names = dataset_names_after_checks

        # End of check_dataset_list.

    ##########

    def escape_dataset_name(self, dataset_name):
        """Escape a DBS dataset name.

        Escape a DBS dataset name such that it does not cause trouble
        with the file system. This means turning each `/' into `__',
        except for the first one which is just removed.

        """

        escaped_dataset_name = dataset_name
        escaped_dataset_name = escaped_dataset_name.strip("/")
        escaped_dataset_name = escaped_dataset_name.replace("/", "__")

        return escaped_dataset_name

    ##########

    def create_harvesting_config_file_name(self, dataset_name):
        "Generate the name to be used for the harvesting config file."

        file_name_base = "harvesting.py"
        dataset_name_escaped = self.escape_dataset_name(dataset_name)
        config_file_name = file_name_base.replace(".py",
                                                  "_%s.py" % \
                                                  dataset_name_escaped)

        # End of create_harvesting_config_file.
        return config_file_name

    ##########

    def create_harvesting_output_file_name(self, dataset_name):
        "Generate the name to be used for the harvesting output file."

        dataset_name_escaped = self.escape_dataset_name(dataset_name)
        # BUG BUG BUG
        # This naming convention is taken from Nuno's script but WTF
        # does it mean?
        output_file_name = "DQM_V0001_R000000001_%s.root" % dataset_name_escaped
        # BUG BUG BUG end

        # End of create_harvesting_output_file_name.
        return output_file_name

    ##########

    def create_multicrab_block_name(self, dataset_name, run_number):
        """Create the block name to use for this dataset/run number.

        This is what appears in the brackets `[]' in multicrab.cfg. It
        is used as the name of the job and to create output
        directories.

        """

        dataset_name_escaped = self.escape_dataset_name(dataset_name)
        block_name = "%s_%09d" % (dataset_name_escaped, run_number)

        # End of create_multicrab_block_name.
        return block_name

    ##########

    def create_crab_config(self):
        """Create a CRAB configuration for a given job.

        NOTE: This is _not_ a complete (as in: submittable) CRAB
        configuration. It is used to store the common settings for the
        multicrab configuration.

        NOTE: Only CERN CASTOR area (/castor/cern.ch/) is supported.

        """

        crab_config_base = """
# WARNING: This file was created automatically!

# %(ident_str)s

[CRAB]
jobtype = cmssw
scheduler = glite

[GRID]
# This removes the default blacklisting of T1 sites.
remove_default_blacklist = 1
rb = CERN

[USER]
copy_data = 1
storage_element=srm-cms.cern.ch
storage_path=/srm/managerv2?SFN=%(castor_prefix)s
#thresholdLevel=70
#eMail=jhegeman@cern.ch

[CMSSW]
# This reveals data hosted on T1 sites, which is normally hidden by CRAB.
show_prod = 1
# Force everything to run in one job.
number_of_jobs = 1
no_block_boundary = 1

"""

        ident_str = self.ident_string()
        castor_prefix = self.castor_prefix
##        castor_dir = self.datasets_information[dataset_name] \
##                     ["castor_path"]
##        castor_dir = castor_dir.replace(castor_prefix, "")

        crab_config = crab_config_base % vars()

        # End of create_crab_config.
        return crab_config

    ##########

    def create_multicrab_config(self):
        """Create a multicrab.cfg file for all samples.

        This creates the contents for a multicrab.cfg file that uses
        the crab.cfg file (generated elsewhere) for the basic settings
        and contains blocks for each run of each dataset.

        """

        multicrab_config_base = """
# WARNING: This file was created automatically!

# %(ident_str)s

[MULTICRAB]
cfg=crab.cfg
"""

        ident_str = self.ident_string()

        multicrab_config_lines = [multicrab_config_base % vars()]
        dataset_names = self.datasets_information.keys()
        dataset_names.sort()
        for dataset_name in dataset_names:
            runs = self.datasets_information[dataset_name]["runs"]
            dataset_name_escaped = self.escape_dataset_name(dataset_name)
            harvesting_config_file_name = self. \
                                          create_harvesting_config_file_name(dataset_name)
            castor_prefix = self.castor_prefix
            castor_dir = self.datasets_information[dataset_name] \
                         ["castor_path"]
            castor_dir = castor_dir.replace(castor_prefix, "")
            for run in runs:
                output_file_name = self. \
                                   create_harvesting_output_file_name(dataset_name)
                # The block name.
                multicrab_block_name = self.create_multicrab_block_name( \
                    dataset_name, run)
                multicrab_config_lines.append("[%s]" % \
                                              multicrab_block_name)
                # The parameter set (i.e. the harvesting configuration
                # for this dataset).
                multicrab_config_lines.append("CMSSW.pset = %s" % \
                                              harvesting_config_file_name)
                # The dataset.
                multicrab_config_lines.append("CMSSW.datasetpath = %s" % \
                                              dataset_name)
                # The run selection: one job (i.e. one block in
                # multicrab.cfg) for each run of each dataset.
                multicrab_config_lines.append("CMSSW.runselection = %d" % \
                                              run)
                # The output file name.
                multicrab_config_lines.append("CMSSW.output_file = %s" % \
                                              output_file_name)

                # CASTOR output dir.
                multicrab_config_lines.append("USER.user_remote_dir = %s" % \
                                              castor_dir)

                # End of block.
                multicrab_config_lines.append("")

        multicrab_config = "\n".join(multicrab_config_lines)

        # End of create_multicrab_config.
        return multicrab_config

    ##########

    def create_harvesting_config(self, dataset_name):
        """Create the Python harvesting configuration for a given job.

        The basic configuration is created by
        Configuration.PyReleaseValidation.ConfigBuilder. (This mimics
        what cmsDriver.py does.) After that we add some specials
        ourselves.

        NOTE: On one hand it may not be nice to circumvent
        cmsDriver.py, on the other hand cmsDriver.py does not really
        do anything itself. All the real work is done by the
        ConfigBuilder so there is not much risk that we miss out on
        essential developments of cmsDriver in the future.

        NOTE: The reason to have a single harvesting configuration per
        sample is to be able to specify the GlobalTag corresponding to
        each sample. Since it has been decided that (apart from the
        promt reco) datasets cannot contain runs with different
        GlobalTags, we don't need a harvesting config per run.

        """

        # BUG BUG BUG
        # First of all let's try and get this to work...
        import Configuration.PyReleaseValidation
        from Configuration.PyReleaseValidation.ConfigBuilder import ConfigBuilder, defaultOptions
        # from Configuration.PyReleaseValidation.cmsDriverOptions import options, python_config_filename

        ###

        # Setup some options needed by the ConfigBuilder.
        config_options = defaultOptions

        # These are fixed for all kinds of harvesting jobs. Some of
        # them are not needed for the harvesting config, but to keep
        # the ConfigBuilder happy.
        config_options.name = "harvesting"
        config_options.scenario = "pp"
        config_options.number = 1
        config_options.arguments = self.ident_string()
        config_options.evt_type = config_options.name
        config_options.customisation_file = None
        config_options.filein = "dummy_value"

        ###

        # These options depend on the type of harvesting we're doing
        # and are stored in self.harvesting_info.

        config_options.step = "HARVESTING:%s" % \
                              self.harvesting_info[self.harvesting_type] \
                              ["step_string"]
        config_options.beamspot = self.harvesting_info[self.harvesting_type] \
                                  ["beamspot"]
        config_options.eventcontent = self.harvesting_info \
                                      [self.harvesting_type] \
                                      ["eventcontent"]
        config_options.harvesting = self.harvesting_info \
                                    [self.harvesting_type] \
                                    ["harvesting"]

        ###

        # This one is required (see also above) for each dataset.

        datatype = self.datasets_information[dataset_name]["datatype"]
        config_options.isMC = (datatype.lower() == "mc")
        globaltag = self.datasets_information[dataset_name]["globaltag"]

        config_options.conditions = "FrontierConditions_GlobalTag,%s" % \
                                    globaltag

        ###

        config_builder = ConfigBuilder(config_options)
        config_builder.prepare(True)

        # End of create_harvesting_config.
        return config_builder.pythonCfgCode

    ##########

    def write_crab_config(self):
        """Write a CRAB job configuration Python file.

        """

        file_name_base = "crab.cfg"

        # Create CRAB configuration.
        crab_contents = self.create_crab_config()

        # Write configuration to file.
        crab_file_name = file_name_base
        try:
            crab_file = file(crab_file_name, "w")
            crab_file.write(crab_contents)
            crab_file.close()
        except IOError:
            self.logger.fatal("Could not write " \
                              "CRAB configuration to file `%s'" % \
                              crab_file_name)
            raise Error("ERROR: Could not write to file `%s'!" % \
                        crab_file_name)

        # End of write_crab_config.

    ##########

    def write_multicrab_config(self):
        """Write a multi-CRAB job configuration Python file.

        """

        file_name_base = "multicrab.cfg"

        # Create multi-CRAB configuration.
        multicrab_contents = self.create_multicrab_config()

        # Write configuration to file.
        multicrab_file_name = file_name_base
        try:
            multicrab_file = file(multicrab_file_name, "w")
            multicrab_file.write(multicrab_contents)
            multicrab_file.close()
        except IOError:
            self.logger.fatal("Could not write " \
                              "multi-CRAB configuration to file `%s'" % \
                              multicrab_file_name)
            raise Error("ERROR: Could not write to file `%s'!" % \
                        multicrab_file_name)

        # End of write_multicrab_config.

    ##########

    def write_harvesting_config(self, dataset_name):
        "Write a harvesting job configuration Python file."

        # Create Python configuration.
        config_contents = self.create_harvesting_config(dataset_name)

        # Write configuration to file.
        config_file_name = self. \
                           create_harvesting_config_file_name(dataset_name)
        try:
            config_file = file(config_file_name, "w")
            config_file.write(config_contents)
            config_file.close()
        except IOError:
            self.logger.fatal("Could not write " \
                              "harvesting configuration to file `%s'" % \
                              config_file_name)
            raise Error("ERROR: Could not write to file `%s'!" % \
                        config_file_name)

        # End of write_harvesting_config.

    ##########

    def build_datasets_information(self):
        """Obtain all information on the datasets that we need to run.

        Use DBS to figure out all required information on our
        datasets, like the run numbers and the GlobalTag. All
        information is stored in the datasets_information member
        variable.

        """

        # Get a list of runs in the dataset.
        # NOTE: The harvesting has to be done run-by-run, so we
        # split up datasets based on the run numbers. Strictly
        # speaking this is not (yet?) necessary for Monte Carlo
        # since all those samples use run number 1. Still, this
        # general approach should work for all samples.

        # Now loop over all datasets in the list and process them.
        # NOTE: This processing has been split into several loops
        # to be easier to follow, sacrificing a bit of efficiency.
        self.datasets_information = {}
        self.logger.info("Collecting information for all datasets")
        for dataset_name in self.dataset_names:
            self.logger.info("  `%s'" % dataset_name)

            runs = self.dbs_resolve_runs(dataset_name)
            self.logger.info("    found %d run(s)" % len(runs))
            if len(runs) > 0:
                self.logger.debug("      run number(s): %s" % \
                                  ", ".join([str(i) for i in runs]))
            else:
                # DEBUG DEBUG DEBUG
                # This should never happen after the DBS checks.
                self.logger.warning("  --> skipping dataset "
                                    "without any runs")
                assert False
                # DEBUG DEBUG DEBUG end

            globaltag = self.dbs_resolve_globaltag(dataset_name)
            self.logger.info("    found GlobalTag `%s'" % globaltag)

            # Figure out if this is data or MC.
            datatype = self.dbs_resolve_datatype(dataset_name)
            self.logger.info("    sample is data or MC? --> %s" % \
                             datatype)

            # DEBUG DEBUG DEBUG
            # This is probably only useful to make sure we don't muck
            # things up, right?
            # Figure out across how many sites this sample has been spread.
            num_sites = self.dbs_resolve_dataset_number_of_sites(dataset_name)
            self.logger.info("    sample is spread across %d sites" % \
                             num_sites)
            if num_sites < 1:
                # NOTE: This _should not_ happen with any valid dataset.
                self.logger.warning("  --> skipping dataset which is not " \
                                    "hosted anywhere")
            # DEBUG DEBUG DEBUG end

            # Now put everything in a place where we can find it again
            # if we need it.
            self.datasets_information[dataset_name] = {}
            self.datasets_information[dataset_name]["runs"] = runs
            self.datasets_information[dataset_name]["globaltag"] = globaltag
            self.datasets_information[dataset_name]["datatype"] = datatype
            self.datasets_information[dataset_name]["num_sites"] = num_sites

            # In principle each job can have a different CASTOR output
            # path.
            castor_path = self.create_castor_path_name(dataset_name)
            self.logger.info("    output will go into `%s'" % \
                             castor_path)

            self.datasets_information[dataset_name]["castor_path"] = \
                                                                   castor_path

        # End of build_datasets_info.

    ##########

    def run(self):
        "Main entry point of the CMS harvester."

        try:

            # Parse all command line options and arguments
            self.parse_cmd_line_options()
            # and check that they make sense.
            self.check_input_status()

            # Check if CMSSW is setup.
            self.check_cmssw()

            # Check if DBS is setup,
            self.check_dbs()
            # and if all is fine setup the Python side.
            self.setup_dbs()

            # Obtain list of dataset names to consider
            self.build_dataset_list()
            # and the list of dataset names to ignore.
            self.build_dataset_list_ignore()

            # Process the list of datasets to ignore and fold that
            # into the list of datasets to consider.
            self.process_dataset_ignore_list()

            # Obtain all required information on the datasets, like
            # run numbers and GlobalTags.
            self.build_datasets_information()

            # Check dataset name(s)
            self.check_dataset_list()
            # and see if there is anything left to do.
            if len(self.dataset_names) < 1:
                self.logger.info("No datasets (left?) to process")
            else:

                # Loop over all datasets and create harvesting config
                # files for all of them. One harvesting config per
                # dataset is enough. The same file will be re-used by
                # CRAB for each run.
                for dataset_name in self.dataset_names:
                    self.write_harvesting_config(dataset_name)

                # Now create one crab and one multicrab configuration
                # for all jobs together.
                self.write_crab_config()
                self.write_multicrab_config()

                # Check if the CASTOR output area exists. If necessary
                # create it.
                self.create_and_check_castor_dirs()

                # TODO TODO TODO
                # Need some closure here.
                # TODO TODO TODO end

            self.cleanup()

        except Usage, err:
            #self.logger.fatal(err.msg)
            #self.option_parser.print_help()
            pass

        except Error, err:
            #self.logger.fatal(err.msg)
            return 1

        except Exception, err:
            # Hmmm, ignore keyboard interrupts from the user. These
            # are not a `serious problem'. We also skip SystemExit,
            # which is the exception thrown when one calls
            # sys.exit(). This, for example, is done by the option
            # parser after calling print_help().
            if not isinstance(err, KeyboardInterrupt) and \
                   not isinstance(err, SystemExit):
                self.logger.fatal("!" * 50)
                self.logger.fatal("  This looks like a serious problem.")
                self.logger.fatal("  If you are sure you followed all " \
                                  "instructions")
                self.logger.fatal("  please copy the below stack trace together")
                self.logger.fatal("  with a description of what you were doing to")
                self.logger.fatal("  jeroen.hegeman@cern.ch.")
                self.logger.fatal("!" * 50)
                self.logger.fatal(str(err))
                import traceback
                traceback.print_exc()
                print "!" * 50
                return 2

        # End of run.

    # End of CMSHarvester.

###########################################################################
## Main entry point.
###########################################################################

if __name__ == "__main__":
    "Main entry point for harvesting."

    CMSHarvester().run()

    # Done.

###########################################################################
