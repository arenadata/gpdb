import sys
from gppylib.commands.gp import GpError, get_coordinatordatadir, get_gphome
from gppylib.gpparseopts import OptParser, OptChecker

_description = ("""
Rebalances the existing segments configuration for getting
optimal performance of whole cluster.
""")

_help = ["""
[-m spread|grouped]
Mode is the desirable mirroring strategy after rebalance. Makes sense only when mirroring
is enabled for the cluster. Default value is "grouped".
[-f <hosts_file>]
The hosts configuration file (YAML format) defines the target hosts on which
the rebalance procedure will distribute segments from existing configuration.
Existing info can be generated from gp_segment_configuration through -g option.
hosts:
- hostname: <host_name>      # Machine hostname
  address: <address>         # Network address for connections
  primary_datadirs:          # Directories for primary segments
    - /datadir1
    - /datadir1
  mirror_datadirs:           # Directories for mirror segments
    - /mdatadir1
    - /mdatadir2
You can include empty hosts in the configuration or shrink the existing number
of hosts, the utility will try to balance the segments strictly across hosts
from the file.
"""]

_usage = """
 gprebalance -g
 gprebalance [-m <mode>] [-c] [-f <hosts_file>] [-s] [-v]
 gprebalance -c
 gprebalance -? | -h | --help | --verbose | -v
"""


class OptionError(Exception):
    pass


def parseargs():
    parser = OptParser(option_class=OptChecker,
                       description=' '.join(_description.split()),
                       version='%prog version $Revision$')
    parser.setHelp(_help)
    parser.set_usage('%prog ' + _usage)
    parser.remove_option('-h')

    parser.add_option('-m', '--mirror-mode', dest='mirroring',
                      help='desirable mirroring strategy')
    parser.add_option('-c', '--clean', action='store_true',
                      help='remove the rebalance schema.')
    parser.add_option('-f', '--target-file', metavar='<hosts_file>', dest='filename',
                      help='yaml containing target hosts configuration')
    parser.add_option('--allow-mirrorless', dest='allow_mirrorless', action='store_true',
                      help='Allow to rebalance a cluster without mirrors', default=False)
    parser.add_option('-g', '--gen-hosts', action='store_true', dest='genconf',
                      help='dump cluster hosts configuration in yaml format')
    parser.add_option('-s', '--silent', action='store_true', default=False,
                      help='Do not prompt for confirmation to proceed on warnings')
    parser.add_option('-v', '--verbose', action='store_true',
                      help='debug output.')
    parser.add_option('-h', '-?', '--help', action='help',
                      help='show this help message and exit.')
    parser.add_option('--usage', action="briefhelp")

    parser.set_defaults(verbose=False)

    # Parse the command line arguments
    (options, args) = parser.parse_args()
    return options, args, parser


def validate_options(options, args):
    if len(args) > 0:
        raise OptionError(f'Unknown argument {args[0]}')

    if options.mirroring and options.mirroring not in ('grouped', 'spread'):
        raise OptionError(
            f'Mirroring strategy {options.mirroring} is not supported')

    if options.genconf:
        for arg in sys.argv[1:]:
            if arg not in ('-g', '--gen-hosts'):
                raise OptionError('-g or --gen-hosts flag must be used alone')

    return options, args
