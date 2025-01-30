import sys
import datetime
from gppylib.gpparseopts import OptParser, OptChecker
from gprebalance_modules.utils import MAX_PARALLEL_WORKERS

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

 gpexpand [-d duration[hh][:mm[:ss]] | [-e 'YYYY-MM-DD hh:mm:ss']]
            [-n parallel_processes]

 gprebalance -r

 gprebalance -c

 gprebalance -p

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
    parser.add_option('-f', '--target-hosts', metavar='<hosts_file>', dest='filename',
                      help='yaml containing target hosts configuration')
    parser.add_option('-p', '--show-plan', dest='show_plan', action='store_true', default=False,
                      help='show rebalance plan')
    parser.add_option('-c', '--clean', action='store_true',
                      help='remove the rebalance schema.')
    parser.add_option('-r', '--rollback', action='store_true',
                      help='remove the rebalance schema.')
    parser.add_option('-d', '--duration', type='duration', metavar='[h][:m[:s]]',
                      help='duration from beginning to end.')
    parser.add_option('-e', '--end', type='datetime', metavar='datetime',
                      help="ending date and time in the format 'YYYY-MM-DD hh:mm:ss'.")
    parser.add_option('-n', '--parallel', type="int", default=1, metavar="<parallel_processes>",
                      help='number of workerks performing segment movements at a time. Valid values are 1-%d.' % MAX_PARALLEL_WORKERS)
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
    parser.add_option('-S', '--simple-progress', action='store_true',
                      help='show simple progress.')
    parser.add_option('', '--hba-hostnames', action='store_true', default=False,
                      help='use hostnames instead of CIDR in pg_hba.conf')
    parser.add_option('--allow-intermediate-mixture', dest='allow_mixture', action='store_true',
                      help='Allow primary and mirror from one pair to be at the same host during balancing', default=False)

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

    if options.parallel > MAX_PARALLEL_WORKERS or options.parallel < 1:
        raise OptionError(
            'Invalid argument.  parallel value must be >= 1 and <= %d' % MAX_PARALLEL_WORKERS)

    if options.end and not isinstance(options.end, datetime.datetime):
        options.end = datetime.datetime.combine(options.end, datetime.time(0))

    if options.end and options.end < datetime.datetime.now():
        raise OptionError('End time occurs in the past')

    if options.end and options.duration:
        if options.end > datetime.datetime.now() + options.duration:
            options.end = datetime.datetime.now() + options.duration
    elif options.duration:
        options.end = datetime.datetime.now() + options.duration

    # -c and -r options are mutually exclusive
    if options.rollback and options.clean:
        rollbackOpt = "--rollback" if "--rollback" in sys.argv else "-r"
        cleanOpt = "--clean" if "--clean" in sys.argv else "-c"
        raise OptionError("%s and %s options cannot be specified together." %
                          (rollbackOpt, cleanOpt))
    return options, args
