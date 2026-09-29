"""linecite-sync — transport companion for linecite's cross-host
snapshot exchange. Channels are dumb storage; the contract does the
guaranteeing (see linecite's contract/ and snapshot-contract spec)."""
from linecite_sync.channels import Channel, ChannelError, DirChannel, GitChannel
from linecite_sync.gate import GateRefused, GateReport, check_package
from linecite_sync.sync import PullOutcome, pull, push, stream_name

__version__ = "0.1.0.dev0"
__all__ = ["Channel", "ChannelError", "DirChannel", "GitChannel",
           "GateRefused", "GateReport", "check_package",
           "PullOutcome", "pull", "push", "stream_name"]
