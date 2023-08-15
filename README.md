# mailstate

script to get all "complete envelope" emails statistics from sendmail
log file and to put all that data into sqlite db file

"complete envelope" is the envelope which has opening `from=<...` and
closing `to=<...` log file records

script is parsing log file provided with option `-l` and groups
records by id

log file expected is freebsd syslogd

results are written to sqlite db

