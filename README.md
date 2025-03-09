# Motivation
AWS CloudWatch is sometimes hard to use since it does not provide an ability / a functionality to allow
a developer to compare the patterns from a time period to another period. For example, a new pattern
is created due to failure in a new code, which may not be emitted as ERROR or WARNING log.
Without a baseline behavior, ie. a set of patterns which are known / acceptable, it may be hard to
discover new patterns.

This repo is to
- practice how to use Golang
- practice how to use Nix
- offer a tool to audit / vet CloudWatch patterns.


# Prerequisite
- [direnv](https://direnv.net/)
- AWS cli
  - Assume AWS CLI is installed and a default configuration is used.
  - https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html

# How to

## Setup environment
For an AWS environment, make a folder and file=`.envrc`
```bash
$mkdir Env1
$cd Env1 && cat <<EOF> .envrc
export AWS_PROFILE=env1
EOF
$direnv allow
```

## Cloudwatch query
```bash
# inside folder=Env1
$cloudwatch query -s 2025-01-31T16:00:00-05:00 -e 2025-02-01T00:00:00-05:00 -q 'fields @timestamp, @message| filter @message like /(?i)error/' -N <log groups separated by `,`>
```

## Audit & Vet Cloudwatch pattern
Given a developer has executed a cloudwatch query with keyword=[pattern](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax-Pattern.html)
1. list patterns for auditting
```bash
python py/compare.py ./data.db audit --list <log group>
>>> # in stdout
>>> Audit id=0, pattern=b'"{\\"asctime\\": <*> <*>,<*>, \\"levelname\\": \\"ERROR\\", \\"name\\": \\"SOMENAME\\", \\"message\\": \\"Failed processing in batch\\", \\"exc_info\\": {\\"type\\": \\"Failure\\", \\"message\\": \\"a segment has no movement.\\", \\"traceback\\": \\"Traceback (most recent calllast):\\\\n  File \\\\\\"/SOME/PYTHON/PATH/batch_runner.py\\\\\\", line <*>, in process_batch\\\\n    "'
>>> Audit id=1, pattern=b'"{\\"asctime\\": <*> <*>,<*>, \\"levelname\\": \\"INFO\\", \\"name\\": \\"SOME_OTHER_NAME\\", \\"message\\": \\"processing in batch\\""'
```
2. mark patterns as vetted
```bash
python py/compare.py ./data.db audit --mark <whitespace-separated audit ids; such as `0 1`>
```

# TODOs
- make task oriented
