# Prerequisite
- [direnv](https://direnv.net/)
- AWS cli
  - Assume AWS CLI is installed and a default configuration is used.
  - https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html

# How to
1. Setup environment
For an AWS environment, make a folder and file=`.envrc`
```bash
$mkdir Env1
$cd Env1 && cat <<EOF> .envrc
export AWS_PROFILE=env1
EOF
$direnv allow
```
2. Run query
```bash
$cloudwatch query -s 2025-01-31T16:00:00-05:00 -e 2025-02-01T00:00:00-05:00 -q 'fields @timestamp, @message| filter @message like /(?i)error/' -N <log groups separated by `,`>
```

# TODOs
- make task oriented
