
# AWS setup
Assume AWS CLI is installed and a default configuration is used.
- https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html

```bash
AWS_PROFILE=<profile> cloudwatch query -s 2025-01-31T16:00:00-05:00 -e 2025-02-01T00:00:00-05:00 -q 'fields @timestamp, @message| filter @message like /(?i)error/' -N <log groups separated by `,`>
```
