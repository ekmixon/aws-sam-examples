from __future__ import print_function

import base64
import re

print('Loading function')


def lambda_handler(event, context):
    output = []
    succeeded_record_cnt = 0
    failed_record_cnt = 0

    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data'])

        p = re.compile(r"^([\d.]+) (\S+) (\S+) \[([\w:/]+\s[\+\-]\d{4})\] \"(.+?)\" (\d{3}) (\d+)")
        if m := p.match(payload):
            succeeded_record_cnt += 1
            output_payload = (
                m[1]
                + ','
                + m[2]
                + ','
                + m[3]
                + ','
                + m[4]
                + ','
                + m[5]
                + ','
                + m[6]
                + ','
                + m[7]
                + '\n'
            )

            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(output_payload)
            }
        else:
            print('Parsing failed')
            failed_record_cnt += 1
            output_record = {
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']
            }

        output.append(output_record)

    print(
        f'Processing completed.  Successful records {succeeded_record_cnt}, Failed records {failed_record_cnt}.'
    )

    return {'records': output}
