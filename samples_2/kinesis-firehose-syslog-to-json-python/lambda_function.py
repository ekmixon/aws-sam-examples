from __future__ import print_function

import base64
import json
import re

print('Loading function')


def lambda_handler(event, context):
    output = []
    succeeded_record_cnt = 0
    failed_record_cnt = 0

    regex_string = (r"^((?:\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?"
                    r"|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b\s+(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])\s+"
                    r"(?:(?:2[0123]|[01]?[0-9]):(?:[0-5][0-9]):(?:(?:[0-5]?[0-9]|60)(?:[:\.,][0-9]+)?)))) (?:<(?:[0-9]+).(?:[0-9]+)> )"
                    r"?((?:[a-zA-Z0-9._-]+)) ([\w\._/%-]+)(?:\[((?:[1-9][0-9]*))\])?: (.*)")
    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data'])

        p = re.compile(regex_string)
        if m := p.match(payload):
            succeeded_record_cnt += 1
            data_field = {
                'timestamp': m[1],
                'hostname': m[2],
                'program': m[3],
                'processid': m[4],
                'message': m[5],
            }

            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(json.dumps(data_field))
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
