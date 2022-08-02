#
# Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#


def client(client_type, *args):
    if client_type == 'lambda':
        from .Lambda import Client
    elif client_type == 'iot-data':
        from .IoTDataPlane import Client
    else:
        raise Exception(f'Client type {repr(client_type)} is not recognized.')

    return Client(*args)
